# RunPod Extraction Prompt — Standalone Cowork Chat Handoff

**Date:** 2026-04-21
**Purpose:** This file is designed to be copy-pasted into a FRESH Cowork chat that will drive H200 GPU extraction jobs on RunPod. It is NOT for Cursor. The sibling file `CURSOR_PROMPT_3_GAP_CLOSURE_20260421.md` covers the SQL-only work that Cursor handles in parallel; this file covers the three LLM extraction jobs that require H200 time and should NOT touch MotherDuck beyond upload-at-end.
**Paste this into a new Cowork chat titled something like "Thyroid 2026 — RunPod extraction round (April 2026)".**

---

## Context for the new Cowork chat

You are being asked to run LLM extraction jobs on a RunPod H200 instance for the Thyroid 2026 manuscript cohort. The cohort is 10,871 thyroid cancer patients at Emory (IRB-approved, PHI-redacted research_id only — never print clinical note text in stdout). The canonical database is MotherDuck `thyroid_canonical_publication_v1_0`. You are authenticated via `scripts/_md_connect.py::connect_locked()` using `/Users/ros/THyroid 2026/motherduck.local.toml`.

Relevant memory Logan already carries:
- **RunPod bootstrap**: fresh pytorch pod needs `zstd`, `rsync`, `pip`, + 5 extra repo dirs + `tenacity`. H200 caps at ~2–5 notes/min on qwen3:32b — budget accordingly.
- **Vast.ai gotcha**: if `nvidia-smi` Pwr shows `ERR!`, destroy the instance immediately; don't debug ollama.
- **Ollama KV-cache OOM**: cap `OLLAMA_CONTEXT_LENGTH=8192`, `KV_CACHE_TYPE=q8_0`, `FLASH_ATTENTION=1` before raising `NUM_PARALLEL` on `qwen3:32b`.
- **PHI rule**: `evidence_text` may be stored in canonical tables but must NEVER be printed to stdout — use `research_id` + `note_id` only in logs.
- **Commit workflow**: every upload/change is staged, lint-checked with pyflakes, committed individually, pushed.

## Three extraction jobs to run (in priority order)

These are the jobs that Cursor canNOT do with SQL alone. Each produces a new or rewritten `note_entities_llm_<domain>` table on MotherDuck.

---

### Job 1 — Re-extract 3 stale domains at qwen2.5-32b (HIGH IMPACT)

**Problem (verified 2026-04-21 against live MotherDuck):** These 3 tables are stuck at **qwen3:32b** (March 30 – April 3, 2026) with only **11,037 rows / 5,641 RIDs** — i.e., only 52% of the 10,871-patient cohort has clinical-note extraction data for these domains:

| Table | Rows | RIDs | Current model | Timestamp window |
|---|---|---|---|---|
| `note_entities_llm_cervical_ln_detail` | 11,037 | 5,641 | qwen3:32b | 2026-04-03 |
| `note_entities_llm_pathology` | 11,037 | 5,641 | qwen3:32b | 2026-03-30 – 2026-03-31 |
| `note_entities_llm_tirads_granular` | 11,037 | 5,641 | qwen3:32b | 2026-03-31 – 2026-04-01 |

The rest of the domains (synoptic_pathology, airway_invasion, frozen_section, parathyroid, vascular_invasion) were re-run at `qwen2.5-32b` via the 9domain_v4 pipeline — Scripts 280–285 on `main`. Mixing qwen3 and qwen2.5 extractions introduces model drift across domains, which is a methodology problem for manuscripts that cross-reference (e.g., LN findings on pathology vs. LN mentions on imaging).

**Also note:** There are local parquet artifacts under `runs/domain_reruns_qwen3_32b_targeted/*/output/` from 2026-04-14 with higher coverage (cervical_ln_detail 10,417 RIDs / 2,937 patients; pathology 19,810 RIDs / 5,884 patients; tirads_granular 10,871 rows / 5,305 patients) — but these mix qwen3:14b and qwen3:32b. DO NOT upload those parquets as-is. Re-extract instead.

**Action:**

1. Spin up an H200 pod (reference `feedback_runpod_bootstrap` memory for exact deps).
2. Use the same 9domain_v4 extraction pipeline that produced Scripts 280–285 (qwen2.5-32b, 9-domain prompt template — it's in the repo under `pipelines/9domain_v4/`).
3. Run domains one at a time, in this order: (a) pathology (highest manuscript-unblocking impact), (b) cervical_ln_detail, (c) tirads_granular.
4. Full 10,871-patient scan — don't stop at 5,641.
5. Expected time: pathology domain on H200 at qwen2.5-32b is ~3 notes/min, so ~60 hours for full cohort. Budget a multi-day run. Tirads_granular is lighter (~30 hours). Cervical LN similar.
6. Upload each completed parquet to MotherDuck as a replacement for the existing `note_entities_llm_<domain>` table. **Archive the old table first** to `archive_pub_v1_0.note_entities_llm_<domain>_preRUNPODrerun_<UTCZ>` before overwriting. Verify byte-parity against parquet.
7. After each successful load, run the Tier 1 rollup (4 `nlp_<domain>_*` columns on CPM) and verify CPM invariants hold (rows=10,871, distinct_rid=10,871).

**Handoff script**: `scripts/runpod_340_pathology_rerun.py`, `scripts/runpod_341_cervical_ln_rerun.py`, `scripts/runpod_342_tirads_granular_rerun.py` (use numbers after the Cursor prompt's 327–336 range to avoid collision).

---

### Job 2 — TIRADS v2 re-extraction queue (4,363 nodules)

**Problem:** `main.tirads_reextraction_queue_v1` has 4,363 nodules for 1,316 patients where `tirads_score_2017` was assigned but `calcifications` IS NULL (parser missed the calcification field on first pass). The extraction script `pipelines/tirads_v2/extract_tirads_from_us_reports.py` already exists in the repo and is queue-compatible.

**Action:**

1. Pull `main.tirads_reextraction_queue_v1` down to the pod as a parquet (4,363 rows is tiny).
2. Join each queue row to `main.clinical_notes_long` to fetch the source note text.
3. Run `extract_tirads_from_us_reports.py` with `--model qwen2.5-32b-instruct-awq` (same model as the v2 extraction in commit `d6ca339`).
4. Upload results to a new table `main.tirads_v2_nodules_requeued_v1` on MotherDuck.
5. Merge into `tirads_v2_nodules_raw` → update `tirads_v2_nodule_patient_rollup_v1` → backfill CPM per Prompt 3 Scripts 328–329 (Cursor will have already run those, so verify the rollup refresh picks up the new rows). If Cursor's 328/329 ran before this extraction completes, they'll need to re-run to incorporate the new nodules — schedule accordingly or build a one-off CPM update script.
6. Expected time: ~2–4 hours on H200 (short free-text per nodule).

**Handoff script**: `scripts/runpod_343_tirads_requeue.py`.

---

### Job 3 — Dedicated esophageal invasion extraction (≥10K operative notes)

**Problem:** `cpm.op_esophageal_inv_any` = 0 nonnull. Cursor's Prompt 3 Script 334 will harvest 381 RIDs worth of incidental "esophag" mentions from the airway_invasion LLM JSON, but full operative-note coverage needs a dedicated extraction pass. No `note_entities_llm_esophageal_invasion` table exists on MotherDuck yet — this is a net-new extraction domain.

**Action:**

1. Identify the note set: all operative notes for all 10,871 patients (roughly 13–15K notes based on operative-detail and procedure entity counts). Pull from `main.clinical_notes_long` where `note_type ILIKE '%operative%' OR note_type ILIKE '%procedure%' OR note_type='OP_NOTE'`.
2. Write a new 1-domain extraction prompt (template under `pipelines/9domain_v4/prompts/`) specifically for esophageal invasion. Entities: `esophageal_invasion_present`, `esophageal_invasion_extent` ∈ {abutting, invading_partial, full_thickness}, `esophageal_invasion_length_cm`, `esophageal_repair_performed`, `esophageal_muscular_invasion`. Standard `{entity_type, entity_value, entity_date, confidence, present_or_negated, evidence_text, source_line}` shape.
3. Run on H200 at qwen2.5-32b. Expected time: ~5–8 hours for 15K operative notes.
4. Upload to `main.note_entities_llm_esophageal_invasion` on MotherDuck.
5. Parse (Tier 2) into `main.esophageal_invasion_event_v1` following the pattern used for airway_invasion in Prompt 2 Script 306.
6. Backfill `cpm.op_esophageal_inv_any` + Constraint-7 companions, superseding whatever Prompt 3 Script 334 produced from the airway_invasion JSON incidental mentions.

**Handoff script**: `scripts/runpod_344_esophageal_invasion_extraction.py`.

---

## Execution order and dependencies

- **Job 1** (stale domain re-extraction) is the LONGEST job (~5–7 days cumulative). Start first.
- **Job 2** (TIRADS re-extraction queue) is SHORT (~half a day) and can run on a second pod in parallel, or in a Job 1 idle window.
- **Job 3** (esophageal) is MEDIUM (~day) and can run after Job 1 completes or on a parallel pod.
- Cursor's Prompt 3 (327–336) runs INDEPENDENTLY of this extraction work. The only coordination point is TIRADS v2: Cursor Scripts 328–329 will fix the cast/report-level gaps against the CURRENT `tirads_v2_*` tables; Job 2 will then ADD 4,363 more nodules, which requires a follow-up CPM refresh (a small post-extraction script, not a re-run of 328/329).

## Upload invariants (every job)

1. Archive existing table before overwriting: `archive_pub_v1_0.<name>_preRUNPODjob<N>_<UTCZ>`.
2. Parquet ↔ MotherDuck byte-parity check after upload (same pattern as Scripts 280–285).
3. CPM row count unchanged (10,871).
4. Distinct research_id count in new table ≤ 10,871.
5. All `extracted_at` timestamps after upload should be from the current run — no silent back-dates.
6. Log the upload to `manuscript_workspace.extraction_upload_log_v1` (create if not exists).

## Commit discipline

Per job:

```bash
cd "/Users/ros/THyroid 2026"
git add scripts/runpod_<N>_*.py pipelines/<relevant>/**
python -m pyflakes scripts/runpod_<N>_*.py
git commit -m "RunPod Job <N>: <summary> (model=qwen2.5-32b, rids=<N>)"
git push origin main
```

## Hand-off checklist at end

1. Three new/rewritten `note_entities_llm_*` tables on MotherDuck, all at qwen2.5-32b.
2. `tirads_v2_nodules_raw` widened by 4,363 rows → CPM TIRADS coverage up from 3,021 RIDs to something closer to 4,300.
3. New `note_entities_llm_esophageal_invasion` table + `esophageal_invasion_event_v1` Tier 2 table + CPM `op_esophageal_inv_any` backfilled.
4. Archive moves for 3 replaced tables (old qwen3:32b versions) logged.
5. CPM invariants still green.
6. Report written back to Logan: `docs/RUNPOD_EXTRACTION_ROUND_REPORT_20260421.md` with row counts, time spent, any failures.

## What NOT to do in this chat

- Do not re-run Scripts 280–285 (those are done; those 5 domains are at qwen2.5-32b already).
- Do not touch any table that Prompt 2 (Tier 2 + verify) is producing — those are pure SQL transforms of existing data, not extractions.
- Do not upload the `runs/domain_reruns_qwen3_32b_targeted/*/output/` parquets as-is (model mix contamination).
- Do not print clinical note text to stdout — research_id and note_id only.
- Do not skip the archive-before-overwrite step.

---

End of handoff. Logan will kick this off after Cursor's Prompt 3 has made progress, so the TIRADS Gap A/B fixes (Cursor Scripts 328–329) are already applied when the TIRADS requeue (Job 2) completes.

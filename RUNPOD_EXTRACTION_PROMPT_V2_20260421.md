# RunPod Extraction Prompt V2 — Standalone Cowork Chat Handoff (concrete specs)

**Date:** 2026-04-21 (rev 2 — exact models, columns, inputs, paths)
**Purpose:** Paste into a FRESH Cowork chat titled something like "Thyroid 2026 — RunPod extraction round (April 2026)". This chat drives three H200 extraction jobs. Cursor's Prompt 5 handles the SQL-only remediation in parallel; this chat handles the LLM extractions that must run on GPU.

---

## Authoritative environment spec

**All three jobs use the same model and pipeline harness.** Do not vary these without explicit approval from Logan.

| Field | Value |
|---|---|
| Model | `qwen2.5-32b` (exact string in `VLLM_MODEL` env var, served via vLLM) |
| Serving runtime | vLLM on H200 — NOT Ollama. (The `qwen3:32b` extractions on MotherDuck are stale; this round replaces them.) |
| Pipeline driver | `scripts/vastai/run_extraction_concurrent.py` |
| Orchestration example | `runs/9domain_v4/run_all_domains.sh` (template for concurrency settings, env vars, per-domain log paths) |
| Env vars (from `runs/9domain_v4/ssh_config.sh`) | `VLLM_URL`, `VLLM_MODEL=qwen2.5-32b`, `EXTRACTION_CONCURRENCY=256`, `LLM_MAX_TOKENS=12000`, `LLM_INPUT_CHAR_LIMIT=12000`, `LLM_TIMEOUT_SECONDS=180` |
| Input parquet location | `processed/remaining/9domain_v4/input_<DOMAIN>.parquet` on the pod |
| Output parquet location | `runs/9domain_v4/<DOMAIN>/output/` on the pod |
| MotherDuck auth | `scripts/_md_connect.py::connect_locked()` using `/Users/ros/THyroid 2026/motherduck.local.toml` |
| PHI rule | `research_id` + `note_row_id` + `note_index` in stdout only — never clinical text |
| Concurrency guidance | H200 at qwen2.5-32b sustains ~3–5 notes/min per concurrent worker. Use 256 concurrent workers. Budget pathology at ~60h, tirads_granular ~30h, cervical_ln ~30h, TIRADS requeue ~3h, esophageal ~5–10h. |

### Pod bootstrap (do this before any job)
Per memory `feedback_runpod_bootstrap`:
```bash
apt-get update && apt-get install -y zstd rsync
pip install tenacity duckdb pandas toml pyarrow
# Plus clone the 5 repo dirs: scripts/, prompts/, processed/remaining/9domain_v4/, runs/, exports/_archive_pre_v1_0/
```

### Vast.ai / GPU sanity check (per memory `feedback_vastai_gpu_err`)
If `nvidia-smi` shows `Pwr` column as `ERR!`, destroy the instance immediately and recreate — do not debug.

### Ollama KV-cache guardrails (per memory `feedback_ollama_kv_cache_oom`)
Not applicable here — we're on vLLM, not Ollama. If any script falls back to Ollama for any reason, set `OLLAMA_CONTEXT_LENGTH=8192`, `KV_CACHE_TYPE=q8_0`, `FLASH_ATTENTION=1` before raising `NUM_PARALLEL`.

---

## Job 1 — Re-extract 3 stale domains at qwen2.5-32b (HIGHEST IMPACT)

**Problem (verified 2026-04-21 on MotherDuck):**

| Stale MD table | Rows | Distinct RIDs | Current model | Extracted window |
|---|---|---|---|---|
| `note_entities_llm_pathology` | 11,037 | 5,641 | qwen3:32b | 2026-03-30 – 2026-03-31 |
| `note_entities_llm_cervical_ln_detail` | 11,037 | 5,641 | qwen3:32b | 2026-04-03 |
| `note_entities_llm_tirads_granular` | 11,037 | 5,641 | qwen3:32b | 2026-03-31 – 2026-04-01 |

Only 52% of the 10,871-patient cohort has extraction. The other 5 Phase-B' domains (airway_invasion, frozen_section, parathyroid_detail, vascular_invasion, synoptic_pathology) were already re-run at qwen2.5-32b — Scripts 282–285 on `main`. Mixing qwen3 and qwen2.5 across the 8 LLM-extraction domains creates methodology drift.

**Order of execution (most manuscript-unblocking first):**

### Job 1a — pathology
- **Input parquet to build on pod:** `processed/remaining/9domain_v4/input_pathology.parquet`
- **How to build it:** source notes from `main.clinical_notes_long WHERE note_type IN ('PATH_REPORT', 'PATHOLOGY', 'PATH_FINAL', 'PATH_ADDENDUM')` OR whatever path-type filter Scripts 282–285 used for their sibling domains. Reference `runs/9domain_v4/run_all_domains.sh` for the exact filter pattern, then mirror to `input_pathology.parquet`.
- **Prompt template:** `prompts/pathology_v4.txt` — if missing, clone `prompts/operative_note_extraction_v1.txt` as a structure template and adapt field names (see entity schema below).
- **Entity schema to extract per note:**
  - `histology` (papillary, follicular, medullary, anaplastic, Hürthle, poorly-differentiated, other)
  - `tumor_size_cm` (largest focus)
  - `focality` (unifocal / multifocal / bilateral)
  - `ete_status` (none / minor / gross)
  - `vascular_invasion` (absent / focal / extensive)
  - `lymphovascular_invasion` (absent / present)
  - `margin_status` (negative / positive / close)
  - `t_stage`, `n_stage`, `m_stage`
  - `ln_positive_count`, `ln_examined_count`
  - `extrathyroidal_extension` (boolean)
  - `psammoma_bodies` (boolean)
  - `background_thyroid` (normal / hyperplasia / thyroiditis / follicular adenoma / other)
  - Standard envelope: `entity_type`, `entity_value`, `entity_date`, `confidence`, `present_or_negated`, `evidence_text`, `source_line`
- **Run command on pod:**
  ```bash
  .venv/bin/python scripts/vastai/run_extraction_concurrent.py \
    --input-parquet processed/remaining/9domain_v4/input_pathology.parquet \
    --output-dir runs/9domain_v4/pathology/output \
    --url "$VLLM_URL" \
    --model "$VLLM_MODEL" \
    --api-key vllm \
    --domains pathology \
    --concurrency 256 \
    2>&1 | tee runs/9domain_v4/logs/pathology.log
  ```
- **Expected rowcount:** ~55K–70K entity rows, ~10,500+ RIDs covered (full cohort minus patients with zero path notes).
- **Upload pattern:** follow Script 285 template (`scripts/285_vasc_v4_md_load_and_rollup.py`). Phases 0–7:
  1. Audit parquet locally
  2. Archive existing `main.note_entities_llm_pathology` to `archive_pub_v1_0.note_entities_llm_pathology_preRUNPODjob1a_<UTCZ>`
  3. Load parquet to MotherDuck with 6 provenance columns (extracted_at, model_name, prompt_version, run_id, pipeline_version, source_parquet_path)
  4. Byte-hash parity check (parquet vs. MD)
  5. Rebuild CPM `nlp_path_*` rollup columns (4-column Tier 1 shape — see Script 285 for the pattern)
  6. Invariants snapshot to `scripts/output/runpod_job1a_invariants.json`
  7. Sync data dictionary / registry / __readme
- **New file to create:** `scripts/runpod_400_pathology_qwen25_rerun.py` (use 400+ to avoid collision with Cursor's 341–345 Prompt-5 range).

### Job 1b — cervical_ln_detail
- **Input parquet:** `processed/remaining/9domain_v4/input_cervical_ln_detail.parquet` (source notes = `note_type IN ('US_REPORT', 'CT_NECK', 'MRI_NECK', 'PATH_REPORT')` — LN findings live in imaging + path reports)
- **Prompt template:** `prompts/cervical_ln_v4.txt` — clone from existing if absent
- **Entity schema:** `ln_region` (central / lateral-level_II / III / IV / V / VI / VII), `ln_size_short_axis_mm`, `suspicious_features` (round, cystic, loss_of_hilum, microcalcifications, hypervascular), `fna_recommended`, `fna_result`, `bilateral`
- **Run command:** same pattern as 1a, `--domains cervical_ln_detail`, output dir `runs/9domain_v4/cervical_ln_detail/output`
- **Expected rowcount:** ~30K–45K entity rows
- **Upload script:** `scripts/runpod_401_cervical_ln_qwen25_rerun.py`

### Job 1c — tirads_granular
- **Input parquet:** `processed/remaining/9domain_v4/input_tirads_granular.parquet` (source notes = `note_type='US_REPORT'`; reuse the same note set that fed `note_entities_llm_tirads_granular` originally)
- **Prompt template:** `prompts/tirads_granular_v4.txt`
- **Entity schema:** per-nodule TIRADS components — `composition` (cystic/mixed/solid/spongiform), `echogenicity` (anechoic/hyperechoic/isoechoic/hypoechoic/very_hypoechoic), `shape` (wider_than_tall / taller_than_wide), `margin` (smooth/ill_defined/lobulated/irregular/extrathyroidal_extension), `echogenic_foci` (none/macrocalcifications/rim_calcifications/punctate_echogenic_foci), `size_cm`, `nodule_position`, `tirads_score_2017`
- **Run command:** same pattern, `--domains tirads_granular`
- **Expected rowcount:** ~40K–60K entity rows
- **Upload script:** `scripts/runpod_402_tirads_granular_qwen25_rerun.py`

**Invariants that must hold after Job 1 completes (all three tables):**
- Every MD table's `extracted_at` timestamps are from the current run window — NO silent re-use of old qwen3:32b timestamps
- `model_name='qwen2.5-32b'` on every row
- Distinct `research_id` > 10,000 (up from 5,641)
- CPM row count unchanged (10,871)
- Pre-run archive rows logged in `manuscript_workspace.archive_move_log_v1`
- Upload logged to `manuscript_workspace.extraction_upload_log_v1` (create table if it doesn't exist with columns `script_n`, `md_table`, `source_parquet`, `n_rows`, `n_rids`, `model_name`, `pipeline_version`, `loaded_at`)

---

## Job 2 — TIRADS v2 requeue extraction (4,363 nodules)

**Problem:** `main.tirads_reextraction_queue_v1` holds 4,363 nodules for 1,316 patients where `tirads_score_2017` was extracted on first pass but the `calcifications` field is NULL. The first-pass extraction happened against the raw Excel US_Nodules workbook; the second pass needs to re-read the **source notes** (not the Excel) and harvest the missing calcifications signal.

**Inputs (verified on disk 2026-04-21):**
- Queue on MotherDuck: `thyroid_canonical_publication_v1_0.main.tirads_reextraction_queue_v1` (4,363 rows).
- Source Excel (for cross-reference only, not for re-extraction): `raw/US Nodules TIRADS 12_1_25.xlsx` and `raw/COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx`.
- Source notes for actual re-extraction: `main.clinical_notes_long WHERE note_type='US_REPORT'` joined to queue by `(research_id, note_row_id)` or `(research_id, note_date)` depending on what the queue carries.

**Procedure:**
1. On pod, pull the queue as a local parquet:
   ```python
   import duckdb, toml
   cfg = toml.load('motherduck.local.toml')
   con = duckdb.connect(f"md:?motherduck_token={cfg['MD_SA_TOKEN']}")
   con.execute("""
     COPY (
       SELECT q.*, cnl.note_row_id, cnl.note_date, cnl.note_text
         FROM thyroid_canonical_publication_v1_0.main.tirads_reextraction_queue_v1 q
         LEFT JOIN thyroid_canonical_publication_v1_0.main.clinical_notes_long cnl
           USING (research_id)
          AND cnl.note_type='US_REPORT'
     ) TO 'processed/remaining/tirads_requeue/input.parquet' (FORMAT PARQUET)
   """)
   ```
2. **Extractor:** `scripts/extract_tirads_from_us_reports.py` already exists — check it accepts a queue parquet. Invoke with:
   ```bash
   .venv/bin/python scripts/extract_tirads_from_us_reports.py \
     --input processed/remaining/tirads_requeue/input.parquet \
     --output runs/tirads_requeue/output/ \
     --model qwen2.5-32b \
     --vllm-url "$VLLM_URL" \
     --concurrency 128 \
     --fields calcifications
   ```
   If the script doesn't expose `--fields`, call it with the full TIRADS component list; we'll just discard the already-populated fields downstream.
3. **Upload** to a new MotherDuck table `main.tirads_v2_nodules_requeued_v1` (do NOT overwrite `main.tirads_v2_nodules_raw`). Archive pattern as usual.
4. **Merge** into `main.tirads_v2_nodules_raw`: UPDATE by `(research_id, nodule_id)` setting `calcifications` from the requeue table where it was NULL. Log the number of rows updated.
5. **Refresh** `main.tirads_v2_nodule_patient_rollup_v1` and CPM columns (`tirads_v2_n_nodules_scored`, `tirads_v2_worst_category`, etc.) — follow Prompt 3 Scripts 328–329 rollup pattern.
6. **Note on Prompt-3 interaction:** Cursor's Prompt 3 already ran Scripts 328–329 against the current TIRADS data. Adding 4,363 more nodules requires re-running the rollup/backfill. Coordinate: either wait for this requeue to complete before Prompt 3's 328–329 execute, OR run a one-off CPM re-backfill after this job lands.

**Expected outcome:** `tirads_v2_nodules_raw.calcifications` nonnull count jumps by ~4,000–4,363 (some nodules may still fail if the source note doesn't mention calcifications at all).

**Upload script:** `scripts/runpod_403_tirads_requeue.py`.

**Expected time:** 3–5 hours on H200.

---

## Job 3 — Dedicated esophageal invasion extraction (4,727 op-notes)

**Problem:** CPM `op_esophageal_inv_any` is 0 nonnull. `operative_episode_detail_v2.esophageal_involvement_flag` is all NULL. Only 2 positive mentions exist in `note_entities_operative_detail` under `entity_type='esophageal_involvement'`. No `note_entities_llm_esophageal_invasion` table exists on MotherDuck yet.

Prompt 5 Script 342 will copy what little signal exists from `note_entities_llm_airway_invasion` JSON (airway-note side mentions). That's 2–20 patients, not real coverage. Real coverage requires dedicated extraction.

**Inputs (verified on disk 2026-04-21):**
- Source notes: `main.clinical_notes_long WHERE note_type='OPNOTE'` → **4,727 notes** across the cohort (verified count).
- No Excel source — esophageal invasion isn't captured in the NSQIP/synoptic/operative Excels.

**Procedure:**

1. **Build input parquet on pod:**
   ```sql
   COPY (
     SELECT cnl.research_id, cnl.note_row_id, cnl.note_date, cnl.note_type, cnl.note_text
       FROM thyroid_canonical_publication_v1_0.main.clinical_notes_long cnl
      WHERE cnl.note_type = 'OPNOTE'
      ORDER BY cnl.research_id, cnl.note_date
   ) TO 'processed/remaining/esophageal/input_esophageal.parquet' (FORMAT PARQUET);
   ```

2. **Write prompt template** `prompts/esophageal_invasion_v1.txt`:
   - Use `prompts/operative_note_extraction_v1.txt` as structural template.
   - Entity schema:
     - `esophageal_invasion_present` (boolean)
     - `esophageal_invasion_extent` ∈ {`abutting`, `invading_partial`, `full_thickness`, `unknown`}
     - `esophageal_invasion_length_cm` (numeric)
     - `esophageal_repair_performed` (boolean)
     - `esophageal_muscularis_invasion` (boolean)
     - `esophageal_mucosal_invasion` (boolean)
     - Standard envelope: `entity_type, entity_value, entity_date, confidence, present_or_negated, evidence_text, source_line`.
   - Guardrails in prompt: explicitly distinguish airway vs. esophagus; many op-notes mention "airway protected" without any esophageal involvement.

3. **Run on pod:**
   ```bash
   .venv/bin/python scripts/vastai/run_extraction_concurrent.py \
     --input-parquet processed/remaining/esophageal/input_esophageal.parquet \
     --output-dir runs/esophageal/output \
     --url "$VLLM_URL" \
     --model "$VLLM_MODEL" \
     --api-key vllm \
     --domains esophageal_invasion \
     --concurrency 256 \
     2>&1 | tee runs/esophageal/esophageal.log
   ```

4. **Upload** output parquet to new MotherDuck table `main.note_entities_llm_esophageal_invasion`. Use the same 6-provenance-column pattern as Scripts 282–285. Archive any previous version if it exists.

5. **Parse Tier 2:** following the Prompt 2 pattern (see existing `tier2.airway_invasion_event_v1` as template), build:
   - `tier2.esophageal_invasion_event_v1` (one row per positive event, typed columns from the schema above)
   - Add a `tier2.esophageal_invasion_patient_wide_v1` OR (given Prompt 4 already collapsed patient_wide tables) extend `tier2.patient_tier2_master_v1` with `esophageal__*` prefixed columns via ALTER TABLE + UPDATE.

6. **Backfill CPM canonical column** `op_esophageal_inv_any` from the new Tier 2 event table — this is the authoritative read-column. Also populate `op_esophageal_inv_first_date`, `_first_source_note_ref`, `_first_evidence_text`, `_n_notes_documenting` per Constraint 7.

7. **Sync `operative_episode_detail_v2.esophageal_involvement_flag`** per-episode (assuming Prompt 5 Script 341 rebuilt the multi-episode structure — this step depends on 341 being done first).

**Expected outcome:** real TRUE count of somewhere between 30 and 200 patients (esophageal invasion is rare in thyroid cancer but not 2-rare). If TRUE count is still < 10, investigate whether the prompt is missing signals rather than accepting the result.

**Upload script:** `scripts/runpod_404_esophageal_invasion.py`.

**Expected time:** 5–10 hours on H200 (4,727 notes × 3–5 notes/min × 256 concurrency → well under a day).

---

## Execution order and parallelism

- **Start Job 1a (pathology)** first — longest, highest manuscript impact.
- **Job 2 (TIRADS requeue)** runs on a second pod in parallel (or in Job 1a idle time) — small, quick.
- **Job 1b (cervical_ln_detail)** + **Job 3 (esophageal)** after Job 1a completes — similar runtime envelopes, can pair on one pod sequentially or two pods in parallel.
- **Job 1c (tirads_granular)** last, since Job 2 is simultaneously repairing TIRADS coverage from a different angle.

Total wall time with one pod: ~5–7 days. With two pods: ~3–4 days.

---

## Upload invariants (apply to every job)

1. Archive existing MD table (if present) to `archive_pub_v1_0.<name>_preRUNPODjob<N>_<UTCZ>` BEFORE overwriting. Log to `manuscript_workspace.archive_move_log_v1`.
2. Byte-hash parity check: local parquet hash == MD-fetched parquet hash (the Script 285 pattern — Phase 3).
3. CPM row count unchanged (10,871 / 10,871 distinct research_id).
4. Every row in the new MD table carries `model_name='qwen2.5-32b'`, `pipeline_version='9domain_v4'` (or `tirads_v2_requeue` / `esophageal_v1` for the specialized jobs), `run_id` UUID, `extracted_at` current-run timestamp.
5. Log to `manuscript_workspace.extraction_upload_log_v1` (create with columns noted above).
6. Never print note_text or evidence_text to stdout — only research_id, note_row_id counts, row counts, timing.

## Commit discipline

Per job:
```bash
cd "/Users/ros/THyroid 2026"
git add scripts/runpod_<N>_*.py prompts/<domain>_v*.txt runs/<domain>/logs/
python -m pyflakes scripts/runpod_<N>_*.py
git commit -m "RunPod Job <N>: <domain> at qwen2.5-32b — <n_rows> rows / <n_rids> RIDs"
git push origin main
```

## Hand-off checklist at end of the round

1. Three re-extracted `note_entities_llm_*` tables on MotherDuck (pathology, cervical_ln_detail, tirads_granular) all at `model_name='qwen2.5-32b'` with > 10,000 RID coverage each.
2. `tirads_v2_nodules_raw.calcifications` nonnull jumped by ≥ 4,000 rows; CPM TIRADS columns re-rolled.
3. New `main.note_entities_llm_esophageal_invasion` table + `tier2.esophageal_invasion_event_v1` + CPM `op_esophageal_inv_any` backfilled with the new dedicated signal (not the Script 342 airway-JSON proxy).
4. Four archive moves for overwritten tables (pathology, cervical_ln_detail, tirads_granular, esophageal if one existed).
5. CPM invariants green.
6. `docs/RUNPOD_EXTRACTION_ROUND_REPORT_20260421.md` written back to Logan with: row counts pre/post, distinct-RID deltas, time spent per job, any failures.

## What NOT to do in this chat

- Do not re-run Scripts 282–285 — those 5 Phase-B' domains are already at qwen2.5-32b and on MotherDuck.
- Do not touch any table that Cursor's Prompt 5 is producing — Scripts 341–345 are pure SQL remediation of existing data.
- Do not upload the legacy parquets under `runs/domain_reruns_qwen3_32b_targeted/*/output/` — those mix qwen3:14b and qwen3:32b and will contaminate the model field.
- Do not touch `main.clinical_notes_long`, `main.canonical_patient_master` schema, or any `tier2`/`verify` schema tables outside the designated upload targets.
- Do not print clinical note text or `evidence_text` to stdout at any time.
- Do not skip the archive-before-overwrite step.
- Do not change the model from `qwen2.5-32b` without asking Logan first.

---

## Cross-reference to Cursor's Prompt 5

Prompt 5 Script 342 populates `op_esophageal_inv_any` from existing airway_invasion JSON + operative entity rows — that's a 2-to-20 patient fix from data already on MD. **Job 3 here** replaces that proxy signal with a dedicated extraction when it completes. Coordination: Script 342 runs first (takes hours, fills the canonical column with the small existing signal); Job 3 completes later (takes days, replaces the column contents with the real extraction result). Script 342 writes to the same column, Job 3's upload script overwrites it — this is intentional.

End of handoff.

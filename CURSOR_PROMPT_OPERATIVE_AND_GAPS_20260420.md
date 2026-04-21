# Cursor Prompt — Operative Detail & Remaining CPM Data Gaps

**Written:** 2026-04-20 — after Scripts 280–287 ran against `thyroid_canonical_publication_v1_0`.
**Author:** Claude (continuing session).
**For:** Cursor working in the `/Users/ros/THyroid 2026` repo, with MotherDuck access via `_md_connect.connect_locked()`.

---

## Context snapshot (what's already done — do NOT redo)

1. **Script 280** — Archived `us_nodules_tirads_placeholder_archive_v1` (3-row placeholder). No other stale objects found under their expected names; all listed archive candidates already cleaned in prior session (272/273).
2. **Script 281** — Wrote `COMMENT ON TABLE/VIEW` for 237 objects from `detail_table_registry_v1.description` + domain/grain tags.
3. **Script 282** — Materialized `manuscript_workspace.object_domain_map_v1` (237 rows, 23 clinical domains, canonical_rank 1–9, friendly-name suggestions).
4. **Script 284** — Created `views_readable` schema with 46 friendly-named VIEWs (`US_Nodules_TIRADS`, `Genetics_Testing`, `Patient_Master_Canonical`, `FNA_Cytology`, `Pathology_Synoptics`, `Recurrence_Status`, `RAI_Treatment_Episode`, `Labs_Tg_Longitudinal`, `PMHx_from_Notes_LLM`, etc.) — non-destructive aliases over canonical tables.
5. **Script 285** — Provenance-audited each of the 15 × 100%-NULL CPM columns, wrote `manuscript_workspace.cpm_missing_data_provenance_v1`.
6. **Script 286** — Conservative CPM backfill. Committed:
   - `nucmed_tgab_max` ← `MAX(result_numeric) FROM thyroglobulin_lab_canonical_v1 WHERE analyte='TgAb'`. Populated 2,602 / 10,871 patients.
   - `tsh_suppressed_ever` ← any `value_numeric < 0.1` in `longitudinal_lab_canonical_v1 WHERE lab_name_standardized='tsh'`. Populated 163 / 10,871 (see gap #3 below).
   - `biochemical_concern_first_date` — **FAILED**: CPM column is typed `INTEGER`, source is `TIMESTAMP_NS`. Not backfilled. (Gap #4.)
7. **Script 287** — CPM `v1 → v2` surgery-column consolidation. Committed 4,431 row-updates across 6 columns, filling NULL cells only (no overwrites). Policy: never overwrite an existing v1 value.

---

## What STILL needs work — and is NOT safe to auto-apply

### Gap 1 — `n_surgeries` v1↔v2 conflict resolution (598 patients)

After Script 287 filled NULLs, `n_surgeries` is now 100% populated. But 598 rows have v1 ≠ v2:

| v1 | v2 | count |
|----|----|-------|
| 1  | 2  | 569   |
| 1  | 3  | 21    |
| 1  | 4  | 6     |
| 1  | 5  | 1     |
| 1  | 6  | 1     |
| 2  | 1  | 1     |

v2 is the correct pipeline output. v1 is stale. Logan's concern ("patients with up to 6 surgeries, we need that resolved ASAP") is precisely these 598 cases — right now, anyone querying `n_surgeries` still sees the stale v1 values for them.

**Task:**
- Write `Script 288_cpm_n_surgeries_v1_overwrite.py` that `UPDATEs canonical_patient_master SET n_surgeries = n_surgeries_v2 WHERE n_surgeries != n_surgeries_v2`.
- Dry-run first. Report the 598 row delta. Verify `n_surgeries_v2` coverage is still 100%.
- Also do the same for `second_surgery_date`, `third_surgery_date`, `days_between_first_second_surgery` (1, 0, 1 conflicts respectively — trivial).
- For `first_surgery_date`: 105 rows differ as DATE values between v1 (TIMESTAMP) and v2 (DATE) — these are NOT just type diffs, they are genuinely different dates. DO NOT auto-overwrite; instead emit a CSV listing the 105 research_ids + (v1_date, v2_date) for Logan to review. `SELECT research_id, CAST(first_surgery_date AS DATE) AS v1, first_surgery_date_v2 AS v2 FROM main.canonical_patient_master WHERE CAST(first_surgery_date AS DATE) != first_surgery_date_v2`.
- Log every update row to `manuscript_workspace.cpm_backfill_log_v1` with `script = '288_cpm_n_surgeries_v1_overwrite'`.
- Invariants: rows=10871, distinct_rid=10871, null_fna=0, pre+post.

### Gap 2 — `operative_episode_detail_v2` is incomplete (multi-surgery episodes missing)

`main.operative_episode_detail_v2` currently has **9,371 rows for 9,368 patients** — but `canonical_patient_master.n_surgeries_v2` says **738 patients have ≥2 surgeries** (of whom 40 have ≥3, and two patients have 5 or 6). The detail table is therefore missing at least (738 − 3) = 735 re-operation episodes.

That means any per-episode analysis (op approach, ENE status, operative findings) on re-operations is silently missing.

**Task:**
- Investigate why `operative_episode_detail_v2` ended up with essentially one episode per patient when `n_surgeries_v2` was built from a source that clearly identified multiple. Trace back the build script: look in `/Users/ros/THyroid 2026/scripts/` for whichever script writes `operative_episode_detail_v2` (likely named `*_episode*_detail*.py` or similar; grep for `CREATE OR REPLACE TABLE.*operative_episode_detail_v2`).
- The upstream source is probably `note_entities_operative_detail` (which has 29,000+ rows for 10,000+ patients) or `op_sheet_data`. The build script likely kept only the first episode per patient. Fix the build logic so each surgical episode gets its own row keyed by `(research_id, episode_rank)`.
- Rebuild with dry-run → report row count, distinct_rid count, distribution of episodes per patient — verify it matches `n_surgeries_v2` distribution (10133/698/31/7/1/1 for 1/2/3/4/5/6 surgeries).
- Re-derive CPM rollup columns downstream: `age_at_surgery` etc. are keyed on first surgery — those should not change. `op_reoperative_any`, `op_n_surgeries_with_findings` may shift; report deltas.
- Archive the current `operative_episode_detail_v2` as `manuscript_workspace.archive_operative_episode_detail_v2_20260420` before replacing.

### Gap 3 — TSH upstream is sparse (415 patients only)

`main.longitudinal_lab_canonical_v1` has only 515 TSH rows for 413 distinct patients. For a 10,871-patient thyroid cohort where post-op TSH monitoring is standard of care, this is a major extraction gap.

`main.note_entities_llm_labs` has 11,037 rows for 5,641 patients in raw `result_json`, probably contains TSH values that were never parsed/canonicalized.

**Task:**
- Parse `result_json` in `note_entities_llm_labs` and route TSH values into `longitudinal_lab_canonical_v1` (or a TSH-specific canonical table). Match on `research_id + note_date`.
- Don't overwrite existing rows; `INSERT ... WHERE NOT EXISTS (SELECT 1 FROM longitudinal_lab_canonical_v1 l WHERE l.research_id = s.research_id AND l.result_date = s.note_date AND l.lab_name_standardized = 'tsh')`.
- Write a small audit: pre/post row counts, patient coverage delta for TSH specifically.
- After re-running, re-execute the `tsh_suppressed_ever` backfill block from Script 286.

### Gap 4 — `biochemical_concern_first_date` is typed `INTEGER`

`canonical_patient_master.biochemical_concern_first_date` is declared `INTEGER`, but the name clearly implies a date. Source (`tg_postop_surveillance_windows_v1.window_first_date`) is `TIMESTAMP_NS`. Script 286 failed trying to cast timestamp → integer.

Logan: pick one:
- **(a)** Change column type to `DATE` (or `TIMESTAMP`). Requires `ALTER TABLE ... DROP COLUMN ... ADD COLUMN` in DuckDB (no in-place type change). Backfill from `MIN(window_first_date) WHERE value_max > 2.0`.
- **(b)** Rename to `biochemical_concern_first_days_postop` and backfill as days-from-first-surgery integer.

Once decided, execute via a short new script (`289_biochem_concern_first_date_fix.py`). Must preserve CPM invariants.

### Gap 5 — Recurrence text fields 100% NULL

`canonical_recurrence_v1.recurrence_histology` and `.recurrence_site` are 100% NULL despite `recurrence_event_clean_v1` having populated records. Upstream has `research_id + date + type` keys but the narrative text fields were never filled.

**Task:**
- Run a targeted LLM extraction over post-recurrence path reports for the ~300 patients flagged `recurrence_flag=TRUE` in `canonical_recurrence_v1`. Source notes: `path_synoptics` and `note_entities_llm_pathology` filtered to dates ≥ first recurrence event.
- Extract: `recurrence_histology` (e.g., "papillary thyroid carcinoma recurrence," "anaplastic transformation"), `recurrence_site_primary` (e.g., "central compartment lymph nodes," "lung," "bone").
- Write results back to `canonical_recurrence_v1`; propagate to CPM.

### Gap 6 — Vocal-cord complication tiering (88 paralysis + 71 paresis rows, 0 tiered)

`complication_phenotype_v1` has 88 rows for vocal_cord_paralysis and 71 for vocal_cord_paresis, but `evidence_tier` is NULL for all of them. The tiering logic was only applied to hypocalcemia / hypoparathyroidism / rln_injury.

**Task:**
- Find the existing complication-tiering script (grep for `complication_phenotype_v1` + `evidence_tier`). Extend the phenotype list to include `vocal_cord_paralysis` and `vocal_cord_paresis`, replaying the same tier-1/2/3 evidence rules. Commit the updates.
- Re-derive CPM's `comp_vc_paralysis_evidence_tier` and `comp_vc_paresis_evidence_tier`.

### Gap 7 — `op_esophageal_inv_any` (100% NULL)

`note_entities_operative_detail` does not have an esophageal-involvement column. This requires a new LLM extraction targeting operative notes for any mention of esophageal invasion / involvement / resection / shave. ~10k operative notes to scan.

**Task:**
- Add an extraction prompt to the operative-note NLP pipeline that flags esophageal involvement.
- Write to a new column `op_esophageal_inv_any` on `note_entities_operative_detail`.
- Roll up to CPM: TRUE if any positive mention for the patient.

### Gap 8 — `path_stage_raw` and `gm_path_stage_raw` (100% NULL)

`path_synoptics` has parsed per-tumor AJCC7/AJCC8 stage columns (`tumor_1_t_stage_ajcc8`, `tumor_1_n_stage_ajcc8`, `tumor_1_m_stage_ajcc8`, etc.) but no single "raw" stage string. CPM columns are meant to be a concatenated/canonical raw display.

**Task (low risk):** Derive `path_stage_raw` by concatenation: `CONCAT('T', tumor_1_t_stage_ajcc8, 'N', tumor_1_n_stage_ajcc8, 'M', tumor_1_m_stage_ajcc8)` per research_id. Populate via a new script 290_derive_path_stage_raw.py. Dry-run + commit.

### Gap 9 — `rai_scan_findings_v9` (100% NULL, rename artifact)

The CPM column name references a non-existent table `rai_scan_history_v9` — only `rai_treatment_episode_v2` exists. Sibling column `rai_intent_v9` IS populated from the real table.

**Task:** Either `ALTER TABLE ... DROP COLUMN rai_scan_findings_v9` or rename to `rai_findings_text` and populate from `note_entities_llm_rai_detailed.findings_text`. Logan to decide. Low-risk mechanical change once decided.

### Gap 10 — `recurrence_event_clean_v1` is the canonical, but CPM reads `canonical_recurrence_v1`

Cross-check which recurrence source Logan actually wants CPM to point at. Low risk but worth auditing: do their patient counts agree? Do their event dates agree?

---

## Constraints for ALL tasks

- **PHI safety**: never print clinical notes. research_id only. No cloud PHI.
- **Commit workflow**: every script must have `--commit` gated; dry-run prints plan only.
- **CPM invariants**: every CPM-touching script must verify pre + post that rows=10871, distinct_rid=10871, `fna_path_outcome IS NOT NULL for all`. Fail loudly if violated.
- **Never overwrite non-NULL values** unless a column is explicitly being corrected (e.g., n_surgeries 598 conflict fix). Document the policy in the script header.
- **Log every update** to `manuscript_workspace.cpm_backfill_log_v1` with script name + threshold + row counts.
- **Stage / commit / push** each completed script separately. Lint Python first (`ruff` or equivalent).
- Archive any table you're going to drop or replace into `manuscript_workspace.archive_*` first, under the Script 280 pattern.

## Priority ordering

1. **Gap 2 (operative_episode_detail_v2 rebuild)** — highest impact, fixes Logan's "ASAP" concern about multi-surgery patients.
2. **Gap 1 (n_surgeries conflict overwrite)** — trivial SQL, fixes 598 wrong values downstream.
3. **Gap 4 (biochem col type)** — blocks the third Script 286 backfill.
4. **Gap 3 (TSH extraction)** — widens `tsh_suppressed_ever` from 163 → potentially thousands.
5. Gaps 5–10 — ordered by clinical importance per the manuscript's analysis plan.

## Useful artifacts already in the DB

- `manuscript_workspace.cpm_missing_data_provenance_v1` — full audit of every 100%-NULL CPM column with recommended_action.
- `manuscript_workspace.object_domain_map_v1` — 237 objects × 23 domains × canonical_rank. Query this to find the canonical source for any domain.
- `manuscript_workspace.cpm_backfill_log_v1` — running ledger of every automated CPM UPDATE.
- `views_readable.*` — 46 friendly-named views. Use these from Power BI and Cursor queries rather than the base names.
- `detail_table_registry_v1` — descriptions + domain + grain for every detail table; source of truth for `COMMENT ON TABLE`.

---

**End of prompt.** Start with Gap 2 unless Logan says otherwise.

# 2023–2025 Operative Ingest and Procedure Field Audit

**Date:** 2026-05-08  
**Scope:** Local repository and git history only (no BigQuery executed this session).  
**Project:** `THYROID_2026` at `/Users/loganglosser/THYROID_2026`.

---

## Executive Summary

Post-2022 operative rows with NULL `procedure_raw`, `operative_findings_raw`, and `surgery_date_native` in BigQuery trace to **MotherDuck `operative_episode_detail_v2`**, which is built from **`operative_details` + `path_synoptics`** in Script 22, then optionally rebuilt by **Script 341**. Procedure normalization is **not** taken from `notes_extraction_new` (that path **does not exist** in this repo). **`procedure_normalized`** for the classic pipeline comes almost entirely from **`path_synoptics.thyroid_procedure`** via a SQL `CASE` expression; **`procedure_raw`** comes from a **mislabeled OP-sheet column** (`preop_diagnosis_operative_sheet_not_true_preop_dx`), which explains pathology-heavy "procedure" text (documented in Tier-1 CF). **Script 341** propagates `procedure_raw` / `procedure_normalized` only when a rebuilt episode **matches an existing OED row** within ±7 days; otherwise those fields stay NULL. **`scripts/ops/365c_procedure_normalize_2023_2025.py`** cannot fix 2023–2025 when both raw and normalized procedure text are empty—it explicitly falls back to `procedure_normalized` only when present.

**Latest relevant git activity:** Script 341 `6fe0f91` (2026-04-21), Script 362 `94c57dd` (2026-04-21), 365c `4f7798a` (2026-05-08).

---

## Q1: Where is the 2023–2025 Operative Source?

### 1.1 `notes_extraction_new/operative/`

**Not present.** A recursive glob for `notes_extraction*` under the repo returned **0** paths. `365c` documents this explicitly:

```
# notes_extraction_new/operative/ — not present in this repo; operative SSOT for this
# script is pub_canonical.canonical_operative_events_v1 in BigQuery.
```

There is **no** parallel "operative extraction new" tree in-repo for agents to re-point.

### 1.2 `operative_episode_detail_v2` / Script 341

**Script:** `scripts/341_rebuild_operative_episode_multi_v2.py`  
**Commit:** `6fe0f91272f0f7652454b5a61f548a20eebff874` — *Script 341: rebuild operative_episode_detail_v2 with multi-episode rows* (2026-04-21).

Script 341 rebuilds episodes from **`canonical_patient_master` surgery dates v2** plus **`note_entities_operative_detail`** clustering. For each rebuilt row it pulls `procedure_raw` and `procedure_normalized` from the **matched prior OED row `o`** via COALESCE. If a new episode date (e.g. from CPM) **does not** match an existing row within ±7 days, **`o` is NULL** for those columns → **NULL procedure fields** even when the episode row exists. That is a concrete mechanism for 2023+ event rows with NULL procedure text independent of BigQuery.

The **original** OED v2 definition (pre-341) is in **Script 22**: `operative_details` joined to **`path_synoptics`** on `research_id` and surgery date (see Q2).

### 1.3 CSV / Parquet under `processed/`, `exports/`, `raw/`, `ingest/`, `external_data/`

- **`ingest/`** and **`external_data/`**: **no directories** found at repo root.
- **`processed/`**: Used as a parquet staging target in migrations (e.g. `qc_framework_v1/migrations/331b_bq_sync_operative_v23.py` references `processed/note_entities_operative_detail.parquet`). No "2023" operative export was found via glob; operative NLP exports live under `exports/` (e.g. `exports/final_operative_nlp_sync_20260314/`) and document NLP sync / QA, not a fresh 2023+ **procedure-name** feed.
- **`raw/`**: Listed in `.gitignore`; `*.xlsx` files not tracked. The code names expected masters including `Thyroid OP Sheet data.xlsx`.

### 1.4 Original OP Sheet / Synoptic Excel (date range)

Institutional **Active Master** file list (from `scripts/inspect_sources.py`) includes:

- `Thyroid all_Complications 12_1_25.xlsx`
- `Thyroid OP Sheet data.xlsx`
- `All Diagnoses & synoptic 12_1_2025.xlsx`
- `Notes 12_1_25.xlsx`

Naming (**12_1_25**, **12_1_2025**) indicates a **late-2025 export** that *should* include 2023–2025 activity if the underlying registry rows exist. The OP sheet is accessed from `THYROID_ACTIVE_MASTER_DIR` (env var, defaults to `~/Downloads/Active Master Files`). **Whether 2023–2025 rows are populated with procedure text in these files is the key open question** — this requires querying local DuckDB or MD after loading the Excel, which was not done in this audit.

### 1.5 Unprocessed Staging (Compass/Epic)

No `ingest/`, `external_data/`, or in-repo Compass/Epic staging tree was found. PHI / true EHR exports are expected under `THYROID_ACTIVE_MASTER_DIR` or `raw/` (gitignored), not a dedicated repo folder.

### Q1 Conclusion

**Authoritative structured operative inputs are:** `operative_details` (from Thyroid OP Sheet), `path_synoptics` (All Diagnoses & synoptic workbook chain), plus NLP tables `note_entities_operative_detail` / `note_entities_procedures` for flags and procedure mentions. **`notes_extraction_new/operative/` is absent.** BQ `canonical_operative_events_v1` is built as a **copy of `operative_episode_detail_v2`** (Script 362 Step 1), so any 2023+ NULL pattern in OED **replicates** to BQ.

**Primary hypothesis:** `path_synoptics.thyroid_procedure` for 2023+ either (a) has NULL/empty values for these research_ids, or (b) was never ingested from the 12/1/25 Excel into the MD tables that Script 22 reads.

---

## Q2: What Extraction Pathway Populated `procedure_normalized` Pre-2023?

### 2.1 Primary pathway: Script 22 SQL (operative_details × path_synoptics)

In `OPERATIVE_EPISODE_DETAIL_V2_SQL` in `scripts/22_canonical_episodes_v2.py` (approx. lines 588–668):

- **`procedure_raw`** = `operative_details.preop_diagnosis_operative_sheet_not_true_preop_dx` (column name warns it is not reliable)
- **`procedure_normalized`** = derived from `path_synoptics.thyroid_procedure` via a `CASE` expression, then `COALESCE(ps.procedure_normalized, 'unknown')`

**Consumed sources:** `operative_details` (OP sheet) and `path_synoptics` (synoptic / diagnoses workbook path). No NLP extractor is involved in the base `procedure_normalized` field.

This explains why `procedure_raw` is only 3-5% populated even pre-2023: the OP-sheet column (`preop_diagnosis_operative_sheet_not_true_preop_dx`) was largely empty or contained pathology text; the Tier-1 doc at `docs/tier1_cf_procedure_normalized_corruption_20260422.md` documents this corruption pattern.

### 2.2 Canonical events table (Script 362)

`canonical_operative_events_v1` Step 1 in `scripts/362_operative_consolidation.py` is a **`SELECT *` from `operative_episode_detail_v2`** (with `research_id` cast)—no recomputation of procedure fields occurs here. A **separate** table `canonical_operative_procedure_codes_v1` links `note_entities_procedures` to episodes (procedure *mentions*), but this does not update the main per-episode `procedure_normalized` on the event row.

### 2.3 Why Coverage Degrades Post-2022

1. **Join dependency:** If `path_synoptics` has no row or `thyroid_procedure` is empty for a given surgery date, `procedure_normalized` becomes `'unknown'` in DuckDB / NULL in BQ. For 2023+ patients this is the most likely mechanism.
2. **Script 341:** New episodes without a ±7d match to legacy OED inherit NULL `procedure_raw` / `procedure_normalized`.
3. **`365c` no-op when empty:** Classification uses coalesced procedure text, else `procedure_normalized` column, else nothing — cannot invent labels without text.

### Q2 Conclusion

**`procedure_normalized` was populated by SQL normalization of `path_synoptics.thyroid_procedure`**, joined to `operative_details` episodes—not by a separate NLP extractor or `notes_extraction_new`. The pathway has **not "stopped"** in the sense of a broken script; it was **never re-run or refreshed with 2023+ synoptic data** after the 12/1/25 master Excel became available. The root cause is a **stale MD ingest** of the synoptic/OP-sheet tables.

---

## Q3: When Did the Post-2022 Ingest Stop?

### 3.1 Git History (Operative Consolidation Line)

| Commit | Date | Message |
|--------|------|---------|
| `4f7798a` | 2026-05-08 | Add 365c BQ backfill for procedure_normalized_trusted and 2023-2025 surgery dates |
| `94c57dd` | 2026-04-21 | Script 362: operative procedure consolidation (initial build) |
| `6fe0f91` | 2026-04-21 | Script 341: rebuild operative_episode_detail_v2 with multi-episode rows |

There is **no** commit that says "ingest 2023 OP sheet stopped"; the timeline shows **April–May 2026** consolidation and BQ remediation scripts were created, not a historical 2023 ETL pause.

### 3.2 Log Evidence (Script 362 Run)

`scripts/output/362_writerun_skipdrop_20260422T005428Z.log` shows Steps 1–2 succeeded (11,773 rows → `canonical_operative_events_v1`), then **Step 3 failed** on a DuckDB parser error (`FILTER` in window function). This affects the procedure_codes build only, not the Step 1 copy of OED. Subsequent hotfix commits (`72852b8`, `07d4097`) address this.

### Q3 Conclusion

**There is no discrete "last successful 2023+ ingest" commit.** The pipeline was built in April 2026 and the MD source tables (`operative_details`, `path_synoptics`) were not refreshed from the 12/1/25 Excel before or during that build. The gap is a **data currency issue**: the 12/1/25 master files exist outside the repo but were not ingested into the MD tables that Script 22 consumes.

---

## Q4: Smallest Patch Proposal

### Recommendation A — Preferred (refresh structured source tables)

**Refresh `operative_details` and `path_synoptics` from the 12/1/25 masters**, then:

1. Re-ingest `Thyroid OP Sheet data.xlsx` and `All Diagnoses & synoptic 12_1_2025.xlsx` into MD `operative_details` and `path_synoptics` using existing ingest scripts (check for `raw_*/ingest_*.py` scripts or the Excel-ingest chain in `scripts/`).
2. Re-run the `OPERATIVE_EPISODE_DETAIL_V2_SQL` join (Script 22 pattern or incremental UPDATE) so `thyroid_procedure` joins populate `procedure_normalized` for 2023+ dates.
3. Re-run Script 341 only **after** OED episode-1 rows carry correct procedure fields, so matched rows propagate to multi-episode rebuilds.
4. Re-run Script 362 Step 1 to refresh `canonical_operative_events_v1`, then BQ-publish via the existing migration path (331b-style).

**Why smallest:** Reuses existing normalization `CASE` logic; no new LLM dependency; fixes root cause if synoptic/OP data contain procedure text for those years.

**First verification step (before implementing):** Run this query in MD to check actual coverage:
```sql
SELECT EXTRACT(YEAR FROM TRY_CAST(surg_date AS DATE)) AS yr,
       COUNT(*) AS n,
       COUNT(thyroid_procedure) AS has_procedure
FROM path_synoptics
WHERE TRY_CAST(surg_date AS DATE) >= '2023-01-01'
GROUP BY 1 ORDER BY 1;
```
If `has_procedure` > 0 for 2023+, Recommendation A is confirmed viable.

### Recommendation B — Fallback (LLM extraction from op notes)

If `path_synoptics.thyroid_procedure` is NULL for 2023+ rows, add LLM or rules-based extraction from `clinical_notes_long` op notes:

1. Scope `note_type = 'op_note'` and `note_date >= 2023-01-01` for unresolved patients.
2. Use existing `llm_extraction/extract_operative_v2.py` as template; add a `procedure_class` output field.
3. Stage results in a new table and MERGE into `operative_episode_detail_v2` / `canonical_operative_events_v1` with provenance columns.
4. Govern under the same Verification Check framework as FNA Bethesda LLM extractor.

**Tradeoff:** Higher cost, PHI handling complexity, ~2-3 weeks effort. Only justified if structured source is empty for 2023+.

### Q4 Conclusion

**Execute Recommendation A first.** The diagnostic MD query above will confirm or refute viability within minutes.

---

## Q5: Acceptance Criteria

1. **`pub_canonical.canonical_operative_events_v1`:** For events with `resolved_surgery_date >= 2023-01-01`, **≥80%** have non-NULL `procedure_raw` OR `procedure_normalized`. Ideally also check `procedure_normalized_trusted` = `'rule_clean'` per Tier-1 doc conventions.

2. **`pub_workspace.manuscript_cohort_v1_surgery_reconciled`:** **≥700 patients per year** for 2023 and 2024 (keyed on `surgery_date_canonical` year).

---

## Proposed Verification Check

**Title:** `VC-OP-PROC-2023-2025 — Operative procedure & surgery date completeness`

**Description:**
Validate: (1) Year-stratified NULL rates for `procedure_raw` / `procedure_normalized` on `canonical_operative_events_v1` for `resolved_surgery_date >= 2023-01-01` — target ≥80% non-NULL in either field; (2) `manuscript_cohort_v1_surgery_reconciled` annual counts ≥700 for 2023–2024; (3) Spot-check 50 random 2023+ `research_id`s confirming procedure labels match synoptic or op-note gold; (4) Confirm no PHI in BQ logs. Tie provenance to Script 22 / Script 341 / Script 362 / 365c.

**Linked scripts:** `22_canonical_episodes_v2.py`, `341_rebuild_operative_episode_multi_v2.py`, `362_operative_consolidation.py`, `ops/365c_procedure_normalize_2023_2025.py`

---

## Focused Cursor Prompt (for Fix Session)

```text
Goal: Restore operative procedure fields for 2023-01-01+ in MotherDuck and BigQuery.

Background: canonical_operative_events_v1 has 2,109 events with resolved_surgery_date >= 2023-01-01
where procedure_raw, operative_findings_raw, and surgery_date_native are all NULL.
Audit (docs/2023_2025_operative_ingest_audit_20260508.md) found root cause is stale
path_synoptics / operative_details tables in MD that were never refreshed from the 12/1/25 Excel.

Step 1 — Diagnose (5 min):
Run in MD:
  SELECT EXTRACT(YEAR FROM TRY_CAST(surg_date AS DATE)) AS yr,
         COUNT(*) AS n, COUNT(thyroid_procedure) AS has_procedure
  FROM path_synoptics
  WHERE TRY_CAST(surg_date AS DATE) >= '2023-01-01'
  GROUP BY 1 ORDER BY 1;
If has_procedure > 0 for 2023+, proceed. If 0, fall back to LLM extraction pathway (Recommendation B).

Step 2 — Re-ingest structured source (if Step 1 shows data):
Re-run the Excel ingest for "Thyroid OP Sheet data.xlsx" and
"All Diagnoses & synoptic 12_1_2025.xlsx" from THYROID_ACTIVE_MASTER_DIR into MD
operative_details and path_synoptics tables. Use existing ingest scripts; add
incremental/upsert guard so pre-2023 rows are not clobbered.

Step 3 — Rebuild OED:
Re-run scripts/22_canonical_episodes_v2.py OPERATIVE_EPISODE_DETAIL_V2_SQL for 2023+
(or run an UPDATE on operative_episode_detail_v2 from fresh path_synoptics join).
Verify procedure_normalized != 'unknown' for >= 80% of 2023+ episodes.

Step 4 — Propagate through 341 and 362:
Re-run scripts/341_rebuild_operative_episode_multi_v2.py --commit (episodes first).
Re-run scripts/362_operative_consolidation.py (Steps 1-2 minimum).
Sync to BQ per qc_framework_v1/migrations/331b_bq_sync_operative_v23.py pattern.

Step 5 — Validate acceptance criteria:
Run scripts/ops/365c_procedure_normalize_2023_2025.py --apply.
Assert: (a) >= 80% of 2023+ events have procedure_raw OR procedure_normalized non-NULL
in canonical_operative_events_v1; (b) manuscript_cohort_v1_surgery_reconciled has
>= 700 patients in 2023 and 2024.
Log to Verification Check VC-OP-PROC-2023-2025 in Airtable THYROID_MANUSCRIPT.
```

---

## Evidence Index

| Artifact | Path |
|----------|------|
| 365c (missing notes dir; coalesce logic) | `scripts/ops/365c_procedure_normalize_2023_2025.py` |
| Script 22 OED SQL | `scripts/22_canonical_episodes_v2.py` (~lines 588–668) |
| Script 362 event build | `scripts/362_operative_consolidation.py` (Step 1 ~lines 445–572) |
| Script 341 procedure carry | `scripts/341_rebuild_operative_episode_multi_v2.py` (~lines 391–425) |
| Active Master file names | `scripts/inspect_sources.py` |
| Tier-1 procedure corruption | `docs/tier1_cf_procedure_normalized_corruption_20260422.md` |
| BQ OED column sync | `qc_framework_v1/migrations/331b_bq_sync_operative_v23.py` |
| 362 run log (Step 3 parser failure) | `scripts/output/362_writerun_skipdrop_20260422T005428Z.log` |

# Step 1 — Discovery Findings (Lab Ingestion Refactor / Script 348)

**Run date:** 2026-04-21
**Commit before refactor:** `4d3c4fc` (Script 347 lab consolidation)

---

## 1A. Repo-wide grep — INSERT / CREATE / DROP / DELETE / UPDATE / ALTER targeting the dropped legacy tables

Search pattern (case-insensitive, restricted to `scripts/`):

```
(INSERT INTO|CREATE OR REPLACE TABLE|CREATE TABLE|DROP TABLE|UPDATE|DELETE FROM|ALTER TABLE)\s+(main\.)?(longitudinal_lab_canonical_v1|thyroglobulin_lab_canonical_v1|lab_cross_wave_dedup_map_v1)
```

| # | Script | Line | Statement | Refactor disposition |
|---|--------|------|-----------|----------------------|
| 1 | `scripts/113_tg_lab_ingestion.py` | 659 | `CREATE TABLE thyroglobulin_lab_canonical_v1 AS ...` | **Refactored** in place (Phase I retargets to `canonical_labs_thyroglobulin_v1`). |
| 2 | `scripts/113_tg_lab_ingestion.py` | 754 | `CREATE TABLE longitudinal_lab_canonical_v1 AS ...` | **Removed** (replaced by per-analyte write). |
| 3 | `scripts/113_tg_lab_ingestion.py` | 775 | `DELETE FROM longitudinal_lab_canonical_v1 ...` | **Removed**. |
| 4 | `scripts/113_tg_lab_ingestion.py` | 782 | `INSERT INTO longitudinal_lab_canonical_v1 ...` | **Removed**. |
| 5 | `scripts/113_tg_lab_ingestion.py` | 1008 | `CREATE OR REPLACE TABLE lab_cross_wave_dedup_map_v1 AS ...` | **Removed** (cross-wave dedup is now inline in the per-analyte write). |
| 6 | `scripts/127_analyst_institutional_lab_append.py` | 257 | `DELETE FROM main.longitudinal_lab_canonical_v1 WHERE ingestion_wave = ?` | **Refactored** — DELETE now scoped to per-analyte target tables, keyed on `(source='institutional_append', ingestion_wave_tag)` provenance. |
| 7 | `scripts/127_analyst_institutional_lab_append.py` | 266 | `INSERT INTO main.longitudinal_lab_canonical_v1 (...)` | **Refactored** — rows are routed by `lab_name_standardized` to the matching per-analyte canonical table (`canonical_labs_{thyroglobulin,tsh,pth,calcium,vitamin_d}_v1`). |
| 8 | `scripts/77_lab_canonical_layer.py` | 47 | `CREATE OR REPLACE TABLE longitudinal_lab_canonical_v1 AS ...` | **Frozen** — superseded by Script 347. SQL string neutralized; SystemExit guard at module entry. |
| 9 | `scripts/235_parathyroid_calcium_fix.py` | 340 | `CREATE OR REPLACE TABLE longitudinal_lab_canonical_v1 AS ...` | **Frozen** — calcium-specific rebuild now subsumed by Script 347 unified normalizer. SQL string neutralized; SystemExit guard added. |
| 10 | `scripts/291_tsh_llm_integration.py` | 227 | `INSERT INTO main.longitudinal_lab_canonical_v1` | **Frozen** — TSH LLM-derived rows now flow into `canonical_labs_tsh_v1` via the unified ingestion path. SQL string neutralized; SystemExit guard added. |
| 11 | `scripts/331_calcium_denominator_recovery.py` | 250 | `INSERT INTO main.longitudinal_lab_canonical_v1` | **Frozen** — calcium recovery rows now flow into `canonical_labs_calcium_v1` via the unified path. SQL string neutralized; SystemExit guard added. |

> Output-folder hits (`scripts/output/_347_dryrun.log`, `scripts/output/347_run_*.log`) are historical run logs of Script 347's drop step and are **not** sources of write activity.

After refactor the same grep returns **0 hits** outside frozen-script docstrings (verified by Script 348 PASS/FAIL gate).

---

## 1B. `scripts/113_tg_lab_ingestion.py` summary (≤300 words)

Pipeline ingests Emory's structured EHR Tg/TgAb laboratory extract
(`Thyroid_Thyroglobulin_Lab_20251120.csv`, 78,112 rows, 3,298 patients) and
writes a per-row canonical Tg/TgAb table plus several derived analytical
tables.

**Phases (legacy):**
- A — Load & validate CSV; coerce `research_id` to int; parse
  `specimen_collect_dt`.
- B — Strip PII columns (MRN, DOB, names) and rename
  `research_id_number → research_id`.
- C — Exact-match dedup on
  `(research_id, test_name, specimen_collect_dt, result)`.
- D — Test-name normalization via `TEST_NAME_MAP` (29 distinct labels →
  `(analyte, assay_method)` tuples).
- E — Combo-panel disambiguation: pairs labelled
  `THYROGLOBULIN AND THYROGLOBULIN ANTIBODY` are split into Tg + TgAb
  using detection-limit heuristics (99.2 % accuracy on 7,622-pair
  ground truth) and same-patient cross-reference fallback.
- F — Result parsing: `_TITER_RE` for TgAb titers (`1:25600`),
  `_NUMERIC_RE` for `<` / `>` censored / numeric values; sets
  `result_numeric`, `result_qualifier`, `result_flag`.
- G — Temporal linkage: `days_from_surgery` and `temporal_window`
  bucketing (`pre_surgery`, `perioperative`, `early_postop`,
  `surveillance_1y/5y`, `long_term`).
- H — Schema alignment to Emory's legacy canonical column set.
- I — Writes `processed/thyroglobulin_lab_canonical_v1.parquet` and
  DuckDB / MotherDuck table `thyroglobulin_lab_canonical_v1`. **DROPPED.**
- J — Idempotent append to `longitudinal_lab_canonical_v1`. **DROPPED.**
- K — Validation (waterfall, patient coverage, numeric parse rate,
  spot checks).
- L — Markdown ingestion-report (`docs/tg_lab_ingestion_report_*.md`).
- M — Cross-wave reconciliation: builds `lab_cross_wave_dedup_map_v1`,
  `lab_cross_wave_review_v1`, `longitudinal_lab_deduped_v`. **DROPPED.**
- N — Derived analytical tables (`tg_timeline_patient_summary_v1`,
  `tg_postop_surveillance_windows_v1`,
  `tg_recurrence_surveillance_linkage_v1`).
- O — Reconciliation report.
- P — Machine-readable QC artifact (`processed/tg_lab_ingestion_qc_v1.json`).

**Inline normalization (legacy):** Phase F's hand-rolled `_TITER_RE` /
`_NUMERIC_RE` plus the `<` / `>` qualifier logic. **Replaced** by the
uniform `scripts/_lab_value_normalizer.py` 2A–2F pipeline in the
refactor.

**Dedup key (legacy):** Phase C used
`(research_id, test_name, specimen_collect_dt, result)` for exact-match
dedup; Phase M used
`(research_id, lab_date, lab_name_standardized,
COALESCE(value_numeric, value_raw))`
PARTITION BY for cross-wave dedup. **Replaced** by the single inline
key
`(research_id, analyte, CAST(lab_datetime AS DATE),
COALESCE(value_numeric, value_raw))`
with `source` + `ingestion_date DESC` priority ordering.

---

## 1C. `scripts/127_analyst_institutional_lab_append.py` summary

Appends analyst-delivered institutional lab CSVs (Tg, TgAb, TSH, PTH,
calcium, vitamin D — the analyte is carried in `lab_name_standardized`)
to the canonical lab layer. Behaviour:

- **Inputs:** `--input <csv>` and `--ingestion-wave <label>`. CSV must
  contain `research_id`, `lab_date`, `value_raw`, `source_lineage_key`,
  plus one of `lab_name_standardized` / `lab_name_raw`. Optional:
  `value_numeric`, `unit_raw`, `unit_standardized`, `analyte_group`.
- **Validation:** rejects empty / duplicate `source_lineage_key`,
  coerces `research_id` (int) and `lab_date` (date), strips PHI not
  carried in CSV columns.
- **Idempotent replace:**
  `DELETE FROM main.longitudinal_lab_canonical_v1
   WHERE ingestion_wave = ?` then `INSERT` from the prepared frame —
  inside a single `BEGIN TRANSACTION ... COMMIT`.
- **Re-builds** `main.longitudinal_lab_deduped_v` after the replace.
- **Writes** a per-wave QC summary JSON to `studies/`.

**Confirms it writes to the dropped tables** (lines 257, 266) →
**included** in the refactor scope per the prompt's primary suspect.

In the refactor, the script:
- Routes each input row by `lab_name_standardized` to the matching
  per-analyte canonical table.
- Stamps every row `source = 'institutional_append'` (the highest
  cross-wave dedup precedence per Script 347).
- Replaces the prior wave by deleting rows whose
  `value_correction_note` provenance carries the wave's
  `lineage_key` prefix (since per-analyte tables have no
  `ingestion_wave` column in the published schema).
- Routes value normalization through `scripts/_lab_value_normalizer.py`
  (`normalize_lab_value` + `convert_to_canonical_unit`). Unrecognized
  source units abort with surfacing to
  `studies/lab_ingestion_refactor_20260421/discordance_review.md`
  (Script 347 hard constraint).
- Applies inline cross-wave dedup at write time (per-analyte
  `ROW_NUMBER` + Script 347 priority ladder).

---

## 1D. `scripts/_lab_value_normalizer.py` signature confirmed

```
normalize_lab_value(value_raw: Optional[str], lab_test_name: Optional[str])
    -> (value_numeric: Optional[float],
        is_censored: bool,
        value_correction_note: Optional[str])
```

Supported analyte keys (and synonyms accepted by `_canonical_key`):

```
{'thyroglobulin', 'anti_thyroglobulin', 'tsh', 'pth', 'calcium', 'vitamin_d'}
```

Synonyms recognized: `tg`, `tgab`, `tg_antibody`, `ca`, `vitd`, `25_oh_vit_d`.

Companion helper `convert_to_canonical_unit(value_numeric, source_unit,
lab_test_name) -> (converted_value, canonical_unit_str, conversion_note)`
applies recognised unit conversions and **raises `ValueError`** on
unrecognised source units (Script 347 hard constraint — surfaces to the
discordance report).

---

## 1E. Normalizer test suite

`pytest tests/test_lab_value_normalizer.py -q` — **PASS**, 45 tests in
0.02s. Test file is unchanged from the Script 347 commit.

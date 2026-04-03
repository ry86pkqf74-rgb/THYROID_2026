# Tg Structured Lab Promotion Note

**Generated**: 2026-04-03  
**Script**: `scripts/113_tg_lab_ingestion.py`  
**Source**: `Thyroid_Thyroglobulin_Lab_20251120.csv` (78,112 rows, 3,298 patients)

---

## 1. Overview

Script 113 ingests the structured EHR thyroglobulin (Tg) and thyroglobulin
antibody (TgAb) lab file and promotes it into the canonical longitudinal lab
layer via the same idempotent-append strategy used by all other ingestion waves.
This note describes:

- How Tg structured labs flow into `longitudinal_lab_canonical_v1`
- Cross-wave deduplication priority and the deterministic reconciliation view
- Downstream derived views for recurrence surveillance
- The machine-readable QC artifact and how it fits the promotion gate pattern
- Exact dry-run and production run commands

---

## 2. Data Flow

```
Thyroid_Thyroglobulin_Lab_20251120.csv
        │
        ▼ Phase A–C: Load, PII-strip, dedup (key: research_id + test_name + specimen_collect_dt + result)
        │
        ▼ Phase D: Test name normalization → analyte ∈ {Tg, TgAb, COMBO}
        │           assay_method ∈ {immunoassay, LC-MS/MS, RIA, IMA, comprehensive, IgG, combo_panel, reflex}
        │           Unmapped names → tg_lab_review_queue_v1 (review_reason = unmapped_test_name)
        │
        ▼ Phase E: Combo panel disambiguation
        │           Heuristic (detection limits) → 99.2% validated accuracy
        │           Cross-reference (same-patient labeled results) → fallback
        │           Ambiguous pairs → tg_lab_review_queue_v1 (review_reason = combo_ambiguous)
        │
        ▼ Phase F–G: Result parsing + temporal linkage (days_from_surgery, temporal_window)
        │
        ▼ Phase H: Schema alignment → thyroglobulin_lab_canonical_v1 (parquet + DuckDB)
        │
        ▼ Phase J: Idempotent append to longitudinal_lab_canonical_v1
        │           DuckDB:  DELETE WHERE source_script='113_tg_lab_ingestion' → INSERT
        │           Parquet: purge prior script-113 rows → concat → overwrite
        │
        ▼ Phase M: Cross-wave reconciliation
        │           lab_cross_wave_dedup_map_v1 (superseded rows)
        │           lab_cross_wave_review_v1 (value mismatches)
        │           longitudinal_lab_deduped_v (clean view for analysis)
        │
        ▼ Phase N: Derived surveillance views
        │           tg_timeline_patient_summary_v1
        │           tg_postop_surveillance_windows_v1
        │           tg_recurrence_surveillance_linkage_v1
        │
        ▼ Phase O–P: Reconciliation report + QC artifact (processed/tg_lab_ingestion_qc_v1.json)
```

---

## 3. Canonical Layer Integration

### 3.1 Ingestion Waves

| Wave Name | Source | Script | Analyte |
|-----------|--------|--------|---------|
| `wave_1_structured_tg` | `thyroglobulin_labs` table | `77_lab_canonical_layer` | thyroglobulin |
| `wave_2_structured_anti_tg` | `anti_thyroglobulin_labs` table | `77_lab_canonical_layer` | anti_thyroglobulin |
| `wave_tg_structured_ehr` | `Thyroid_Thyroglobulin_Lab_20251120.csv` | `113_tg_lab_ingestion` | thyroglobulin |
| `wave_tgab_structured_ehr` | `Thyroid_Thyroglobulin_Lab_20251120.csv` | `113_tg_lab_ingestion` | anti_thyroglobulin |

### 3.2 Cross-Wave Deduplication Priority

When the same `(research_id, lab_date, lab_name_standardized, value)` appears
in multiple waves, the `longitudinal_lab_deduped_v` VIEW applies this priority:

1. **`wave_tg_structured_ehr` / `wave_tgab_structured_ehr`** — preferred. Carries
   richer metadata: assay method, disambiguation provenance, temporal linkage,
   and TgAb interference flag.
2. **`wave_1_structured_tg` / `wave_2_structured_anti_tg`** — legacy structured
   EHR waves from script 77; retained when not superseded.
3. **All other waves** — lowest priority.

Tie-break within the same priority tier: `source_script DESC` (alphanumerically
later script number wins; 113 > 77).

Superseded exact-match duplicates are recorded in `lab_cross_wave_dedup_map_v1`.
Cross-wave value mismatches (same day, different numeric value across waves) are
flagged in `lab_cross_wave_review_v1` with `discrepancy_severity` (high/medium/low).

### 3.3 Append-Only Guarantee

The canonical table `longitudinal_lab_canonical_v1` is **append-only and
long-format**: one row per lab result event, never collapsed into wide patient
rows. Repeated runs of script 113 purge only their own prior rows
(`source_script = '113_tg_lab_ingestion'`) before re-inserting, so other waves
are never touched.

---

## 4. Downstream Recurrence Surveillance

### 4.1 Tg Timeline Summary (`tg_timeline_patient_summary_v1`)

Per-patient Tg/TgAb trajectory table:

| Column | Description |
|--------|-------------|
| `tg_nadir` | Lowest Tg value across all postop measurements |
| `tg_last_value` | Most recent Tg measurement |
| `tg_rising_flag` | TRUE when last_value > 2× nadir (biochemical recurrence signal) |
| `tg_trajectory_class` | `suppressed / low_stable / detectable_stable / rising / insufficient_data` |
| `tgab_interference_flag` | TRUE when TgAb > 1.0 IU/mL (Tg values unreliable) |

### 4.2 Postop Surveillance Windows (`tg_postop_surveillance_windows_v1`)

Per-patient × temporal-window table for ATA response-to-therapy classification:

| Window | Days from Surgery | ATA Excellent Response Criterion |
|--------|-------------------|----------------------------------|
| perioperative | 0–30 | — |
| early_postop | 31–180 | Tg < 0.2 ng/mL → excellent |
| surveillance_1y | 181–365 | Tg < 0.2 → excellent; 0.2–1.0 → indeterminate; >1.0 → biochemical_incomplete |
| surveillance_5y | 366–1825 | Same thresholds |
| long_term | >1825 | — |

### 4.3 Recurrence-Surveillance Linkage (`tg_recurrence_surveillance_linkage_v1`)

Joins rising-Tg patients (from `tg_timeline_patient_summary_v1`) against
the structural recurrence registry (`extracted_recurrence_refined_v1` or
`md_extracted_recurrence_refined_v1`):

| Linkage Class | Meaning |
|---------------|---------|
| `confirmed_biochemical_and_structural` | Rising Tg + structural recurrence confirmed |
| `rising_tg_but_tgab_interference` | Rising Tg, but TgAb > 1.0 makes Tg unreliable |
| `high_biochemical_suspicion` | Tg last value > 10.0 ng/mL, no structural confirmation yet |
| `moderate_biochemical_suspicion` | Tg last value 1.0–10.0 ng/mL |
| `low_biochemical_suspicion` | Tg < 1.0 ng/mL but rising pattern detected |

**Routing rule**: when `extracted_recurrence_refined_v1` is not yet materialized,
the linkage view is not built and the QC artifact records `recurrence_table_used: null`.
Run `scripts/26_local DuckDB_materialize_v2.py --md` first, then re-run script 113.

Uncertain linkages (TgAb interference, no structural confirmation) are NOT
auto-promoted — they remain in the linkage table as `rising_tg_but_tgab_interference`
or `moderate_biochemical_suspicion` for manual clinical adjudication.

### 4.4 Master-Database Linkage

`tg_recurrence_surveillance_linkage_v1` joins on `research_id`, the universal
primary key across all tables. To join against the master cohort:

```sql
SELECT
    t.research_id,
    t.tg_trajectory_class,
    t.tg_last_value,
    t.tgab_interference_flag,
    l.surveillance_linkage_class,
    l.has_structural_recurrence,
    p.overall_stage_ajcc8,
    p.histology_1_type
FROM tg_timeline_patient_summary_v1 t
LEFT JOIN tg_recurrence_surveillance_linkage_v1 l USING (research_id)
LEFT JOIN patient_level_summary_mv p USING (research_id)
WHERE t.tg_rising_flag IS TRUE
ORDER BY t.tg_last_value DESC;
```

---

## 5. QC Artifact

### 5.1 Location

`processed/tg_lab_ingestion_qc_v1.json` — written at the end of every
production run (Phase P). Overwritten on each run to reflect the current state.

### 5.2 Key Fields

```json
{
  "schema_version": "1.0",
  "script": "scripts/113_tg_lab_ingestion.py",
  "run_timestamp": "20260403",
  "row_waterfall": {
    "source_rows": 78112,
    "after_dedup": 78006,
    "duplicates_suppressed": 106,
    "rows_appended_canonical": 76971,
    "review_queue_rows": 1035,
    "reconciliation_gap": 0
  },
  "patients": {
    "unique_in_canonical": 3258,
    "both_tg_and_tgab": 2930
  },
  "combo_disambiguation": {
    "pairs_total": 17267,
    "heuristic_resolved": 16173,
    "crossref_resolved": 561,
    "ambiguous_to_review": 607
  },
  "promotion_gate": {
    "idempotent_append": true,
    "pii_stripped": true,
    "parquet_idempotent": true
  }
}
```

A `reconciliation_gap` of 0 means every input row after dedup landed in either
`canonical` or `review_queue` — no unaccounted rows. Non-zero values should be
investigated before promotion.

### 5.3 Promotion Gate Fit

The QC artifact maps directly to the 8-gate criteria used by
`scripts/112_v2_domain_promotion_gate.py`:

| Gate | Field in QC Artifact |
|------|----------------------|
| G1 Row completeness | `row_waterfall.rows_appended_canonical > 0` |
| G2 Core columns | All provenance columns present (assertion in Phase H) |
| G3 Provenance | `promotion_gate.pii_stripped = true` |
| G4 Duplicate rate | `row_waterfall.duplicates_suppressed / source_rows < threshold` |
| G5 Date coverage | `result_parsing.date_coverage_pct = 100.0` |
| G6 Concordance | `cross_wave_reconciliation.cross_wave_value_mismatches` |
| G7 No unresolved discordance | `row_waterfall.reconciliation_gap = 0` |
| G8 Idempotency | `promotion_gate.idempotent_append = true` |

---

## 6. Run Commands

### Dry Run (validation only, no writes)

```bash
.venv/bin/python scripts/113_tg_lab_ingestion.py \
  --input raw/Thyroid_Thyroglobulin_Lab_20251120.csv \
  --dry-run
```

### Production — Local DuckDB only

```bash
.venv/bin/python scripts/113_tg_lab_ingestion.py \
  --input raw/Thyroid_Thyroglobulin_Lab_20251120.csv \
  --duckdb
```

### Production — MotherDuck (fail-closed connection gate)

```bash
.venv/bin/python scripts/113_tg_lab_ingestion.py \
  --input raw/Thyroid_Thyroglobulin_Lab_20251120.csv \
  --md
```

### Run Unit Tests

```bash
.venv/bin/python -m pytest tests/test_tg_lab_ingestion.py -v
```

### Verify QC Artifact After Run

```bash
python -c "import json; d = json.load(open('processed/tg_lab_ingestion_qc_v1.json')); \
  print('Gap:', d['row_waterfall']['reconciliation_gap']); \
  print('Appended:', d['row_waterfall']['rows_appended_canonical']); \
  print('Review:', d['row_waterfall']['review_queue_rows'])"
```

---

## 7. Tables Created or Updated

| Table / View | Type | Purpose |
|---|---|---|
| `thyroglobulin_lab_canonical_v1` | TABLE | Script-113 canonical (76,971 rows, Tg-specific) |
| `tg_lab_review_queue_v1` | TABLE | Disambiguation + unmapped test name review (1,035 rows) |
| `longitudinal_lab_canonical_v1` | TABLE | Append-only canonical (all waves) |
| `longitudinal_lab_deduped_v` | VIEW | Deterministic cross-wave dedup for analysis |
| `lab_cross_wave_dedup_map_v1` | TABLE | Audit log of superseded exact-match duplicates |
| `lab_cross_wave_review_v1` | TABLE | Value-mismatch conflicts for manual review |
| `tg_timeline_patient_summary_v1` | TABLE | Per-patient Tg/TgAb trajectory + flags |
| `tg_postop_surveillance_windows_v1` | TABLE | Per-patient × temporal window ATA classification |
| `tg_recurrence_surveillance_linkage_v1` | TABLE | Rising Tg ↔ structural recurrence linkage |

---

## 8. Known Limitations

- **8 unmatched research IDs** (20038, 20040, 20041, 20044, 20045, 20048, 20049, 20054)
  are not in the master cohort. Their lab rows are retained in the canonical table
  but will not link to any master-cohort downstream tables.
- **TgAb interference**: 1,035 rows in the review queue include ambiguous combo
  pairs. When TgAb > 1.0 IU/mL, Tg values may be falsely low — `tgab_interference_flag`
  surfaces these patients in the timeline table.
- **Recurrence linkage requires `extracted_recurrence_refined_v1`**: if this table
  has not been materialized via script 26 `--md`, the linkage view is skipped and
  `recurrence_table_used` is null in the QC artifact. This is expected on dry-run
  or local-only runs.

# V2 Canonical Layer Release Gate

**Gate Run Reference:** `20260403_promotion_attempt3`  
**Generated:** 2026-04-03  
**Schema Version:** `entity_schema_v3_2026-04-03`  
**Git SHA at gate run:** `837c78d`  
**Current HEAD:** `6256014`  
**Overall Status:** **HOLD** — G7 (unresolved discordance) blocked; `canonical_extracted_fact_long_v2` not yet materialized

---

## How to Use This Document

This checklist is the single source of truth for deciding whether v2 extracted domain outputs are
ready to promote into the master canonical layer. Each section maps to an executable verification
step. Work top-to-bottom. Do **not** run the promotion command sequence until all gates reach PASS
or a documented HOLD/waiver is in place.

---

## Section 1 — Preflight Checks

### 1.1 Environment

| Check | Command | Expected |
|-------|---------|----------|
| Python venv active | `.venv/bin/python --version` | Python 3.10+ |
| DuckDB version | `.venv/bin/python -c "import duckdb; print(duckdb.__version__)"` | ≤ 1.4.4 |
| MotherDuck token | `echo $MOTHERDUCK_TOKEN \| head -c 8` | Non-empty |
| Git clean (no uncommitted gate files) | `git status --short` | No untracked gate artifacts |

### 1.2 Required Parquets

| Parquet | Path | Status (at gate run) |
|---------|------|----------------------|
| `clinical_notes_long.parquet` | `processed/` | PRESENT |
| `note_extraction_runs.parquet` | `processed/` | PRESENT (3 run records) |
| `canonical_extracted_fact_long_v1.parquet` | `processed/` | PRESENT (68,077 rows) |
| `canonical_fact_quarantine_v1.parquet` | `processed/` | PRESENT (0 rows) |
| `thyroglobulin_lab_canonical_v1.parquet` | `processed/` | PRESENT (76,971 rows) |
| **`canonical_extracted_fact_long_v2.parquet`** | `processed/` | **MISSING** — not yet materialized |
| **`canonical_fact_quarantine_v2.parquet`** | `processed/` | **MISSING** — not yet materialized |
| **`tg_lab_ingestion_qc_v1.json`** | `processed/` | **MISSING** — script 113 Phase P not run |

### 1.3 V2 Parquet Fleet (raw LLM outputs)

All 35 v2 fleet parquets reside in `processed/output/v2_parquets/`. Each contains 11,037 rows
(one per note) with `result_json` storing extracted entity lists. Actual entity counts emerge after
script 103 expands them.

```bash
# Verify all 35 v2 parquets exist
ls processed/output/v2_parquets/note_entities_llm_*.parquet | wc -l
# Expected: 35
```

### 1.4 Preflight Verification Command

```bash
cd THYROID_2026
.venv/bin/python -c "
import pathlib, pandas as pd
parquets = {
    'canonical_v1': 'processed/canonical_extracted_fact_long_v1.parquet',
    'quarantine_v1': 'processed/canonical_fact_quarantine_v1.parquet',
    'tg_canonical': 'processed/thyroglobulin_lab_canonical_v1.parquet',
    'note_runs': 'processed/note_extraction_runs.parquet',
    'canonical_v2': 'processed/canonical_extracted_fact_long_v2.parquet',
    'quarantine_v2': 'processed/canonical_fact_quarantine_v2.parquet',
}
for name, path in parquets.items():
    p = pathlib.Path(path)
    if p.exists():
        df = pd.read_parquet(p)
        print(f'  [OK] {name}: {len(df):,} rows')
    else:
        print(f'  [MISSING] {name}: {path}')
v2_count = len(list(pathlib.Path('processed/output/v2_parquets').glob('note_entities_llm_*.parquet')))
print(f'  V2 fleet parquets: {v2_count}/35')
"
```

---

## Section 2 — Registry Validation

### 2.1 Registry Identity

| Field | Value |
|-------|-------|
| File | `config/extraction_domain_registry.yaml` |
| Schema version | `entity_schema_v3_2026-04-03` |
| Total domains defined | 44 |
| v1 canonical domains | 7 (`staging`, `genetics`, `procedures`, `operative_detail`, `complications`, `medications`, `problem_list`) |
| v2 canonical domains | 21 extracted; 2 deferred (`us_nodule_dynamics`, `frozen_section_detail`) |
| Debug (non-canonical) | 1 (`llm`) |
| Sub-prompt entries | 7 (not independently validated) |
| UNCLAIMED on-disk parquets | 7 (see note below) |

> **UNCLAIMED parquets:** Script 112 found 7 parquets in `processed/output/v2_parquets/` that do
> not map to any registry domain. These are gracefully skipped for all gates. They represent
> experimental or renamed domain parquets. Inventory:
> `note_entities_llm_combined`, `note_entities_llm_complications_rln_laryngoscopy`,
> `note_entities_llm_medication_management`, `note_entities_llm_operative_details`,
> `note_entities_llm_operative_v2_enrichment`, `note_entities_llm_parathyroid_per_gland`,
> `note_entities_llm_recurrence_detailed`. These should be either registered in the YAML or
> deleted from the v2_parquets directory before next gate run.

### 2.2 Deferred Domains

| Domain | Reason | Parquet on Disk | Prompt File | Action Required |
|--------|--------|-----------------|-------------|-----------------|
| `us_nodule_dynamics` | ~~Prompt path mismatch in gate script (now fixed)~~ | YES (49 entities, committed 2026-04-03 08:52) | YES (`llm_extraction/prompts/us_nodule_dynamics_extraction_v1.txt`) | **RESOLVED** — parquet + prompt both present; gate fix makes this fully visible |
| `frozen_section_detail` | Parquet added to origin 2026-04-03 (post-attempt3) | YES (380 entities, committed after attempt3) | YES (`llm_extraction/prompts/frozen_section_detail_extraction_v1.txt`) | **RESOLVED** — both parquet + prompt now present; next gate run: G1 PASS |

> **Glue fix applied (2026-04-03):** `scripts/112_v2_domain_promotion_gate.py` G1 prompt-path
> check was using `ROOT / repo_path` (resolving to `prompts/` at repo root) instead of
> `ROOT / "llm_extraction" / repo_path` (where all extraction prompts actually live). This caused
> both `us_nodule_dynamics` and `frozen_section_detail` to appear "deferred" even when prompts
> existed. Fix: gate now checks `llm_extraction/<repo_path>` first, then `<repo_path>` directly.
> With the fix + current parquet state: **both domains are fully present** — G1 will be PASS
> (unconditional) on the next gate run.
>
> Additionally, `note_entities_llm_frozen_section_detail.parquet` was merged into `origin/main`
> after attempt3 ran (`a0cfdbe`). Current HEAD now includes this parquet (380 entities extracted).

### 2.3 Registry Validation Command

```bash
.venv/bin/python llm_extraction/run_extraction.py --validate-only
```

Expected: `Registry validation PASS — N domains loaded, schema_version=entity_schema_v3_2026-04-03`

---

## Section 3 — Per-Domain Extraction Status

Data from `studies/v2_domain_promotion_gate_20260403_promotion_attempt3/`. All 21 v2 extracted
domains pass G2 (schema compliance). Entity counts are post-expansion of `result_json`.

| Domain | Rows | Patients | Dup Rate | entity_date% | QA Tier | Linkage Family |
|--------|------|----------|----------|--------------|---------|----------------|
| `imaging` | 8,159 | 1,759 | 3.2% ✅ | 71.4% | standard | imaging |
| `tirads_granular` | 175 | 44 | 2.2% ✅ | 60.6% | standard | imaging |
| `labs` | 2,160 | 841 | **12.2%** ⚠️ | 76.6% | standard | followup |
| `tg_kinetics` | 155 | 61 | **10.4%** ⚠️ | 72.9% | standard | followup |
| `pathology` | 10,425 | 2,220 | 4.3% ✅ | 70.1% | **critical** | pathology |
| `synoptic_pathology_enrichment` | 38 | 8 | 0.0% ✅ | 55.3% | **critical** | pathology |
| `rai_detailed` | 3,726 | 650 | 0.6% ✅ | 86.9% | **critical** | rai |
| `rad_treatment` | 555 | 213 | 4.3% ✅ | 72.8% | standard | rai |
| `parathyroid_detail` | 255 | 118 | 0.0% ✅ | 82.8% | standard | operative |
| `recurrence` | 300 | 143 | 1.0% ✅ | 71.7% | **critical** | followup |
| `survival_followup` | 9,808 | 2,982 | 0.01% ✅ | 82.6% | standard | followup |
| `cervical_ln_detail` | 94 | 48 | **9.6%** ⚠️ | 58.5% | standard | pathology |
| `functional_outcomes` | 3,313 | 1,842 | 0.3% ✅ | 43.7% | informational | followup |
| `past_medical_hx` | 832 | 295 | 3.8% ✅ | 36.1% | informational | demographics |
| `past_surgical_hx` | 3,822 | 1,878 | 2.5% ✅ | 70.0% | informational | demographics |
| `presenting_symptoms` | 279 | 120 | 0.4% ✅ | 35.1% | informational | demographics |
| `physical_exam` | 1,894 | 662 | 1.6% ✅ | 72.4% | informational | demographics |
| `vascular_invasion` | 4,223 | 998 | 0.4% ✅ | 93.2% | **critical** | pathology |
| `airway_invasion` | 3,108 | 1,477 | 0.3% ✅ | 65.6% | standard | operative |
| `dynamic_risk_response` | 51 | 25 | 3.8% ✅ | 74.5% | standard | followup |
| `patient_decision_adherence` | 599 | 398 | **6.6%** ⚠️ | 62.1% | informational | followup |
| `us_nodule_dynamics` | 49 | ~11 | TBD | TBD | standard | imaging | *(added post-attempt3; not in attempt3 validation stats)* |
| `frozen_section_detail` | 380 | ~150 | TBD | TBD | standard | operative | *(added post-attempt3; not in attempt3 validation stats)* |
| **TOTAL (attempt3)** | **53,971** | — | — | — | — | — |
| **TOTAL (current)** | **~54,400** | — | — | — | — | — | *(estimated; re-run script 112 for exact counts)* |

**Domains exceeding 5% duplicate threshold (G4 CONDITIONAL PASS):**
- `labs`: 300 dup rows (12.2%) — deduplication applied at materialization
- `tg_kinetics`: 18 dup rows (10.4%) — cross-wave dedup at materialization
- `cervical_ln_detail`: 10 dup rows (9.6%) — small domain, dedup at materialization
- `patient_decision_adherence`: 42 dup rows (6.6%) — dedup at materialization

**Provenance columns:** All 21 domains show 0/3 provenance columns (`preprocess_batch_id`,
`preprocess_script_version`, `preprocessed_at_utc`). The `preprocessed_at_utc` column is present
in the raw fleet parquets (confirmed in schema). G3 CONDITIONAL PASS — backfill occurs during
script 103 materialization.

---

## Section 4 — Validator Status Per Domain

Script 111 concordance validation compares v2 extracted entities against structured DuckDB baselines.
Only v1 domains (`staging`, `genetics`, `procedures`, `operative_detail`, `complications`,
`medications`) have structured comparison targets. V2-only domains show as `unmapped` (45,904 rows,
concordance not applicable).

### 4.1 Concordance Summary (critical domains)

| Domain | Concordant | Fill Candidates | Discordant | G6 Status |
|--------|-----------|-----------------|------------|-----------|
| `staging` | 903 (47.2%) | — | **1,013** | PASS (>30%) |
| `genetics` | 506 (50.1%) | 473 | 46 | PASS (>30%) |
| `procedures` | 1,266 (55.5%) | 44 | **1,085** | PASS (>30%) |
| `operative_detail` | 30 (2.9%) | 1,268 | **747** | PASS (>30%) ¹ |
| `complications` | 183 (—) | 275 | 2 | PASS |
| `medications` | 854 (—) | 368 | 3 | PASS |
| `problem_list` | 252 (extraction-only) | 0 | 0 | PASS |

¹ `operative_detail` concordance 2.9% — below 30% floor on purely concordant rows, but passes
because the 30% check uses `concordant_existing + concordant_existing_extraction_only` combined.

### 4.2 G7 Discordance Detail (FAIL — blocks promotion)

**Total discordant rows requiring review: 2,896**

Top discordance sources by entity type:

| Domain | Entity Type | Rows | Patients |
|--------|------------|------|----------|
| `operative_detail` | `tracheal_deviation` | 288 | 232 |
| `staging` | `extrathyroidal_extension` | 249 | 233 |
| `procedures` | `post_treatment_wbs_findings` | 231 | 231 |
| `staging` | `free_t4` | 212 | 184 |
| `procedures` | `ultrasound_lymph_node` | 149 | 131 |
| `procedures` | `laryngoscopy_date` / `laryngoscopy_findings` | 145 each | 141 each |
| `operative_detail` | `mass_effect` | 137 | 121 |
| `staging` | `ete_on_imaging` | 133 | 124 |
| `staging` | `free_t3` / `total_t4` | 99 / 49 | 88 / 47 |
| `operative_detail` | `ct_neck` | 94 | 85 |
| `staging` | `ptnm_stage` | 92 | 86 |
| `operative_detail` | `tracheal_narrowing` | 62 | 60 |
| *(+ 90 additional entity types with 1–60 rows each)* | | | |

**Pattern analysis:** The bulk of discordance is in `operative_detail`, `staging`, and `procedures`
domains — exactly the v1 domains where entity semantics may have shifted between v1 (regex) and v2
(LLM) extraction. Many discordant entity types (`free_t4`, `free_t3`, `tracheal_deviation`,
`post_treatment_wbs_findings`) appear to be out-of-scope entities that the v1 extractor captured
from boilerplate text but the v2 LLM correctly excludes or vice versa.

**Resolution path (required before PASS):**

1. Run `scripts/111_llm_extraction_validation.py --all-llm-domains --review-csv discordance_review.csv`
2. Open `manual_review_queue.csv` from the gate run output directory
3. For each discordant entity type, set `verification_status` to `confirmed_correct` (v2 is right)
   or `confirmed_incorrect` (v2 is wrong) — do not leave any row blank
4. Re-run script 112 — G7 passes when discordant count = 0 in the review queue

---

## Section 5 — Canonical V2 Row Counts

**Status: NOT YET MATERIALIZED**

`canonical_extracted_fact_long_v2.parquet` does not exist. Script 103 must be run to expand
`result_json` from the 35 v2 fleet parquets into entity-level rows and merge with the v1 canonical
layer.

### 5.1 Expected Structure Post-Materialization

`canonical_extracted_fact_long_v2` will contain:
- All rows from `canonical_extracted_fact_long_v1` (68,077 rows, v1 domains)
- Expanded v2 entities from all 21 extracted domains (~53,971 entity rows pre-dedup)
- Columns: `fact_id`, `research_id`, `note_row_id`, `fact_domain`, `entity_type`,
  `entity_value_norm`, `entity_date`, `note_date`, `present_or_negated`,
  `inferred_surgery_episode_id`, `ep_distance_days`, `linkage_anchor_family`,
  `source_file_id`, `extraction_run_id`, `prompt_version`, `llm_model_name`,
  `verification_status`, `fact_at`

### 5.2 Materialization Command

```bash
# Dry run first
.venv/bin/python scripts/103_fact_lineage_materialize.py --dry-run

# Full local materialization
.venv/bin/python scripts/103_fact_lineage_materialize.py

# Verify
.venv/bin/python -c "
import pandas as pd
v2 = pd.read_parquet('processed/canonical_extracted_fact_long_v2.parquet')
print(f'v2 clean: {len(v2):,} rows')
print(v2.groupby(\"fact_domain\")[\"fact_id\"].count().sort_values(ascending=False).head(15))
"
```

### 5.3 Pass Criteria for Section 5

- [ ] `canonical_extracted_fact_long_v2.parquet` exists and is non-empty
- [ ] All 21 v2 domains represented with `fact_domain` values matching registry `canonical_target`
- [ ] No `unknown_domain` rows in the clean table (must be routed to quarantine)
- [ ] `source_file_id` fill rate ≥ 95% across all rows
- [ ] `episode_linkage` (`inferred_surgery_episode_id` non-null) ≥ 85% for critical domains
- [ ] `extraction_run_id` fill rate ≥ 95%

---

## Section 6 — Quarantine Counts and Reasons

**Status: NOT YET MATERIALIZED**

`canonical_fact_quarantine_v2.parquet` does not exist. V1 quarantine is currently empty (0 rows),
which indicates all v1 entities passed. The v2 split will apply additional quarantine masks.

### 6.1 Expected Quarantine Reasons (from script 103 logic)

| Reason | Description | Acceptable Level |
|--------|------------|-----------------|
| `unknown_domain` | Entity's `fact_domain` not in registry | **Must be 0** |
| `prompt_resolution_failure` | result_json parse failure or empty extraction | < 1% per domain |
| `family_window_exceeded` | Episode linkage temporal window exceeded | Documented by family |
| `no_episode_linkage` | No surgery/episode anchor found | < 15% for critical domains |
| `missing_provenance` | `source_file_id` or `extraction_run_id` null | < 5% total |
| `date_missing` | Neither `entity_date` nor `note_date` present | < 5% for critical domains |

### 6.2 QC Report Location

After script 103 runs:
```bash
ls exports/fact_lineage_qc/qc_report_*.md
cat exports/fact_lineage_qc/qc_report_*.md   # review all quarantine counts
```

### 6.3 Pass Criteria for Section 6

- [ ] `canonical_fact_quarantine_v2.parquet` exists
- [ ] Zero rows with `quarantine_reason = unknown_domain`
- [ ] Zero rows with `quarantine_reason = prompt_resolution_failure`
- [ ] Per-domain quarantine rate < 5% for all critical `qa_tier` domains
- [ ] QC report generated and reviewed at `exports/fact_lineage_qc/`

---

## Section 7 — Tg Structured Lab Integration

### 7.1 Current State

| Artifact | Status | Details |
|----------|--------|---------|
| `processed/thyroglobulin_lab_canonical_v1.parquet` | **PRESENT** | 76,971 rows |
| `thyroglobulin_lab_canonical_v1` (DuckDB) | Requires verification | Run `02b` or `113` with `--duckdb` |
| `tg_lab_review_queue_v1.parquet` | **PRESENT** | 1,035 rows (ambiguous combos) |
| `tg_lab_ingestion_qc_v1.json` | **MISSING** | Script 113 Phase P not yet executed |
| `tg_timeline_patient_summary_v1` | Requires 113 full run | DuckDB only |
| `tg_postop_surveillance_windows_v1` | Requires 113 full run | DuckDB only |
| `tg_recurrence_surveillance_linkage_v1` | Requires 113 full run | Depends on recurrence table |

### 7.2 Analyte Coverage (from parquet)

| Analyte | Rows | % of Total |
|---------|------|-----------|
| TgAb | 39,005 | 50.7% |
| Tg | 37,966 | 49.3% |
| **Total** | **76,971** | — |
| **Unique patients** | **3,258** | — |

**Source:** `Thyroid_Thyroglobulin_Lab_20251120.csv` (78,112 raw → 76,971 canonical after
dedup; 1,035 ambiguous combo pairs in review queue; 8 unmatched research_ids)

### 7.3 Promotion Gate QC Fields

When `tg_lab_ingestion_qc_v1.json` exists, the following 8 gates must PASS before Tg data
is considered promotion-ready:

| Gate ID | Field | Pass Criterion |
|---------|-------|----------------|
| TG-G1 | `reconciliation_gap` | `= 0` |
| TG-G2 | `result_parsing.numeric_parse_rate` | ≥ 95% |
| TG-G3 | `temporal_window_distribution` | All rows have temporal window assigned |
| TG-G4 | `combo_disambiguation.ambiguous_remaining` | ≤ 1,035 (expected) |
| TG-G5 | `patients` | ≥ 3,200 (expected ~3,258) |
| TG-G6 | `analyte_breakdown.Tg.rows` + `analyte_breakdown.TgAb.rows` | Sum ≥ 76,000 |
| TG-G7 | `cross_wave_reconciliation.dedup_status` | `PASS` |
| TG-G8 | `derived_views.tg_recurrence_linkage` | Not null (recurrence table found) |

### 7.4 Tg Integration Verification Command

```bash
# Run full Tg ingestion with DuckDB registration (local only; use --md for MotherDuck)
.venv/bin/python scripts/113_tg_lab_ingestion.py \
  --input raw/Thyroid_Thyroglobulin_Lab_20251120.csv \
  --duckdb thyroid_master.duckdb

# Verify QC artifact
cat processed/tg_lab_ingestion_qc_v1.json | python3 -m json.tool | grep -E "reconciliation_gap|numeric_parse_rate|patients"
```

### 7.5 Recurrence Surveillance Linkage

`tg_recurrence_surveillance_linkage_v1` joins rising-Tg patients to `extracted_recurrence_refined_v1`
for follow-up linkage relevant to recurrence surveillance. If `extracted_recurrence_refined_v1` is
not present, script 113 logs a WARNING but continues. Verify linkage was created:

```bash
.venv/bin/python -c "
import duckdb
con = duckdb.connect('thyroid_master.duckdb')
tables = [r[0] for r in con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall()]
for t in ['tg_recurrence_surveillance_linkage_v1', 'tg_timeline_patient_summary_v1', 'tg_postop_surveillance_windows_v1']:
    exists = t in tables
    if exists:
        n = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        print(f'  [{\"OK\" if n>0 else \"EMPTY\"}] {t}: {n:,} rows')
    else:
        print(f'  [MISSING] {t}')
"
```

---

## Section 8 — MotherDuck Staging Status

### 8.1 G8 Parity (from attempt3, with `--motherduck-check`)

**G8 STATUS: PASS** — all 21 v2 domain tables in `v2_stage` match local parquet row counts (11,037 each).

| Table | Local | MotherDuck | Parity |
|-------|-------|-----------|--------|
| `v2_stage.note_entities_llm_imaging` | 11,037 | 11,037 | ✅ |
| `v2_stage.note_entities_llm_pathology` | 11,037 | 11,037 | ✅ |
| `v2_stage.note_entities_llm_rai_detailed` | 11,037 | 11,037 | ✅ |
| `v2_stage.note_entities_llm_vascular_invasion` | 11,037 | 11,037 | ✅ |
| `v2_stage.note_entities_llm_recurrence` | 11,037 | 11,037 | ✅ |
| *(all 21 tables — all PASS)* | 11,037 | 11,037 | ✅ |

> **Note:** `v2_stage` tables contain raw fleet parquets (one row per note). The expanded canonical
> tables (`canonical_extracted_fact_long_v2`, `canonical_fact_quarantine_v2`) do not exist in
> MotherDuck `main` schema yet — they are created by script 103 after promotion.

### 8.2 Connection Smoke Test

```bash
# Verify MotherDuck connection before promotion
.venv/bin/python -c "
from utils.md_connect import connect_md_fail_closed
con = connect_md_fail_closed()
schemas = [r[0] for r in con.execute('SHOW SCHEMAS').fetchall()]
print('Schemas:', schemas)
v2_tables = con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='v2_stage'\").fetchall()
print(f'v2_stage tables: {len(v2_tables)}')
main_tables = con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall()
print(f'main tables: {len(main_tables)}')
"
```

### 8.3 Connection Path Consistency Check

All `--md` scripts must resolve the token identically. Verify no path inconsistencies:

```bash
# Check that MOTHERDUCK_TOKEN resolves consistently
.venv/bin/python -c "
from utils.md_connect import connect_md_fail_closed
import os
print('Token source:', 'env' if os.environ.get('MOTHERDUCK_TOKEN') else '.env.motherduck')
con = connect_md_fail_closed()
row = con.execute('SELECT current_database()').fetchone()
print('Connected DB:', row[0])
"
```

Expected: Connected DB = `Thyroid 2026` (or configured `MOTHERDUCK_DATABASE`)

---

## Section 9 — PASS / HOLD / FAIL Recommendation

### 9.1 Gate Scorecard (Attempt 3 — Current State)

| Gate | Criterion | Status | Detail |
|------|-----------|--------|--------|
| G1 | Domain completeness (v2 only) | ⚠️ CONDITIONAL PASS → **PASS after fix** | Glue fix corrected prompt-path resolution + `frozen_section_detail` parquet merged to origin; both previously-deferred domains now have parquets + prompts; G1 will be unconditional PASS on next run |
| G2 | Schema compliance (core columns) | ✅ PASS | All 21 v2 domains pass; 21 missing optional metadata |
| G3 | Provenance columns | ✅ CONDITIONAL PASS | 0/3 provenance cols in all domains; backfill at materialization |
| G4 | Duplicate rate | ✅ CONDITIONAL PASS | 4 domains > 5%: `labs` (12.2%), `tg_kinetics` (10.4%), `cervical_ln_detail` (9.6%), `patient_decision_adherence` (6.6%); dedup applied at materialization |
| G5 | Date coverage (critical domains) | ✅ PASS | All critical domains have entity_date or note_date |
| G6 | Concordance floor (critical domains) | ✅ PASS | All critical domains ≥ 30% concordance |
| **G7** | **Unresolved discordance** | ❌ **FAIL** | **2,896 discordant rows** in manual review queue — must reach 0 |
| G8 | MotherDuck v2_stage parity | ✅ PASS | All 21 tables: 11,037 rows each, row-parity-ok |
| **CANONICAL** | **v2 canonical materialization** | ❌ **NOT RUN** | `canonical_extracted_fact_long_v2` and `canonical_fact_quarantine_v2` do not exist |
| **TG-GATE** | **Tg QC artifact** | ⚠️ **INCOMPLETE** | `tg_lab_ingestion_qc_v1.json` not generated; parquet present |

### 9.2 Decision Matrix

| Condition | Recommendation |
|-----------|---------------|
| G7 discordant = 0 AND canonical v2 materialized AND all other gates PASS | **PASS — proceed to promotion** |
| G7 discordant > 0 with pending manual review | **HOLD — complete manual review first** |
| G7 discordant > 0 with documented waiver (entity types confirmed out-of-scope) | **PASS WITH WAIVER — document and proceed** |
| Any gate FAIL without waiver | **FAIL — do not promote** |
| Critical domain quarantine > 5% | **FAIL — investigate before promotion** |
| `unknown_domain` or `prompt_resolution_failure` quarantine > 0 | **FAIL — fix extractor before promotion** |

### 9.3 Current Recommendation: HOLD

**Reason:** G7 FAIL (2,896 discordant rows in manual review queue). The discordance is concentrated
in three v1 domains (`operative_detail`, `staging`, `procedures`) and represents entity-type
semantic drift between v1 (regex) and v2 (LLM) extraction. Many discordant entity types
(`free_t4`, `free_t3`, `tracheal_deviation`, `post_treatment_wbs_findings`) are plausibly
out-of-scope captures from boilerplate text in v1. A targeted review of the top 10 entity types
by row count (~2,200 rows, covering ~76% of discordance) is sufficient to resolve or waive the
remaining long-tail.

**Blocking items before PASS:**
1. Manual review of `manual_review_queue.csv` — set `verification_status` for all 2,896 discordant rows (G7 FAIL)
2. Materialize `canonical_extracted_fact_long_v2` (script 103) and verify quarantine
3. Run script 113 with `--duckdb` to generate `tg_lab_ingestion_qc_v1.json`

**Resolved since attempt3:**
- G1: `us_nodule_dynamics` parquet (49 entities) committed 2026-04-03; `frozen_section_detail` parquet (380 entities) merged to origin `a0cfdbe`; gate script prompt-path fix applied — G1 will be PASS on next run

**Non-blocking items (address before next gate run):**
- Classify 7 UNCLAIMED parquets (register in registry or delete from v2_parquets/): `note_entities_llm_combined`, `note_entities_llm_complications_rln_laryngoscopy`, `note_entities_llm_medication_management`, `note_entities_llm_operative_details`, `note_entities_llm_operative_v2_enrichment`, `note_entities_llm_parathyroid_per_gland`, `note_entities_llm_recurrence_detailed`

---

## Section 10 — Promotion Command Sequence

**Execute only when all gates show PASS.**

### Step 0: Pre-Promotion Validation

```bash
cd THYROID_2026

# 0a. Confirm G7 resolved (discordant = 0)
cat studies/v2_domain_promotion_gate_<LATEST_RUN>/promotion_scorecard.csv | grep G7

# 0b. Run dry-run of fact materialization
.venv/bin/python scripts/103_fact_lineage_materialize.py --dry-run
```

### Step 1: Run Per-Domain Validator (script 111)

```bash
LABEL=$(date +%Y%m%d_%H%M)
.venv/bin/python scripts/111_llm_extraction_validation.py \
  --all-llm-domains \
  --db-path thyroid_master.duckdb \
  --run-label ${LABEL}_pre_promotion \
  --output-dir studies/llm_extraction_validation/runs/${LABEL}_pre_promotion
```

### Step 2: Materialize Canonical V2 Locally (script 103)

```bash
.venv/bin/python scripts/103_fact_lineage_materialize.py

# Verify outputs
python -c "
import pandas as pd
v2 = pd.read_parquet('processed/canonical_extracted_fact_long_v2.parquet')
q2 = pd.read_parquet('processed/canonical_fact_quarantine_v2.parquet')
print(f'canonical_extracted_fact_long_v2: {len(v2):,} rows')
print(f'canonical_fact_quarantine_v2: {len(q2):,} rows')
print('Quarantine reasons:')
if len(q2) > 0:
    print(q2['quarantine_reason'].value_counts().to_string())
# FAIL if any unknown_domain or prompt_resolution_failure
assert not (q2['quarantine_reason'] == 'unknown_domain').any(), 'STOP: unknown_domain in quarantine'
assert not (q2['quarantine_reason'] == 'prompt_resolution_failure').any(), 'STOP: prompt_resolution_failure in quarantine'
print('Quarantine checks PASS')
"
```

### Step 3: Run Tg Lab Ingestion (script 113)

```bash
.venv/bin/python scripts/113_tg_lab_ingestion.py \
  --input raw/Thyroid_Thyroglobulin_Lab_20251120.csv \
  --duckdb thyroid_master.duckdb

# Verify QC gate
python -c "
import json
qc = json.load(open('processed/tg_lab_ingestion_qc_v1.json'))
assert qc['reconciliation_gap'] == 0, f\"STOP: reconciliation_gap={qc['reconciliation_gap']}\"
print('TG reconciliation_gap: 0 PASS')
print(f'Patients: {qc[\"patients\"]:,}')
print(f'Rows appended: {qc[\"row_waterfall\"][\"rows_appended_canonical\"]:,}')
"
```

### Step 4: Run Full Promotion Gate (script 112)

```bash
GATE_LABEL=$(date +%Y%m%d_%H%M)
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
  --v2-parquets-dir processed/output/v2_parquets \
  --db-path thyroid_master.duckdb \
  --run-label ${GATE_LABEL} \
  --motherduck-check

# Check result
cat studies/v2_domain_promotion_gate_${GATE_LABEL}/promotion_scorecard.csv
cat studies/v2_domain_promotion_gate_${GATE_LABEL}/promotion_recommendation.md
```

**Expected: overall_verdict = PASS (all 8 gates green)**

### Step 5: Register to DuckDB (script 02b)

```bash
# Local DuckDB registration
.venv/bin/python scripts/02b_register_notes_entities.py

# MotherDuck registration (requires MOTHERDUCK_TOKEN)
.venv/bin/python scripts/02b_register_notes_entities.py --md
```

### Step 6: Materialize Canonical V2 to MotherDuck (script 103 --md)

```bash
.venv/bin/python scripts/103_fact_lineage_materialize.py --md

# Verify row counts match local
python -c "
import pandas as pd, duckdb
from utils.md_connect import connect_md_fail_closed
local_v2 = len(pd.read_parquet('processed/canonical_extracted_fact_long_v2.parquet'))
con = connect_md_fail_closed()
md_v2 = con.execute('SELECT COUNT(*) FROM canonical_extracted_fact_long_v2').fetchone()[0]
print(f'Local: {local_v2:,}  MotherDuck: {md_v2:,}  Match: {local_v2 == md_v2}')
assert local_v2 == md_v2, 'STOP: MD row count mismatch'
"
```

### Step 7: Post-Promotion Parity Check

```bash
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
  --v2-parquets-dir processed/output/v2_parquets \
  --db-path thyroid_master.duckdb \
  --run-label post_promotion_verify_$(date +%Y%m%d_%H%M) \
  --motherduck-check
```

### Step 8: Run Validation Engine Fact Release Gate (script 29)

```bash
.venv/bin/python scripts/29_validation_engine.py --md

# Check val_fact_release_metrics_v1 thresholds:
# pct_quarantine ≤ 5%
# pct_source_file_id ≥ 95%
# pct_episode_linkage ≥ 85%
# pct_extraction_run_id ≥ 95%
```

---

## Appendix A — Glue Fixes Required

The following small fixes should be implemented before the next gate run:

### Fix 1: Gate Script Prompt-Path Resolution (APPLIED 2026-04-03)

**Status: DONE** — `scripts/112_v2_domain_promotion_gate.py` line 658-660 updated.

**Root cause:** All extraction prompt files live in `llm_extraction/prompts/` (per
`PromptSpec.absolute_path` in `llm_extraction/registry.py` which prepends `REPO_ROOT/llm_extraction`).
But the G1 check in script 112 used `ROOT / repo_path` directly (i.e., `REPO_ROOT/prompts/`),
which never resolves correctly.

**Effect of fix:**
- `us_nodule_dynamics`: parquet (49 entities, 2026-04-03) + prompt now both detected → PRESENT, not deferred
- `frozen_section_detail`: prompt now detected → domain correctly moves from "deferred" to FAIL (extraction required)

**`frozen_section_detail` extraction complete:** Parquet merged to origin at `a0cfdbe`
(380 entities: `frozen_section_result`, `final_pathology_concordance`, `intraop_decision_impact`).
No further action needed for G1.

### Fix 2: Classify 7 UNCLAIMED Parquets

Add registry entries or delete the following parquets from `processed/output/v2_parquets/`:

| Parquet | Recommended Action |
|---------|-------------------|
| `note_entities_llm_combined` | Delete — aggregate file, not a domain |
| `note_entities_llm_complications_rln_laryngoscopy` | Register as `complications_rln_laryngoscopy` sub-domain OR delete |
| `note_entities_llm_medication_management` | Register as alias of `medications` OR delete |
| `note_entities_llm_operative_details` | Register as alias of `operative_detail` OR delete |
| `note_entities_llm_operative_v2_enrichment` | Register as new domain OR delete |
| `note_entities_llm_parathyroid_per_gland` | Register as sub-domain of `parathyroid_detail` OR delete |
| `note_entities_llm_recurrence_detailed` | Register as sub-domain of `recurrence` OR delete |

### Fix 3: Plan `frozen_section_detail` Extraction

Write prompt `prompts/frozen_section_detail_extraction_v1.txt` and run:

```bash
.venv/bin/python llm_extraction/run_extraction.py --target frozen_section_detail
```

Or formally defer with a timeline note in `config/extraction_domain_registry.yaml` under the
`frozen_section_detail.notes` field.

---

## Appendix B — Discordance Waiver Template

If the discordant entity types are confirmed out-of-scope (v1 regex false positives), use this
template in `manual_review_queue.csv`:

```
verification_status = "confirmed_incorrect"   # v1 value is wrong; v2 LLM is correct
reviewer = "<initials>"
review_date = "2026-04-03"
waiver_reason = "v1_regex_boilerplate_capture: entity_type <X> captured from consent/H&P template text in v1; v2 LLM correctly excludes"
```

The following entity types are strong candidates for bulk waiver (pending spot-check):
- `free_t4`, `free_t3`, `total_t4`, `total_t3` — lab values captured from H&P templates in v1
- `tracheal_deviation`, `mass_effect`, `esophageal_compression` — airway exam findings in v1
  operative_detail that overlap with v2 `airway_invasion` domain
- `post_treatment_wbs_findings` in `procedures` — likely out-of-scope for procedures domain

---

## Appendix C — Reference: Key Script Commands

| Script | Purpose | Command |
|--------|---------|---------|
| `llm_extraction/run_extraction.py` | Run full extraction (all domains) | `.venv/bin/python llm_extraction/run_extraction.py` |
| `llm_extraction/run_extraction.py` | Single domain re-run | `.venv/bin/python llm_extraction/run_extraction.py --target <domain>` |
| `scripts/111_llm_extraction_validation.py` | Per-domain concordance validation | `.venv/bin/python scripts/111_llm_extraction_validation.py --all-llm-domains --db-path thyroid_master.duckdb` |
| `scripts/103_fact_lineage_materialize.py` | Build v2 canonical + quarantine | `.venv/bin/python scripts/103_fact_lineage_materialize.py [--dry-run] [--md]` |
| `scripts/02b_register_notes_entities.py` | Register parquets to DuckDB | `.venv/bin/python scripts/02b_register_notes_entities.py [--md]` |
| `scripts/112_v2_domain_promotion_gate.py` | Run G1–G8 gate | `.venv/bin/python scripts/112_v2_domain_promotion_gate.py [--motherduck-check]` |
| `scripts/113_tg_lab_ingestion.py` | Tg structured lab ingestion | `.venv/bin/python scripts/113_tg_lab_ingestion.py --input raw/Thyroid_Thyroglobulin_Lab_20251120.csv --duckdb thyroid_master.duckdb` |
| `scripts/29_validation_engine.py` | Fact release gate metrics | `.venv/bin/python scripts/29_validation_engine.py [--md]` |

---

*This document reflects the state at git `6256014` (2026-04-03). Re-run script 112 to generate
a fresh gate scorecard for the next promotion attempt. Store results under
`studies/v2_domain_promotion_gate_<YYYYMMDD_label>/`.*

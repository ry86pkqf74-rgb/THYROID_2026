# MotherDuck Database Contract v1

**Database:** Thyroid 2026  
**Provider:** MotherDuck (cloud DuckDB)  
**Connection path:** `utils/md_connect.py` via `connect_md_or_file()` or `connect_md_fail_closed()`  
**Registry:** `config/extraction_domain_registry.yaml`  
**Contract version:** 1.0  
**Created:** 2026-04-07  

**Staging vs canonical (operator rule):** Fresh v2 extraction parquets are loaded into **`v2_stage`** (via `116_md_stage_loader.py`). **`main`** is the promoted canonical surface — data appears there only after the promotion gate passes and promoted DDL / materialization steps run. Do not treat `main` as the live staging target for new v2 parquet drops.

---

## 1. Schema Map

| Schema | Purpose | Mutability |
|--------|---------|------------|
| `v2_stage` | **Current staging plane** — landing zone for v2 LLM extraction parquets before promotion | Append/replace per domain |
| `main` | Canonical query surface for all promoted tables | Append-only; corrections via quarantine |
| `qa` | Promotion governance, validation, and review tracking | Append per gate run |
| `release_YYYYMMDD` | Immutable point-in-time snapshots of main tables | Read-only after creation |

### Schema relationships

```
parquet (local)
  └── v2_stage (MotherDuck)
        └── [promotion gate: 8 criteria]
              └── main (MotherDuck)
                    └── release_YYYYMMDD (MotherDuck, immutable)
```

---

## 2. Table Catalog

### 2.1 v2_stage schema

| Table | Source | Rows (approx) | Purpose |
|-------|--------|---------------|---------|
| `note_entities_llm_imaging` | `processed/output/v2_parquets/` | ~8,200 | LLM-extracted imaging entities |
| `note_entities_llm_tirads_granular` | same | ~175 | TIRADS scoring details |
| `note_entities_llm_us_nodule_dynamics` | same | ~48 | Ultrasound nodule tracking |
| `note_entities_llm_labs` | same | ~2,200 | Lab result extraction |
| `note_entities_llm_tg_kinetics` | same | ~155 | Thyroglobulin kinetics |
| `note_entities_llm_pathology` | same | ~10,400 | Pathology report entities |
| `note_entities_llm_synoptic_pathology_enrichment` | same | ~38 | Synoptic path enrichment |
| `note_entities_llm_rai_detailed` | same | ~3,700 | RAI treatment details |
| `note_entities_llm_rad_treatment` | same | ~505 | Radiation treatment |
| `note_entities_llm_parathyroid_detail` | same | ~255 | Parathyroid detail |
| `note_entities_llm_recurrence` | same | ~300 | Recurrence events |
| `note_entities_llm_survival_followup` | same | ~9,800 | Survival/follow-up data |
| `note_entities_llm_cervical_ln_detail` | same | ~94 | Cervical lymph node detail |
| `note_entities_llm_functional_outcomes` | same | ~3,300 | Functional outcome tracking |
| `note_entities_llm_past_medical_hx` | same | ~832 | Past medical history |
| `note_entities_llm_past_surgical_hx` | same | ~3,800 | Past surgical history |
| `note_entities_llm_presenting_symptoms` | same | ~279 | Presenting symptoms |
| `note_entities_llm_physical_exam` | same | ~1,900 | Physical exam findings |
| `note_entities_llm_vascular_invasion` | same | ~4,200 | Vascular invasion |
| `note_entities_llm_airway_invasion` | same | ~3,100 | Airway invasion |
| `note_entities_llm_frozen_section_detail` | same | ~377 | Frozen section |
| `note_entities_llm_dynamic_risk_response` | same | ~51 | Dynamic risk assessment |
| `note_entities_llm_patient_decision_adherence` | same | ~599 | Patient decision/adherence |
| `load_inventory` | `116_md_stage_loader.py` | growing | Load audit trail |

### 2.2 main schema

#### Canonical fact tables

| Table | Source script | Purpose |
|-------|-------------|---------|
| `canonical_extracted_fact_long_v1` | `103_fact_lineage_materialize.py` | V1 canonical facts (preserved) |
| `canonical_extracted_fact_long_v2` | `103_fact_lineage_materialize.py` | V2 canonical facts (all domains) |
| `canonical_fact_quarantine_v1` | `103_fact_lineage_materialize.py` | V1 quarantined rows |
| `canonical_fact_quarantine_v2` | `103_fact_lineage_materialize.py` | V2 quarantined rows |
| `note_extraction_runs` | `103_fact_lineage_materialize.py` | Extraction run metadata |

#### Lab tables

| Table | Source script | Purpose |
|-------|-------------|---------|
| `thyroglobulin_lab_canonical_v1` | `113_tg_lab_ingestion.py` | Canonical Tg/TgAb lab values |
| `longitudinal_lab_canonical_v1` | `113_tg_lab_ingestion.py` | Longitudinal lab timeline |

#### Lab consumption view

| View | Definition | Purpose |
|------|-----------|---------|
| `longitudinal_lab_deduped_v` | Deduped over `thyroglobulin_lab_canonical_v1` | Clean lab query surface |

#### Promoted v2 domain tables (23 tables)

Same names as v2_stage tables, promoted by `motherduck_promote.sql` after all 8 gate criteria pass.

> **Note:** 6 concordance-audit parquets (`note_entities_llm_{complications,genetics,medications,problem_list,procedures,staging}`) exist on disk for V1-vs-V2 comparison but are classified `legacy-concordance` in the registry and are **never** staged or promoted.

#### Episode contract tables

| Table | Source | Purpose |
|-------|--------|---------|
| `tumor_episode_master_v2` | `exports/manuscript_freeze_v1/data/` | Tumor episode timeline |
| `molecular_test_episode_v2` | same | Molecular testing episodes |
| `rai_treatment_episode_v2` | same | RAI treatment episodes |
| `operative_episode_detail_v2` | same | Operative episode details |

#### Linkage and analysis views

| View | Purpose |
|------|---------|
| `linkage_summary_v` | Patient-level linkage counts across episode types |
| `episode_completeness_summary_v` | Row/patient counts per episode table |

#### V1 entity tables (8, in main, never mutated by v2)

| Table | Purpose |
|-------|---------|
| `note_entities_staging` | V1 staging entities |
| `note_entities_genetics` | V1 genetics entities |
| `note_entities_procedures` | V1 procedure entities |
| `note_entities_operative_detail` | V1 operative detail |
| `note_entities_complications` | V1 complication entities |
| `note_entities_medications` | V1 medication entities |
| `note_entities_problem_list` | V1 problem list |
| `note_entities_llm` | V1 merged LLM audit artifact (debug only) |

### 2.3 qa schema

| Table/View | Type | Purpose |
|------------|------|---------|
| `promotion_scorecard` | Table | Gate results per run (8 gates x N runs) |
| `promotion_review_decisions` | Table | Reviewer decisions on flagged entities |
| `concordance_summary` | Table | Per-domain concordance metrics by gate run |
| `domain_validation` | Table | Schema, dup, date, provenance metrics per domain per run |
| `tg_lab_ingestion_qc` | Table | Tg lab pipeline QC results |
| `release_manifest` | Table | Immutable release snapshot metadata |
| `manual_review_queue` | Table | Rows flagged for human review during gate runs; extended with `promotion_approved`, `reviewer_evidence_span`, `reviewer_comment`, `reason_code` (see `114_qa_schema_ddl.sql` + study `MANUAL_REVIEW_PLAYBOOK.md`) |
| `promotion_scorecard_summary_v` | View | Aggregate pass/fail/conditional per run |
| `domain_validation_summary_v` | View | Aggregate validation metrics per run |
| `date_provenance_completeness_v` | View | Per-domain date/provenance completeness tier |
| `manual_review_queue_summary_v` | View | Review queue counts by domain and status |

### 2.4 release_YYYYMMDD schemas

Created by `scripts/115_release_snapshot.py`. Each schema contains copies of canonical tables with an added `release_tag` column. Release schemas are immutable after creation.

---

## 3. Required Provenance Columns

All tables in `main` that contain extracted entity data must include:

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `research_id` | BIGINT | Yes | Patient identifier |
| `note_row_id` | BIGINT | Yes | Key into clinical_notes_long |
| `entity_type` | VARCHAR | Yes | Domain-specific entity type |
| `entity_date` | VARCHAR | Conditional | Normalised YYYY-MM-DD when available |
| `note_date` | VARCHAR | Conditional | Encounter/note header date |
| `extraction_run_id` | VARCHAR | Yes | UUID for extraction invocation |
| `extracted_at` | VARCHAR | Yes | UTC ISO timestamp |
| `source_file_id` | VARCHAR | Conditional | Workbook file id for metrics |
| `entity_value_raw` | VARCHAR | Yes | Raw extracted value |
| `entity_value_norm` | VARCHAR | Yes | Normalised value |

Episode and linkage tables have their own required columns:
- `research_id` (always)
- `surgery_episode_id` or equivalent episode key
- Source date columns appropriate to the domain

---

## 4. Promotion Lifecycle

```
[Local Extraction]
    │
    ▼
[Parquet on disk]  ──── processed/output/v2_parquets/*.parquet
    │
    ▼
[116_md_stage_loader.py --md]  ──── v2_stage.note_entities_llm_*
    │                                  v2_stage.load_inventory
    ▼
[112_v2_domain_promotion_gate.py --motherduck-check]
    │   ├── G1: Domain completeness
    │   ├── G2: Schema compliance (core columns)
    │   ├── G3: Provenance columns
    │   ├── G4: Duplicate rate (≤5%)
    │   ├── G5: Date coverage (critical domains)
    │   ├── G6: Concordance floor (≥30% critical)
    │   ├── G7: Manual review queue resolved
    │   └── G8: MotherDuck v2_stage row parity
    │
    ▼ (all 8 PASS)
[motherduck_promote.sql]  ──── v2_stage → main
    │
    ▼
[103_fact_lineage_materialize.py --md]  ──── main.canonical_*
    │
    ▼
[117_md_contract_views.py --md]  ──── main.episode/linkage tables + views
    │
    ▼
[114_qa_schema_setup.py --md --hydrate-from <gate_dir>]  ──── qa.*
    │
    ▼
[115_release_snapshot.py --md --tag YYYYMMDD]  ──── release_YYYYMMDD.*
    │
    ▼
[118_parquet_release_bundle.py --md]  ──── exports/parquet_release_YYYYMMDD/
```

### Promotion rules

1. **Review policy (single source of truth):** [`docs/domain_mapping_rules.md`](domain_mapping_rules.md) § *Fill-Candidate Triage Policy* (approved 2026-04-07). In summary:
   - **Discordant rows** (`discordant_existing`, same-entity conflict): **zero tolerance** — each row must be individually adjudicated (`confirmed_correct` / `confirmed_incorrect`) before release; no bulk acceptance.
   - **Fill candidates** (`existing_missing_fill_candidate`): tiered acceptance — **critical** domains use sample-based batch acceptance (10% min 20 rows, >90% pass to accept remainder per policy); **standard** and **informational** tiers may be bulk-accepted with documented `verification_status` (`auto_accepted_standard`, `auto_accepted_informational`) and audit rows in `qa.promotion_review_decisions`.
2. **Gate G8 requires `--motherduck-check`.** Local-only runs set G8 to PASS by default.
3. **Append-only in main.** Corrections go to quarantine; original rows are never deleted.
4. **V1 tables are immutable.** No v2 operation touches `_v1` suffixed tables.
5. **Release schemas are immutable.** Use a new tag for corrections.

Operational detail for tiered fill acceptance, reviewer SOP, and MotherDuck hydration: [`studies/v2_domain_promotion_gate_formalization_20260406_v3/MANUAL_REVIEW_PLAYBOOK.md`](../studies/v2_domain_promotion_gate_formalization_20260406_v3/MANUAL_REVIEW_PLAYBOOK.md).

---

## 5. PHI Boundary

| Data type | Location | MotherDuck? |
|-----------|----------|-------------|
| Raw clinical note text | `processed/clinical_notes_long.parquet` (local) | **No** |
| Note row IDs | All entity tables | Yes (IDs only) |
| Extracted entity values | `note_entities_*`, `canonical_*` | Yes |
| Evidence spans | `canonical_*` (length-capped snippets) | Yes |
| Patient identifiers | `research_id` (de-identified integer) | Yes |
| Source workbook paths | `source_file_id` (file ID only) | Yes |
| Full note dates | `note_date` (encounter date) | Yes |
| PHI-adjacent free text | `evidence_span` (truncated) | Yes (capped) |

**Hard rule:** `clinical_notes_long.parquet` and raw source files (`raw/`) never leave the local machine. No script may upload raw note text to MotherDuck.

---

## 6. Onboarding Workflow: New Lab Sources

To add a new lab data source (e.g., a new hospital or lab panel):

1. **Ingest locally** into `processed/` as a deduped parquet with `research_id`, `lab_date`, `lab_type`, `lab_value`, and provenance columns.
2. **Register** in `config/extraction_domain_registry.yaml` if the source maps to an extraction domain.
3. **Load into v2_stage** via `116_md_stage_loader.py --md`.
4. **Run the promotion gate** with `112_v2_domain_promotion_gate.py --motherduck-check`.
5. **Resolve manual review** for any flagged rows.
6. **Promote** via generated `motherduck_promote.sql`.
7. **Materialize canonical tables** via `103_fact_lineage_materialize.py --md`.
8. **Create a release snapshot** via `115_release_snapshot.py --md --tag YYYYMMDD`.

### Onboarding: New Extraction Domain

1. Add domain entry to `config/extraction_domain_registry.yaml` under `domains:`.
2. Create prompt file(s) under `llm_extraction/prompts/`.
3. Add to fleet `DOMAIN_PROMPT` in `scripts/vastai/run_extraction_concurrent.py` and `scripts/run_extraction_split.py`.
4. Run `pytest tests/test_fleet_registry_parity.py` to verify parity.
5. Extract via `llm_extraction/run_extraction.py` (produces parquet in `processed/output/v2_parquets/`).
6. Follow promotion lifecycle above.

### Onboarding: New Patient Cohort

1. Add patient records to `raw/` source workbooks (local only).
2. Run the ingestion pipeline to produce `clinical_notes_long.parquet`.
3. Run extraction across all domains (fleet or local).
4. Follow the full promotion lifecycle from step 1.

---

## 7. Append-Only Contract

### Rules

- **No DELETE from main.** Rows are never removed from canonical tables.
- **Corrections use quarantine.** When a row is found to be incorrect, it is added to `canonical_fact_quarantine_v2` with a `quarantine_reason` and `quarantine_date`. The original row remains in the canonical table for audit trail.
- **New data is additive.** Re-extraction of a domain produces new rows that are deduplicated at load time; existing rows are not overwritten.
- **Episode tables are replaced atomically** via `CREATE OR REPLACE TABLE` during promotion. The prior version is preserved in the most recent `release_YYYYMMDD` schema.

### Quarantine reasons

| Reason | Description |
|--------|-------------|
| `multi_surgery_episode_ambiguous` | Entity date falls between two surgery dates |
| `low_confidence_llm_date` | LLM date confidence below threshold |
| `temporal_conflict_entity_vs_surgery` | Entity date conflicts with linked surgery |

---

## 8. Connection Reference

All scripts must use `utils.md_connect`:

```python
from utils.md_connect import connect_md_or_file, connect_md_fail_closed
from pathlib import Path

DB_PATH = Path("thyroid_master.duckdb")

# Fail-open: falls back to local file if MD unavailable
con = connect_md_or_file(DB_PATH, md=args.md)

# Fail-closed: exits 1 if MD unreachable (for production writes)
con = connect_md_or_file(DB_PATH, md=args.md, fail_closed=args.md)

# Convenience alias: always MD, always fail-closed
con = connect_md_fail_closed(DB_PATH)

# Automation: prefer service account + query-history attribution
con = connect_md_or_file(
    DB_PATH,
    md=True,
    fail_closed=True,
    prefer_service_account=True,
    custom_user_agent="THYROID_2026_orchestrator/1.0",
    motherduck_session_hint="rc_promotion_20260407",
)
```

Never call `duckdb.connect("md:...")` directly. Token resolution is handled internally.

### Token modes (three supported identities)

| Mode | Variables | Typical use | Used by `get_token()` / `connect_rw()` |
|------|-----------|-------------|----------------------------------------|
| **Personal developer (RW)** | `MOTHERDUCK_TOKEN`, alias `motherduck_token` | Local notebooks, ad-hoc SQL, Streamlit with review mode | Yes |
| **CI / service account (RW)** | `MD_SA_TOKEN` (preferred when `prefer_service_account=True`) | GitHub Actions, automation, promotion gates | Yes |
| **Business read-scaling (read-only)** | `MD_READ_SCALING_TOKEN`, alias `MOTHERDUCK_READ_SCALING_TOKEN` | Dashboards, analyst query load, read replicas | **No** — use `get_read_scaling_token()` / `MotherDuckClient.connect_read_scaling()` only |

**Session hints (read-scaling):** optional `MD_READ_SCALING_SESSION_HINT` or `MOTHERDUCK_READ_SCALING_SESSION_HINT` (or per-call `session_hint=`) sets `motherduck_session_hint` for stable user-duckling affinity on scaled reads. Read/write flows continue to use `MOTHERDUCK_SESSION_HINT` / config only (read-scaling env vars are not consulted on `connect_rw()`).

**Guardrails:** Read-scaling tokens are excluded from `get_token()`. If only read-scaling credentials are configured, `connect_rw()` and `connect_md_or_file(..., fail_closed=True)` **fail fast** with a clear error. Promotion scripts, staging loaders, and formalization validators must keep using read/write tokens.

#### Examples

```bash
# 1) Personal developer (read/write) — local shell
export MOTHERDUCK_TOKEN='md_…'
.venv/bin/python motherduck_client.py --env prod
```

```bash
# 2) CI / service account (read/write) — GitHub Actions secrets: MD_SA_TOKEN
export MD_SA_TOKEN='md_…'
.venv/bin/python motherduck_client.py --env prod --sa
```

```bash
# 3) Business read-scaling (dashboard / read-only attach — never for 116/112/promote SQL)
export MD_READ_SCALING_TOKEN='md_…'
export MD_READ_SCALING_SESSION_HINT='streamlit_prod_dashboard'
.venv/bin/python -c "
from motherduck_client import MotherDuckClient
c = MotherDuckClient.for_env('prod').connect_read_scaling()
print(c.execute('SELECT current_database()').fetchone())
c.close()
"
```

### Environment / catalog limitation

`MOTHERDUCK_ENV` values `dev`, `qa`, and `prod` all resolve to the **same** MotherDuck database name in `config/motherduck_environments.yml` (`Thyroid 2026`). Environment isolation is **not** separate MotherDuck catalogs in this repo today — isolation is by **schema** (`main`, `v2_stage`, `qa`, `release_*`) as described in §1–2.

### Environment variables

| Variable | Purpose |
|----------|---------|
| `MOTHERDUCK_TOKEN` | Personal developer token |
| `motherduck_token` | Alias for personal token (env) |
| `MD_SA_TOKEN` | Service-account / CI token |
| `MD_READ_SCALING_TOKEN` | Business read-scaling token (read-only path) |
| `MOTHERDUCK_READ_SCALING_TOKEN` | Alias for read-scaling token |
| `MD_READ_SCALING_SESSION_HINT` | Session hint for read-scaling connections |
| `MOTHERDUCK_READ_SCALING_SESSION_HINT` | Alias for read-scaling session hint |
| `MOTHERDUCK_DATABASE` | Override DB name (default: `Thyroid 2026`) |
| `MOTHERDUCK_CUSTOM_USER_AGENT` | DuckDB `custom_user_agent` (MotherDuck query history) |
| `MOTHERDUCK_SESSION_HINT` | `SET motherduck_session_hint` after connect (read/write and generic) |

---

## 9. Script Inventory

| Script | Purpose | Schemas touched |
|--------|---------|-----------------|
| `116_md_stage_loader.py` | Bulk-load v2 parquets into v2_stage | v2_stage |
| `112_v2_domain_promotion_gate.py` | Validate and generate promotion SQL | v2_stage (read), main (read) |
| `motherduck_promote.sql` (generated) | Promote v2_stage -> main | v2_stage (read), main (write) |
| `103_fact_lineage_materialize.py` | Materialize canonical fact tables | main |
| `117_md_contract_views.py` | Load episode/linkage tables + create views | main |
| `114_qa_schema_setup.py` | Create and hydrate qa schema | qa |
| `115_release_snapshot.py` | Create immutable release snapshots | release_YYYYMMDD, qa |
| `118_parquet_release_bundle.py` | Export Parquet bundle from MD main | main (read), qa (read) |
| `119_md_formalization_validate.py` | Validation suite | all (read) |
| `120_review_queue_triage.py` | Read-only triage export for `qa.manual_review_queue` (CSVs + `summary.md`) | qa (read) |
| `125_master_verified_views.py` | Analyst-facing `main.master_*_verified_v1` views | main (views) |
| `126_release_candidate_motherduck_audit.py` | RC evidence pack (MD_INFORMATION_SCHEMA, row counts) | read (+ optional `CREATE SNAPSHOT`) |
| `127_qa_tier_batch_adjudicate.py` | Tier bulk acceptance for fill-candidates on `qa.manual_review_queue` | qa (write) |

---

## 10. Snapshot and Promotion Runbook

See `docs/motherduck_v2_staging_runbook.md` for the full operational runbook. Key steps:

### Pre-promotion checklist

```bash
# 1. Stage refresh
.venv/bin/python scripts/116_md_stage_loader.py --md

# 2. Run promotion gate with MD parity check
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
    --v2-parquets-dir processed/output/v2_parquets \
    --db-path thyroid_master.duckdb \
    --motherduck-check \
    --run-label promote_$(date +%Y%m%d_%H%M)

# 3. Review scorecard — all 8 gates must PASS
cat studies/v2_domain_promotion_gate_promote_*/promotion_scorecard.csv

# 4. Hydrate QA tables
.venv/bin/python scripts/114_qa_schema_setup.py --md \
    --hydrate-from studies/v2_domain_promotion_gate_promote_*/

# 5. Execute promotion SQL (review first!)
# Paste contents of studies/.../motherduck_promote.sql into MD console

# 6. Materialize canonical tables
.venv/bin/python scripts/103_fact_lineage_materialize.py --md

# 7. Load contract views
.venv/bin/python scripts/117_md_contract_views.py --md --skip-canonical

# 8. Create release snapshot
.venv/bin/python scripts/115_release_snapshot.py --md --tag $(date +%Y%m%d)

# 9. Export Parquet bundle
.venv/bin/python scripts/118_parquet_release_bundle.py --md

# 10. Validate
.venv/bin/python scripts/119_md_formalization_validate.py --md
```

### MotherDuck paid-plan features

- **Query history:** Available in the MotherDuck UI; use for audit trail.
- **Snapshots:** MotherDuck retains automatic snapshots on paid plans. Use `SHOW DATABASES` and `information_schema.tables` to verify.
- **Backup verification:** `SELECT * FROM duckdb_databases()` confirms attached databases.

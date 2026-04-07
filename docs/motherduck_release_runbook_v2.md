# MotherDuck Release Runbook v2

**Created**: 2026-04-07  
**Supersedes**: `docs/motherduck_v2_staging_runbook.md`  
**Scope**: End-to-end formalization and release of the Thyroid 2026 canonical dataset to the live MotherDuck "Thyroid 2026" database — from preflight through validated release snapshot.

---

## Contents

1. [Architecture recap](#1-architecture-recap)
2. [Prerequisites](#2-prerequisites)
3. [Quick start — orchestrator](#3-quick-start--orchestrator)
4. [Manual step-by-step](#4-manual-step-by-step)
5. [Presentation view contracts](#5-presentation-view-contracts)
6. [Release snapshot and parquet bundle](#6-release-snapshot-and-parquet-bundle)
7. [Validation checklist](#7-validation-checklist)
8. [Rollback procedures](#8-rollback-procedures)
9. [Constraints and invariants](#9-constraints-and-invariants)
10. [Script quick reference](#10-script-quick-reference)

---

## 1. Architecture recap

```
Local disk                           MotherDuck "Thyroid 2026"
──────────────────────────────────   ──────────────────────────────────────────
processed/output/v2_parquets/    ──► v2_stage.<domain>         (116)
                                     v2_stage.load_inventory   (116)
                                 ──► main.<domain>             (112 promote SQL)
processed/canonical_*.parquet    ──► main.canonical_*          (103)
exports/manuscript_freeze_v1/    ──► main.<episode tables>     (117)
                                     qa.*                       (114)
                                     main.master_*_verified_v1 (125)
                                     release_YYYYMMDD.*        (115)
                                     └─► qa.release_manifest   (115)
```

All writes use `connect_md_or_file(..., fail_closed=True)`.  
No raw note text leaves local disk.  
v1 canonical tables are never touched by v2 operations.

---

## 2. Prerequisites

### 2.1 Token

At least one of the following must be set in the environment:

| Variable | Purpose |
|----------|---------|
| `MOTHERDUCK_TOKEN` | Personal developer token (interactive) |
| `MD_SA_TOKEN` | Service-account / CI token (preferred for automation) |

Check with:

```bash
.venv/bin/python -c "
from motherduck_client import token_mode
mode = token_mode()
print(f'Token source: {mode}')
assert mode != 'none', 'ERROR: no token found'
"
```

Optional: create `.env.motherduck` at repo root (gitignored):

```bash
MOTHERDUCK_TOKEN=your_token_here
```

### 2.2 Local parquets must exist

```bash
# v2 domain parquets (one per registry domain with canonical_output=true)
ls processed/output/v2_parquets/*.parquet | head -5

# Canonical fact parquets (output of 103 or pre-existing)
ls processed/canonical_extracted_fact_long_v2.parquet \
   processed/canonical_fact_quarantine_v2.parquet

# Manuscript freeze episode tables (required by 117)
ls exports/manuscript_freeze_v1/data/*.parquet | head -5
```

### 2.3 Smoke test

```bash
.venv/bin/python scripts/smoke_test_md_connection.py --md
```

---

## 3. Quick start — orchestrator

`scripts/124_md_live_release_audit.py` chains all steps, captures evidence, and writes a dated audit directory.

### 3.1 Standard run (release tag = today)

```bash
.venv/bin/python scripts/124_md_live_release_audit.py --md
```

### 3.2 Final-release mode (strict: blocks on pending reviews)

```bash
.venv/bin/python scripts/124_md_live_release_audit.py --md --final-release
```

### 3.3 Dry run (no writes to MotherDuck)

```bash
.venv/bin/python scripts/124_md_live_release_audit.py --md --dry-run
```

### 3.4 Custom release tag

```bash
.venv/bin/python scripts/124_md_live_release_audit.py --md --tag 20260407
```

### 3.5 Skip stage refresh (re-run after gate-only failure)

```bash
.venv/bin/python scripts/124_md_live_release_audit.py --md --skip-stage --skip-gate
```

### 3.6 Orchestrator flags

| Flag | Description |
|------|-------------|
| `--md` | Connect to MotherDuck (fail-closed; required for a live run) |
| `--dry-run` | Pass --dry-run to each subscript; no MotherDuck writes |
| `--final-release` | Halt if any promotable review row is pending |
| `--tag YYYYMMDD` | Release tag (default: today) |
| `--skip-stage` | Skip 116 stage refresh |
| `--skip-gate` | Skip 112 promotion gate |
| `--output-dir PATH` | Override audit directory (default: `studies/YYYYMMDD_motherduck_live_release_audit/`) |

### 3.7 Deliverables written to `studies/YYYYMMDD_motherduck_live_release_audit/`

| File | Content |
|------|---------|
| `preflight_db_list.json` | `PRAGMA database_list` + `md_information_schema.databases` + schemas |
| `stage_refresh_output.log` | stdout from 116 |
| `stage_parity_report.csv` | `v2_stage.load_inventory` dump (row counts per domain) |
| `promotion_gate_output.log` | stdout from 112 |
| `promotion_scorecard.csv` | G1–G8 gate scorecard |
| `manual_review_queue.csv` | Review queue state at gate time |
| `motherduck_promote.sql` | Generated promotion SQL (for audit record) |
| `canonical_output.log` | stdout from 103 |
| `qa_setup_output.log` | stdout from 114 |
| `contract_views_output.log` | stdout from 117 |
| `presentation_views_output.log` | stdout from 125 |
| `release_snapshot_output.log` | stdout from 115 |
| `release_schema_manifest.json` | Release schema table list + `qa.release_manifest` dump |
| `parquet_bundle_output.log` | stdout from 118 |
| `parquet_bundle_manifest.json` | Parquet file list with SHA-256 checksums and row counts |
| `validation_output.log` | stdout from 119 |
| `validation_report.md` | Structured validation report (119 output) |
| `release_validation_strict.json` | Full live MD evidence: query log, schema counts, manifest |
| `snapshot_metadata.json` | `md_information_schema.snapshots` |
| `audit_summary.md` | Step-by-step status table + deliverables index |

---

## 4. Manual step-by-step

Use this when you need to re-run individual steps or when the orchestrator is not appropriate.

### Step 0: Preflight

```bash
# Verify token
.venv/bin/python -c "
from motherduck_client import token_mode
print(token_mode())
"

# Smoke test
.venv/bin/python scripts/smoke_test_md_connection.py --md

# List existing schemas in MotherDuck
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
rows = con.execute('''
    SELECT table_schema, COUNT(*) AS n_tables
    FROM information_schema.tables
    GROUP BY 1 ORDER BY 1
''').fetchall()
for s, n in rows:
    print(f'  {s}: {n} table(s)')
con.close()
"
```

### Step 1: Stage refresh

```bash
.venv/bin/python scripts/116_md_stage_loader.py --md
```

Verify:

```bash
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
inv = con.execute('SELECT domain_name, local_row_count, md_row_count, row_match FROM v2_stage.load_inventory ORDER BY domain_name').fetchdf()
print(inv.to_string())
mismatches = inv[~inv['row_match']]
print(f'  Mismatches: {len(mismatches)}')
con.close()
"
```

### Step 2: Promotion gate

```bash
RUN_LABEL=promote_$(date +%Y%m%d_%H%M)
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
    --v2-parquets-dir processed/output/v2_parquets \
    --db-path thyroid_master.duckdb \
    --motherduck-check \
    --run-label "${RUN_LABEL}" \
    --output-dir "studies/v2_domain_promotion_gate_${RUN_LABEL}"

# Review scorecard — all 8 gates must be PASS
cat "studies/v2_domain_promotion_gate_${RUN_LABEL}/promotion_scorecard.csv"

# Review pending items
cat "studies/v2_domain_promotion_gate_${RUN_LABEL}/manual_review_queue.csv"
```

**Gate summary (G1–G8):**

| Gate | Criterion | Threshold |
|------|-----------|-----------|
| G1 | Registry domains with parquets | >0 |
| G2 | Schema completeness (core columns) | All present |
| G3 | Provenance columns | CONDITIONAL PASS accepted |
| G4 | Duplicate rate | ≤5% per domain |
| G5 | Date coverage (critical domains) | entity_date or note_date >0% |
| G6 | Concordance floor (critical domains) | ≥30% |
| G7 | Manual review queue | Empty or all verified |
| G8 | MotherDuck v2_stage parity | Row counts match |

### Step 3: Hydrate QA schema

```bash
.venv/bin/python scripts/114_qa_schema_setup.py --md \
    --hydrate-from "studies/v2_domain_promotion_gate_${RUN_LABEL}"
```

### Step 4: Canonical materialization

```bash
.venv/bin/python scripts/103_fact_lineage_materialize.py --md
```

### Step 5: Contract views

```bash
.venv/bin/python scripts/117_md_contract_views.py --md
```

### Step 6: Presentation views

```bash
.venv/bin/python scripts/125_master_verified_views.py --md
```

### Step 7: Release snapshot

```bash
TAG=$(date +%Y%m%d)
.venv/bin/python scripts/115_release_snapshot.py --md --tag "${TAG}"
```

### Step 8: Parquet release bundle

```bash
.venv/bin/python scripts/118_parquet_release_bundle.py --md --tag "${TAG}"
# Manifest written to: exports/parquet_release_${TAG}/manifest.json
```

### Step 9: Validation

```bash
OUT_DIR="studies/${TAG}_motherduck_live_release_audit"
mkdir -p "${OUT_DIR}"

# Structural validation (default)
.venv/bin/python scripts/119_md_formalization_validate.py --md \
    --output-dir "${OUT_DIR}"

# Strict release-mode validation (blocks on failures)
.venv/bin/python scripts/119_md_formalization_validate.py --md --release-mode \
    --output-dir "${OUT_DIR}"
```

---

## 5. Presentation view contracts

All three views live in the `main` schema and are created by `scripts/125_master_verified_views.py --md`.

### 5.1 `main.master_fact_long_verified_v1`

One row per extracted entity fact. The canonical analyst surface for per-fact release data.

| Column | Type | Description |
|--------|------|-------------|
| `research_id` | INTEGER | De-identified patient identifier |
| `fact_id` | VARCHAR | Stable MD5 fact hash |
| `source_domain` | VARCHAR | Extraction domain (e.g. `staging`, `genetics`) |
| `source_object_id` | BIGINT | `note_row_id` — source note / object identifier |
| `extraction_run_id` | VARCHAR | Matched extraction run ID from `note_extraction_runs` |
| `extractor_build_version` | VARCHAR | Version of the extractor that produced this fact |
| `llm_model` | VARCHAR | LLM model used (if LLM extraction) |
| `extraction_started_at` | TIMESTAMP | When the extraction run began |
| `entity_type` | VARCHAR | Entity category (e.g. `T_stage`, `BRAF`) |
| `entity_value_norm` | VARCHAR | Normalized entity value |
| `entity_date` | DATE | Date associated with this entity |
| `present_or_negated` | VARCHAR | `present` or `negated` |
| `confidence` | FLOAT | Extraction confidence [0–1] |
| `linkage_anchor_family` | VARCHAR | Domain family used for episode linkage |
| `inferred_surgery_episode_id` | INTEGER | Linked episode identifier |
| `linkage_confidence` | FLOAT | Linkage confidence score [0–1] |
| `reviewer_status` | VARCHAR | Verification status from `qa.manual_review_queue` |
| `reviewer_verified_by` | VARCHAR | Who verified this row |
| `reviewer_decision_at` | TIMESTAMP | When the review decision was made |
| `release_tag` | VARCHAR | YYYYMMDD tag from latest `qa.release_manifest` |

**Prerequisite tables**: `main.canonical_extracted_fact_long_v2`, `main.note_extraction_runs`, `qa.manual_review_queue`, `qa.release_manifest`.

### 5.2 `main.master_patient_rollup_verified_v1`

Per-patient summary aggregated from `master_fact_long_verified_v1`.

| Column | Type | Description |
|--------|------|-------------|
| `research_id` | INTEGER | De-identified patient identifier |
| `total_facts` | BIGINT | Total extracted fact rows for patient |
| `domains_covered` | BIGINT | Number of distinct source domains |
| `unique_entity_types` | BIGINT | Number of distinct entity types |
| `pathology_facts` | BIGINT | Facts from pathology-anchored domains |
| `operative_facts` | BIGINT | Facts from operative-anchored domains |
| `imaging_facts` | BIGINT | Facts from imaging-anchored domains |
| `molecular_facts` | BIGINT | Facts from molecular-anchored domains |
| `followup_facts` | BIGINT | Facts from followup-anchored domains |
| `rai_facts` | BIGINT | Facts from RAI-anchored domains |
| `demographics_facts` | BIGINT | Facts from demographics-anchored domains |
| `episode_linked_facts` | BIGINT | Facts with a non-null `inferred_surgery_episode_id` |
| `pct_episode_linked` | FLOAT | Episode linkage rate (%) |
| `reviewed_facts` | BIGINT | Facts with a non-null `reviewer_status` |
| `pct_reviewed` | FLOAT | Review coverage rate (%) |
| `release_tag` | VARCHAR | YYYYMMDD release tag |

### 5.3 `main.master_source_lineage_v1`

Full provenance chain: from extraction run through reviewer decision to release.

| Column | Type | Description |
|--------|------|-------------|
| `research_id` | INTEGER | De-identified patient identifier |
| `source_domain` | VARCHAR | Extraction domain |
| `source_object_id` | BIGINT | `note_row_id` — source note identifier |
| `entity_type` | VARCHAR | Entity category |
| `entity_date` | DATE | Entity date |
| `extraction_run_id` | VARCHAR | Run that produced this fact |
| `extractor_build_version` | VARCHAR | Extractor version |
| `llm_model` | VARCHAR | LLM model (if applicable) |
| `extraction_started_at` | TIMESTAMP | Extraction run start time |
| `extraction_method` | VARCHAR | `llm_v2_fleet`, `regex_extractor`, etc. |
| `extracted_at` | TIMESTAMP | Entity extraction timestamp |
| `linkage_anchor_family` | VARCHAR | Domain family |
| `inferred_surgery_episode_id` | INTEGER | Linked episode |
| `ep_source_table` | VARCHAR | Episode source table used for linkage |
| `reviewer_status` | VARCHAR | Verification status |
| `reviewer_verified_by` | VARCHAR | Reviewer identity |
| `reviewer_decision_at` | TIMESTAMP | Decision timestamp |
| `reviewer_notes` | VARCHAR | Free-text reviewer notes |
| `release_tag` | VARCHAR | YYYYMMDD release tag |

---

## 6. Release snapshot and parquet bundle

### 6.1 Release snapshot (`release_YYYYMMDD` schema)

`scripts/115_release_snapshot.py` copies the following canonical `main` tables to an immutable `release_YYYYMMDD` schema with a `release_tag` column appended:

- `canonical_extracted_fact_long_v1`
- `canonical_extracted_fact_long_v2`
- `canonical_fact_quarantine_v1`
- `canonical_fact_quarantine_v2`
- `thyroglobulin_lab_canonical_v1`
- `note_extraction_runs`

Each release schema is **read-only after creation**. Corrections require a new release tag (never overwrite canonical history).

### 6.2 Parquet release bundle

`scripts/118_parquet_release_bundle.py` exports a directory-partitioned bundle to `exports/parquet_release_YYYYMMDD/`:

```
exports/parquet_release_YYYYMMDD/
  main/
    canonical_extracted_fact_long_v2.parquet
    canonical_fact_quarantine_v2.parquet
    note_extraction_runs.parquet
    note_entities_llm_<domain>.parquet  (one per registry v2 domain)
    tumor_episode_master_v2.parquet
    molecular_test_episode_v2.parquet
    rai_treatment_episode_v2.parquet
    operative_episode_detail_v2.parquet
  qa/
    promotion_scorecard.parquet
    domain_validation.parquet
    manual_review_queue.parquet
  manifest.json
```

The `manifest.json` contains file-level metadata: row counts, SHA-256 checksums, release tag, and git SHA.

---

## 7. Validation checklist

After a complete orchestrator run, confirm the following against the audit directory:

### 7.1 Preflight evidence (`preflight_db_list.json`)

- [ ] `md_confirmed: true`
- [ ] `schemas_present` includes at least `main`, `v2_stage`, `qa`

### 7.2 Stage parity (`stage_parity_report.csv`)

- [ ] `row_match = true` for all domains
- [ ] Zero mismatch rows

### 7.3 Promotion gate (`promotion_scorecard.csv`)

- [ ] All 8 gates (G1–G8) show PASS
- [ ] `manual_review_queue.csv` — zero unverified promotable rows (in final-release mode)

### 7.4 Canonical tables

```sql
SELECT table_name,
       (SELECT COUNT(*) FROM main.{table_name}) AS n_rows
FROM information_schema.tables
WHERE table_schema = 'main'
  AND table_name LIKE 'canonical%'
ORDER BY 1;
```

- [ ] `canonical_extracted_fact_long_v2` > 0 rows
- [ ] `canonical_fact_quarantine_v2` >= 0 rows

### 7.5 Presentation views

```sql
SELECT COUNT(*) FROM main.master_fact_long_verified_v1;
SELECT COUNT(*) FROM main.master_patient_rollup_verified_v1;
SELECT COUNT(*) FROM main.master_source_lineage_v1;
```

- [ ] All three views return rows
- [ ] `release_tag` column is non-null

### 7.6 Release schema

```sql
SELECT table_schema, COUNT(*) AS n_tables
FROM information_schema.tables
WHERE table_schema LIKE 'release_%'
GROUP BY 1;
```

- [ ] At least one `release_YYYYMMDD` schema exists

### 7.7 QA release manifest

```sql
SELECT release_tag, created_at, tables_included
FROM qa.release_manifest
ORDER BY created_at DESC
LIMIT 5;
```

- [ ] Current release tag present with correct timestamp

### 7.8 Validation report (`validation_report.md`)

- [ ] `VERDICT: PASS`
- [ ] Zero FAILs (WARNs are informational)

---

## 8. Rollback procedures

### 8.1 Drop a release schema (if created in error)

```bash
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
tag = 'YYYYMMDD'  # replace with actual tag
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
con.execute(f'DROP SCHEMA IF EXISTS release_{tag} CASCADE')
print(f'Dropped release_{tag}')
# Also clean up qa.release_manifest
con.execute(f\"DELETE FROM qa.release_manifest WHERE release_tag = '{tag}'\")
print(f'Removed from qa.release_manifest')
con.close()
"
```

### 8.2 Roll back v2 canonical tables

```bash
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
for tbl in ['canonical_extracted_fact_long_v2', 'canonical_fact_quarantine_v2']:
    con.execute(f'DROP TABLE IF EXISTS main.\"{tbl}\"')
    print(f'Dropped: {tbl}')
# v1 tables are never dropped
con.close()
"
```

### 8.3 Roll back v2_stage tables

```bash
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
tables = con.execute(
    \"SELECT table_name FROM information_schema.tables WHERE table_schema = 'v2_stage'\"
).fetchall()
for (t,) in tables:
    con.execute(f'DROP TABLE IF EXISTS v2_stage.\"{t}\"')
    print(f'Dropped: v2_stage.{t}')
con.execute('DROP SCHEMA IF EXISTS v2_stage')
print('Dropped schema v2_stage')
con.close()
"
```

### 8.4 Roll back presentation views

```bash
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
for v in ['master_fact_long_verified_v1', 'master_patient_rollup_verified_v1', 'master_source_lineage_v1']:
    con.execute(f'DROP VIEW IF EXISTS main.\"{v}\"')
    print(f'Dropped view: {v}')
con.close()
"
```

---

## 9. Constraints and invariants

| Constraint | Enforcement |
|-----------|------------|
| No silent local fallback when `--md` is requested | `fail_closed=True` in all `--md` scripts; exits 1 if MD unreachable |
| Raw note text never leaves local disk | Only extracted entity fields are written to MotherDuck; source note columns (`note_text`, `full_text`) are excluded from all parquet outputs |
| No auto-promotion of pending review rows | `112` requires `verification_status IS NOT NULL` for all queue entries; orchestrator halts if pending rows exist in `--final-release` mode |
| No destructive rewrites of canonical history | Release schemas are immutable; `115` refuses to overwrite an existing `release_YYYYMMDD` schema — use a new tag |
| v1 canonical tables are never mutated by v2 ops | `canonical_extracted_fact_long_v1` / `canonical_fact_quarantine_v1` are read-only for all v2 pipeline steps |
| Service-account token preferred in automation | `get_token(prefer_service_account=True)` picks `MD_SA_TOKEN` first |
| Connection verification required | `PRAGMA database_list` is checked for `md:` or `md_information_schema` before any `fail_closed` operation |

---

## 10. Script quick reference

| Script | Purpose | Key flags |
|--------|---------|-----------|
| `124_md_live_release_audit.py` | End-to-end orchestrator | `--md --final-release --tag --dry-run` |
| `116_md_stage_loader.py` | Load v2 parquets → v2_stage + load_inventory | `--md --dry-run` |
| `112_v2_domain_promotion_gate.py` | 8-gate promotion validation | `--motherduck-check --run-label` |
| `103_fact_lineage_materialize.py` | Canonical fact tables (v1+v2) → main | `--md --dry-run` |
| `114_qa_schema_setup.py` | QA schema DDL + hydration from gate output | `--md --hydrate-from` |
| `117_md_contract_views.py` | Episode tables + contract views → main | `--md --dry-run` |
| `125_master_verified_views.py` | 3 analyst presentation views → main | `--md --dry-run` |
| `115_release_snapshot.py` | Immutable release_YYYYMMDD schema | `--md --tag --dry-run` |
| `118_parquet_release_bundle.py` | Curated parquet export bundle | `--md --tag --dry-run` |
| `119_md_formalization_validate.py` | Structural + release-mode validation | `--md --release-mode --output-dir` |
| `smoke_test_md_connection.py` | Quick connectivity smoke test | `--md` |

### Connection utility

All scripts must use the canonical connection helper — never `duckdb.connect("md:...")` directly:

```python
from utils.md_connect import connect_md_or_file, connect_md_fail_closed

# Standard (fail-open): falls back to local file if MD unreachable
con = connect_md_or_file(DB_PATH, md=args.md)

# Fail-closed when --md is set: exits 1 if MD unreachable
con = connect_md_or_file(DB_PATH, md=args.md, fail_closed=args.md)

# Convenience alias: always MD, always fail-closed
con = connect_md_fail_closed(DB_PATH)
```

Token resolution is handled internally by `motherduck_client.get_token()`. Do not pass tokens as arguments.

---

*End of runbook v2*

# MotherDuck Database Formalization — 2026-04-07

## Objective

Formalize the shared MotherDuck database structure so the project has a clean
stage/main/qa flow and can safely onboard additional labs and patients.

## Deliverables

| Artifact | Path |
|----------|------|
| Stage loader (v2_stage + load_inventory) | `scripts/116_md_stage_loader.py` |
| Contract views DDL | `scripts/sql/117_contract_views_ddl.sql` |
| Contract views loader | `scripts/117_md_contract_views.py` |
| QA DDL (extended) | `scripts/sql/114_qa_schema_ddl.sql` |
| QA setup (extended) | `scripts/114_qa_schema_setup.py` |
| Parquet release bundle | `scripts/118_parquet_release_bundle.py` |
| Validation suite | `scripts/119_md_formalization_validate.py` |
| Database contract doc | `docs/motherduck_database_contract_v1.md` |
| Staging runbook (updated) | `docs/motherduck_v2_staging_runbook.md` |
| Validation report | `studies/20260407_motherduck_formalization/validation_report.md` |

## Schema Architecture

```
v2_stage          main                    qa                  release_YYYYMMDD
-----------       --------------------    ----------------    ----------------
22 domain tables  22 promoted domains     promotion_scorecard immutable copies
load_inventory    canonical_fact_long_v2  domain_validation   + release_tag col
                  canonical_quarantine_v2 concordance_summary
                  note_extraction_runs    manual_review_queue
                  episode contract tables promotion_review_decisions
                  linkage views           tg_lab_ingestion_qc
                  lab deduped view        release_manifest
                                          summary views (4)
```

## Execution sequence

```bash
# 1. Stage v2 parquets
.venv/bin/python scripts/116_md_stage_loader.py --md

# 2. Run promotion gate
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
    --v2-parquets-dir processed/output/v2_parquets \
    --motherduck-check --run-label formalization_20260407

# 3. Hydrate QA tables
.venv/bin/python scripts/114_qa_schema_setup.py --md \
    --hydrate-from studies/20260406_domain_inventory_current/gate_dry_run

# 4. Execute promotion SQL (review first)

# 5. Materialize canonical tables
.venv/bin/python scripts/103_fact_lineage_materialize.py --md

# 6. Load contract views
.venv/bin/python scripts/117_md_contract_views.py --md

# 7. Create release snapshot
.venv/bin/python scripts/115_release_snapshot.py --md --tag 20260407

# 8. Export Parquet bundle
.venv/bin/python scripts/118_parquet_release_bundle.py --md

# 9. Validate
.venv/bin/python scripts/119_md_formalization_validate.py --md \
    --output-dir studies/20260407_motherduck_formalization
```

## What is now ready for future labs/patients

After this formalization pass:

1. New extraction domains can be added to the registry and automatically flow
   through the stage/gate/promote pipeline.
2. New lab data sources follow the onboarding workflow in the database contract.
3. New patient cohorts are ingested locally, extracted, and promoted using the
   same 8-gate process.
4. All QA artifacts are tracked in MotherDuck with full provenance.
5. Release snapshots provide immutable point-in-time audit trail.
6. Parquet bundles are exportable for downstream Fabric/OneLake consumption.

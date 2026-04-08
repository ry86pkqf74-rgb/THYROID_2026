# MotherDuck Live Release Audit Summary

**Release tag:** `20260408`
**Generated:** 2026-04-08T08:48:56.858757+00:00
**Mode:** Dry-run
**Verdict:** **PASS**

---

## Pipeline Step Results

| Step | Status | Started | Finished | Log |
|------|--------|---------|----------|-----|
| Preflight | PASS | 2026-04-08T08:07:22.090390+00:00 | 2026-04-08T08:07:26.864747+00:00 | `preflight_db_list.json` |
| Stage refresh (116) | PASS | 2026-04-08T08:07:26.868833+00:00 | 2026-04-08T08:07:45.102763+00:00 | `stage_refresh_output.log` |
| Promotion gate (112) | PASS | 2026-04-08T08:07:45.103645+00:00 | 2026-04-08T08:09:48.250445+00:00 | `promotion_gate_output.log` |
| Canonical materialization (103) | PASS | 2026-04-08T08:09:48.255584+00:00 | 2026-04-08T08:43:34.596127+00:00 | `canonical_output.log` |
| QA schema setup (114) | PASS | 2026-04-08T08:43:34.598914+00:00 | 2026-04-08T08:44:06.455847+00:00 | `qa_setup_output.log` |
| Contract views (117) | PASS | 2026-04-08T08:44:06.458236+00:00 | 2026-04-08T08:44:28.026111+00:00 | `contract_views_output.log` |
| Molecular lineage views (132) | PASS | 2026-04-08T08:44:28.026445+00:00 | 2026-04-08T08:44:43.825237+00:00 | `molecular_lineage_views_output.log` |
| Presentation views (125) | PASS | 2026-04-08T08:44:55.106951+00:00 | 2026-04-08T08:45:14.369187+00:00 | `presentation_views_output.log` |
| Release snapshot (115) | PASS | 2026-04-08T08:45:14.369546+00:00 | 2026-04-08T08:45:26.458828+00:00 | `release_snapshot_output.log` |
| Parquet release bundle (118) | PASS | 2026-04-08T08:45:26.459124+00:00 | 2026-04-08T08:47:56.140059+00:00 | `parquet_bundle_output.log` |
| Formalization validation (119) | PASS | 2026-04-08T08:47:56.141150+00:00 | 2026-04-08T08:48:56.850095+00:00 | `validation_output.log` |

---

## Deliverables

| Artifact | Description |
|----------|-------------|
| `preflight_db_list.json` | PRAGMA database_list + md_information_schema evidence |
| `stage_parity_report.csv` | v2_stage.load_inventory row-count parity |
| `promotion_scorecard.csv` | 112 gate scorecard (G1–G8) |
| `manual_review_queue.csv` | Pending review rows at gate time |
| `release_schema_manifest.json` | Release schema + qa.release_manifest dump |
| `parquet_bundle_manifest.json` | Parquet bundle file list with SHA-256 checksums |
| `validation_report.md` | 119 structural + release-mode validation (includes molecular contract) |
| `molecular_lineage_views_output.log` | 132 unified molecular fact views |
| `release_validation_strict.json` | Full MD evidence: query_history, schema counts, manifest |
| `snapshot_metadata.json` | MotherDuck snapshot catalog (database_snapshots / DATABASE_SNAPSHOTS / snapshots) |
| `audit_summary.md` | This file |

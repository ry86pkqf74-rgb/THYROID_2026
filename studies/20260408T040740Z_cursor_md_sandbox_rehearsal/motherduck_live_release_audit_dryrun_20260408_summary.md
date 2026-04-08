# MotherDuck Live Release Audit Summary

**Release tag:** `20260408`
**Generated:** 2026-04-08T03:39:02.446482+00:00
**Mode:** Dry-run
**Verdict:** **PASS**

---

## Pipeline Step Results

| Step | Status | Started | Finished | Log |
|------|--------|---------|----------|-----|
| Preflight | PASS | 2026-04-08T03:08:09.173195+00:00 | 2026-04-08T03:08:12.433571+00:00 | `preflight_db_list.json` |
| Stage refresh (116) | PASS | 2026-04-08T03:08:12.437336+00:00 | 2026-04-08T03:08:17.765013+00:00 | `stage_refresh_output.log` |
| Promotion gate (112) | PASS | 2026-04-08T03:08:17.765711+00:00 | 2026-04-08T03:09:54.493556+00:00 | `promotion_gate_output.log` |
| Canonical materialization (103) | PASS | 2026-04-08T03:09:54.498277+00:00 | 2026-04-08T03:37:05.544904+00:00 | `canonical_output.log` |
| QA schema setup (114) | PASS | 2026-04-08T03:37:05.545406+00:00 | 2026-04-08T03:37:19.915031+00:00 | `qa_setup_output.log` |
| Contract views (117) | PASS | 2026-04-08T03:37:19.915497+00:00 | 2026-04-08T03:37:24.097307+00:00 | `contract_views_output.log` |
| Molecular lineage views (132) | PASS | 2026-04-08T03:37:24.098184+00:00 | 2026-04-08T03:37:36.003125+00:00 | `molecular_lineage_views_output.log` |
| Presentation views (125) | PASS | 2026-04-08T03:37:45.271256+00:00 | 2026-04-08T03:37:48.898802+00:00 | `presentation_views_output.log` |
| Release snapshot (115) | PASS | 2026-04-08T03:37:48.899674+00:00 | 2026-04-08T03:37:55.244698+00:00 | `release_snapshot_output.log` |
| Parquet release bundle (118) | PASS | 2026-04-08T03:37:55.245091+00:00 | 2026-04-08T03:38:29.192457+00:00 | `parquet_bundle_output.log` |
| Formalization validation (119) | PASS | 2026-04-08T03:38:29.193747+00:00 | 2026-04-08T03:39:02.443555+00:00 | `validation_output.log` |

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

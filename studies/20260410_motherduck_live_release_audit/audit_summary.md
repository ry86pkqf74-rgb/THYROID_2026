# MotherDuck Live Release Audit Summary

**Release tag:** `20260410`
**Generated:** 2026-04-07T13:35:56.793820+00:00
**Mode:** Dry-run
**Verdict:** **PASS**

---

## Pipeline Step Results

| Step | Status | Started | Finished | Log |
|------|--------|---------|----------|-----|
| Preflight | PASS | 2026-04-07T13:34:25.260268+00:00 | 2026-04-07T13:34:26.099086+00:00 | `preflight_db_list.json` |
| Stage refresh (116) | PASS | 2026-04-07T13:34:26.102330+00:00 | 2026-04-07T13:34:30.614697+00:00 | `stage_refresh_output.log` |
| Promotion gate (112) | PASS | 2026-04-07T13:34:30.614893+00:00 | 2026-04-07T13:34:51.002257+00:00 | `promotion_gate_output.log` |
| Canonical materialization (103) | PASS | 2026-04-07T13:34:51.003629+00:00 | 2026-04-07T13:35:08.183064+00:00 | `canonical_output.log` |
| QA schema setup (114) | PASS | 2026-04-07T13:35:08.183413+00:00 | 2026-04-07T13:35:18.167114+00:00 | `qa_setup_output.log` |
| Contract views (117) | PASS | 2026-04-07T13:35:18.167303+00:00 | 2026-04-07T13:35:21.592914+00:00 | `contract_views_output.log` |
| Molecular lineage views (132) | PASS | 2026-04-07T13:35:21.593051+00:00 | 2026-04-07T13:35:25.017043+00:00 | `molecular_lineage_views_output.log` |
| Presentation views (125) | PASS | 2026-04-07T13:35:25.017198+00:00 | 2026-04-07T13:35:27.807311+00:00 | `presentation_views_output.log` |
| Release snapshot (115) | PASS | 2026-04-07T13:35:27.808594+00:00 | 2026-04-07T13:35:31.122521+00:00 | `release_snapshot_output.log` |
| Parquet release bundle (118) | PASS | 2026-04-07T13:35:31.122645+00:00 | 2026-04-07T13:35:39.896894+00:00 | `parquet_bundle_output.log` |
| Formalization validation (119) | PASS | 2026-04-07T13:35:39.897151+00:00 | 2026-04-07T13:35:56.791742+00:00 | `validation_output.log` |

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
| `release_validation_strict.json` | Full MD evidence: query log, schema counts, manifest |
| `snapshot_metadata.json` | md_information_schema.snapshots (if accessible) |
| `audit_summary.md` | This file |

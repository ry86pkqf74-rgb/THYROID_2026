# MotherDuck Live Release Audit Summary

**Release tag:** `20260407_final`
**Generated:** 2026-04-07T05:09:40.575549+00:00
**Mode:** Final-release
**Verdict:** **BLOCKED**

---

## Pipeline Step Results

| Step | Status | Started | Finished | Log |
|------|--------|---------|----------|-----|
| Preflight | PASS | 2026-04-07T05:06:05.132403+00:00 | 2026-04-07T05:06:05.518515+00:00 | `preflight_db_list.json` |
| Stage refresh (116) | PASS | 2026-04-07T05:06:05.518878+00:00 | 2026-04-07T05:06:44.494515+00:00 | `stage_refresh_output.log` |
| Promotion gate (112) | PASS | 2026-04-07T05:06:44.609863+00:00 | 2026-04-07T05:07:01.660616+00:00 | `promotion_gate_output.log` |
| Canonical materialization (103) | PASS | 2026-04-07T05:07:01.933690+00:00 | 2026-04-07T05:07:24.028662+00:00 | `canonical_output.log` |
| QA schema setup (114) | PASS | 2026-04-07T05:07:24.028776+00:00 | 2026-04-07T05:07:31.666057+00:00 | `qa_setup_output.log` |
| Contract views (117) | PASS | 2026-04-07T05:07:31.666162+00:00 | 2026-04-07T05:07:47.727354+00:00 | `contract_views_output.log` |
| Presentation views (125) | PASS | 2026-04-07T05:07:47.727477+00:00 | 2026-04-07T05:07:53.441626+00:00 | `presentation_views_output.log` |
| Release snapshot (115) | PASS | 2026-04-07T05:07:53.441793+00:00 | 2026-04-07T05:08:12.431237+00:00 | `release_snapshot_output.log` |
| Parquet release bundle (118) | PASS | 2026-04-07T05:08:12.816994+00:00 | 2026-04-07T05:08:27.825760+00:00 | `parquet_bundle_output.log` |
| Formalization validation (119) | FAIL | 2026-04-07T05:08:27.826254+00:00 | 2026-04-07T05:08:39.040528+00:00 | `validation_output.log` |

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
| `validation_report.md` | 119 structural + release-mode validation |
| `release_validation_strict.json` | Full MD evidence: query log, schema counts, manifest |
| `snapshot_metadata.json` | md_information_schema.snapshots (if accessible) |
| `audit_summary.md` | This file |

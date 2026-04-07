# MotherDuck Live Release Audit Summary

**Release tag:** `20260406`
**Generated:** 2026-04-07T04:01:42.149173+00:00
**Mode:** Dry-run
**Verdict:** **PASS**

---

## Pipeline Step Results

| Step | Status | Started | Finished | Log |
|------|--------|---------|----------|-----|
| Preflight | PASS | 2026-04-07T04:00:32.924389+00:00 | 2026-04-07T04:00:33.439954+00:00 | `preflight_db_list.json` |
| Stage refresh (116) | PASS | 2026-04-07T04:00:33.440302+00:00 | 2026-04-07T04:00:35.734752+00:00 | `stage_refresh_output.log` |
| Promotion gate (112) | PASS | 2026-04-07T04:00:35.734965+00:00 | 2026-04-07T04:00:57.669217+00:00 | `promotion_gate_output.log` |
| Canonical materialization (103) | PASS | 2026-04-07T04:00:57.670616+00:00 | 2026-04-07T04:01:10.680558+00:00 | `canonical_output.log` |
| QA schema setup (114) | PASS | 2026-04-07T04:01:10.680707+00:00 | 2026-04-07T04:01:15.826288+00:00 | `qa_setup_output.log` |
| Contract views (117) | PASS | 2026-04-07T04:01:15.826417+00:00 | 2026-04-07T04:01:17.731913+00:00 | `contract_views_output.log` |
| Presentation views (125) | PASS | 2026-04-07T04:01:17.732065+00:00 | 2026-04-07T04:01:19.189001+00:00 | `presentation_views_output.log` |
| Release snapshot (115) | PASS | 2026-04-07T04:01:19.189249+00:00 | 2026-04-07T04:01:21.253783+00:00 | `release_snapshot_output.log` |
| Parquet release bundle (118) | PASS | 2026-04-07T04:01:21.253968+00:00 | 2026-04-07T04:01:34.609563+00:00 | `parquet_bundle_output.log` |
| Formalization validation (119) | PASS | 2026-04-07T04:01:34.609921+00:00 | 2026-04-07T04:01:42.148472+00:00 | `validation_output.log` |

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

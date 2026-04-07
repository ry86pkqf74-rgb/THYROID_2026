# MotherDuck Live Release Audit Summary

**Release tag:** `20260407`
**Generated:** 2026-04-07T16:18:04.439856+00:00
**Mode:** Dry-run
**Verdict:** **PASS**

---

## Pipeline Step Results

| Step | Status | Started | Finished | Log |
|------|--------|---------|----------|-----|
| Preflight | PASS | 2026-04-07T16:16:17.672567+00:00 | 2026-04-07T16:16:20.465945+00:00 | `preflight_db_list.json` |
| Stage refresh (116) | PASS | 2026-04-07T16:16:20.467552+00:00 | 2026-04-07T16:16:24.127215+00:00 | `stage_refresh_output.log` |
| Promotion gate (112) | PASS | 2026-04-07T16:16:24.127527+00:00 | 2026-04-07T16:16:46.141082+00:00 | `promotion_gate_output.log` |
| Canonical materialization (103) | PASS | 2026-04-07T16:16:46.142860+00:00 | 2026-04-07T16:17:07.685105+00:00 | `canonical_output.log` |
| QA schema setup (114) | PASS | 2026-04-07T16:17:07.685383+00:00 | 2026-04-07T16:17:19.325076+00:00 | `qa_setup_output.log` |
| Contract views (117) | PASS | 2026-04-07T16:17:19.325333+00:00 | 2026-04-07T16:17:22.979883+00:00 | `contract_views_output.log` |
| Molecular lineage views (132) | PASS | 2026-04-07T16:17:22.980030+00:00 | 2026-04-07T16:17:31.352925+00:00 | `molecular_lineage_views_output.log` |
| Presentation views (125) | PASS | 2026-04-07T16:17:31.353215+00:00 | 2026-04-07T16:17:34.363406+00:00 | `presentation_views_output.log` |
| Release snapshot (115) | PASS | 2026-04-07T16:17:34.363627+00:00 | 2026-04-07T16:17:37.927101+00:00 | `release_snapshot_output.log` |
| Parquet release bundle (118) | PASS | 2026-04-07T16:17:37.927240+00:00 | 2026-04-07T16:17:45.842560+00:00 | `parquet_bundle_output.log` |
| Formalization validation (119) | PASS | 2026-04-07T16:17:45.843122+00:00 | 2026-04-07T16:18:04.438955+00:00 | `validation_output.log` |

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

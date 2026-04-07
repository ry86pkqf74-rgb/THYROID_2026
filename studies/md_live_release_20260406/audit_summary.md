# MotherDuck Live Release Audit Summary

**Release tag:** `20260406`
**Generated:** 2026-04-07T04:08:59.253908+00:00
**Mode:** Final-release
**Verdict:** **BLOCKED** (orchestrator exit: validation 119 failed while `promotion_gate` rows were still pending).

**Follow-up:** After queue adjudication (see `MANUAL_QUEUE_RESOLUTION.md`), **`validation_report_release_mode.md`** records **16 PASS / 0 FAIL** for `--release-mode`. **`release_validation_strict.json`** was refreshed to match.

---

## Pipeline Step Results

| Step | Status | Started | Finished | Log |
|------|--------|---------|----------|-----|
| Preflight | PASS | 2026-04-07T04:05:41.631052+00:00 | 2026-04-07T04:05:42.212909+00:00 | `preflight_db_list.json` |
| Stage refresh (116) | PASS | 2026-04-07T04:05:42.213324+00:00 | 2026-04-07T04:06:22.229939+00:00 | `stage_refresh_output.log` |
| Promotion gate (112) | PASS | 2026-04-07T04:06:22.346531+00:00 | 2026-04-07T04:06:48.624630+00:00 | `promotion_gate_output.log` |
| Canonical materialization (103) | PASS | 2026-04-07T04:06:48.886902+00:00 | 2026-04-07T04:07:15.097826+00:00 | `canonical_output.log` |
| QA schema setup (114) | PASS | 2026-04-07T04:07:15.098078+00:00 | 2026-04-07T04:07:21.362439+00:00 | `qa_setup_output.log` |
| Contract views (117) | PASS | 2026-04-07T04:07:21.362557+00:00 | 2026-04-07T04:07:36.537597+00:00 | `contract_views_output.log` |
| Presentation views (125) | PASS | 2026-04-07T04:07:36.537771+00:00 | 2026-04-07T04:07:41.314788+00:00 | `presentation_views_output.log` |
| Release snapshot (115) | PASS | 2026-04-07T04:07:41.314932+00:00 | 2026-04-07T04:07:52.772164+00:00 | `release_snapshot_output.log` |
| Parquet release bundle (118) | PASS | 2026-04-07T04:07:53.174894+00:00 | 2026-04-07T04:08:02.763868+00:00 | `parquet_bundle_output.log` |
| Formalization validation (119) | FAIL | 2026-04-07T04:08:02.764538+00:00 | 2026-04-07T04:08:11.187794+00:00 | `validation_output.log` |

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

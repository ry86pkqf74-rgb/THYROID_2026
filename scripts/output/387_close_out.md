# Script 387 — `thyroid_canonical_publication_v1_0` cleanup close-out

**Date:** 2026-04-22 12:58:15Z
**Script:** `scripts/387_pub_v1_0_cleanup.py`
**Prompt:** `cursor_prompts/CURSOR_PROMPT_PUB_V1_0_CLEANUP_20260422.md`

## End-state object counts (PUB)

| Schema | BASE | VIEW | total |
|---|---:|---:|---:|
| main | 88 | 6 | 94 |
| manuscript_workspace | 61 | 66 | 127 |
| raw | 2 | 0 | 2 |
| tier2 | 0 | 0 | 0 |
| verify | 0 | 0 | 0 |
| views_readable | 0 | 62 | 62 |
| **TOTAL** | | | **285** |

Note: `manuscript_workspace.script_387_prestate_v1` and `manuscript_workspace.script_387_dedup_probe_v1` were created by this script and remain for audit; they are included in the BASE count above (so the net delta differs from the prompt's idealised -11 by +2 in BASE).

## Archive DB delta (`md:"Thyroid 2026 UPdated"`)

| Archive schema | objects archived |
|---|---:|
| `tier2_legacy_20260422` | 12 |
| `verify_legacy_20260422` | 2 |
| `manuscript_workspace_legacy_20260422` | 13 |

## `archive_move_log_v1` rows (script = `387_pub_v1_0_cleanup`)

* Archived (CTAS + DROP): 27
* Drop-only (no archive): 1 (the views_readable duplicate)
* Total log rows for this script: 28

## Phase 6 — dedup probe outcomes

* Tables probed: 36
* `ok` (collapse=0): 27
* `flag_event` (events with key collapse — review only): 7
* `fail_rollup` (rollup invariant violation — HARD FAIL): 0
* `all_null_key` (chosen key NULL on every row — needs alternate key): 2
* `no_key` / `missing`: 0

Full table-by-table report: `scripts/output/387_dedup_probe_report.md`.

### Event-table flags (carry-forward; not auto-fixed)

* `canonical_complications_events_v1` — key `(research_id, evidence_span_hash)`, 15 duplicate-key rows out of 10,954
* `canonical_invasion_events_v1` — key `(invasion_event_id)`, 7,578 duplicate-key rows out of 51,773
* `canonical_medications_events_v1` — key `(research_id, evidence_span_hash)`, 2,512 duplicate-key rows out of 7,501
* `canonical_molecular_genetics_v2` — key `(molecular_episode_id)`, 856 duplicate-key rows out of 1,384
* `canonical_path_malignant_events_v1` — key `(specimen_focus_id)`, 442 duplicate-key rows out of 6,689
* `canonical_pmh_events_v1` — key `(research_id, evidence_span_hash)`, 816 duplicate-key rows out of 12,444
* `canonical_psh_events_v1` — key `(research_id, evidence_span_hash)`, 233 duplicate-key rows out of 3,919

## Reusable patterns (for the next tier-2 / verify-style close-out)

* Cross-DB CTAS works in a single MotherDuck session because the archive DB is auto-attached; no explicit `ATTACH` needed.
* `frozen_section_event_v1` was a VIEW — CTAS materialises the result-set into a TABLE in the archive DB; drop the source with `DROP VIEW` (never `DROP TABLE` on a view).
* `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` is mandatory for any new `build_ts` column to avoid the DuckDB TIMESTAMPTZ → pytz pull-in.
* `archive_and_drop` is idempotent: pre-existing archive copies are row-count verified and the CTAS step skipped.
* Pre-state snapshot tables (`script_387_prestate_v1`, `script_387_dedup_probe_v1`) are intentionally retained — they are the post-mortem record for this run.

## Carry-forward items

* Event-table key collapses above need manual key-richness review (e.g. add `note_row_id` / `evidence_start` to the partition key in upstream rollup builders).
* Sham-key tables above (`all_null_key`) need an alternate partition key choice in `PER_TABLE_KEY_OVERRIDES` for the next probe pass.


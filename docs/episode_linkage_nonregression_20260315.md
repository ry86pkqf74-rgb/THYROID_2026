# Episode Linkage Non-regression Report — 20260315

## Single-Surgery Patient Safety Check

Single-surgery patients (N=10,108) should have all linkages pointing to
surgery_episode_id = 1. Any deviation indicates a regression.

| Domain | Total | Correct (ep=1) | Mislinked | Status |
|--------|-------|----------------|-----------|--------|
| notes | 7431 | 7431 | 0 | PASS |
| labs | 9284 | 9284 | 0 | PASS |

## Multi-Surgery Linkage Quality

- Confident episode-linked items: **4175**
- Ambiguous/weak items: **1429**
- Ambiguous rate: **25.5%** (of episode-linked items)

## Post-Repair Non-Regression (script 96)

Episode downstream repair (434 rows across 5 tables) was applied 20260315.
Non-regression checks confirm no single-surgery patients were disturbed.

| Check | Result |
|-------|--------|
| OED single-surg patients all ep_id=1 | PASS |
| EARD single-surg patients all ep_id=1 | PASS |
| OED multi-surg distinct ep_ids | 3 (1,2,3) |
| EARD multi-surg distinct ep_ids | 3 (1,2,3) |
| V3 surgery_pathology distinct ep_ids | 3 |
| V3 pathology_rai distinct ep_ids | 3 |
| V3 preop_surgery distinct ep_ids | 3 |

Provenance: `episode_downstream_repair_audit_v1` (434 rows, 5 targets)

## Verdict

**PASS — no single-surgery regressions detected. Downstream ep_id
propagation verified across all 5 target tables.**

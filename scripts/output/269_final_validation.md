# Script 269 - Final Validation Scorecard
_Generated 2026-04-17T05:03:03.940782+00:00_

## Overall: PASS

| # | Check | Status | Observed | Note |
|---:|---|:---:|---|---|
| 1 | Spine integrity | PASS | 10871 |  |
| 2 | CPM column count | PASS | 1499 | Net 1,491 + 9 added - 1 excluded (bethesda_final replaced) = 1,499 (prompt's 1,500 was off by 1) |
| 3 | F7 legacy cols gone | PASS | 0 |  |
| 4 | Bethesda convention row | PASS | 1 |  |
| 5 | Bethesda single feeder | PASS | 1 |  |
| 6 | Bethesda derivation dominance (>=90% calculated vs number-only) | PASS | 98.81 |  |
| 7 | No 'unresolved' Bethesda derivation methods at patient level | PASS | 0 |  |
| 8 | F1 episode gap closed (mte_v2 in [10600, 10700]) | PASS | 10650 |  |
| 9 | Molecular pinned feeders intact (5 cols, each 1 feeder) | PASS | 5 |  |
| 10 | Ghost RID 7744 still purged | PASS | 0 |  |
| 11 | All workspace views compile | PASS | 65/65 |  |
| 12 | Backfilled rows tagged correctly | PASS | 525 |  |

## View compile detail
- Views tested: 65
- Pass: 65
- Fail: 0
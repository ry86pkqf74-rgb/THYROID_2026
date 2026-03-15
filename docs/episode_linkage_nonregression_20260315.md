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

## Verdict

**PASS — no single-surgery regressions detected**

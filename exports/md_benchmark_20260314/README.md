# MotherDuck Query Benchmark Baseline — 2026-03-14

## Purpose

This directory stores the performance regression baseline for `scripts/92_query_benchmark.py`.

- **`benchmark_baseline.csv`** — median execution times (ms) per query label, set from a
  live MotherDuck run on the date in this directory's name.  CI compares every subsequent
  run against this baseline and flags any query that regresses beyond **2×** the stored
  median.

## Populating / Updating the Baseline

Run once against live MotherDuck (requires `MOTHERDUCK_TOKEN` or `MD_SA_TOKEN` in env):

```bash
# First-time baseline (current directory stub has zero rows):
.venv/bin/python scripts/92_query_benchmark.py --md --update-baseline

# After a planned infrastructure change (duckling upgrade, materialize refresh, etc.)
# when you intentionally accept new timing numbers:
.venv/bin/python scripts/92_query_benchmark.py --md --update-baseline
```

This overwrites `benchmark_baseline.csv` in this directory.  Commit the updated baseline
alongside whatever code or infra change caused the performance shift.

## CSV Schema

| Column       | Type   | Description                                      |
|--------------|--------|--------------------------------------------------|
| `label`      | str    | Human-readable query identifier                  |
| `tier`       | str    | `hot` (materialized table, SLA 500 ms) or `warm` (view/join, SLA 5 000 ms) |
| `median_ms`  | float  | Median wall-clock time across N benchmark runs   |

## Interpreting CI Output

- **PASS** — all queries within 2× baseline median
- **WARN (SLA)** — query completed but exceeded tier SLA threshold
- **REGRESSION** — query exceeded 2× baseline (reported but non-fatal by default;
  set `REGRESSION_EXIT_CODE = 1` in `92_query_benchmark.py` to make it blocking)

## Benchmark Tiers

| Tier   | SLA     | Source table type               |
|--------|---------|----------------------------------|
| `hot`  | 500 ms  | Materialized `md_*` tables       |
| `warm` | 5 000 ms| Views, multi-join queries        |

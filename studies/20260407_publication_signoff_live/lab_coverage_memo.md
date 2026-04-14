# Lab coverage memo — manuscript panel vs live `longitudinal_lab_canonical_v1`

> **Update (2026-04-14):** **`final_institutional_20260407`** is on prod (see [`../live_state_refresh_20260408_074310/lab_wave_distribution.csv`](../live_state_refresh_20260408_074310/lab_wave_distribution.csv) and [`../live_state_refresh_20260408_074310/lab_analyte_distribution.csv`](../live_state_refresh_20260408_074310/lab_analyte_distribution.csv)). The **April 2026** wave + analyte tables below the banner are **current** for operator narrative; the **March-era** “no institutional wave” paragraph at the bottom is **retained only** as history.

> **Historical capture (2026-04-07):** Early versions of this memo predated `final_institutional_20260407`. Do **not** use those snapshots alone to argue “missing institutional wave.”

## Ingestion waves (live — 2026-04-08 prod refresh)

| ingestion_wave | rows |
|----------------|-----:|
| wave_tgab_structured_ehr | 39,005 |
| wave_tg_structured_ehr | 37,966 |
| **final_institutional_20260407** | **989** |

## Analyte grouping (same refresh)

| analyte_group | n |
|---------------|--:|
| thyroid_tumor_markers | 76,971 |
| thyroid_function | 515 |
| metabolic_panel_nlp_canonical | 284 |
| metabolic_panel_postop_structured | 190 |

**Interpretation:** Institutional non-Tg chemistry **is partially present** via `final_institutional_20260407` + metabolic / thyroid_function groups; coverage is still **thin vs** structured Tg/TgAb volume — manuscript claims on population-wide TSH/PTH/Ca need row-level verification, not this memo alone.

## Panel coverage vs manuscript scope (README / evidence pack)

Institutional non-Tg analytes called out in repo docs:

| Analyte | Approximate live coverage | Assessment |
|---------|---------------------------|------------|
| TSH / thyroid function | Rolled into **`thyroid_function`** (515 rows, same refresh) — not broken out by test name here | **Limited** vs Tg volume; verify `lab_name_raw` / filters before claims |
| PTH / calcium / vitamin D | Rolled into **`metabolic_panel_*`** + institutional wave — see [`lab_analyte_distribution.csv`](../live_state_refresh_20260408_074310/lab_analyte_distribution.csv) | **Partial** — thin vs Tg/TgAb; row-level naming varies |
| Thyroglobulin / Tg axis | `thyroid_tumor_markers` + tg waves (76,971 rows) | **Present** (scope-aligned with current ingest) |

## Manuscript sufficiency

For any manuscript plank requiring **dense, population-wide** TSH / PTH / calcium / vitamin D from the institutional spine, expect **residual sparsity** even after `final_institutional_20260407`: non-Tg rows exist but are **orders of magnitude fewer** than Tg/TgAb structured pulls — cite live counts from `144 --md` / `longitudinal_lab_canonical_v1` exports, not this memo alone.

### Historical (pre–institutional wave)

Previously: only Tg-family structured waves were loaded and the institutional extract had not been appended — **superseded** by `127` + `final_institutional_20260407` on live prod.

## Next ingest command (when CSV exists)

```bash
unset LOCAL_DB_PATH
export MD_SA_TOKEN='…'   # or MOTHERDUCK_TOKEN; never read-scaling token
export MOTHERDUCK_CUSTOM_USER_AGENT='THYROID_2026_institutional_lab/1.0'
export MOTHERDUCK_SESSION_HINT='institutional_lab_<UTC_YYYYMMDD_HHMM>'
.venv/bin/python scripts/127_analyst_institutional_lab_append.py --md \
  --input exports/incoming/<final_institutional_lab>.csv \
  --ingestion-wave final_institutional_YYYYMMDD
```

Use real column headers per script docstring in `scripts/127_analyst_institutional_lab_append.py`.

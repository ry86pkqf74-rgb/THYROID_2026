# Lab blocker — institutional non-Tg panel

**Date:** 2026-04-07  
**Status:** Open (for manuscript scope requiring TSH / PTH / calcium / vitamin D)

## Live state

`main.longitudinal_lab_canonical_v1`:

- **76,971** rows across **`wave_tgab_structured_ehr`** and **`wave_tg_structured_ehr`** only.
- **`analyte_group`:** exclusively `thyroid_tumor_markers` in aggregated counts.
- **No** `final_institutional*` ingestion wave.

## Gap

README and evidence pack call out **institutional non-Tg** chemistry (TSH, PTH, Ca, vit D). Those analytes are **not** represented in current waves.

## Next command

When analyst CSV is ready:

```bash
unset LOCAL_DB_PATH
export MD_SA_TOKEN='…'
export MOTHERDUCK_CUSTOM_USER_AGENT='THYROID_2026_institutional_lab/1.0'
export MOTHERDUCK_SESSION_HINT='institutional_lab_<UTC_YYYYMMDD_HHMM>'
.venv/bin/python scripts/127_analyst_institutional_lab_append.py --md \
  --input exports/incoming/final_institutional_lab.csv \
  --ingestion-wave final_institutional_YYYYMMDD
```

Dry-run first:

```bash
.venv/bin/python scripts/127_analyst_institutional_lab_append.py --md \
  --input exports/incoming/final_institutional_lab.csv \
  --ingestion-wave final_institutional_YYYYMMDD \
  --dry-run
```

See `scripts/127_analyst_institutional_lab_append.py` docstring for required CSV columns.

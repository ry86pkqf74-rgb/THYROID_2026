# Lab coverage memo — manuscript panel vs live `longitudinal_lab_canonical_v1`

> **Historical capture (2026-04-07):** The wave table below reflects a **point-in-time** export **before** `final_institutional_20260407` appeared in MotherDuck. **Current** catalog includes that wave (see [`../20260407_repo_delta_gap_audit/blockers_matrix.md`](../20260407_repo_delta_gap_audit/blockers_matrix.md) and top-level `README.md`). Do **not** use this file alone to argue “missing institutional wave.”

## Ingestion waves (live)

| ingestion_wave | rows | distinct patients |
|----------------|-----:|------------------:|
| wave_tgab_structured_ehr | 39,005 | 3,170 |
| wave_tg_structured_ehr | 37,966 | 3,057 |

No `final_institutional*` (or other analyst-derived institutional) wave present.

## Analyte grouping

- **Dominant `analyte_group`:** `thyroid_tumor_markers` — **76,971** rows (all rows in table).  
- Consistent with Thyroglobulin / anti-Tg antibody structured EHR pulls; **not** a full chemistry panel.

## Panel coverage vs manuscript scope (README / evidence pack)

Institutional non-Tg analytes called out in repo docs:

| Analyte | Approximate live coverage (ILIKE spot-check earlier) | Assessment |
|---------|------------------------------------------------------|------------|
| TSH | 0 rows (naive name match) | **Not present** in current waves |
| PTH | 0 | **Not present** |
| Calcium | 0 | **Not present** |
| Vitamin D | 0 | **Not present** |
| Thyroglobulin / Tg axis | Present via `thyroid_tumor_markers` + tg waves | **Present** (scope-aligned with current ingest) |

## Manuscript sufficiency

For any manuscript plank requiring **TSH / PTH / calcium / vitamin D** from the institutional lab spine, current coverage is **insufficient**: only Tg-family structured waves are loaded; the documented **institutional non-Tg extract** has not been appended as a final wave.

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

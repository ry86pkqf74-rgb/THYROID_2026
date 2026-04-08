# Institutional lab append readiness — script 127

**THYROID_2026 — 2026-04-08 UTC**

## Verdict: **ready for validated append workflow**; **no additional production ingest required** for the already-delivered wave unless you are shipping a *new* extract.

### CSV contract (`scripts/127_analyst_institutional_lab_append.py`)

**Required columns:**

- `research_id`
- `lab_date` (parsable as date)
- `value_raw`
- `source_lineage_key` — **unique per row** (institutional id / hash / stable composite)
- **`lab_name_standardized` *or* `lab_name_raw`** (at least one)

**Optional:** `value_numeric`, `unit_raw`, `unit_standardized`, `analyte_group`, `lab_date_status`, `source_table`, `provenance_note`, `is_censored`

**Default analyte_group** when omitted: `institutional_deliverable`

**Idempotency:** Re-running the same `--ingestion-wave` deletes **only** rows for that wave before insert.

### Repo candidate

| File | 127 `--dry-run` |
|------|-----------------|
| `exports/incoming/final_institutional_chemistry_20260407.csv` | **OK** — `Prepared 989 lab row(s), ingestion_wave=final_institutional_20260407` |

**Note:** `README.md` and `studies/20260411_final_master_release/EVIDENCE_PACK.md` state this wave (**`final_institutional_20260407`**) is **already present** on MotherDuck (`longitudinal_lab_canonical_v1` row counts reflect post-ingest state). Treat this CSV as the **deliverable template** and historical source file — **do not re-append the same wave** unless intentionally replacing that wave after QA.

### Dry-run validation (no MotherDuck write)

`build_frame` runs before connect; **no RW token required** for `--dry-run`:

```bash
cd /Users/loganglosser/THYROID_2026
.venv/bin/python scripts/127_analyst_institutional_lab_append.py \
  --input exports/incoming/final_institutional_chemistry_20260407.csv \
  --ingestion-wave final_institutional_20260407 \
  --dry-run
```

**Live append** (MotherDuck RW token required — **not executed** in this audit):

```bash
unset LOCAL_DB_PATH   # if it shadows RW JWT guard unexpectedly
export MOTHERDUCK_TOKEN='md_…'   # or MD_SA_TOKEN + --md-sa
.venv/bin/python scripts/127_analyst_institutional_lab_append.py --md --md-sa \
  --input exports/incoming/<new_institutional_labs>.csv \
  --ingestion-wave final_institutional_YYYYMMDD
```

### Analyte expectations

- Script does **not** enforce a closed vocabulary; **`analyte_group`** is carried through (default `institutional_deliverable`).
- Dedup view **`longitudinal_lab_deduped_v`** ranks `final_institutional*` waves favorably over older waves (see `DEDUP_VIEW_SQL` in script).
- For **non-Tg** manuscript claims, cross-check coverage in MotherDuck after ingest (see `README.md` lens **C**; `docs/lab_layer_scaffold_plan_20260313.md` for future analytes).

### Tests

- **`tests/test_127_analyst_institutional_lab_append.py`** — local DuckDB transaction / rollback / dedup (54 lab + tg tests passed in this audit run with `test_tg_lab_ingestion.py`).
- **`tests/test_tg_lab_ingestion.py`** — covers **script 113** (structured Tg/TgAb EHR), **not** 127; keep scope separate when citing test coverage.

### If a *new* institutional CSV arrives

1. Validate headers against §1.
2. Ensure **`source_lineage_key` uniqueness**.
3. Run **`127 --dry-run`**.
4. Optionally slot into **`126`** with `--lab-csv` and `--ingestion-wave final_institutional_YYYYMMDD` **after** MRQ/promotion readiness (see `HUMAN_REVIEW_READINESS.md`).

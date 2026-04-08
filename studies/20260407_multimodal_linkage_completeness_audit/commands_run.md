# Commands run (audit session)

```bash
cd /Users/loganglosser/THYROID_2026

# Required pytest bundle
.venv/bin/python -m pytest -q \
  tests/test_imaging_fna_linkage_mm_v1.py \
  tests/test_multimodal_contract_mm_v1.py \
  tests/test_specimen_identity_layer.py \
  tests/test_specimen_genomics_binding.py \
  tests/test_specimen_fhir_layer.py
```

**Exit code:** 0  
**Summary line:** `37 passed, 1 warning in 7.03s`

```bash
# Local DuckDB file presence check
test -f /Users/loganglosser/THYROID_2026/thyroid_master.duckdb && echo EXISTS || echo MISSING
```

**Output:** `EXISTS`

```bash
# Read-only schema / rowcount probe (workspace local file)
cd /Users/loganglosser/THYROID_2026 && .venv/bin/python - <<'PY'
import duckdb
con = duckdb.connect("thyroid_master.duckdb", read_only=True)
sch = con.execute(
    "SELECT DISTINCT table_schema FROM information_schema.tables "
    "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY 1"
).fetchall()
print("schemas:", [r[0] for r in sch])
want = [
    "imaging_nodule_master_v1",
    "fna_episode_master_v2",
    "molecular_test_episode_v2",
    "tumor_episode_master_v2",
    "preop_surgery_linkage_v3",
    "fna_molecular_linkage_v3",
    "surgery_pathology_linkage_v3",
    "imaging_fna_linkage_mm_v1",
    "path_synoptics_encounter_qc_v1",
    "synoptic_tumor_long_v1",
    "specimen_master_v1",
    "specimen_genomic_assay_v1",
]
for t in want:
    r = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        [t],
    ).fetchone()[0]
    if r:
        n = con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
        print(f"main.{t}: rows={n}")
    else:
        print(f"main.{t}: ABSENT")
mm = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'mm_contract_dev'"
).fetchone()[0]
print("mm_contract_dev object count:", mm)
con.close()
PY
```

**Output:** captured in `validation_summary.md` (schemas: `['main']`; several chain tables **ABSENT** on this file; `mm_contract_dev` count **0**).

## Not run

- `scripts/128_multimodal_contract_mm_v1.py --md` (writes catalog — excluded per **no prod mutation** / user consent for writes).
- `scripts/129_imaging_fna_linkage_mm_v1.py --md` (writes catalog).
- Any `ALTER`, `CREATE`, or `UPDATE` against user canonical local DuckDB beyond read-only `connect(..., read_only=True)` for inspection.

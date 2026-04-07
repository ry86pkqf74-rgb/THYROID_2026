# MotherDuck read-only preflight — agent onboarding

**Task folder:** `studies/motherduck_readonly_preflight_20260406_234811/`  
**Date:** 2026-04-06  
**Catalog:** `Thyroid 2026` (default per `motherduck_client.resolve_database_for_env` / contract)  
**Connection helpers:** `utils.md_connect.connect_md_fail_closed(Path("thyroid_master.duckdb"))`  
**Token disclosure:** This report records `token_mode()` only; no secret values.

## Commands run

Working directory: repo root `THYROID_2026/`.

```bash
cd "/Users/ros/THyroid 2026/THYROID_2026"
.venv/bin/python << 'PY'
from pathlib import Path
from motherduck_client import token_mode
from utils.md_connect import connect_md_fail_closed

print("token_mode:", token_mode())
con = connect_md_fail_closed(Path("thyroid_master.duckdb"))
cur_db = con.execute("SELECT current_database()").fetchone()
print("current_database:", cur_db[0] if cur_db else None)
dbs = con.execute("PRAGMA database_list").fetchall()
md_evidence = any("md:" in str(r) or "md_information_schema" in str(r) for r in dbs)
print("md_verified_pragma:", md_evidence)
print("pragma_database_list_first_8:")
for r in dbs[:8]:
    print(" ", r)
for schema in ("v2_stage", "main", "qa"):
    row = con.execute(
        f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = current_database()
          AND table_schema = '{schema}'
        """
    ).fetchone()
    print(f"information_schema.tables count [{schema}]:", row[0] if row else None)
rel = con.execute(
    """
    SELECT table_schema, COUNT(*) AS n
    FROM information_schema.tables
    WHERE table_catalog = current_database()
      AND table_schema LIKE 'release_%'
    GROUP BY 1
    ORDER BY 1
    LIMIT 15
    """
).fetchall()
print("release_* schemas (sample):", len(rel), "rows")
for r in rel[:10]:
    print(" ", r)
con.close()
PY
```

## Stdout (evidence)

```
token_mode: secrets.toml:MOTHERDUCK_TOKEN
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
current_database: Thyroid 2026
md_verified_pragma: True
pragma_database_list_first_8:
  (1027, 'md_information_schema', None)
  (1090, 'thyroid_research_ro_v2', '_share/thyroid_research_ro_v2/2558f066-1c5d-46a5-afbc-800fd5f7568d')
  (1092, 'sample_data', '_share/sample_data/23b0d623-1361-421d-ae77-62d701d471e6')
  (1094, 'Thyroid 2026', 'Thyroid 2026')
  (1096, 'rosflow', 'rosflow')
  (1098, 'my_db', 'my_db')
information_schema.tables count [v2_stage]: 38
information_schema.tables count [main]: 85
information_schema.tables count [qa]: 11
release_* schemas (sample): 3 rows
  ('release_20260407', 5)
  ('release_20260408', 10)
  ('release_20260409', 10)
```

## Interpretation

- **MotherDuck vs local file:** `PRAGMA database_list` includes `md_information_schema` and the `Thyroid 2026` catalog entry; `connect_md_fail_closed` printed the verification line. This is evidence the session reached MotherDuck, not a local `thyroid_master.duckdb` file path.
- **Schemas:** Contract schemas `v2_stage`, `main`, and `qa` are present with non-zero table counts. `release_*` snapshots exist as `release_YYYYMMDD` schemas in this catalog.
- **Operational note:** In this shell, `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN` were unset; `token_mode()` resolved to **Streamlit secrets** (`.streamlit/secrets.toml`). For CI, use `MD_SA_TOKEN` with `prefer_service_account=True` as documented in `docs/motherduck_database_contract_v1.md`.

## Files read for this session

- `AGENTS.md`
- `.github/copilot-instructions.md`
- `motherduck_client.py`
- `utils/md_connect.py`
- `docs/motherduck_database_contract_v1.md`

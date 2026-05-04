# Cursor Composer Dispatch — mig_293: Mirror SF VALIDATION_RUN_LOG to MD (audit trail)

**Generated:** 2026-05-04 by Cowork at HEAD `170ee3d`.
**Lane:** mig_293 — Cowork stood up `THYROID_VALIDATION.PUBLIC.VALIDATION_RUN_LOG_v1` + `VALIDATE_ALL_COHORTS()` SP on Snowflake (committed in `snowflake_trial/scripts/sf_infrastructure_deploy_v2.py`). The audit log lives only on SF. Mirror to MD as `main.cowork_sf_validation_log_v1` so downstream consumers (signoff dashboards, reproducibility audits) can see SF-side checks alongside MD-native signoff_migration.
**Recommended agent:** **Cursor Composer** — write a periodic SF→MD pull script + register MD table.
**Estimated runtime:** 30 min.
**Severity:** LOW (convenience for cross-platform audit).

---

## §0 — First message to paste into Cursor Composer

> mig_293 dispatch. Build SF→MD pull script for VALIDATION_RUN_LOG_v1 + create MD mirror table + add to refresh pipeline. MotherDuck DB is `thyroid_canonical_publication_v1_0`. SF source `THYROID_VALIDATION.PUBLIC.VALIDATION_RUN_LOG_V1`.

---

## §1 — Apply

### §1a — Create MD mirror table

```sql
CREATE TABLE IF NOT EXISTS main.cowork_sf_validation_log_v1 (
  sf_run_id BIGINT,
  sf_run_ts TIMESTAMP,
  check_name VARCHAR,
  expected VARCHAR,
  observed VARCHAR,
  status VARCHAR,
  notes VARCHAR,
  pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### §1b — SF→MD pull script

Add `snowflake_trial/scripts/35_pull_sf_validation_log.py`:

```python
"""Pull SF VALIDATION_RUN_LOG_v1 into MD main.cowork_sf_validation_log_v1."""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
import duckdb, pandas as pd

ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION; USE SCHEMA PUBLIC")
sf_df = cur.execute("SELECT * FROM VALIDATION_RUN_LOG_V1").fetch_pandas_all()
ctx.close()

md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")
md.execute("DELETE FROM main.cowork_sf_validation_log_v1")  # full refresh; could switch to incremental
md.execute("""
INSERT INTO main.cowork_sf_validation_log_v1 (sf_run_id, sf_run_ts, check_name, expected, observed, status, notes)
SELECT RUN_ID, RUN_TS, CHECK_NAME, EXPECTED, OBSERVED, STATUS, NOTES
FROM sf_df
""")
print(f"Mirrored {len(sf_df)} rows to MD")
md.close()
```

### §1c — Add to refresh pipeline

Append to `snowflake_trial/scripts/04_build_flat_views.py` postlude OR run as standalone post-validation step.

### §1d — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_293', CURRENT_TIMESTAMP, 'cursor_composer_mig293',
 'mig_293: Created main.cowork_sf_validation_log_v1 mirror of SF VALIDATION_RUN_LOG_v1 + added pull script (snowflake_trial/scripts/35_pull_sf_validation_log.py) for periodic refresh. Cross-platform audit trail enabled.');
```

---

## §2 — Surgical git add

```
qc_framework_v1/migrations/293_validation_log_md_mirror_20260504.sql
snowflake_trial/scripts/35_pull_sf_validation_log.py
scripts/output/mig_293_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_293_VALIDATION_LOG_MD_MIRROR_20260504.md
```

---

**End of mig_293 dispatch.**

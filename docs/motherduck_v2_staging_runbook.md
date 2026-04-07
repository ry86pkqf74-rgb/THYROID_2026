# MotherDuck v2 Canonical Staging Runbook

**Created**: 2026-04-03  
**Scope**: Staging v2 extraction/canonical outputs in MotherDuck while keeping extraction local-to-parquet.  
**Related docs**: `docs/motherduck_connection_hardening_20260403.md`, `docs/motherduck_canonical_upload_20260402.md`

---

## MotherDuck quickstart (env tokens, fail-closed)

1. **Set a read/write token** (never commit real values): copy `.env.motherduck.example` → `.env.motherduck` at repo root, or export `MOTHERDUCK_TOKEN` (personal) and/or `MD_SA_TOKEN` (CI). Optional: add the same keys to `.streamlit/secrets.toml` (gitignored).
2. **Verify token resolution**:  
   `.venv/bin/python -c "from motherduck_client import token_mode; m=token_mode(); print(m); assert m != 'none'"`
3. **Smoke-test MotherDuck** (fail-closed): `scripts/smoke_test_md_connection.py --md` uses `connect_md_fail_closed` and the same `PRAGMA database_list` verification as `utils/md_connect.py`. Exits 1 if there is no read/write token, the cloud connection fails, or the session is not actually attached to MotherDuck (no silent local fallback). Local-file behavior is unchanged when `--md` is omitted.  
   Run: `.venv/bin/python scripts/smoke_test_md_connection.py --md` or `make md-smoke` (Make pre-checks for `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN`).
4. **Stage vs canonical**: **New v2 domain parquets land in schema `v2_stage`** (`116_md_stage_loader.py --md`). **`main` holds promoted canonical / entity tables only after** gate pass + `motherduck_promote.sql` (and downstream materialization). Do not assume fresh extraction appears in `main` until promotion completes.
5. **Prefer fail-closed writes**: pass `--md` to in-scope scripts and use `connect_md_or_file(..., fail_closed=True)` (or `connect_md_fail_closed`) so unreachable MotherDuck exits 1 instead of silently using local `thyroid_master.duckdb`.

Full token matrix: `docs/motherduck_database_contract_v1.md` (Connection Reference).

---

## Architecture

| Layer | Location | Description |
|-------|----------|-------------|
| Extraction | Local (CPU/GPU) | LLM/regex extractors write `processed/output/v2_parquets/*.parquet` |
| Canonical materialization | Local → `processed/` | `scripts/103_fact_lineage_materialize.py` merges domains, writes `canonical_extracted_fact_long_v2.parquet` + `canonical_fact_quarantine_v2.parquet` |
| MotherDuck staging (current v2 landing zone) | `"Thyroid 2026".v2_stage` | `116_md_stage_loader.py --md` loads v2 domain parquets + `load_inventory`; optional `02b_register_notes_entities.py --md` registers additional entity tables per ops |
| MotherDuck canonical surface (post-promotion) | `"Thyroid 2026".main` | Promoted domains + `canonical_*` fact tables after gate + promotion SQL + materialization |
| QC / promotion gate | MotherDuck + local | `scripts/112_v2_domain_promotion_gate.py --motherduck-check` validates parity (`v2_stage` vs local parquets, etc.) |

MotherDuck is the **staging and QC plane only** — it is not the LLM compute engine.

### Schema isolation

- **`v2_stage` schema** — **default landing zone** for v2 LLM entity parquets from disk (`note_entities_llm_*`, `load_inventory`); this is where current staging lands before promotion
- **`main` schema** — promoted canonical tables; `_v1` and `_v2` suffixes prevent collision
- v1 tables in `main` are **never overwritten** by any v2 operation

---

## Required environment variables

| Variable | Purpose | Source |
|----------|---------|--------|
| `MOTHERDUCK_TOKEN` | Personal developer token | MotherDuck UI → Settings → Tokens |
| `MD_SA_TOKEN` | Service-account / CI token | GitHub Actions secret or team vault |
| `MOTHERDUCK_DATABASE` | Override DB name (default: `Thyroid 2026`) | Set only when targeting a non-default catalog |
| `MOTHERDUCK_ENV` | Target environment: `dev`, `qa`, `prod` (default: `prod`) | Optional; all envs currently map to `Thyroid 2026` |

Token resolution order (see `motherduck_client.py::get_token`):

1. `MOTHERDUCK_TOKEN` (interactive)
2. `MD_SA_TOKEN` (CI / service account)
3. `LOCAL_DB_PATH` when it looks like a JWT (`eyJ...`) or md PAT (`md_...`)
4. `.streamlit/secrets.toml` keys `MOTHERDUCK_TOKEN` → `MD_SA_TOKEN`

Optional: create `.env.motherduck` (gitignored) at repo root with:

```bash
MOTHERDUCK_TOKEN=your_personal_token_here
# or for CI:
# MD_SA_TOKEN=your_sa_token_here
```

Script `112_v2_domain_promotion_gate.py` loads `.env.motherduck` automatically when present.

---

## Preflight checks

Run from repo root with `.venv/bin/python` for all checks.

```bash
# 1. Verify a token is resolvable
.venv/bin/python -c "
from motherduck_client import token_mode
mode = token_mode()
print(f'Token source: {mode}')
assert mode != 'none', 'ERROR: no token found — set MOTHERDUCK_TOKEN or MD_SA_TOKEN'
"

# 2. Smoke-test the MotherDuck connection (fail-closed: exits 1 if unreachable)
.venv/bin/python scripts/smoke_test_md_connection.py --md

# 3. List tables in main + v2_stage schemas
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
rows = con.execute('''
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN ('"'"'main'"'"', '"'"'v2_stage'"'"')
    ORDER BY 1, 2
''').fetchall()
for schema, tbl in rows:
    print(f'  {schema}.{tbl}')
print(f'  Total: {len(rows)} tables')
con.close()
"

# 4. Verify v1 canonical tables are intact in MotherDuck
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
for tbl in ['canonical_extracted_fact_long_v1', 'canonical_fact_quarantine_v1']:
    try:
        cnt = con.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]
        print(f'  OK  {tbl}: {cnt:,} rows')
    except Exception as e:
        print(f'  MISSING  {tbl}: {e}')
con.close()
"

# 5. Verify local v2 parquet inputs exist (from LLM extraction)
ls -la processed/output/v2_parquets/*.parquet 2>/dev/null | head -30
echo "---"
ls -la processed/canonical_extracted_fact_long_v1.parquet \
        processed/canonical_fact_quarantine_v1.parquet
```

**All 5 checks must pass before proceeding to dry-run or promotion.**

---

## Dry-run flow

Dry-run validates the full pipeline without writing to MotherDuck.

```bash
# Step 1: Generate v2 canonical parquets locally (dry-run — no parquet written)
.venv/bin/python scripts/103_fact_lineage_materialize.py --dry-run

# Step 2: Validate v2 domain inventory + schema + concordance (no MD writes)
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
    --v2-parquets-dir processed/output/v2_parquets \
    --db-path thyroid_master.duckdb \
    --output-dir studies/v2_domain_promotion_gate_dryrun

# Step 3: Review gate scorecard (all 8 gates must be PASS or CONDITIONAL PASS)
cat studies/v2_domain_promotion_gate_dryrun/promotion_scorecard.csv

# Step 4: Review manual review queue (must be empty or all items verified=manual)
cat studies/v2_domain_promotion_gate_dryrun/manual_review_queue.csv

# Step 5: Inspect generated promotion SQL (do NOT execute yet)
cat studies/v2_domain_promotion_gate_dryrun/motherduck_promote.sql
```

Gate scorecard criteria:

| Gate | Criterion | Threshold |
|------|-----------|-----------|
| G1 | Registry domains with parquets | >0 |
| G2 | Schema completeness (core columns) | All present |
| G3 | Provenance columns | CONDITIONAL PASS accepted |
| G4 | Duplicate rate | ≤5% per domain |
| G5 | Date coverage (critical domains) | entity_date or note_date >0% |
| G6 | Concordance floor (critical domains) | ≥30% |
| G7 | Manual review queue | Empty or all verified |
| G8 | MotherDuck v2_stage parity | Row counts match (requires `--motherduck-check`) |

---

## Promotion flow

**Prerequisites**: all preflight checks pass; dry-run gate scorecard shows no FAIL.

```bash
# Step 1: Generate v2 canonical parquets (writes processed/canonical_extracted_fact_long_v2.parquet
#         and processed/canonical_fact_quarantine_v2.parquet)
.venv/bin/python scripts/103_fact_lineage_materialize.py
# Verify outputs:
ls -la processed/canonical_extracted_fact_long_v2.parquet \
        processed/canonical_fact_quarantine_v2.parquet

# Step 2: Register all canonical parquets in MotherDuck (fail-closed: exits 1 if no MD)
.venv/bin/python scripts/02b_register_notes_entities.py --md

# Step 3: Materialize v2 fact tables in MotherDuck (fail-closed)
.venv/bin/python scripts/103_fact_lineage_materialize.py --md

# Step 4: Confirm canonical_extracted_fact_long_v2 + canonical_fact_quarantine_v2
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
for tbl in ['canonical_extracted_fact_long_v2', 'canonical_fact_quarantine_v2',
            'canonical_extracted_fact_long_v1', 'canonical_fact_quarantine_v1']:
    cnt = con.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]
    print(f'  {tbl}: {cnt:,} rows')
con.close()
"

# Step 5: Inspect row counts by domain (v2 fact table)
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
print('=== canonical_extracted_fact_long_v2 by entity_type ===')
rows = con.execute('''
    SELECT COALESCE(entity_type, '"'"'UNKNOWN'"'"') AS domain,
           COUNT(*) AS n_rows,
           COUNT(DISTINCT research_id) AS n_patients
    FROM canonical_extracted_fact_long_v2
    GROUP BY 1
    ORDER BY 2 DESC
''').fetchall()
print(f'  {\"domain\":<40} {\"rows\":>9} {\"patients\":>9}')
for domain, n, pts in rows:
    print(f'  {domain:<40} {n:>9,} {pts:>9,}')
print()
print('=== canonical_fact_quarantine_v2 by entity_type ===')
rows = con.execute('''
    SELECT COALESCE(entity_type, '"'"'UNKNOWN'"'"') AS domain,
           COUNT(*) AS n_rows,
           COALESCE(quarantine_reason, '"'"'unspecified'"'"') AS reason
    FROM canonical_fact_quarantine_v2
    GROUP BY 1, 3
    ORDER BY 2 DESC
    LIMIT 30
''').fetchall()
for domain, n, reason in rows:
    print(f'  {domain:<40} {n:>8,}  reason={reason}')
con.close()
"

# Step 6: Confirm quarantine / manual-review counts
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
total_v2 = con.execute('SELECT COUNT(*) FROM canonical_extracted_fact_long_v2').fetchone()[0]
total_q2 = con.execute('SELECT COUNT(*) FROM canonical_fact_quarantine_v2').fetchone()[0]
pct = 100 * total_q2 / (total_v2 + total_q2) if (total_v2 + total_q2) > 0 else 0
print(f'  v2 canonical facts  : {total_v2:>10,}')
print(f'  v2 quarantined rows : {total_q2:>10,}  ({pct:.1f}% quarantine rate)')
con.close()
"

# Step 7: Run full promotion gate with MotherDuck parity check
RUN_LABEL=promote_$(date +%Y%m%d_%H%M)
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
    --v2-parquets-dir processed/output/v2_parquets \
    --db-path thyroid_master.duckdb \
    --motherduck-check \
    --run-label "${RUN_LABEL}"
echo "Artifacts: studies/v2_domain_promotion_gate_${RUN_LABEL}/"
```

---

## Rollback / fail-closed behavior

### Fail-closed guarantees

All four in-scope scripts now use `fail_closed=True` when `--md` is passed:

| Script | Behavior when MotherDuck unreachable |
|--------|--------------------------------------|
| `02b_register_notes_entities.py --md` | Exits 1, no local write |
| `103_fact_lineage_materialize.py --md` | Exits 1, no local write |
| `113_tg_lab_ingestion.py --md` | Exits 1, no local write |
| Any script using `connect_md_fail_closed()` | Exits 1, no local write |

Scripts run **without** `--md` always use the local `thyroid_master.duckdb` file regardless of the fail-closed setting.

### Rollback v2 tables from MotherDuck

If v2 tables were promoted but need to be removed:

```bash
.venv/bin/python -c "
from utils.md_connect import connect_md_or_file
from pathlib import Path
con = connect_md_or_file(Path('thyroid_master.duckdb'), md=True, fail_closed=True)
for tbl in ['canonical_extracted_fact_long_v2', 'canonical_fact_quarantine_v2']:
    con.execute(f'DROP TABLE IF EXISTS main.\"{tbl}\"')
    print(f'  Dropped: {tbl}')
con.close()
"
```

v1 tables (`canonical_extracted_fact_long_v1`, `canonical_fact_quarantine_v1`) are **never touched** by v2 promotion steps.

### Rollback entity registrations

If `02b_register_notes_entities.py --md` registered tables that need to be removed, list and selectively drop from the `main` schema. Entity tables have the `note_entities_*` prefix and are recreated on each `02b` run — they are safe to drop and re-register.

---

## Version isolation summary

| Table name | Version | Location | Mutated by v2 ops? |
|-----------|---------|----------|--------------------|
| `canonical_extracted_fact_long_v1` | v1 | MD `main` + `processed/` | No |
| `canonical_fact_quarantine_v1` | v1 | MD `main` + `processed/` | No |
| `canonical_extracted_fact_long_v2` | v2 | MD `main` + `processed/` | Yes (created) |
| `canonical_fact_quarantine_v2` | v2 | MD `main` + `processed/` | Yes (created) |
| `note_entities_llm_*` (15 tables) | v2 LLM | MD `v2_stage` | Independent |

The `_v1` / `_v2` suffix on all canonical tables guarantees no collision.

---

## Connection layer reference

All scripts must use `utils.md_connect.connect_md_or_file()` — never call `MotherDuckClient().connect_rw()` directly. See `docs/motherduck_connection_hardening_20260403.md` for the full list of patched scripts.

```python
from utils.md_connect import connect_md_or_file, connect_md_fail_closed

# Standard (fail-open): falls back to local file if MD unreachable
con = connect_md_or_file(DB_PATH, md=args.md)

# Fail-closed when --md is set: exits 1 if MD unreachable
con = connect_md_or_file(DB_PATH, md=args.md, fail_closed=args.md)

# Convenience alias: always MD, always fail-closed
con = connect_md_fail_closed(DB_PATH)
```

Token resolution is handled internally by `motherduck_client.get_token()`. Do not pass tokens as function arguments.

---

## CI and Make — formalization path (2026-04-07)

GitHub Actions (`.github/workflows/ci.yml`) runs a **motherduck-formalization** job after Ruff/Mypy + workflow YAML validation and the main MotherDuck lint job: `116_md_stage_loader.py --md --dry-run`, then `112_v2_domain_promotion_gate.py --motherduck-check` (same `--v2-parquets-dir` / `--db-path` defaults as local), then `119_md_formalization_validate.py --md`. That job sets **`LOCAL_DB_PATH` empty** and uses only **`MD_SA_TOKEN` / `MOTHERDUCK_TOKEN`**; do not print tokens in logs.

For **tag** pushes matching `refs/tags/v*` (and optional **manual** workflow dispatch), job **md-live-release-audit-dryrun** runs `124_md_live_release_audit.py --md --dry-run`.

**Make (fail-closed MotherDuck):** `make md-v2-gate-md-dryrun` (same three steps as CI), `make md-live-release-dryrun`, `make md-live-release-final`. **Local / legacy** gate without cloud: `make md-v2-gate-local-dryrun` (or `make md-v2-gate-dryrun`). Formalization CI needs `processed/output/v2_parquets` on the runner for gate G1/G8; restore via DVC or artifacts if github-hosted checkout has none.

---

## Formalized promotion runbook (v2 — 2026-04-07)

The full end-to-end promotion sequence, suitable for MotherDuck paid-plan workflows.
See also: `docs/motherduck_database_contract_v1.md` for the full schema and table catalog.

### Step 0: Preflight

```bash
# Verify token is resolvable
.venv/bin/python -c "
from motherduck_client import token_mode
mode = token_mode()
print(f'Token source: {mode}')
assert mode != 'none', 'ERROR: no token found'
"

# Smoke-test MD connection
.venv/bin/python scripts/smoke_test_md_connection.py --md
```

### Step 1: Stage refresh

```bash
.venv/bin/python scripts/116_md_stage_loader.py --md
```

Loads all v2 domain parquets into `v2_stage`, verifies row counts, writes to `v2_stage.load_inventory`.

### Step 2: Promotion gate

```bash
RUN_LABEL=promote_$(date +%Y%m%d_%H%M)
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
    --v2-parquets-dir processed/output/v2_parquets \
    --db-path thyroid_master.duckdb \
    --motherduck-check \
    --run-label "${RUN_LABEL}" \
    --output-dir "studies/v2_domain_promotion_gate_${RUN_LABEL}"
```

All 8 gates must be PASS. Review `promotion_scorecard.csv` and `manual_review_queue.csv`.

### Step 3: Hydrate QA tables

```bash
.venv/bin/python scripts/114_qa_schema_setup.py --md \
    --hydrate-from "studies/v2_domain_promotion_gate_${RUN_LABEL}"
```

### Step 4: Execute promotion SQL

Review the generated `motherduck_promote.sql` carefully, then execute it against MotherDuck.

### Step 5: Canonical materialization

```bash
.venv/bin/python scripts/103_fact_lineage_materialize.py --md
```

### Step 6: Contract views

```bash
.venv/bin/python scripts/117_md_contract_views.py --md --skip-canonical
```

### Step 7: Release snapshot

```bash
.venv/bin/python scripts/115_release_snapshot.py --md --tag $(date +%Y%m%d)
```

### Step 8: Parquet release bundle

```bash
.venv/bin/python scripts/118_parquet_release_bundle.py --md
```

### Step 9: Validation

```bash
.venv/bin/python scripts/119_md_formalization_validate.py --md
```

### MotherDuck paid-plan snapshot verification

```sql
-- Verify databases attached
SELECT * FROM duckdb_databases();

-- List schemas with table counts
SELECT table_schema, COUNT(*) AS n_tables
FROM information_schema.tables
GROUP BY table_schema
ORDER BY 1;

-- Check release schemas exist
SHOW SCHEMAS;

-- Verify release manifest
SELECT * FROM qa.release_manifest ORDER BY created_at DESC LIMIT 5;

-- Check query history (available in MD UI for audit)
-- MotherDuck paid plans retain 90-day query history
```

### Fail-closed script inventory (updated)

| Script | Behavior when MotherDuck unreachable |
|--------|--------------------------------------|
| `116_md_stage_loader.py --md` | Exits 1, no local write |
| `02b_register_notes_entities.py --md` | Exits 1, no local write |
| `103_fact_lineage_materialize.py --md` | Exits 1, no local write |
| `113_tg_lab_ingestion.py --md` | Exits 1, no local write |
| `117_md_contract_views.py --md` | Exits 1, no local write |
| `114_qa_schema_setup.py --md` | Exits 1, no local write |
| `115_release_snapshot.py --md` | Exits 1, no local write |
| `118_parquet_release_bundle.py --md` | Exits 1, no local write |
| `119_md_formalization_validate.py --md` | Exits 1, no local write |

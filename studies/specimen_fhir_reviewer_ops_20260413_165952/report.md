# Specimen / FHIR reviewer ops — live run report

**Run ID (export folder suffix):** `20260413_165952`  
**UTC:** 2026-04-13 (manifest `build_timestamp_utc` below)  
**Git SHA at export:** `619dc89243fb93a6177a9035a3a6b57a98159385`

## Commands executed (exact)

From repository root `/Users/ros/THyroid 2026`:

```bash
git fetch origin && git pull --rebase origin main
```

```bash
# Writer snapshot (RW via motherduck.local.toml / MD_SA_TOKEN — token not printed)
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod --prefer-sa
```

**Note:** The runbook sometimes shows `--md-sa`; in this repo the flag is **`--prefer-sa`** (prefer service-account token for `MotherDuckClient.connect_rw()`).

```bash
# FHIR NDJSON export (custom UA for reviewer-ops telemetry / attribution)
export MOTHERDUCK_CUSTOM_USER_AGENT=specimen_fhir_reviewer_ops_v1
export MOTHERDUCK_SESSION_HINT=specimen_fhir_reviewer_ops_v1
.venv/bin/python scripts/141_fhir_specimen_json_export.py --md --output-root exports
```

**Output directory created:** `exports/fhir_specimen_20260413_165952/` (gitignored).

```bash
# Verification query (count match — same RW connection pattern as 141)
.venv/bin/python -c "
from pathlib import Path
import sys
sys.path.insert(0, '.')
from utils.md_connect import connect_md_fail_closed
con = connect_md_fail_closed(Path('thyroid_master.duckdb'))
n = con.execute('SELECT COUNT(*) FROM main.fhir_bundle_specimen_export_v1').fetchone()[0]
print('fhir_bundle_specimen_export_v1 count:', n)
con.close()
"
```

**Attempted (not available in this environment — no read-scaling token in config):**

```bash
export MOTHERDUCK_SESSION_HINT=specimen_fhir_reviewer_ops_v1
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod
# RuntimeError: No read-scaling MotherDuck token. Set MD_READ_SCALING_TOKEN ...

export MOTHERDUCK_CUSTOM_USER_AGENT=specimen_fhir_reviewer_ops_v1
export MOTHERDUCK_SESSION_HINT=specimen_fhir_reviewer_ops_v1
.venv/bin/python scripts/141_fhir_specimen_json_export.py --read-scaling --output-root exports
# Same: no MD_READ_SCALING_TOKEN configured locally
```

## Export counts

| Metric | Value |
|--------|------:|
| `main.fhir_bundle_specimen_export_v1` (live query) | 10,139 |
| `export_source_row_count` (manifest) | 10,139 |
| `bundle_row_count` (NDJSON lines) | 10,139 |
| Export route | `bundle_table` (from `main.fhir_bundle_specimen_export_v1`) |

Per-table row counts from manifest `source_tables_main`:

- `fhir_patient_deid_map_v1`: 8,422  
- `fhir_specimen_v1`, `fhir_procedure_collection_v1`, `fhir_encounter_v1`, `fhir_bundle_specimen_export_v1`: 10,139 each  
- `fhir_episode_of_care_v1`: 9,486  

## Manifest excerpt (key fields)

```json
{
  "export_kind": "specimen_fhir_analytic_v1",
  "timestamp": "2026-04-13T16:59:55Z",
  "git_sha": "619dc89243fb93a6177a9035a3a6b57a98159385",
  "custom_user_agent": "specimen_fhir_reviewer_ops_v1",
  "motherduck_session_hint": "specimen_fhir_reviewer_ops_v1",
  "source_catalog": "Thyroid 2026",
  "source_views": ["main.fhir_bundle_specimen_export_v1"],
  "export_route": "bundle_table",
  "from_prebuilt_bundle_view": true,
  "export_source_row_count": 10139,
  "bundle_row_count": 10139
}
```

Full manifest: `exports/fhir_specimen_20260413_165952/manifest.json` (local only; not committed).

## Reviewer access method (this run)

**Used:** Operator path **A** — **RW token** (from `motherduck_client.get_token()` / `motherduck.local.toml`) for:

1. `CREATE SNAPSHOT OF "Thyroid 2026"` via script **136** `writer --prefer-sa`
2. Export via **141** `--md` with `MOTHERDUCK_CUSTOM_USER_AGENT=specimen_fhir_reviewer_ops_v1`

**Not exercised here:** Restricted hidden share (MotherDuck UI) or **read-scaling token** (`MD_READ_SCALING_TOKEN`) + **141** `--read-scaling` — requires org-issued read-scaling credentials not present in this workspace.

**Attach steps for reviewers (reference — org admin):**

1. Issue **read-scaling** token **or** invite reviewer to a **restricted** share with **Read** only on the target database/catalog.
2. Store `MD_READ_SCALING_TOKEN` in a reviewer secret channel (never commit). Optional: `MD_READ_SCALING_SESSION_HINT` (e.g. `thy_review_01`) for stable session affinity.
3. After each **writer** snapshot, reviewer runs **`REFRESH DATABASE`** on the read-scaling connection (or `scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod`).
4. Then: `MOTHERDUCK_CUSTOM_USER_AGENT=specimen_fhir_reviewer_ops_v1` (optional) + `scripts/141_fhir_specimen_json_export.py --read-scaling --output-root exports`

## Refresh instructions

- **Writers:** After material changes, run **136** `writer` so read replicas and share consumers see a snapshot boundary.
- **Readers (read-scaling or share):** Run **`REFRESH DATABASE`** (or **136** `reader`) **after** the writer snapshot, before relying on **141** `--read-scaling` or dashboard queries.

## Query history / telemetry

Queried `md_information_schema.recent_queries` for `query_text` containing `fhir_bundle_specimen_export_v1`:

- Rows returned: 4 (includes verification `COUNT(*)` and the large `SELECT cast(bundle_json ...)` from export).
- **`user_agent` column:** `duckdb/v1.5.1(osx_arm64) python/3.14` — matches project note that custom UA may not appear in this column; filter by `query_text` for operational audit.

## Artifacts

| Path | In git |
|------|--------|
| `exports/fhir_specimen_20260413_165952/specimen_bundles.ndjson` | No (gitignored) |
| `exports/fhir_specimen_20260413_165952/manifest.json` | No |
| `exports/fhir_specimen_20260413_165952/README.md` | No |
| This report | Yes |

No PHI or raw note text in bundle payloads (analytic de-identified FHIR only per script contract).

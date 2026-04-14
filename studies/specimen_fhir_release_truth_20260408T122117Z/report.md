> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# Specimen + analytic FHIR — live release truth (prod)

**Captured (UTC):** 2026-04-08 (see `motherduck_repo_state_prod.md` machine-generated timestamp)  
**Primary catalog:** `Thyroid 2026` (**prod**)  
**Writer attribution (119 run):** `specimen_fhir_release_truth_v2` / session hint `specimen_fhir_release_truth_v2`  
**Repo git SHA (at `144` generation):** `550d1938574086cc270e361fe4b413b7b486c3b0` — re-run `144` after your next commit to refresh.

## Catalog probe (prod)

| Field | Value |
|--------|--------|
| `current_database()` | `Thyroid 2026` |
| `md_information_schema.databases.type` | **DUCKLAKE** |
| Named `CREATE SNAPSHOT` | **Do not assume** native named semantics — use dev/qa/prepromote-backup runbook patterns. |

## Specimen / FHIR row counts (prod)

| Table | Rows |
|-------|-----:|
| `main.specimen_master_v1` | 10,139 |
| `main.specimen_tumor_focus_v1` | 11,103 |
| `main.specimen_genomic_assay_v1` | 10,862 |
| `main.fhir_bundle_specimen_export_v1` | 10,139 |

## `qa.release_manifest` (latest, prod)

| release_tag | git_sha | created_at |
|-------------|---------|------------|
| `20260408r4` | `d9b9dc9` | 2026-04-08 08:56:49.086697 |
| `20260408r3` | `a593544` | 2026-04-08 05:20:40.752314 |
| `20260408r2` | `a593544` | 2026-04-08 05:18:04.189662 |

**Checked-in artifact:** `exports/release_manifests/LATEST_MANIFEST.json` remains **stale** (March 2026 manuscript id / `8c18892`) — **authoritative** promotion tags are **`qa.release_manifest`** on the live catalog.

## `119_md_formalization_validate.py --md --release-mode` (prod)

**Outcome:** **PASS** — `33 PASS / 6 WARN / 0 FAIL` (exit 0).

### Check 13 (specimen + analytic FHIR)

| Sub-check | Result |
|-----------|--------|
| Tables present | **PASS** (10 objects) |
| Specimen master fingerprints | **PASS** (distinct) |
| `qa.val_specimen_contract_v1` | **PASS** (no FAIL rows) |
| `qa.val_specimen_genomic_binding_v1` | **PASS** (no FAIL rows) |
| QA diagnostics (142 + focus metrics) | **PASS** (clean) |
| Specimen-adjacent review burden | **WARN** — `genomic_link_review` open/pending **10,705**; `specimen_merge_review` open/pending **1** |

### Other WARNs (non–Check 13)

- **Canonical export parity:** local `processed/*.parquet` **absent** on this machine — **WARN** only; MotherDuck counts authoritative for this run (see `119_release_validation_prod/validation_report.md`). *Repo fix:* `119` now treats missing local canonical parquets as WARN, not FAIL, when MD counts are available (`scripts/119_md_formalization_validate.py`).
- **Molecular contract:** panel_version / assay dictionary pairing WARNs (ThyroSeq-style panels).

## QA sandbox catalog (`Thyroid 2026 Molecular QA 20260407`)

`144 --md-env qa` and `119 --release-mode --md-env qa` were also run for comparison. **QA is not specimen-complete:** `synoptic_tumor_long_v1` **absent** → Check **13 skipped**; synthetic MRQ / missing `molecular_testing` etc. **FAIL** release on QA. **Do not** use QA for specimen/FHIR sign-off until bootstrapped to mirror prod.

## Query telemetry

`md_information_schema.query_history` returned **no rows** for filtered specimen user_agent values in the sampled window (empty table). **RECENT_QUERIES** not queried in `144` after successful `query_history` — treat as **role/plan limitation or delay**; see `motherduck_repo_state_prod.md`.

## Exact commands (redact tokens)

```bash
# Prod truth (authoritative for specimen/FHIR)
python3 scripts/144_md_repo_current_state_summary.py --md --md-env prod \
  --output studies/specimen_fhir_release_truth_20260408T122117Z/motherduck_repo_state_prod.md \
  --also-write studies/CURRENT_MOTHERDUCK_REPO_STATE.md

python3 scripts/119_md_formalization_validate.py --md --release-mode --md-env prod \
  --md-user-agent specimen_fhir_release_truth_v2 \
  --md-session-hint specimen_fhir_release_truth_v2 \
  --output-dir studies/specimen_fhir_release_truth_20260408T122117Z/119_release_validation_prod
```

Use **`MOTHERDUCK_TOKEN`** or **`MD_SA_TOKEN`** in the environment, or **`motherduck.local.toml`** (never commit).

## Security

**If a JWT was pasted into chat or logs, rotate/revoke it in MotherDuck** and prefer `motherduck.local.toml` (gitignored) going forward.

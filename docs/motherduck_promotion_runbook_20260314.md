# MotherDuck Promotion Runbook — 2026-03-14

Authoritative step-by-step guide for promoting changes through the
DEV → QA → PROD environment chain.

---

## Environment Overview

| Environment | Database Name                   | Purpose                           |
|-------------|---------------------------------|-----------------------------------|
| `dev`       | `thyroid_research_2026_dev`     | Active development; may be dirty  |
| `qa`        | `thyroid_research_2026_qa`      | Validated staging; stable snapshot|
| `prod`      | `thyroid_research_2026`         | Live production; RO share exposed |

Only `prod` is accessible via the publication RO share link:
`md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c`

---

## Pre-Requisites

```bash
# 1. Activate virtual env
source .venv/bin/activate

# 2. Confirm token available
echo $MOTHERDUCK_TOKEN | head -c 10   # should start with "md_..."

# 3. Confirm script 26 MAP has 0 duplicates
python scripts/94_map_dedup_validator.py

# 4. Confirm git working tree is clean (optional but recommended)
git status
```

---

## Phase 1: DEV → QA Promotion

### Step 1.1 — Materialize into QA

Re-run full materialization using QA as the target database:

```bash
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/26_motherduck_materialize_v2.py \
    --md --db thyroid_research_2026_qa
```

> **Note:** Script 26 currently materializes into whichever DB the token
> connects to. If the script hard-codes `thyroid_research_2026`, you may need
> to temporarily set the env override or run with `--db` if supported.
> Verify target DB with `SELECT current_database()` before proceeding.

### Step 1.2 — Run Promotion Gate (dry-run first)

```bash
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/95_environment_promotion.py \
    --from dev --to qa --dry-run
```

Review output. All gates should show `PASS` or `SKIP`.

### Step 1.3 — Run Promotion Gate (live)

```bash
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/95_environment_promotion.py \
    --from dev --to qa
```

Expected output:
```
✓  PROMOTION DEV → QA : APPROVED
Promotion ID: promo_dev_qa_20260314_HHMMSS
```

A promotion receipt is written to `exports/release_manifests/`.

---

## Phase 2: QA → PROD Promotion

### Step 2.1 — Run Validation Tests on QA

```bash
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/29_validation_runner.py --md
```

Expected: 0 failing val_* tables.

### Step 2.2 — Run Manuscript Freeze Rebuild (dry-run) on QA

```bash
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/90_manuscript_freeze_rebuild.py \
    --md --dry-run
```

All CRITICAL_TABLE validations must pass.

### Step 2.3 — Materialize into PROD

```bash
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/26_motherduck_materialize_v2.py --md
```

This targets `thyroid_research_2026` (prod) by default.

### Step 2.4 — Run Full Promotion Gate (dev→qa→prod sequence)

```bash
MD_SA_TOKEN=... .venv/bin/python scripts/95_environment_promotion.py \
    --from qa --to prod --sa
```

This runs the `full+share` gate level which includes RO share accessibility.

Expected:
```
✓  PROMOTION QA → PROD : APPROVED
```

### Step 2.5 — Generate Release Manifest

```bash
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/96_release_manifest.py --md
```

Verify `overall_status = RELEASE_READY` in the output manifest.

### Step 2.6 — Tag the Release

```bash
git tag -a "v2026.03.14-release" -m "Final release hardening pass"
git push origin v2026.03.14-release
```

The tagged push triggers the `share-publication-check` and `promotion-gate`
CI jobs automatically.

---

## Phase 3: Manuscript Bundle Rebuild (Post-Promotion)

Run after prod promotion is confirmed:

```bash
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/90_manuscript_freeze_rebuild.py --md
```

Expected final lines:
```
MANUSCRIPT FREEZE REBUILD COMPLETE — ALL CHECKS PASSED
Bundle: exports/MANUSCRIPT_FREEZE_BUNDLE_20260314/
```

---

## Rollback Procedure

### Scenario A: Bad materialization into PROD

MotherDuck does not have built-in table versioning for personal databases.
The recommended rollback approach is:

1. **If QA content is intact**: Re-run Step 2.3 from a known-good QA state.
2. **If both QA and PROD are corrupted**: Roll back from a local DuckDB
   backup or the last Zenodo archive snapshot.
3. **For individual tables**: Restore from the most recent `*_backup` sidecar
   tables (created by scripts that follow the `CREATE TABLE x_backup AS ...`
   convention before destructive operations).

```bash
# Example: restore a single table from backup
.venv/bin/python -c "
import duckdb, os
token = os.environ['MOTHERDUCK_TOKEN']
con = duckdb.connect(f'md:thyroid_research_2026?motherduck_token={token}')
con.execute('CREATE OR REPLACE TABLE molecular_test_episode_v2 AS SELECT * FROM molecular_test_episode_v2_backup')
print('Restored from backup')
con.close()
"
```

### Scenario B: RO share returns stale data

The production RO share (`thyroid_research_ro`) reflects whatever is
materialized in `thyroid_research_2026`. After re-materializing a corrected
state, the share updates automatically (no manual re-share needed for existing
tables).

### Scenario C: Promotion gate blocked in CI

```
1. Fix the failing gate condition (e.g., metric out of range)
2. Re-materialize: python scripts/26_motherduck_materialize_v2.py --md
3. Re-run dry-run: python scripts/95_environment_promotion.py --from qa --to prod --dry-run
4. Confirm PASS, then commit and push or re-tag
```

---

## Gate Reference

| Gate name              | Level        | Blocks on   | Source              |
|------------------------|-------------|-------------|---------------------|
| Table existence        | smoke+      | FAIL        | CRITICAL_TABLES     |
| Metric bounds          | full+       | FAIL        | METRIC_BOUNDS (×11) |
| Row multiplication     | full+       | FAIL        | patient/episode dedup |
| Null core columns      | full+       | FAIL        | research_id null pct |
| Hardening tables       | full+       | WARN        | val_* tables        |
| MAP dedup              | all         | FAIL        | script 94           |
| RO share accessible    | full+share  | FAIL        | prod only           |

---

## Token Strategy

| Context             | Token var       | Notes                                |
|---------------------|-----------------|---------------------------------------|
| Local development   | MOTHERDUCK_TOKEN | From .streamlit/secrets.toml fallback |
| CI automated checks | MOTHERDUCK secret | GitHub Actions secret (personal)     |
| Production gating   | MD_SA_TOKEN     | Service-account token (GitHub secret) |
| Promotion gate      | MD_SA_TOKEN     | Required for `--sa` flag              |

---

## Checklist: Before Any Production Promotion

- [ ] `scripts/94_map_dedup_validator.py` exits 0
- [ ] `scripts/29_validation_runner.py --md` passes (0 FAIL rows in val_* tables)
- [ ] `scripts/90_manuscript_freeze_rebuild.py --md --dry-run` passes
- [ ] `scripts/96_release_manifest.py --md` shows `RELEASE_READY`
- [ ] git working tree is clean (or changes are intentional)
- [ ] Branch is `main` (or a tagged release branch)
- [ ] `scripts/95_environment_promotion.py --from qa --to prod --sa --dry-run` passes

# MotherDuck Release Promotion Runbook

**Project:** THYROID_2026  
**Date:** 2026-03-14  
**Who runs this:** Data engineer or lead developer before every tagged release.

---

## Overview

This runbook covers the full release cycle:

```
local dev work
     ↓ push main
     ↓ CI passes (lint + unit + motherduck-ci)
     ↓ run share publication check
     ↓ run promotion gate (qa → prod)
     ↓ tag release
     ↓ Streamlit Cloud picks up new tables
     ↓ announce
```

Estimated time for a well-prepared release: **15–25 minutes**.

---

## Pre-Promotion Checklist (Before Tagging)

Run these steps in order from the project root.

### Step 0 — Environment

```bash
cd "/Users/ros/THyroid 2026/THYROID_2026"
source .venv/bin/activate
# Verify token available
echo ${MOTHERDUCK_TOKEN:0:8}…  # should print token prefix, not empty
```

---

### Step 1 — Run Full Materialization (if new scripts were added)

Only required if the MotherDuck materialized tables have changed since the last
release.  Skip if you are publishing documentation or analysis artifact updates only.

```bash
.venv/bin/python scripts/26_motherduck_materialize_v2.py --md
```

Expected: all 131+ `md_*` tables refreshed, exit 0.

**MotherDuck hang prevention note:** If any table hangs for >5 min, kill the
process and re-run.  Script 26 uses `CREATE OR REPLACE TABLE` which is
idempotent.  Tables completed before the hang are preserved.

---

### Step 2 — Dashboard Smoke Test via RO Share

```bash
.venv/bin/python scripts/93_dashboard_smoke.py --share
```

Expected: all 25 queries PASS, no FAIL rows.  WARN (SLA) entries are acceptable.
Fix any FAIL before proceeding.

---

### Step 3 — RO Share Publication Check

```bash
.venv/bin/python scripts/94_share_publication_check.py
```

For CI / service-account use:

```bash
.venv/bin/python scripts/94_share_publication_check.py --sa --no-count-check
```

Expected exit 0.  On failure, the output will identify which check family (C1–C5)
failed.  Common issues and fixes:

| Symptom                             | Fix                                                |
|-------------------------------------|----------------------------------------------------|
| C2: table missing minimum rows      | Re-run `26_motherduck_materialize_v2.py --md`      |
| C2: table not found in share        | Table must be a TABLE (not VIEW) in prod; materialize it |
| C3: PHI column found                | Audit the view/table definition and remove the column |
| C4: catalog alias not resolving     | Verify share path in `motherduck_environments.yml` |
| C5: count drift > 2%                | Re-run `26_motherduck_materialize_v2.py --md` to sync |

---

### Step 4 — Promotion Gate (QA → Prod)

```bash
.venv/bin/python scripts/91_promotion_gate.py --from qa --to prod --sa
```

This runs 6 gates.  All must PASS before tagging.

```
G1 Critical tables exist .............. PASS/FAIL
G2 Canonical metric bounds ............. PASS/FAIL
G3 No row multiplication ............... PASS/FAIL
G4 Column null rates within ceiling .... PASS/FAIL
G5 Val tables: 0 FAIL rows (prod) ...... PASS/FAIL
G6 RO share accessible (prod) .......... PASS/FAIL
```

Expected: exit 0.  If any gate fails, do not tag.

---

### Step 5 — Update RELEASE_NOTES.md

Add an entry at the top of `RELEASE_NOTES.md`:

```markdown
## vYYYY.MM.DD - Release title

**Date:** YYYY-MM-DD

### Changed
- ...

### Scripts added / updated
- scripts/XX_...

### MotherDuck tables affected
- table_name — short description
```

---

### Step 6 — Tag the Release

```bash
git tag v2026.MM.DD-release
git push origin v2026.MM.DD-release
```

This triggers the `share-publication-check` and `promotion-gate` CI jobs.
Both must pass for the release to be considered complete.

Monitor at: `https://github.com/<owner>/<repo>/actions`

---

### Step 7 — Restart Streamlit Cloud

RO share data is cached by the dashboard with a 300-second TTL.  Force an
immediate refresh:

1. Go to [share.streamlit.io](https://share.streamlit.io) → your app.
2. Click **⋮ → Reboot app**.
3. Wait ~60 seconds for the app to come back online.
4. Verify the Overview tab shows the expected patient counts:
   - Surgical cohort: ~10,871
   - Analysis-eligible cancer: ~4,136

---

### Step 8 — Announce

Post a brief summary to the team (Slack / email / lab notebook) covering:
- What changed (data, scripts, analysis)
- New/updated tables
- Link to CI run and to Streamlit app

---

## Rollback Procedure

MotherDuck does not support PITR at the table level.  Rollback is a re-materialize
from the previous git commit.

```bash
# 1. Check out the last known-good release tag
git checkout v2026.MM.DD-previous-release

# 2. Re-run full materialization into prod
.venv/bin/python scripts/26_motherduck_materialize_v2.py --md

# 3. Verify smoke tests pass
.venv/bin/python scripts/93_dashboard_smoke.py --share

# 4. Reboot Streamlit Cloud app
```

Full re-materialization takes approximately 20–40 minutes depending on
MotherDuck duckling tier and table count.

---

## Emergency Share Revocation

If PHI is accidentally exposed via the RO share:

1. In the MotherDuck console, navigate to **Shares** → revoke the share immediately.
2. Remove the share path from `config/motherduck_environments.yml`.
3. In dashboard.py, set `_ACTIVE_CATALOG = "thyroid_research_2026"` as the
   fallback (bypasses share lookup).
4. Investigate which table/view contained the PHI column.
5. Drop or ALTER the offending table to remove PHI column.
6. Re-create and re-enable the share once PHI removal is verified.
7. Run `scripts/94_share_publication_check.py` C3 check to confirm.

---

## Quick Promotion Commands (All-in-One)

```bash
# Full pre-tag promotion sequence (assumes materialization already done)
cd "/Users/ros/THyroid 2026/THYROID_2026"
.venv/bin/python scripts/93_dashboard_smoke.py --share && \
.venv/bin/python scripts/94_share_publication_check.py --sa --no-count-check && \
.venv/bin/python scripts/91_promotion_gate.py --from qa --to prod --sa && \
echo "ALL GATES PASS — safe to tag"
```

If this command exits 0, proceed to Step 5 (RELEASE_NOTES) and Step 6 (tag).

---

## Reference

| Document                                          | Purpose                                |
|---------------------------------------------------|----------------------------------------|
| `docs/final_md_business_operationalization_20260314.md` | Architecture + config reference  |
| `config/motherduck_environments.yml`              | Environment database names             |
| `scripts/91_promotion_gate.py`                    | Promotion gate implementation          |
| `scripts/92_query_benchmark.py`                   | Performance regression suite           |
| `scripts/93_dashboard_smoke.py`                   | Dashboard smoke tests                  |
| `scripts/94_share_publication_check.py`           | Share PHI + table hardening check      |
| `exports/md_benchmark_20260314/`                  | Benchmark baseline and results         |

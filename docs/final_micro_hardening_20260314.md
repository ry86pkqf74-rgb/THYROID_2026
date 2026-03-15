# Final Micro-Hardening — 2026-03-14

Post-commit c87179a release-engineering clean-up pass.
All changes are additive, non-breaking, and independently
reversible.

---

## 1. RO Share Gate Split (scripts/95_environment_promotion.py)

### Problem
The single `ro_share` gate in `gate_ro_share()` opened a direct connection to
`md:thyroid_research_2026` (the prod DB) and queried `master_cohort`.
This validated prod-DB reachability but **not** the actual publication RO share
path (`md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c`)
that the Streamlit dashboard and external collaborators use.
A prod-DB-accessible / share-inaccessible failure would silently PASS the gate.

### Fix
Replaced `gate_ro_share()` with two independent gates, both evaluated only at
the `full+share` level (i.e., QA → PROD promotions):

| Gate name | What it checks | Connection used |
|-----------|---------------|-----------------|
| `prod_db_accessible` | `thyroid_research_2026` via direct token | `MotherDuckClient.for_env("prod").connect()` |
| `prod_ro_share_accessible` | `thyroid_share` catalog alias via share URL | `MotherDuckClient.for_env("prod").connect_ro_share()` |

Both gates count `DISTINCT research_id FROM master_cohort` and require ≥10,000
patients.  The share gate issues `USE thyroid_share;` before querying, matching
the exact connection path used by the dashboard.

### Backwards compatibility
- Old gate name `ro_share` no longer appears in promotion manifests; operators
  watching for that string in CI logs should update their grep patterns to
  `prod_db_accessible` / `prod_ro_share_accessible`.
- Gate count in the manifest increases from N to N+1 for a prod promotion (one
  gate replaced by two).  All bounds/metrics remain unchanged.

---

## 2. Docs Sync with Real CLI Interfaces

### 2a. `docs/final_release_hardening_20260314.md`

| Section | Before (incorrect) | After (correct) |
|---------|-------------------|-----------------|
| Gates run list (script 95) | "7. RO share accessible (prod only)" | Two gates listed: `prod_db_accessible` + `prod_ro_share_accessible` |
| Script 96 usage | `scripts/96_release_manifest.py --md` | `scripts/96_release_manifest.py` (no `--md`; default env=prod) |

### 2b. `docs/motherduck_promotion_runbook_20260314.md`

| Step | Before (incorrect) | After (correct) |
|------|--------------------|-----------------|
| Step 1.1 | `--db thyroid_research_2026_qa` (unsupported flag) | Explanatory note: script 26 targets the DB the token connects to; `--db` is not implemented; verify with `SELECT current_database()` |
| Step 2.5 | `scripts/96_release_manifest.py --md` | `scripts/96_release_manifest.py` (uses `--env`, default=prod) |

### Root cause
Script 96 was designed with `--env {dev|qa|prod}` from the start; the `--md`
shorthand was never implemented.  Script 26 was never given a `--db` flag;
multi-environment materialisation requires a token scoped to the target DB.

---

## 3. Manuscript Publication Provenance (scripts/90_manuscript_freeze_rebuild.py)

### Problem
The `source_audit_<ts>.json` artifact written into the bundle lacked:
- Full git SHA at freeze time
- Indication of which connection type was used (MotherDuck vs local DuckDB)
- The canonical RO share path (for external reproducibility)
- The list of critical tables validated in this freeze

### Fix
Added `_get_git_sha_full()` helper (returns the 40-char SHA).
Enriched the audit dict in `_verify_prod_source()` with four new fields:

| Field | Content |
|-------|---------|
| `git_sha` | Full 40-char HEAD SHA (`unknown` if git unavailable) |
| `source_connection_type` | `motherduck_prod` \| `local_duckdb` |
| `ro_share_path` | Full share URL when prod; `not_applicable` for local |
| `tables_validated` | The `CRITICAL_TABLES` constant from script 90 |

The existing fields (`actual_db`, `is_prod`, `override_used`, `status`) are
unchanged.  The enriched JSON is written to
`manuscript_cohort_freeze/source_audit_<ts>.json` and copied into the bundle
ZIP for easy inspection alongside the manuscript data.

---

## 4. Script 94 Parser Robustness (scripts/94_map_dedup_validator.py)

### Problem
The MATERIALIZATION_MAP termination condition used `line.strip() == "]"`.
This would fail to detect the closing bracket if:
- The line had trailing whitespace (`"]  "`)
- There was an inline comment (`"]  # 163 entries"`)
causing the parser to scan the rest of the file and potentially pick up
false-positive `"md_..."` references outside the MAP.

### Fix
Changed termination to:
```python
if in_map and re.match(r'^\s*\]\s*(#.*)?$', line):
    break
```
This accepts any combination of leading/trailing whitespace and an optional
`# comment` suffix, while still being strictly MAP-scoped.

---

## Summary

### What was fixed

1. **Script 95 RO share gate** — now validates the actual publication share
   path, not just the prod DB; split into two named gates for clear failure attribution.

2. **Docs CLI accuracy** — removed two non-existent flags (`--md` for script 96,
   `--db` for script 26) that would cause operators to get `unrecognized arguments`
   errors when following the runbook.

3. **Manuscript provenance artifact** — enriched with git SHA, connection type,
   RO share URL, and table list; the bundle now carries a self-describing
   provenance record suitable for peer review.

4. **Script 94 parser** — hardened against whitespace / comment variations at
   the closing `]` of MATERIALIZATION_MAP; no functional change for current
   map formatting.

### Remaining manual steps

None required before tagging.  The RO share gate will require
`MD_SA_TOKEN` (or a token with share-read permission) to pass in CI; this
was true of the old `ro_share` gate too.

### Release candidate status

**RELEASE_CANDIDATE_CLEAN** ✓

All four release-engineering mismatches identified post-c87179a are closed.
The CI pipeline, promotion gate, release manifest, and manuscript bundle are
now internally consistent with the actual script interfaces and MotherDuck
share topology.

| Area | Status |
|------|--------|
| Gate semantics (prod DB vs RO share) | ✓ FIXED |
| Docs CLI examples (`--md`, `--db`) | ✓ FIXED |
| Provenance artifact completeness | ✓ FIXED |
| Map parser robustness | ✓ FIXED |
| Metric values / analytical results | unchanged (no discrepancy found) |
| CI behaviour | unchanged (additive gate; same token requirements) |

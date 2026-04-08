# Specimen + analytic FHIR — release-truth reconciliation (repo session)

**Generated (UTC):** 2026-04-08T12:10:42Z  
**Repo git SHA:** use `git rev-parse HEAD` after checkout (this report is pinned by the commit that last touched this path).  
**MotherDuck writer UA / session (code default):** `specimen_fhir_release_truth_v2` / `specimen_fhir_release_truth_v2` (override via `MOTHERDUCK_CUSTOM_USER_AGENT` / `MOTHERDUCK_SESSION_HINT`).

## Live MotherDuck capture status (this CI/agent run)

**Not executed here:** no `motherduck.local.toml` (or `MD_SA_TOKEN` / `MOTHERDUCK_TOKEN`) was available in this environment. The following blocks must be filled by an operator with RW access.

| Expected field | Status |
|----------------|--------|
| `current_database()` | _pending local `--md` run_ |
| Catalog type / named snapshot policy | _script 144 now emits § Catalog probe when `--md` succeeds_ |
| `qa.release_manifest` (latest rows) | _pending_ |
| Check 13 (119 `--release-mode`) | _pending_ |
| Row counts: `specimen_master_v1`, `specimen_tumor_focus_v1`, `specimen_genomic_assay_v1`, `fhir_bundle_specimen_export_v1` | _pending_ |
| Query telemetry (`QUERY_HISTORY` / `RECENT_QUERIES`) | _pending; 144 filters v1+v2 specimen UAs_ |

### Operator command block (prefer QA, then prod)

Prefer **`qa`** when the token can attach to `Thyroid 2026 Molecular QA 20260407` (see `config/motherduck_environments.yml`). Use **prod** only when QA is uninitialized or permissions require it.

```bash
cd "/Users/ros/THyroid 2026"
# Ensure RW token resolves: motherduck.local.toml or MD_SA_TOKEN / MOTHERDUCK_TOKEN
STUDY="studies/specimen_fhir_release_truth_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$STUDY"

# 1) Fresh live state → study copy + canonical CURRENT file
python3 scripts/144_md_repo_current_state_summary.py --md --md-env qa \
  --output "$STUDY/motherduck_repo_state.md" \
  --also-write studies/CURRENT_MOTHERDUCK_REPO_STATE.md
# If QA attach fails, repeat with: --md-env prod

# 2) Formal validation (same catalog as above)
python3 scripts/119_md_formalization_validate.py --md --release-mode --md-env qa \
  --md-user-agent specimen_fhir_release_truth_v2 \
  --md-session-hint specimen_fhir_release_truth_v2 \
  --output-dir "$STUDY/119_release_validation"
# If Check 13 fails on missing v_diag_* : python3 scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md
# If specimen tables missing/stale: python3 scripts/138_md_specimen_fhir_layer.py --md  (qa first)

# 3) Merge key stdout + validation_report.md excerpts back into this report (or replace this file).
```

**Connect helper:** `utils/md_connect.connect_md_or_file(..., fail_closed=True)`; **never** use `MD_READ_SCALING_TOKEN` for writes.

---

## Checked-in narrative reconciliation

| Artifact | Role | Freshness |
|----------|------|-----------|
| **`studies/specimen_fhir_release_truth_20260408T121042Z/report.md`** (this file) | **Current** authoritative *reconciliation* story for writer UA v2 + operator checklist | **Structural** truth; **live** numbers require commands above |
| **`README.md`** (top “Source of truth” section) | **Current** reader-facing index; updated in this session to point here + describe manifest lag | Edited 2026-04-08 |
| **`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`** | **Canonical** auto-summary path for `144` | **Stale** until a successful local `--md` run overwrites via `--also-write` |
| **`studies/specimen_fhir_release_truth_20260408T065318Z/`** | Historical specimen/FHIR rebaseline | **Superseded** for “latest folder” pointers; retained for audit |
| **`studies/manuscript_blocker_rebaseline_20260408T073548Z/`** | Manuscript lens + MRQ triage | **Conditionally true**; governance narrative may still apply; not the specimen-USSOT |
| **`studies/20260411_final_master_release/EVIDENCE_PACK.md`** | Final-master operator evidence (labs, release_20260411, etc.) | **Current** for that program; **complements** (does not replace) specimen/FHIR QA story |
| **`studies/20260409_final_master_release/EVIDENCE_PACK.md`** | Point-in-time tag `20260409` | **Superseded** — file banner points to 20260411 pack |
| **`studies/20260409_final_master_release/SAFE_TO_START_STATS_MEMO.md`** | Preconditions memo for 20260409 | **Stale / historical** — verify gates on live MD before reuse |
| **`studies/20260407_publication_signoff_live/final_verdict_memo.md`** | Publication governance | **Conditionally true** — automation vs governance vs source-limited trichotomy; **not** a live row-count SSOT |

---

## Checked-in vs cloud release manifests

| Source | Notes |
|--------|-------|
| `exports/release_manifests/LATEST_MANIFEST.json` | **Point-in-time Git artifact** (e.g. `release_8c18892_20260315_170027`, `git_sha` `8c18892`) — **lags** live promotion history |
| `qa.release_manifest` on MotherDuck | **Authoritative** ordered promotion tags for the attached catalog — refresh via `144 --md` |

---

## Code changes in this session (UA v2 + probe)

- `utils/md_pipeline_attribution.py`: default specimen writer UA / session hint → **`specimen_fhir_release_truth_v2`**.
- `scripts/144_md_repo_current_state_summary.py`: **`--md-env`**, **`--also-write`**, **catalog probe** (type + snapshot policy note), telemetry lists **v1 + v2**, optional **`recent_queries`** fallback.
- `docs/specimen_fhir_contract_review.md`, `docs/motherduck_database_contract_v1.md`, `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`: documentation alignment to v2.

---

## Issues fixed (repo)

1. **Writer attribution drift:** v1 → **v2** for specimen/FHIR release truth (contract docs + attribution module + 144 telemetry filters).
2. **144 gap:** no environment selector / no dual output / no catalog preflight — **added** (`--md-env`, `--also-write`, probe section).
3. **README / study pointer drift:** older `specimen_fhir_release_truth_20260408T065318Z` **superseded** as the “latest folder” link in README (replaced with this folder + explicit manifest note).

---

## Telemetry note

If `md_information_schema.query_history` / `recent_queries` are blocked or show driver-default `user_agent` strings, state that explicitly in the operator’s paste-back —see `docs/specimen_fhir_contract_review.md`.

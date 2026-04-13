# GitHub public sync / specimen–FHIR visibility audit

**Generated:** 2026-04-13T15:32:14Z (UTC)  
**Repo:** `https://github.com/ry86pkqf74-rgb/THYROID_2026`  
**Task:** Reconcile local / `origin` / GitHub so 2026-04-08 specimen/FHIR work is visible on `origin/main`, document truth, refresh stale pointers where appropriate.

## Local HEAD

| Item | Value |
|------|--------|
| Branch | `main` |
| `HEAD` | `6892495d4e4b44fa906433bba9242c600d73e2a1` |
| Short | `6892495` — feat: add YAML data contract gate (145) with hash-chained audit |

## `origin/main` before / after `git fetch --all --prune`

| | SHA |
|---|-----|
| Before fetch (local ref) | `6892495d4e4b44fa906433bba9242c600d73e2a1` |
| After fetch | `6892495d4e4b44fa906433bba9242c600d73e2a1` |

**Result:** Local `main` and `origin/main` were **already identical**; **no push** was required to publish specimen/FHIR commits.

## Commits vs `origin/main`

All of the following are **ancestors of** `origin/main` (`git merge-base --is-ancestor <sha> origin/main` → success):

| Commit | Present on `origin/main` |
|--------|---------------------------|
| `b80ace7` | Yes |
| `ab30d88` | Yes |
| `9e23ee9` | Yes |
| `4041e36` | Yes |
| `b9e219f` | Yes |

## Study folders (local + GitHub `main`)

Paths exist under `studies/` locally **and** resolve on GitHub **`main`** via `gh api repos/.../contents/<path>?ref=main` (object `sha` returned for each):

- `studies/specimen_fhir_release_truth_20260408T141710Z/` (checked: `report.md`)
- `studies/specimen_fhir_focus_diag_hardening_20260408_210500/` (checked: `report.md`)
- `studies/specimen_fhir_ref_integrity_20260408_103349/` (checked: `report.md`)
- `studies/specimen_fhir_export_restore_20260408_204500/` (checked: `report.md`)
- `studies/specimen_fhir_ci_hardening_20260408_143842/` (checked: `report.md`)
- `studies/manuscript_blocker_rebaseline_20260408T144500Z/` (checked: `report.md`)

## Root cause analysis (why “not visible” might have been suspected)

- **Wrong branch / failed push:** Not observed — `main` matches `origin/main` at `6892495`.
- **Mirror lag:** Not observed — `fetch` did not advance `origin/main`.
- **Private repo + anonymous raw URLs:** The repository is **`private`** (`gh api repos/...` → `"private": true`). Anonymous `https://raw.githubusercontent.com/.../main/...` and `https://github.com/.../raw/main/...` return **404** without authentication. **Authenticated** access (browser session, `gh`, PAT, or Git clone) is required. This is **not** a missing-commit problem on `main`.
- **`CURRENT_MOTHERDUCK_REPO_STATE.md` stale guard:** The checked-in file listed **Commit SHA** `bd785614629b99589ee12c3d9d662ef87b037f14`, which **does not match** current `HEAD` and is **not contained in** current `main` (orphaned / amended lineage). That made the doc **incorrectly** flag live bullets as stale relative to the branch tip even though the specimen/FHIR study folders are on `main`. **Remediation:** Align **Commit SHA** to `6892495d4e4b44fa906433bba9242c600d73e2a1` and note that live MotherDuck introspection was not re-run in this sync (no RW token in environment).

## Public URL verification

| URL pattern | Anonymous result | Notes |
|-------------|------------------|--------|
| `https://raw.githubusercontent.com/ry86pkqf74-rgb/THYROID_2026/main/README.md` | HTTP 404 | Expected for **private** repo without auth |
| GitHub Contents API with `gh` + `ref=main` | 200 / object returned | Confirms files on **`main`** |

**Last push to origin (from API):** `pushed_at` ≈ **2026-04-08T17:14:02Z** (repo metadata at audit time).

## README / top-level pointers

`README.md` on `main` already references the authoritative 2026-04-08 specimen/FHIR paths (e.g. `studies/specimen_fhir_release_truth_20260408T141710Z/report.md`, manuscript rebaseline folder). **No README edit** was required for this sync-only task beyond verifying those paths exist on GitHub.

## Exact commands run

```bash
cd "/Users/ros/THyroid 2026"
git remote -v
git branch --show-current
git rev-parse HEAD
git log --oneline --decorate -n 20
git fetch --all --prune
git rev-parse origin/main
git log --oneline origin/main -n 20
for c in b80ace7 ab30d88 9e23ee9 4041e36 b9e219f; do
  git merge-base --is-ancestor "$c" origin/main && echo "$c OK" || echo "$c MISSING"
done
curl -sI "https://raw.githubusercontent.com/ry86pkqf74-rgb/THYROID_2026/main/README.md" | head -5
gh api repos/ry86pkqf74-rgb/THYROID_2026 --jq '{default_branch, private, pushed_at}'
gh api "repos/ry86pkqf74-rgb/THYROID_2026/contents/README.md?ref=main" --jq '{name, sha}'
# … plus contents API for studies paths listed above
.venv/bin/python scripts/144_md_repo_current_state_summary.py --md
# Exit 1: no MotherDuck RW token in environment — documented above
```

## Files changed in this task (scope)

- `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` — `Commit SHA` aligned to `HEAD`; sync note added.
- `studies/github_public_sync_20260413T153214Z/report.md` — this report.

**Force-push:** Not used. **Not justified:** `main` was not behind/ahead of `origin` in a way requiring history rewrite.

## Post-push `origin/main` (this task)

Docs-only commits pushed to `origin/main` for this audit:

1. `1a4791086d100ce2197d99f599a9dca5e458f8e1` — sync report (initial) + `CURRENT_MOTHERDUCK_REPO_STATE.md` SHA alignment.
2. `c1323e9be7fa25220c9a6224ccd3b56b836e646c` — report correction (accurate full SHA for item 1).
3. *(follow-up)* — small report edits (wording / post-push section); see `git log --oneline -5` on `main` for the exact tip.

**Verify tip:** `git fetch origin && git rev-parse origin/main` (must equal local `main` after a successful push).

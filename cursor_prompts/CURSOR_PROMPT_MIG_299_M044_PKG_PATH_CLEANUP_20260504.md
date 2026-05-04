# Cursor Composer Dispatch — mig_299: M044/M038 package hardcoded path cleanup (`/Users/loganglosser/`)

**Generated:** 2026-05-04 by Cowork at HEAD `e590e40`.
**Lane:** mig_299 — `M044_submission_package_v1_0/08_analysis_code/build_m044_master_excel.py` (and similar build scripts in M038) have hardcoded paths to `/Users/loganglosser/THYROID_2026/...` from the prior session. Logan's current path is `/Users/ros/THyroid 2026`. These scripts won't run for reproducibility checks until paths are corrected.
**Recommended agent:** **Cursor Composer** — mechanical find/replace.
**Estimated runtime:** 20 min.
**Severity:** LOW (cosmetic/reproducibility).

---

## §0 — First message

> mig_299 dispatch. Find/replace `/Users/loganglosser/THYROID_2026` → `/Users/ros/THyroid 2026` (or use `Path(__file__).resolve().parents[N]` for portability) across all M044 + M038 + M025 + M032 + M037 submission package build scripts.

## §1 — Audit

```bash
cd "/Users/ros/THyroid 2026"
grep -rn "/Users/loganglosser" M044_submission_package_v1_0/ M038_submission_package_v1_0/ M025_submission_package_v1_0/ M032_submission_package_v1_0/ M037_submission_package_v1_0/ scripts/ 2>/dev/null
```

## §2 — Apply

Per file: replace the hardcoded prefix with `Path(__file__).resolve().parents[N] / "submission_package_v1_0" / "..."` to make paths relative to script location.

## §3 — Test

Run a single script (e.g., `python M044_submission_package_v1_0/08_analysis_code/build_m044_master_excel.py --dry-run` if supported, else just `--help`) and confirm no hardcoded-path errors.

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_299', CURRENT_TIMESTAMP, 'cursor_composer_mig299',
 'mig_299: Replaced N hardcoded /Users/loganglosser/ paths with portable Path(__file__).resolve().parents[] across M044/M038/M025/M032/M037 submission package build scripts. Reproducibility audit unblocked.');
```

## §5 — Surgical git add

Per touched file. Expected ~10-20 file edits.

```
qc_framework_v1/migrations/299_pkg_path_cleanup_20260504.sql
scripts/output/mig_299_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_299_M044_PKG_PATH_CLEANUP_20260504.md
```

---

**End of mig_299 dispatch.**

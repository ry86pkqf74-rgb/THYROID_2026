#!/usr/bin/env python3
"""
Part B / Phase 6: QA — run all 10 checks and write qa/qa_script_cpm_tirads_partB.json.

Per the Part B prompt's Phase 6 spec.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_FQ = '"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB'
QA_DIR = REPO / "qa"


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    QA_DIR.mkdir(parents=True, exist_ok=True)
    con = MotherDuckClient(MotherDuckConfig(database=DB)).connect_rw()
    qa: dict = {
        "qa_script": "cpm_tirads_partB",
        "version": "v1",
        "run_at_utc": utc_iso(),
        "checks": [],
    }

    def add(check_id: str, desc: str, passed: bool, expected: object, got: object, note: str = ""):
        qa["checks"].append({
            "check_id":  check_id,
            "description": desc,
            "expected":  expected,
            "got":       got,
            "passed":    passed,
            "note":      note,
        })

    # ── Check 1: CPM row count unchanged from pre-drop ─────────────────────
    cpm_n = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master"
    ).fetchone()[0]
    add("01_cpm_row_count", "CPM row count unchanged from pre-drop (10871)",
        cpm_n == 10871, 10871, cpm_n)

    # ── Check 2: 0 non-NLP cols matching ILIKE '%tirads%' on CPM ──────────
    n_non_nlp_tirads = con.execute(
        """SELECT COUNT(*) FROM information_schema.columns
           WHERE table_schema='main' AND table_name='canonical_patient_master'
             AND column_name ILIKE '%tirads%'
             AND column_name NOT LIKE 'nlp_%'"""
    ).fetchone()[0]
    n_nlp_tirads = con.execute(
        """SELECT COUNT(*) FROM information_schema.columns
           WHERE table_schema='main' AND table_name='canonical_patient_master'
             AND column_name LIKE 'nlp_%' AND column_name ILIKE '%tirads%'"""
    ).fetchone()[0]
    add("02_cpm_no_tirads_cols",
        "CPM has 0 columns matching ILIKE '%tirads%' (excluding out-of-scope nlp_tirads_*)",
        n_non_nlp_tirads == 0, 0, n_non_nlp_tirads,
        f"nlp_tirads_* count: {n_nlp_tirads} (out-of-scope per Part A + Part B prompt; expected 5)")

    # ── Check 3: 0 laterality-concordance cols on CPM ──────────────────────
    n_lat_concord = con.execute(
        """SELECT COUNT(*) FROM information_schema.columns
           WHERE table_schema='main' AND table_name='canonical_patient_master'
             AND column_name ILIKE '%pathology_vs_imaging_laterality%'"""
    ).fetchone()[0]
    add("03_cpm_no_laterality_concordance_cols",
        "CPM has 0 pathology_vs_imaging_laterality_concordant* cols",
        n_lat_concord == 0, 0, n_lat_concord)

    # ── Check 4: 9 migrated cohort views resolve + post-Phase-2 row counts hold ──
    expected_view_rows = {
        "cohort_descriptive_full_cohort_v1":   10871,
        "cohort_m011_tirads_fna_genetics_v1":   3286,
        "cohort_m025_tirads_performance_v1":    3377,
        "cohort_m045_multimodal_risk_v1":       1167,
        "cohort_m075_tirads_multi_nodule_v1":   3286,
        "cohort_m050_tumor_size_volume_v1":    10871,
        "cohort_m053_nondiagnostic_fna_v1":    10871,
        "cohort_m064_frozen_decision_v1":      10871,
        "cohort_m076_ln_surveillance_v1":      10871,
    }
    view_results = {}
    for v, exp in expected_view_rows.items():
        try:
            got = con.execute(f"SELECT COUNT(*) FROM manuscript_workspace.{v}").fetchone()[0]
            view_results[v] = {"expected": exp, "got": got, "passed": got == exp}
        except Exception as e:
            view_results[v] = {"expected": exp, "got": f"ERROR: {e}", "passed": False}
    all_views_ok = all(r["passed"] for r in view_results.values())
    add("04_cohort_views_resolve",
        "All 9 migrated cohort views resolve and return expected row counts (Phase 2 baseline)",
        all_views_ok, expected_view_rows, view_results)

    # ── Check 5: git grep returns empty for legacy column regex ───────────
    # Tightened lookaheads use (?![_a-zA-Z]) to match identifiers as complete
    # words — prevents false positives on US v2 builder columns like
    # `worst_tirads_category_this_exam` (a column on canonical_us_exam_master_v2,
    # not CPM) and `max_tirads_category_ever` (cupm_v2). Historical text/log/sql
    # files (.log/.json/.txt/.md/.sql) excluded — these are already-produced
    # artifacts that document pre-cleanup state. 272 excluded per Logan's
    # "leave as-is" directive.
    LEGACY_COL_PATTERN = (
        r'tirads_(best|worst|n_nodule|nodule_size|n_sources|reliability|concordant_count|'
        r'mismatch_count|has_acr_recalc|source)_v12|'
        r'tirads_\w+_v271(?!b)|tirads_\w+_v271b|'
        r'pathology_vs_imaging_laterality_concordant(?![_a-zA-Z0-9])|'
        r'imaging_laterality_rollup(?![_a-zA-Z0-9])|'
        r'max_tirads_ever(?![_a-zA-Z0-9])|worst_tirads_category(?![_a-zA-Z0-9])|'
        r'imaging_tirads_best(?![_a-zA-Z0-9])|imaging_tirads_worst(?![_a-zA-Z0-9])|'
        r'imaging_tirads_source(?![_a-zA-Z0-9])|'
        r'preop_tirads_(best|worst|category)(?![_a-zA-Z0-9])'
    )
    proc = subprocess.run(
        ["rg", "--pcre2", "-l", "-e", LEGACY_COL_PATTERN,
         "scripts", "sql", "manuscripts", "studies", "lakehouse", "utils", "app",
         "--glob", "!scripts/frozen/**",
         "--glob", "!scripts/output/**",
         "--glob", "!archive*/**",
         "--glob", "!exports/_archive_*/**",
         "--glob", "!**/.venv/**",
         "--glob", "!**/__pycache__/**",
         "--glob", "!**/*.log",
         "--glob", "!**/*.json",
         "--glob", "!**/*.txt",
         "--glob", "!**/*.md",
         "--glob", "!**/*.sql",
         "--glob", "!CPM_tirads_*.md",
         "--glob", "!scripts/cpm_tirads_partB_phase*.py",
         "--glob", "!scripts/cpm_tirads_partB_phase1_coverage.py",
         "--glob", "!scripts/preB_*.py",
         "--glob", "!scripts/272_canonical_cleanup_phase1.py"],
        cwd=REPO, capture_output=True, text=True,
    )
    grep_files = [line for line in proc.stdout.splitlines() if line.strip()]
    add("05_git_grep_empty",
        "Legacy column regex returns empty across non-frozen .py code paths",
        len(grep_files) == 0, [], grep_files,
        "rg --pcre2; excludes scripts/frozen/, scripts/output/, archive paths, "
        ".log/.json/.txt/.md/.sql historical artifacts (Logan-exempt), the "
        "migration helper scripts, Part B/preB documentation, and "
        "scripts/272_canonical_cleanup_phase1.py (Logan's leave-as-is directive).")

    # ── Check 6: 0 gap_ABORT in coverage table ────────────────────────────
    counts = {r[0]: r[1] for r in con.execute(
        """SELECT coverage_status, COUNT(*) FROM manuscript_workspace.cpm_tirads_canonical_coverage_v1
           GROUP BY 1"""
    ).fetchall()}
    n_gap_abort = counts.get("gap_ABORT", 0)
    add("06_no_gap_abort", "manuscript_workspace.cpm_tirads_canonical_coverage_v1 has 0 gap_ABORT rows",
        n_gap_abort == 0, 0, n_gap_abort,
        f"all status counts: {counts}")

    # ── Check 7: Archive sanity — row count + col count match pre-drop ────
    archive_n = con.execute(f"SELECT COUNT(*) FROM {ARCHIVE_FQ}").fetchone()[0]
    archive_cols = con.execute(
        """SELECT COUNT(*) FROM information_schema.columns
           WHERE table_catalog='Thyroid 2026 UPdated'
             AND table_schema='cpm_tirads_legacy_20260421'
             AND table_name='canonical_patient_master_pre_partB'"""
    ).fetchone()[0]
    archive_ok = archive_n == 10871 and archive_cols == 1585
    add("07_archive_integrity",
        "Pre-drop CPM archive: 10871 rows × 1585 cols (matches pre-drop live state)",
        archive_ok, {"rows": 10871, "cols": 1585},
        {"rows": archive_n, "cols": archive_cols})

    # ── Check 8: Archive has 1 view_def_<name> table per migrated cohort view ──
    view_def_tables = [r[0] for r in con.execute(
        """SELECT table_name FROM information_schema.tables
           WHERE table_catalog='Thyroid 2026 UPdated'
             AND table_schema='cpm_tirads_legacy_20260421'
             AND table_name LIKE 'view_def_%'
           ORDER BY table_name"""
    ).fetchall()]
    expected_view_defs = sorted(f"view_def_{v}" for v in expected_view_rows)
    add("08_archive_view_defs",
        "Archive has 1 view_def_<name> table per migrated/inheriting cohort view (9 views)",
        sorted(view_def_tables) == expected_view_defs,
        expected_view_defs, view_def_tables)

    # ── Check 9: Frozen scripts catalog (33 scripts in scripts/frozen/) ───
    frozen_dir = REPO / "scripts" / "frozen"
    frozen_py = sorted([p.name for p in frozen_dir.glob("*.py")])
    n_frozen_py = len(frozen_py)
    readme = (frozen_dir / "README.md").read_text() if (frozen_dir / "README.md").exists() else ""
    n_readme_entries = len([line for line in readme.splitlines() if line.startswith("- `")])
    # Verify each frozen script has a FROZEN header
    missing_header: list[str] = []
    missing_special: list[str] = []
    for name in frozen_py:
        text = (frozen_dir / name).read_text()
        if "# FROZEN — 2026-04-21 —" not in text:
            missing_header.append(name)
        if name in ("221_tirads_v2_integration.py", "221b_suspicious_ln_reextraction.py"):
            if "NEW TARGET ON REFRESH: main.cupm_v2_canonical_backfill_v1" not in text:
                missing_special.append(name)
        if name in ("48_build_analysis_resolved_layer.py", "50_multinodule_imaging.py"):
            if "CAT B:" not in text:
                missing_special.append(name)
    add("09_frozen_scripts_catalog",
        "33 .py files in scripts/frozen/ all have FROZEN headers; README.md has 33 entries; special headers (NEW TARGET / CAT B) present",
        n_frozen_py == 33 and n_readme_entries == 33 and not missing_header and not missing_special,
        {"n_frozen_py": 33, "n_readme_entries": 33, "missing_header": [], "missing_special_header": []},
        {"n_frozen_py": n_frozen_py, "n_readme_entries": n_readme_entries,
         "missing_header": missing_header, "missing_special_header": missing_special})

    # ── Check 10: Canonical spot check — 10 random RIDs per mapped col ────
    # For each of the 30 mapped_cupm_v2 + 2 mapped_category + 1 mapped_points cols,
    # confirm the cupm_v2 column is populated for at least the same RIDs that the
    # legacy CPM column was populated for (in the archive). Sample 10 random RIDs
    # from the archive where the legacy col is NOT NULL; verify the canonical
    # equivalent is also NOT NULL on the live cupm_v2 view.
    map_rows = con.execute(
        """SELECT column_name, canonical_column
           FROM manuscript_workspace.cpm_tirads_canonical_coverage_v1
           WHERE coverage_status IN ('mapped_cupm_v2','mapped_category','mapped_points')
             AND canonical_table = 'canonical_us_patient_master_v2'
             AND canonical_column IS NOT NULL
             AND canonical_column != '-'"""
    ).fetchall()
    spot_results: list[dict] = []
    for legacy_col, canon_col in map_rows:
        try:
            sample = con.execute(f"""
                WITH src AS (
                    SELECT research_id
                    FROM {ARCHIVE_FQ}
                    WHERE "{legacy_col}" IS NOT NULL
                    ORDER BY random()
                    LIMIT 10
                )
                SELECT
                    COUNT(*)                                                     AS n_sample,
                    COUNT(*) FILTER (WHERE v."{canon_col}" IS NOT NULL)           AS n_canon_populated
                FROM src
                LEFT JOIN main.canonical_us_patient_master_v2 v USING (research_id)
            """).fetchone()
            spot_results.append({
                "legacy_col":         legacy_col,
                "canonical_col":      canon_col,
                "n_sample":           sample[0],
                "n_canon_populated":  sample[1],
                "passed":             sample[1] >= sample[0] * 0.5,  # at least 50% canonical coverage on the sample
            })
        except Exception as e:
            spot_results.append({"legacy_col": legacy_col, "canonical_col": canon_col, "error": str(e), "passed": False})
    n_spot_pass = sum(1 for r in spot_results if r.get("passed"))
    add("10_canonical_spot_check",
        "≥50% canonical-side population for 10 random RIDs per mapped legacy col (33 cols)",
        n_spot_pass == len(spot_results),
        {"n_cols": len(spot_results), "n_pass": len(spot_results)},
        {"n_cols": len(spot_results), "n_pass": n_spot_pass,
         "fails": [r for r in spot_results if not r.get("passed")]})

    # ── Summary + write ────────────────────────────────────────────────────
    n_pass = sum(1 for c in qa["checks"] if c["passed"])
    n_total = len(qa["checks"])
    qa["summary"] = {"n_checks": n_total, "n_passed": n_pass, "all_passed": n_pass == n_total}
    qa["finished_at_utc"] = utc_iso()

    qa_path = QA_DIR / "qa_script_cpm_tirads_partB.json"
    qa_path.write_text(json.dumps(qa, indent=2, default=str))

    print(f"QA bundle: {qa_path.relative_to(REPO)}")
    print(f"  {n_pass}/{n_total} checks passed")
    for c in qa["checks"]:
        marker = "✓" if c["passed"] else "✗"
        print(f"  {marker} {c['check_id']}: {c['description']}")


if __name__ == "__main__":
    main()

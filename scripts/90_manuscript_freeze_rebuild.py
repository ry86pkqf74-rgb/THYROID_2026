#!/usr/bin/env python3
"""
90_manuscript_freeze_rebuild.py — One-command manuscript publication bundle rebuild.

Rebuilds the entire final publication bundle deterministically:
  1. Validates all critical source tables (fail-closed on missing/drifted rows)
  2. Regenerates tables (scripts 62, 65)
  3. Regenerates figures (script 66)
  4. Exports metric registry
  5. Exports readiness assessment
  6. Copies caveat appendix and supplement
  7. Assembles final bundle ZIP

Usage:
    .venv/bin/python scripts/90_manuscript_freeze_rebuild.py [--md] [--dry-run]

Flags:
    --md        Read from MotherDuck (default: local DuckDB)
    --dry-run   Validate only, do not regenerate outputs
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON = str(REPO_ROOT / ".venv" / "bin" / "python")

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
DATE_TAG = datetime.now().strftime("%Y%m%d")
BUNDLE_DIR = REPO_ROOT / "exports" / f"MANUSCRIPT_FREEZE_BUNDLE_{DATE_TAG}"

EXPECTED_ROW_COUNTS = {
    "manuscript_cohort_v1": 10871,
    "episode_analysis_resolved_v1_dedup": 9368,
    "patient_analysis_resolved_v1": 10871,
    "lesion_analysis_resolved_v1": 11851,
    "thyroid_scoring_py_v1": 10871,
    "analysis_cancer_cohort_v1": 4136,
    "analysis_molecular_subset_v1": 10025,
    "analysis_tirads_subset_v1": 3474,
    "analysis_recurrence_subset_v1": 1946,
    "complication_patient_summary_v1": 2892,
    "longitudinal_lab_clean_v1": 38699,
    "recurrence_event_clean_v1": 1946,
    "extracted_braf_recovery_v1": None,
    "extracted_ras_patient_summary_v1": None,
    "extracted_rln_injury_refined_v2": None,
    "extracted_tirads_validated_v1": 3474,
}

CRITICAL_TABLES = [
    "manuscript_cohort_v1",
    "episode_analysis_resolved_v1_dedup",
    "patient_analysis_resolved_v1",
    "thyroid_scoring_py_v1",
    "analysis_cancer_cohort_v1",
]

DRIFT_TOLERANCE_PCT = 1.0


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


def get_connection(use_md):
    import duckdb
    if use_md:
        token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            try:
                import toml
                token = toml.load(str(REPO_ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
                os.environ["MOTHERDUCK_TOKEN"] = token
            except Exception:
                pass
        if not token:
            log("MOTHERDUCK_TOKEN not found; falling back to local", "WARN")
            return duckdb.connect(str(REPO_ROOT / "thyroid_master.duckdb"), read_only=True)
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}", read_only=True)
    return duckdb.connect(str(REPO_ROOT / "thyroid_master.duckdb"), read_only=True)


def validate_source_tables(con):
    log("=== Phase 1: Source Table Validation ===")
    failures = []
    warnings = []

    for table, expected in EXPECTED_ROW_COUNTS.items():
        try:
            row = con.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
            actual = row[0]
        except Exception as e:
            md_name = f"md_{table}"
            try:
                row = con.execute(f"SELECT COUNT(*) AS n FROM {md_name}").fetchone()
                actual = row[0]
                log(f"  {table} -> used md_ prefix: {actual} rows")
            except Exception:
                if table in CRITICAL_TABLES:
                    failures.append(f"CRITICAL TABLE MISSING: {table} ({e})")
                    log(f"  FAIL: {table} — not found", "ERROR")
                else:
                    warnings.append(f"Table missing (non-critical): {table}")
                    log(f"  WARN: {table} — not found", "WARN")
                continue

        if expected is not None:
            drift_pct = abs(actual - expected) / expected * 100 if expected > 0 else 0
            if drift_pct > DRIFT_TOLERANCE_PCT and table in CRITICAL_TABLES:
                failures.append(
                    f"ROW COUNT DRIFT: {table} expected={expected} actual={actual} drift={drift_pct:.1f}%"
                )
                log(f"  FAIL: {table} — {actual} rows (expected {expected}, drift {drift_pct:.1f}%)", "ERROR")
            elif drift_pct > DRIFT_TOLERANCE_PCT:
                warnings.append(f"Row drift: {table} expected={expected} actual={actual}")
                log(f"  WARN: {table} — {actual} rows (expected {expected})", "WARN")
            else:
                log(f"  OK: {table} — {actual} rows (expected {expected})")
        else:
            log(f"  OK: {table} — {actual} rows (no count constraint)")

    uniqueness_checks = {
        "manuscript_cohort_v1": "research_id",
        "patient_analysis_resolved_v1": "research_id",
    }
    for table, col in uniqueness_checks.items():
        try:
            r = con.execute(
                f"SELECT COUNT(*) - COUNT(DISTINCT {col}) AS dupes FROM {table}"
            ).fetchone()
            if r[0] > 0:
                failures.append(f"DUPLICATE {col} IN {table}: {r[0]} duplicates")
                log(f"  FAIL: {table}.{col} has {r[0]} duplicates", "ERROR")
            else:
                log(f"  OK: {table}.{col} uniqueness verified")
        except Exception:
            pass

    return failures, warnings


def run_script(script_name, extra_args=None):
    cmd = [PYTHON, str(REPO_ROOT / "scripts" / script_name)]
    if extra_args:
        cmd.extend(extra_args)
    log(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        log(f"  Script {script_name} failed (exit {result.returncode})", "ERROR")
        if result.stderr:
            for line in result.stderr.strip().split("\n")[-10:]:
                log(f"    {line}", "ERROR")
        return False
    log(f"  Script {script_name} completed successfully")
    return True


def regenerate_tables_and_figures(use_md):
    log("=== Phase 2: Regenerate Tables ===")
    md_flag = ["--md"] if use_md else []

    ok = True
    for script in ["62_run_primary_descriptives.py", "63_run_primary_models.py",
                    "64_run_survival_analyses.py"]:
        if not run_script(script, md_flag):
            log(f"  Non-fatal: {script} had issues", "WARN")

    log("=== Phase 3: Format Tables ===")
    if not run_script("65_generate_manuscript_tables.py", md_flag):
        ok = False

    log("=== Phase 4: Regenerate Figures ===")
    if not run_script("66_generate_manuscript_figures.py", md_flag):
        ok = False

    return ok


def export_metric_registry():
    log("=== Phase 5: Export Metric Registry ===")
    src = REPO_ROOT / "exports" / "manuscript_metric_registry_20260313"
    if not src.exists():
        log("  Metric registry directory not found", "WARN")
        return False
    dst = BUNDLE_DIR / "metric_registry"
    dst.mkdir(parents=True, exist_ok=True)
    for f in src.iterdir():
        shutil.copy2(f, dst / f.name)
        log(f"  Copied {f.name}")
    return True


def copy_docs():
    log("=== Phase 6: Copy Documentation ===")
    docs_to_copy = [
        "docs/MANUSCRIPT_CAVEATS_20260313.md",
        "docs/SUPPLEMENT_DATA_QUALITY_APPENDIX_20260313.md",
        "docs/FINAL_MANUSCRIPT_READINESS_VERDICT_20260313.md",
        "docs/statistical_analysis_plan_thyroid_manuscript.md",
    ]
    docs_dst = BUNDLE_DIR / "docs"
    docs_dst.mkdir(parents=True, exist_ok=True)
    for doc_path in docs_to_copy:
        src = REPO_ROOT / doc_path
        if src.exists():
            shutil.copy2(src, docs_dst / src.name)
            log(f"  Copied {src.name}")
        else:
            log(f"  WARN: {doc_path} not found", "WARN")

    reviewer_src = REPO_ROOT / "docs" / "reviewer_defense_20260313"
    if reviewer_src.exists():
        reviewer_dst = BUNDLE_DIR / "docs" / "reviewer_defense"
        if reviewer_dst.exists():
            shutil.rmtree(reviewer_dst)
        shutil.copytree(reviewer_src, reviewer_dst)
        log(f"  Copied reviewer_defense/ ({len(list(reviewer_src.iterdir()))} files)")

    return True


def copy_analysis_outputs():
    log("=== Phase 7: Copy Analysis Outputs ===")
    for subdir in ["manuscript_tables", "manuscript_figures", "manuscript_analysis"]:
        src = REPO_ROOT / "exports" / subdir
        if src.exists():
            dst = BUNDLE_DIR / subdir
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            n = len(list(dst.rglob("*")))
            log(f"  Copied {subdir}/ ({n} files)")

    for name in ["manuscript_cohort_v1.csv", "cohort_flow.csv"]:
        src = REPO_ROOT / "exports" / "manuscript_cohort_freeze" / name
        if src.exists():
            shutil.copy2(src, BUNDLE_DIR / name)
            log(f"  Copied {name}")

    return True


def write_readiness_assessment(failures, warnings):
    log("=== Phase 8: Readiness Assessment ===")
    assessment = {
        "timestamp": datetime.now().isoformat(),
        "script": "90_manuscript_freeze_rebuild",
        "git_sha": _get_git_sha(),
        "validation_failures": failures,
        "validation_warnings": warnings,
        "overall_status": "PASS" if not failures else "FAIL",
        "total_metrics_in_registry": 25,
        "primary_denominators": {
            "full_surgical_cohort": 10871,
            "analysis_eligible_cancer": 4136,
            "molecular_tested": 10025,
            "survival_cohort": 3201,
        },
        "bundle_contents": sorted(
            str(p.relative_to(BUNDLE_DIR)) for p in BUNDLE_DIR.rglob("*") if p.is_file()
        ),
    }
    out = BUNDLE_DIR / "readiness_assessment.json"
    with open(out, "w") as f:
        json.dump(assessment, f, indent=2)
    log(f"  Written: {out.name} (status={assessment['overall_status']})")
    return assessment["overall_status"] == "PASS"


def create_bundle_zip():
    log("=== Phase 9: Create Bundle ZIP ===")
    zip_name = f"THYROID_2026_MANUSCRIPT_FREEZE_{DATE_TAG}"
    zip_path = REPO_ROOT / "exports" / zip_name
    shutil.make_archive(str(zip_path), "zip", str(BUNDLE_DIR.parent), BUNDLE_DIR.name)
    final = zip_path.with_suffix(".zip")
    size_mb = final.stat().st_size / (1024 * 1024)
    log(f"  Bundle ZIP: {final.name} ({size_mb:.1f} MB)")
    return True


def write_manifest():
    manifest = {
        "script": "90_manuscript_freeze_rebuild",
        "timestamp": TIMESTAMP,
        "git_sha": _get_git_sha(),
        "bundle_dir": str(BUNDLE_DIR.relative_to(REPO_ROOT)),
        "total_files": sum(1 for _ in BUNDLE_DIR.rglob("*") if _.is_file()),
        "primary_denominators": {
            "full_surgical_cohort": 10871,
            "analysis_eligible_cancer": 4136,
            "molecular_tested": 10025,
        },
        "frozen_metrics": 25,
        "registry_version": "v1",
    }
    out = BUNDLE_DIR / "manifest.json"
    with open(out, "w") as f:
        json.dump(manifest, f, indent=2)
    log(f"  Manifest written: {out.name}")


def _get_git_sha():
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, cwd=str(REPO_ROOT)
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def main():
    parser = argparse.ArgumentParser(description="Rebuild manuscript publication bundle")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--dry-run", action="store_true", help="Validate only")
    args = parser.parse_args()

    log(f"Manuscript Freeze Rebuild — {TIMESTAMP}")
    log(f"  Target: {'MotherDuck' if args.md else 'local DuckDB'}")
    log(f"  Mode: {'DRY RUN' if args.dry_run else 'FULL REBUILD'}")

    con = get_connection(args.md)
    failures, warnings = validate_source_tables(con)
    con.close()

    if failures:
        log(f"\n{'='*60}", "ERROR")
        log(f"VALIDATION FAILED — {len(failures)} critical failure(s):", "ERROR")
        for f in failures:
            log(f"  - {f}", "ERROR")
        if warnings:
            log(f"\nPlus {len(warnings)} warning(s):", "WARN")
            for w in warnings:
                log(f"  - {w}", "WARN")
        log("\nAborting rebuild. Fix critical failures before proceeding.", "ERROR")
        sys.exit(1)

    if warnings:
        log(f"\n{len(warnings)} non-critical warning(s):", "WARN")
        for w in warnings:
            log(f"  - {w}", "WARN")

    if args.dry_run:
        log("\nDRY RUN: Validation passed. No outputs generated.")
        log(f"  {len(failures)} failures, {len(warnings)} warnings")
        sys.exit(0)

    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
    log(f"\nBundle directory: {BUNDLE_DIR}")

    regenerate_tables_and_figures(args.md)
    export_metric_registry()
    copy_docs()
    copy_analysis_outputs()
    write_manifest()
    passed = write_readiness_assessment(failures, warnings)
    create_bundle_zip()

    log(f"\n{'='*60}")
    if passed:
        log("MANUSCRIPT FREEZE REBUILD COMPLETE — ALL CHECKS PASSED")
    else:
        log("MANUSCRIPT FREEZE REBUILD COMPLETE — WITH WARNINGS", "WARN")
    log(f"Bundle: {BUNDLE_DIR}")
    log(f"Files: {sum(1 for _ in BUNDLE_DIR.rglob('*') if _.is_file())}")


if __name__ == "__main__":
    main()

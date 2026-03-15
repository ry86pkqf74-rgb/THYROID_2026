#!/usr/bin/env python3
"""
103_source_limited_enforcement.py  —  Source-Limited & Pipeline-Limited Field Enforcement

Reads the 43-field source_limited_field_registry CSV, extends it with governance
columns, and materialises enforcement & audit tables to DuckDB/MotherDuck:

  source_limited_enforcement_registry_v2  — extended field-level registry (43+ rows)
  source_limited_enforcement_summary_v1   — per-tier summary
  val_source_limited_enforcement_v1       — validation assertions

Enforcement tiers (from registry):
  CANONICAL              → manuscript_allowed=yes, analysis_any=yes
  SOURCE_LIMITED         → manuscript_allowed=conditional, requires caveat wording
  DERIVED_APPROXIMATE    → exploratory only, not in primary models
  MANUAL_REVIEW_ONLY     → cannot use as population denominator

Limitation categories:
  source_feed    — data physically absent from corpus (e.g. nuclear med notes)
  template       — structured field uses placeholder (e.g. 'x' means present_ungraded)
  pipeline       — NLP/ETL gap (extractors exist but output not materialised)
  review         — needs human adjudication to resolve discordance

Usage:
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/103_source_limited_enforcement.py --md
  .venv/bin/python scripts/103_source_limited_enforcement.py --local
  .venv/bin/python scripts/103_source_limited_enforcement.py --dry-run
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ── Registry CSV location ──────────────────────────────────────────────────
REGISTRY_CSV = ROOT / "exports" / "final_release_hardening_20260314" / "source_limited_field_registry.csv"

# ── Extended governance metadata per tier ──────────────────────────────────
TIER_RULES = {
    "CANONICAL": {
        "analysis_tier_allowed": "primary_and_secondary",
        "table1_eligible": True,
        "regression_eligible": True,
        "survival_eligible": True,
        "requires_caveat": False,
        "safe_manuscript_wording": "",
    },
    "SOURCE_LIMITED": {
        "analysis_tier_allowed": "secondary_with_caveat",
        "table1_eligible": True,
        "regression_eligible": True,
        "survival_eligible": True,
        "requires_caveat": True,
        "safe_manuscript_wording": "Field coverage limited by absence of institutional data feed; reported values represent available subset only.",
    },
    "DERIVED_APPROXIMATE": {
        "analysis_tier_allowed": "exploratory_only",
        "table1_eligible": False,
        "regression_eligible": False,
        "survival_eligible": False,
        "requires_caveat": True,
        "safe_manuscript_wording": "Values derived from approximate heuristic; not suitable for primary or sensitivity analyses.",
    },
    "MANUAL_REVIEW_ONLY": {
        "analysis_tier_allowed": "prohibited_for_population_denominators",
        "table1_eligible": False,
        "regression_eligible": False,
        "survival_eligible": False,
        "requires_caveat": True,
        "safe_manuscript_wording": "Field contains unresolved ambiguity requiring manual adjudication; do not use as population-level denominator.",
    },
}

# ── Per-field limitation_category overrides (from AGENTS.md knowledge) ────
LIMITATION_CATEGORY_MAP: dict[str, str] = {
    # source_feed: physically absent from corpus
    "non_tg_lab_date": "source_feed",
    "tsh_result": "source_feed",
    "free_t4_result": "source_feed",
    "vitamin_d_result": "source_feed",
    "calcitonin_result": "source_feed",
    "rai_dose_mci": "source_feed",
    "rai_receipt_confirmed": "source_feed",
    "recurrence_date_exact": "source_feed",
    "imaging_nodule_size_cm": "source_feed",
    "bmi": "source_feed",
    "vocal_cord_status_detailed": "source_feed",

    # template: structured placeholder
    "ete_grade_v9": "template",
    "vascular_invasion_who_grade": "template",
    "margin_r_classification": "template",
    "ene_grade": "template",
    "operative_boolean_defaults": "template",

    # pipeline: extractors exist but output not materialised
    "esophageal_involvement_flag": "pipeline",
    "berry_ligament_flag": "pipeline",
    "frozen_section_flag": "pipeline",
    "ebl_ml_nlp": "pipeline",
    "parathyroid_identified_count": "pipeline",
    "tirads_score": "pipeline",
    "provenance_lab_dates": "pipeline",

    # review: needs human adjudication
    "rln_monitoring_flag": "review",
    "recurrence_flag_structured": "review",
    "specimen_weight_g": "review",
    "braf_positive_final": "review",
    "ras_positive_final": "review",
    "tert_positive_final": "review",
}


def load_registry() -> list[dict]:
    """Load the CSV registry and extend each row with governance metadata."""
    if not REGISTRY_CSV.exists():
        print(f"  ERROR: Registry CSV not found: {REGISTRY_CSV}")
        sys.exit(2)

    rows = []
    with open(REGISTRY_CSV, newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            field_name = r.get("field_name", "").strip()
            status = r.get("status", "CANONICAL").strip()
            domain = r.get("domain", "").strip()
            rationale = r.get("rationale", "").strip()
            primary_source = r.get("primary_source_tables", "").strip()
            missingness = r.get("expected_missingness_behavior", "").strip()
            manuscript_allowed = r.get("manuscript_allowed", "").strip()
            notes = r.get("notes", "").strip()

            tier = TIER_RULES.get(status, TIER_RULES["CANONICAL"])
            lim_cat = LIMITATION_CATEGORY_MAP.get(field_name, "review")

            rows.append({
                "field_name": field_name,
                "domain": domain,
                "status": status,
                "limitation_category": lim_cat,
                "rationale": rationale,
                "primary_source_tables": primary_source,
                "expected_missingness_behavior": missingness,
                "manuscript_allowed": manuscript_allowed,
                "notes": notes,
                "analysis_tier_allowed": tier["analysis_tier_allowed"],
                "table1_eligible": tier["table1_eligible"],
                "regression_eligible": tier["regression_eligible"],
                "survival_eligible": tier["survival_eligible"],
                "requires_caveat": tier["requires_caveat"],
                "safe_manuscript_wording": tier["safe_manuscript_wording"],
            })

    print(f"  Loaded {len(rows)} fields from {REGISTRY_CSV.name}")
    return rows


def safe_str(v: str) -> str:
    """Escape single quotes for SQL string literals."""
    return v.replace("'", "''")


def build_enforcement_table(con, rows: list[dict], dry_run: bool = False) -> int:
    """Materialise source_limited_enforcement_registry_v2."""
    print("\n  Phase A: Building source_limited_enforcement_registry_v2")

    values = []
    for r in rows:
        val = (
            f"('{safe_str(r['field_name'])}', '{safe_str(r['domain'])}', '{safe_str(r['status'])}', "
            f"'{safe_str(r['limitation_category'])}', '{safe_str(r['rationale'])}', "
            f"'{safe_str(r['primary_source_tables'])}', '{safe_str(r['expected_missingness_behavior'])}', "
            f"'{safe_str(r['manuscript_allowed'])}', '{safe_str(r['notes'])}', "
            f"'{safe_str(r['analysis_tier_allowed'])}', "
            f"{r['table1_eligible']}, {r['regression_eligible']}, "
            f"{r['survival_eligible']}, {r['requires_caveat']}, "
            f"'{safe_str(r['safe_manuscript_wording'])}', CURRENT_TIMESTAMP)"
        )
        values.append(val)

    ddl = """
    CREATE OR REPLACE TABLE source_limited_enforcement_registry_v2 (
        field_name VARCHAR,
        domain VARCHAR,
        status VARCHAR,
        limitation_category VARCHAR,
        rationale VARCHAR,
        primary_source_tables VARCHAR,
        expected_missingness_behavior VARCHAR,
        manuscript_allowed VARCHAR,
        notes VARCHAR,
        analysis_tier_allowed VARCHAR,
        table1_eligible BOOLEAN,
        regression_eligible BOOLEAN,
        survival_eligible BOOLEAN,
        requires_caveat BOOLEAN,
        safe_manuscript_wording VARCHAR,
        updated_at TIMESTAMP
    )
    """

    insert_sql = f"""
    INSERT INTO source_limited_enforcement_registry_v2 VALUES
    {','.join(values)}
    """

    if dry_run:
        print(f"  [DRY-RUN] Would create table with {len(rows)} rows")
        return len(rows)

    try:
        con.execute(ddl)
        con.execute(insert_sql)
        cnt = con.execute("SELECT COUNT(*) FROM source_limited_enforcement_registry_v2").fetchone()[0]
        print(f"  ✓ source_limited_enforcement_registry_v2: {cnt} rows")
        return int(cnt)
    except Exception as e:
        print(f"  ERROR: {e}")
        return -1


def build_summary_table(con, dry_run: bool = False) -> int:
    """Build per-tier summary."""
    print("\n  Phase B: Building source_limited_enforcement_summary_v1")

    sql = """
    CREATE OR REPLACE TABLE source_limited_enforcement_summary_v1 AS
    SELECT
        status AS tier,
        limitation_category,
        COUNT(*) AS n_fields,
        SUM(CASE WHEN manuscript_allowed = 'yes' THEN 1 ELSE 0 END) AS manuscript_yes,
        SUM(CASE WHEN manuscript_allowed = 'conditional' THEN 1 ELSE 0 END) AS manuscript_conditional,
        SUM(CASE WHEN manuscript_allowed = 'no' THEN 1 ELSE 0 END) AS manuscript_no,
        SUM(CASE WHEN table1_eligible THEN 1 ELSE 0 END) AS table1_eligible_count,
        SUM(CASE WHEN regression_eligible THEN 1 ELSE 0 END) AS regression_eligible_count,
        CURRENT_TIMESTAMP AS computed_at
    FROM source_limited_enforcement_registry_v2
    GROUP BY status, limitation_category
    ORDER BY status, limitation_category
    """
    if dry_run:
        print("  [DRY-RUN] Would create summary")
        return 0

    try:
        con.execute(sql)
        cnt = con.execute("SELECT COUNT(*) FROM source_limited_enforcement_summary_v1").fetchone()[0]
        print(f"  ✓ source_limited_enforcement_summary_v1: {cnt} rows")
        return int(cnt)
    except Exception as e:
        print(f"  ERROR: {e}")
        return -1


def build_validation_table(con, dry_run: bool = False) -> int:
    """Build val_source_limited_enforcement_v1 — validation assertions."""
    print("\n  Phase C: Building val_source_limited_enforcement_v1")

    sql = """
    CREATE OR REPLACE TABLE val_source_limited_enforcement_v1 AS
    WITH checks AS (
        -- Check 1: All fields have a status
        SELECT 'all_fields_have_status' AS check_name,
               COUNT(*) FILTER (WHERE status IS NULL OR status = '') AS violations,
               COUNT(*) AS total,
               CASE WHEN COUNT(*) FILTER (WHERE status IS NULL OR status = '') = 0
                    THEN 'PASS' ELSE 'FAIL' END AS result
        FROM source_limited_enforcement_registry_v2

        UNION ALL

        -- Check 2: All fields have a limitation_category
        SELECT 'all_fields_have_limitation_category',
               COUNT(*) FILTER (WHERE limitation_category IS NULL OR limitation_category = ''),
               COUNT(*),
               CASE WHEN COUNT(*) FILTER (WHERE limitation_category IS NULL OR limitation_category = '') = 0
                    THEN 'PASS' ELSE 'FAIL' END
        FROM source_limited_enforcement_registry_v2

        UNION ALL

        -- Check 3: MANUAL_REVIEW_ONLY fields not in Table 1
        SELECT 'manual_review_not_in_table1',
               COUNT(*) FILTER (WHERE status = 'MANUAL_REVIEW_ONLY' AND table1_eligible),
               COUNT(*) FILTER (WHERE status = 'MANUAL_REVIEW_ONLY'),
               CASE WHEN COUNT(*) FILTER (WHERE status = 'MANUAL_REVIEW_ONLY' AND table1_eligible) = 0
                    THEN 'PASS' ELSE 'FAIL' END
        FROM source_limited_enforcement_registry_v2

        UNION ALL

        -- Check 4: DERIVED_APPROXIMATE fields not in regression
        SELECT 'derived_approx_not_in_regression',
               COUNT(*) FILTER (WHERE status = 'DERIVED_APPROXIMATE' AND regression_eligible),
               COUNT(*) FILTER (WHERE status = 'DERIVED_APPROXIMATE'),
               CASE WHEN COUNT(*) FILTER (WHERE status = 'DERIVED_APPROXIMATE' AND regression_eligible) = 0
                    THEN 'PASS' ELSE 'FAIL' END
        FROM source_limited_enforcement_registry_v2

        UNION ALL

        -- Check 5: SOURCE_LIMITED fields require caveat
        SELECT 'source_limited_requires_caveat',
               COUNT(*) FILTER (WHERE status = 'SOURCE_LIMITED' AND NOT requires_caveat),
               COUNT(*) FILTER (WHERE status = 'SOURCE_LIMITED'),
               CASE WHEN COUNT(*) FILTER (WHERE status = 'SOURCE_LIMITED' AND NOT requires_caveat) = 0
                    THEN 'PASS' ELSE 'FAIL' END
        FROM source_limited_enforcement_registry_v2

        UNION ALL

        -- Check 6: >=10 CANONICAL fields exist
        SELECT 'minimum_canonical_count',
               CASE WHEN COUNT(*) FILTER (WHERE status = 'CANONICAL') >= 10 THEN 0 ELSE 1 END,
               COUNT(*) FILTER (WHERE status = 'CANONICAL'),
               CASE WHEN COUNT(*) FILTER (WHERE status = 'CANONICAL') >= 10
                    THEN 'PASS' ELSE 'FAIL' END
        FROM source_limited_enforcement_registry_v2
    )
    SELECT *, CURRENT_TIMESTAMP AS checked_at
    FROM checks
    """
    if dry_run:
        print("  [DRY-RUN] Would create validation table with 6 assertions")
        return 6

    try:
        con.execute(sql)
        cnt = con.execute("SELECT COUNT(*) FROM val_source_limited_enforcement_v1").fetchone()[0]
        # Check for failures
        fails = con.execute(
            "SELECT check_name FROM val_source_limited_enforcement_v1 WHERE result = 'FAIL'"
        ).fetchall()
        status = "ALL PASS" if not fails else f"FAIL: {[f[0] for f in fails]}"
        print(f"  ✓ val_source_limited_enforcement_v1: {cnt} assertions — {status}")
        return int(cnt)
    except Exception as e:
        print(f"  ERROR: {e}")
        return -1


def export_artifacts(con, dry_run: bool = False) -> Path:
    """Export enforcement tables to CSV."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    export_dir = ROOT / "exports" / f"source_limited_enforcement_{ts}"
    if dry_run:
        print(f"\n  [DRY-RUN] Would export to {export_dir}")
        return export_dir

    export_dir.mkdir(parents=True, exist_ok=True)

    for tbl, fname in [
        ("source_limited_enforcement_registry_v2", "enforcement_registry.csv"),
        ("source_limited_enforcement_summary_v1", "enforcement_summary.csv"),
        ("val_source_limited_enforcement_v1", "enforcement_validation.csv"),
    ]:
        try:
            df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
            df.to_csv(export_dir / fname, index=False)
            print(f"  Exported {fname}: {len(df):,} rows")
        except Exception as e:
            print(f"  WARN: Could not export {tbl}: {e}")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "script": "103_source_limited_enforcement.py",
        "tables_created": [
            "source_limited_enforcement_registry_v2",
            "source_limited_enforcement_summary_v1",
            "val_source_limited_enforcement_v1",
        ],
        "registry_csv_source": str(REGISTRY_CSV.relative_to(ROOT)),
    }
    (export_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest: {export_dir / 'manifest.json'}")
    return export_dir


def connect(args) -> "duckdb.DuckDBPyConnection":
    import duckdb

    if args.md:
        token = os.environ.get("MOTHERDUCK_TOKEN", "")
        if not token:
            print("ERROR: MOTHERDUCK_TOKEN not set")
            sys.exit(2)
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    else:
        local_path = ROOT / "thyroid_master.duckdb"
        if not local_path.exists():
            local_path = ROOT / "thyroid_master_local.duckdb"
        return duckdb.connect(str(local_path))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--md", action="store_true", help="Target MotherDuck")
    ap.add_argument("--local", action="store_true", help="Target local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    args = ap.parse_args()
    if not args.md and not args.local and not args.dry_run:
        args.local = True

    print("\n" + "=" * 72)
    print("  103_source_limited_enforcement.py  —  Source-Limited Field Enforcement")
    print(f"  Target: {'MotherDuck' if args.md else 'Local DuckDB'}")
    print("=" * 72)

    t0 = time.time()
    rows = load_registry()

    if args.dry_run:
        build_enforcement_table(None, rows, dry_run=True)
        build_summary_table(None, dry_run=True)
        build_validation_table(None, dry_run=True)
        export_artifacts(None, dry_run=True)
    else:
        con = connect(args)
        build_enforcement_table(con, rows, dry_run=False)
        build_summary_table(con, dry_run=False)
        build_validation_table(con, dry_run=False)
        export_dir = export_artifacts(con, dry_run=False)
        con.close()
        print(f"\n  Exports: {export_dir.relative_to(ROOT)}")

    print(f"\n  Completed in {time.time() - t0:.1f}s\n")


if __name__ == "__main__":
    main()

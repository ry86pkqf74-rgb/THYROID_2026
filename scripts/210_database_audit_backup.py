#!/usr/bin/env python3
"""
THYROID_2026 — Script 210: Full Database Audit + Parquet Backup
Database: thyroid_ete_fix_20260413

Goals:
  1. Comprehensive inventory of ALL tables (rows, patients, columns, tier)
  2. Gap analysis: source/extracted columns NOT yet in canonical_patient_master_v1
  3. Parquet backup of every critical table (ZSTD compressed)
  4. Write scripts/output/database_audit_report.md

Run:
  .venv/bin/python scripts/210_database_audit_backup.py [--dry-run] [--skip-backup]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB = "thyroid_ete_fix_20260413"
OUTPUT_DIR = REPO / "scripts" / "output"
BACKUP_DIR = OUTPUT_DIR / "parquet_backup"
REPORT_PATH = OUTPUT_DIR / "database_audit_report.md"
MANIFEST_PATH = BACKUP_DIR / "MANIFEST.txt"

CANONICAL_TABLE = "canonical_patient_master_v1"

# ---------------------------------------------------------------------------
# Tier classification rules  (checked in order; first match wins)
# ---------------------------------------------------------------------------
TIER_RULES: list[tuple[str, list[str]]] = [
    ("CANONICAL", [
        "canonical_patient_master_v1",
        "canonical_diagnosis_unified_v1",
        "canonical_recurrence_v1",
        "canonical_survival_followup_v1",
        "canonical_molecular_tested_v1",
        "canonical_benign_diagnosis_v1",
        "canonical_malignant_diagnosis_v1",
    ]),
    ("SOURCE_STRUCTURED", [
        "gold_master_",
        "patient_refined_master_",
        "tumor_pathology",
        "path_synoptics",
        "ultrasound_reports",
        "ct_imaging",
        "nuclear_med",
        "fna_",
        "operative_episode_",
        "imaging_nodule_",
        "imaging_patient_",
        "imaging_exam_",
        "longitudinal_lab_",
        "molecular_results",
        "molecular_variant_",
        "molecular_test_episode_",
        "specimen_master_",
        "clinical_notes_long",
    ]),
    ("EXTRACTED_SCORED", [
        "extracted_",
        "thyroid_scoring_",
        "tg_timeline_",
        "complication_",
        "recurrence_event_",
        "survival_cohort_",
        "rai_treatment_",
        "rai_episode_",
        "tirads_llm_",
        "thyroglobulin_lab_canonical_",
        "ln_master_rollup_",
        "ln_crossval_",
        "demographics_harmonized_",
    ]),
    ("NLP_ENTITIES", [
        "note_entities_",
    ]),
    ("LINKAGE_QC", [
        "linkage_",
        "val_",
        "review_queue_",
        "imaging_fna_linkage_",
        "surgery_pathology_linkage_",
        "fna_molecular_linkage_",
        "pathology_rai_linkage_",
        "preop_surgery_linkage_",
        "_backup",
        "adjudication_",
    ]),
    ("ANALYSIS_SUBSETS", [
        "analysis_",
        "manuscript_cohort_",
        "episode_analysis_",
        "lesion_analysis_",
        "patient_analysis_",
    ]),
    ("OTHER", []),  # catch-all
]

# ---------------------------------------------------------------------------
# Critical tables for parquet backup
# ---------------------------------------------------------------------------
CRITICAL_TABLES = [
    # Canonical
    "canonical_patient_master_v1",
    "canonical_diagnosis_unified_v1",
    "canonical_recurrence_v1",
    "canonical_survival_followup_v1",
    "canonical_molecular_tested_v1",
    "canonical_benign_diagnosis_v1",
    "canonical_malignant_diagnosis_v1",
    # Source structured
    "gold_master_patient_facts_v1",
    "patient_refined_master_clinical_v12",
    "tumor_pathology",
    "path_synoptics",
    "ultrasound_reports",
    "ct_imaging",
    "nuclear_med",
    "fna_cytology",
    "fna_episode_master_v2",
    "fna_history",
    "operative_episode_detail_v2",
    "imaging_nodule_master_v1",
    "imaging_patient_summary_v1",
    "imaging_exam_master_v1",
    "longitudinal_lab_canonical_v1",
    "molecular_results",
    "molecular_variant_long",
    "molecular_test_episode_v2",
    "specimen_master_v1",
    "clinical_notes_long",
    # Extracted / scored
    "extracted_tirads_validated_v1",
    "tirads_llm_extracted_v2",
    "extracted_ete_subgraded_v1",
    "extracted_braf_recovery_v1",
    "extracted_ras_patient_summary_v1",
    "extracted_rln_injury_refined_v2",
    "extracted_complications_refined_v5",
    "extracted_postop_labs_expanded_v1",
    "extracted_fna_bethesda_v1",
    "thyroid_scoring_py_v1",
    "tg_timeline_patient_summary_v1",
    "thyroglobulin_lab_canonical_v1",
    "complication_patient_summary_v1",
    "complication_phenotype_v1",
    "recurrence_event_clean_v1",
    "survival_cohort_enriched",
    "rai_treatment_episode_v2",
    "ln_master_rollup_v1",
    "ln_crossval_v1",
    # NLP entities
    "note_entities_llm_tirads_granular",
    "note_entities_llm_cervical_ln_detail",
    "note_entities_llm_pathology",
    "note_entities_llm_recurrence",
    "note_entities_llm_survival_followup",
    "note_entities_llm_rai_detailed",
    "note_entities_llm_tg_kinetics",
    "note_entities_llm_imaging",
    "note_entities_llm_labs",
    "note_entities_llm_frozen_section_detail",
    "note_entities_llm_airway_invasion",
    "note_entities_llm_vascular_invasion",
    "note_entities_llm_functional_outcomes",
    "note_entities_llm_parathyroid_detail",
    "note_entities_llm_dynamic_risk_response",
    "note_entities_llm_past_medical_hx",
    "note_entities_llm_past_surgical_hx",
    "note_entities_llm_presenting_symptoms",
    "note_entities_llm_physical_exam",
    "note_entities_llm_rad_treatment",
    "note_entities_llm_patient_decision_adherence",
    "note_entities_llm_synoptic_pathology_enrichment",
    "note_entities_llm_us_nodule_dynamics",
    "note_entities_complications",
    "note_entities_genetics",
    "note_entities_medications",
    "note_entities_operative_detail",
    "note_entities_problem_list",
    "note_entities_procedures",
    "note_entities_staging",
    # Linkage
    "linkage_master_v1",
    "imaging_fna_linkage_v3",
    "surgery_pathology_linkage_v3",
    "fna_molecular_linkage_v3",
]

# ---------------------------------------------------------------------------
# Deep gap-analysis: source tables + what columns matter
# ---------------------------------------------------------------------------
DEEP_AUDIT_TABLES = {
    "complication_phenotype_v1": {
        "description": "Per-complication-type detail (5,928 rows, 2,892 patients)",
        "canonical_analogues": [
            "any_confirmed_complication", "n_confirmed_complications",
            "has_low_pth_flag", "has_low_calcium_flag", "rln_status",
        ],
    },
    "recurrence_event_clean_v1": {
        "description": "Event-level recurrence (1,946 rows)",
        "canonical_analogues": [
            "recurrence_confirmed", "recurrence_type", "recurrence_date",
            "recurrence_source", "recurrence_detection",
        ],
    },
    "rai_treatment_episode_v2": {
        "description": "Per-episode RAI detail (1,857 rows, 862 patients)",
        "canonical_analogues": [
            "n_rai_episodes", "rai_dose_v9", "rai_intent_v9",
            "rai_date_v9", "rai_response_v9",
        ],
    },
    "survival_cohort_enriched": {
        "description": "Survival cohort (61,134 rows, 10,507 patients)",
        "canonical_analogues": [
            "survival_time_days", "survival_event", "death_occurred",
            "follow_up_complete", "follow_up_months",
        ],
    },
    "clinical_notes_long": {
        "description": "Source notes for NLP (rows, 5,593 patients)",
        "canonical_analogues": [],
    },
    "molecular_variant_long": {
        "description": "Per-variant molecular results",
        "canonical_analogues": [
            "braf_positive_final", "ras_positive_v7", "tert_positive_v9",
        ],
    },
    "thyroglobulin_lab_canonical_v1": {
        "description": "Longitudinal Tg lab (76,971 rows, 3,258 patients)",
        "canonical_analogues": [
            "tg_nadir", "tg_max", "tg_last", "tg_rising_flag",
        ],
    },
    "extracted_postop_labs_expanded_v1": {
        "description": "Post-op PTH/Ca detail",
        "canonical_analogues": [
            "has_low_pth_flag", "has_low_calcium_flag",
            "pth_nadir", "calcium_nadir",
        ],
    },
    "extracted_ete_subgraded_v1": {
        "description": "ETE subgrading detail",
        "canonical_analogues": [
            "ete_grade", "ete_grade_v9", "ete_microscopic_confirmed",
        ],
    },
    "extracted_braf_recovery_v1": {
        "description": "BRAF multi-source recovery",
        "canonical_analogues": [
            "braf_positive_final", "braf_detection_method_v11",
        ],
    },
    "extracted_ras_patient_summary_v1": {
        "description": "RAS subtype summary",
        "canonical_analogues": [
            "ras_positive_v7", "nras_positive_v11", "hras_positive_v11",
            "kras_positive_v11", "ras_primary_subtype_v11",
        ],
    },
    "extracted_rln_injury_refined_v2": {
        "description": "Refined RLN injury classification",
        "canonical_analogues": [
            "rln_status", "rln_injury_tier",
        ],
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_tier(name: str) -> str:
    name_lower = name.lower()
    for tier, prefixes in TIER_RULES:
        if tier == "OTHER":
            return "OTHER"
        for p in prefixes:
            if name_lower == p or name_lower.startswith(p) or p in name_lower:
                return tier
    return "OTHER"


def safe_count(con: duckdb.DuckDBPyConnection, table: str, col: str = "*") -> int | None:
    try:
        return con.execute(f'SELECT COUNT({col}) FROM "{table}"').fetchone()[0]
    except Exception:
        return None


def safe_distinct(con: duckdb.DuckDBPyConnection, table: str, col: str = "research_id") -> int | None:
    try:
        return con.execute(f'SELECT COUNT(DISTINCT {col}) FROM "{table}"').fetchone()[0]
    except Exception:
        return None


def has_column(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    try:
        con.execute(f'SELECT "{col}" FROM "{table}" LIMIT 0')
        return True
    except Exception:
        return False


def get_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    try:
        # MotherDuck information_schema returns duplicate rows per attached catalog;
        # DISTINCT + table_schema='main' collapses them (per AGENTS.md).
        row = con.execute(
            "SELECT DISTINCT column_name FROM information_schema.columns "
            "WHERE table_name = ? AND table_schema = 'main'",
            [table],
        ).fetchall()
        # Re-order by actually querying the table pragma when possible
        return [r[0] for r in row]
    except Exception:
        return []


def has_date_col(cols: list[str]) -> bool:
    date_words = {"date", "dt", "time", "timestamp"}
    return any(any(w in c.lower() for w in date_words) for c in cols)


# ---------------------------------------------------------------------------
# Phase A — full table inventory (batch queries for speed)
# ---------------------------------------------------------------------------

def phase_a_inventory(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    print("\n[Phase A] Building full table inventory …")

    # 1. Deduplicated table list (MotherDuck info_schema has duplicate rows per catalog)
    tables = con.execute(
        "SELECT DISTINCT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    print(f"  Found {len(tables)} distinct tables")

    # 2. All columns in one shot — then build a dict keyed by table_name
    print("  Fetching all column metadata …")
    col_df = con.execute(
        "SELECT DISTINCT table_name, column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = 'main' "
        "ORDER BY table_name, column_name"
    ).df()
    cols_by_table: dict[str, list[str]] = (
        col_df.groupby("table_name")["column_name"].apply(list).to_dict()
    )

    # 3. Row counts and patient counts — use UNION ALL trick per-table is slow;
    #    iterate but avoid calling info_schema again.
    rows_list = []
    for t_name, t_type in tables:
        cols = cols_by_table.get(t_name, [])
        row_count = safe_count(con, t_name) or 0
        has_rid = "research_id" in cols
        n_patients = safe_distinct(con, t_name) if has_rid else None
        tier = get_tier(t_name)
        rows_list.append({
            "tier": tier,
            "table_name": t_name,
            "table_type": t_type,
            "row_count": row_count,
            "n_distinct_research_ids": n_patients,
            "n_columns": len(cols),
            "has_date_column": has_date_col(cols),
            "key_columns": ", ".join(cols[:10]),
        })
        print(f"  {tier:20s}  {t_name:55s}  {row_count:>8,}")

    df = pd.DataFrame(rows_list)
    tier_order = ["CANONICAL", "SOURCE_STRUCTURED", "EXTRACTED_SCORED",
                  "NLP_ENTITIES", "LINKAGE_QC", "ANALYSIS_SUBSETS", "OTHER"]
    df["tier"] = pd.Categorical(df["tier"], categories=tier_order, ordered=True)
    df = df.sort_values(["tier", "table_name"]).reset_index(drop=True)
    print(f"  → {len(df)} tables inventoried")
    return df, cols_by_table


# ---------------------------------------------------------------------------
# Phase B — gap analysis vs canonical_patient_master_v1
# ---------------------------------------------------------------------------

def phase_b_gaps(con: duckdb.DuckDBPyConnection, cols_by_table: dict[str, list[str]] | None = None) -> dict:
    print("\n[Phase B] Column gap analysis vs canonical_patient_master_v1 …")
    if cols_by_table:
        canonical_cols = set(cols_by_table.get(CANONICAL_TABLE, []))
    else:
        canonical_cols = set(get_columns(con, CANONICAL_TABLE))
    print(f"  canonical has {len(canonical_cols)} columns")

    gap_report: dict[str, dict] = {}

    for table, meta in DEEP_AUDIT_TABLES.items():
        print(f"  Checking {table} …")
        src_cols = cols_by_table.get(table, []) if cols_by_table else get_columns(con, table)
        if not src_cols:
            print(f"    ⚠ table not found / no columns")
            gap_report[table] = {"status": "MISSING", "description": meta["description"],
                                  "missing_cols": [], "coverage_note": "Table not found"}
            continue

        # Columns not in canonical
        missing = [c for c in src_cols if c not in canonical_cols
                   and c not in ("research_id", "row_id", "id", "index")]
        # Canonical analogues coverage
        analogues = meta.get("canonical_analogues", [])
        covered = [a for a in analogues if a in canonical_cols]

        # Get row count
        rows = safe_count(con, table) or 0
        patients = safe_distinct(con, table) if "research_id" in src_cols else None

        gap_report[table] = {
            "status": "FOUND",
            "description": meta["description"],
            "rows": rows,
            "patients": patients,
            "n_src_cols": len(src_cols),
            "n_missing_from_canonical": len(missing),
            "missing_cols": missing[:50],  # cap for readability
            "canonical_analogues": analogues,
            "analogues_in_canonical": covered,
            "analogues_coverage_pct": round(100 * len(covered) / len(analogues), 1) if analogues else None,
        }
        print(f"    {rows:>8,} rows | {len(missing):>4} cols not in canonical")

    return gap_report


# ---------------------------------------------------------------------------
# Phase C — note-type breakdown for clinical_notes_long
# ---------------------------------------------------------------------------

def phase_c_note_types(con: duckdb.DuckDBPyConnection) -> pd.DataFrame | None:
    print("\n[Phase C] Note type breakdown for clinical_notes_long …")
    try:
        df = con.execute(
            "SELECT note_type, COUNT(*) AS n_notes, COUNT(DISTINCT research_id) AS n_patients "
            "FROM clinical_notes_long "
            "GROUP BY note_type ORDER BY n_notes DESC"
        ).df()
        return df
    except Exception as e:
        print(f"  ⚠ {e}")
        return None


# ---------------------------------------------------------------------------
# Phase D — parquet backup
# ---------------------------------------------------------------------------

def phase_d_backup(con: duckdb.DuckDBPyConnection, dry_run: bool) -> list[dict]:
    print("\n[Phase D] Parquet backup …")
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    for t in CRITICAL_TABLES:
        out = BACKUP_DIR / f"{t}.parquet"
        try:
            rows = safe_count(con, t)
            if rows is None:
                results.append({"table": t, "status": "SKIP_NOT_FOUND", "rows": 0, "size_mb": 0})
                print(f"  SKIP (not found): {t}")
                continue
            if not dry_run:
                con.execute(
                    f'COPY (SELECT * FROM "{t}") TO \'{out}\' '
                    f"(FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                size_mb = round(out.stat().st_size / 1_048_576, 2) if out.exists() else 0
            else:
                size_mb = 0
            results.append({"table": t, "status": "OK", "rows": rows, "size_mb": size_mb})
            print(f"  {'DRY-RUN ' if dry_run else ''}OK: {t} ({rows:,} rows, {size_mb:.1f} MB)")
        except Exception as e:
            results.append({"table": t, "status": f"FAIL: {e}", "rows": 0, "size_mb": 0})
            print(f"  FAIL: {t} — {e}")

    return results


# ---------------------------------------------------------------------------
# Phase E — write markdown report
# ---------------------------------------------------------------------------

def phase_e_report(
    inventory: pd.DataFrame,
    gaps: dict,
    note_types: pd.DataFrame | None,
    backup_results: list[dict],
    dry_run: bool,
    cols_by_table: dict[str, list[str]] | None = None,
) -> None:
    print("\n[Phase E] Writing markdown report …")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []

    def h(level: int, text: str) -> None:
        lines.append(f"\n{'#' * level} {text}\n")

    def para(text: str) -> None:
        lines.append(text + "\n")

    h(1, "THYROID_2026 — Database Audit Report")
    para(f"**Database:** `{DB}`  |  **Generated:** {now}  |  "
         f"**Script:** `scripts/210_database_audit_backup.py`  |  "
         f"**Dry-run:** {'yes' if dry_run else 'no'}")

    # ── Table inventory by tier ──────────────────────────────────────────────
    h(2, "1. Table Inventory by Tier")
    tier_summary = inventory.groupby("tier").agg(
        n_tables=("table_name", "count"),
        total_rows=("row_count", "sum"),
    ).reset_index()
    lines.append("| Tier | Tables | Total rows |")
    lines.append("|------|-------:|-----------:|")
    for _, r in tier_summary.iterrows():
        lines.append(f"| {r['tier']} | {r['n_tables']:,} | {r['total_rows']:,} |")
    lines.append("")

    # Per-tier table details
    for tier in inventory["tier"].cat.categories:
        sub = inventory[inventory["tier"] == tier]
        if sub.empty:
            continue
        h(3, f"Tier: {tier} ({len(sub)} tables)")
        lines.append("| Table | Type | Rows | Patients | Cols | Has Date |")
        lines.append("|-------|------|-----:|---------:|-----:|---------|")
        for _, r in sub.iterrows():
            pts = f"{r['n_distinct_research_ids']:,}" if pd.notna(r['n_distinct_research_ids']) else "—"
            lines.append(
                f"| `{r['table_name']}` | {r['table_type']} "
                f"| {r['row_count']:,} | {pts} | {r['n_columns']} | {'✓' if r['has_date_column'] else '✗'} |"
            )
        lines.append("")

    # ── Gap analysis ─────────────────────────────────────────────────────────
    h(2, "2. Column Gap Analysis (Source → Canonical)")
    n_canon_cols = len(cols_by_table.get(CANONICAL_TABLE, [])) if cols_by_table else len(get_columns_cached(inventory))
    para(f"`{CANONICAL_TABLE}` has **{n_canon_cols} columns** "
         "(see Phase B for per-table detail).")

    for table, info in gaps.items():
        h(3, f"`{table}`")
        para(f"*{info['description']}*")
        if info["status"] == "MISSING":
            para("⚠ **Table not found on this database.**")
            continue
        para(f"- **Rows:** {info['rows']:,}  |  **Patients:** {info.get('patients') or '—'}")
        para(f"- **Source columns:** {info['n_src_cols']}")
        para(f"- **Columns NOT in canonical:** {info['n_missing_from_canonical']}")
        if info.get("analogues_coverage_pct") is not None:
            para(f"- **Canonical analogue coverage:** {info['analogues_coverage_pct']}%  "
                 f"({len(info['analogues_in_canonical'])}/{len(info['canonical_analogues'])} matched)")

        if info["missing_cols"]:
            h(4, "Columns NOT yet in canonical_patient_master_v1")
            lines.append("```")
            for chunk in [info["missing_cols"][i:i+5] for i in range(0, len(info["missing_cols"]), 5)]:
                lines.append("  " + ",  ".join(chunk))
            lines.append("```")

        if info.get("analogues_in_canonical"):
            para(f"**Already covered by canonical:** {', '.join('`' + a + '`' for a in info['analogues_in_canonical'])}")

        uncovered = [a for a in info.get("canonical_analogues", [])
                     if a not in info.get("analogues_in_canonical", [])]
        if uncovered:
            para(f"**Canonical analogues NOT found:** {', '.join('`' + a + '`' for a in uncovered)}")

    # ── Note type breakdown ───────────────────────────────────────────────────
    if note_types is not None and not note_types.empty:
        h(2, "3. clinical_notes_long — Note Type Breakdown")
        lines.append("| Note type | Notes | Patients |")
        lines.append("|-----------|------:|---------:|")
        for _, r in note_types.iterrows():
            lines.append(f"| {r['note_type']} | {r['n_notes']:,} | {r['n_patients']:,} |")
        lines.append("")

    # ── Backup results ────────────────────────────────────────────────────────
    h(2, "4. Parquet Backup Results")
    ok = [r for r in backup_results if r["status"] == "OK"]
    skipped = [r for r in backup_results if "SKIP" in r["status"]]
    failed = [r for r in backup_results if "FAIL" in r["status"]]
    total_mb = sum(r["size_mb"] for r in ok)
    total_rows = sum(r["rows"] for r in ok)
    para(f"- **Tables backed up:** {len(ok)} / {len(backup_results)}")
    para(f"- **Tables skipped (not found):** {len(skipped)}")
    para(f"- **Failures:** {len(failed)}")
    para(f"- **Total rows exported:** {total_rows:,}")
    para(f"- **Total size:** {total_mb:.1f} MB")

    lines.append("| Table | Status | Rows | Size (MB) |")
    lines.append("|-------|--------|-----:|----------:|")
    for r in backup_results:
        lines.append(f"| `{r['table']}` | {r['status']} | {r['rows']:,} | {r['size_mb']:.1f} |")
    lines.append("")

    if failed:
        h(3, "Failures")
        for r in failed:
            para(f"- `{r['table']}`: {r['status']}")

    # ── Recommendations ───────────────────────────────────────────────────────
    h(2, "5. Recommendations — Gap Fill Priority")
    recommendations = [
        ("HIGH",   "complication_phenotype_v1",
         "Add per-entity confirmed/transient/permanent flags; currently canonical only has aggregate counts"),
        ("HIGH",   "extracted_rln_injury_refined_v2",
         "RLN tier + confidence not in canonical; useful for complications manuscript"),
        ("HIGH",   "extracted_braf_recovery_v1",
         "braf_detection_method not in canonical; needed for molecular source attribution"),
        ("HIGH",   "extracted_ras_patient_summary_v1",
         "ras_subtype (NRAS/HRAS/KRAS) not in canonical; already in v11 columns but verify"),
        ("MEDIUM", "rai_treatment_episode_v2",
         "Per-episode RAI intent/response useful for multi-RAI patients; canonical has rollup only"),
        ("MEDIUM", "thyroglobulin_lab_canonical_v1",
         "Tg velocity / doubling time not in canonical; consider adding tg_velocity_per_year"),
        ("MEDIUM", "extracted_postop_labs_expanded_v1",
         "PTH/calcium nadir day and exact value not in canonical; only flags present"),
        ("LOW",    "survival_cohort_enriched",
         "Survival table has duplicate rows per patient; canonical_survival_followup_v1 is the SSOT"),
        ("LOW",    "molecular_variant_long",
         "Variant-level data (allele freq, codon) lost at patient rollup — acceptable for manuscript"),
        ("LOW",    "extracted_ete_subgraded_v1",
         "ete_grade_v9 in canonical covers this; subgrading source label may be worth adding"),
    ]
    lines.append("| Priority | Table | Recommendation |")
    lines.append("|----------|-------|----------------|")
    for pri, tbl, rec in recommendations:
        lines.append(f"| {pri} | `{tbl}` | {rec} |")
    lines.append("")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  → Report written: {REPORT_PATH}")


def get_columns_cached(inventory: pd.DataFrame) -> list[str]:
    """Return canonical column names from a cached inventory row if present."""
    row = inventory[inventory["table_name"] == CANONICAL_TABLE]
    if not row.empty:
        return row.iloc[0]["key_columns"].split(", ")
    return []


# ---------------------------------------------------------------------------
# Phase F — write backup manifest
# ---------------------------------------------------------------------------

def phase_f_manifest(backup_results: list[dict]) -> None:
    print("\n[Phase F] Writing MANIFEST.txt …")
    lines = [
        f"# Parquet Backup Manifest",
        f"# Database: {DB}",
        f"# Generated: {datetime.now().isoformat()}",
        "",
    ]
    for r in backup_results:
        p = BACKUP_DIR / f"{r['table']}.parquet"
        if p.exists():
            size_b = p.stat().st_size
            lines.append(f"{size_b:>14,}  {r['rows']:>10,} rows  {r['table']}.parquet")
        else:
            lines.append(f"{'MISSING':>14}  {'':>10}       {r['table']}.parquet")
    MANIFEST_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  → Manifest: {MANIFEST_PATH}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Script 210 — Database Audit + Parquet Backup")
    parser.add_argument("--dry-run", action="store_true", help="Skip actual parquet writes")
    parser.add_argument("--skip-backup", action="store_true", help="Skip parquet backup (audit only)")
    args = parser.parse_args()

    token = get_token()
    print(f"[init] Token: {'SET' if token else 'MISSING'} (length={len(token) if token else 0})")
    print(f"[init] Database: {DB}")
    print(f"[init] Dry-run: {args.dry_run} | Skip-backup: {args.skip_backup}")

    con = duckdb.connect(f"md:{DB}?motherduck_token={token}")

    t0 = time.time()

    # Phase A — inventory (also returns batched cols_by_table for reuse)
    inventory, cols_by_table = phase_a_inventory(con)

    # Phase B — gap analysis (reuses batch column metadata)
    gaps = phase_b_gaps(con, cols_by_table=cols_by_table)

    # Phase C — note types
    note_types = phase_c_note_types(con)

    # Phase D — parquet backup
    if not args.skip_backup:
        backup_results = phase_d_backup(con, dry_run=args.dry_run)
    else:
        print("\n[Phase D] Skipped (--skip-backup)")
        backup_results = []

    # Phase E — report
    phase_e_report(inventory, gaps, note_types, backup_results, args.dry_run,
                   cols_by_table=cols_by_table)

    # Phase F — manifest
    if not args.skip_backup and not args.dry_run:
        phase_f_manifest(backup_results)

    elapsed = round(time.time() - t0, 1)
    print(f"\n✓ Script 210 complete in {elapsed}s")

    # Summary stats
    ok_count = sum(1 for r in backup_results if r["status"] == "OK")
    total_rows = sum(r["rows"] for r in backup_results if r["status"] == "OK")
    total_mb = sum(r["size_mb"] for r in backup_results if r["status"] == "OK")
    print(f"  Tables inventoried : {len(inventory)}")
    print(f"  Tables backed up   : {ok_count} / {len(CRITICAL_TABLES)}")
    print(f"  Rows exported      : {total_rows:,}")
    print(f"  Total size         : {total_mb:.1f} MB")
    print(f"  Report             : {REPORT_PATH}")
    if not args.skip_backup and not args.dry_run:
        print(f"  Manifest           : {MANIFEST_PATH}")

    con.close()


if __name__ == "__main__":
    main()

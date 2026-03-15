#!/usr/bin/env python3
"""
100_canonical_metrics_registry.py — Canonical Metrics Governance Registry

Creates a single governed canonical metrics layer that all manuscript-facing docs,
release artifacts, and dashboard summary surfaces can trust.

Capabilities:
  1. Live-query MotherDuck prod (or dev/qa) for every metric.
  2. Materialize `canonical_metrics_registry_v1` TABLE in the target env.
  3. Export CSV / JSON / Markdown to exports/canonical_metrics_registry_YYYYMMDD_HHMM/.
  4. Generate docs/canonical_metrics_registry_YYYYMMDD.md.
  5. Stage in dev, promote through qa, then prod via --env flag.

Usage:
  # Run against dev (default for safety)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/100_canonical_metrics_registry.py --env dev

  # Validate live prod metrics
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/100_canonical_metrics_registry.py --env prod

  # Dry-run (no table writes, exports only)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/100_canonical_metrics_registry.py --env prod --dry-run

  # Write table + exports in prod
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/100_canonical_metrics_registry.py --env prod --write
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient  # noqa: E402

NOW = datetime.now(timezone.utc)
DATESTAMP = NOW.strftime("%Y%m%d")
TIMESTAMP = NOW.strftime("%Y%m%d_%H%M")


# ═══════════════════════════════════════════════════════════════════════════
# METRIC DEFINITIONS — single source of truth
# ═══════════════════════════════════════════════════════════════════════════
@dataclass
class MetricDef:
    """One canonical metric definition."""
    metric_id: str                 # Stable identifier (snake_case)
    metric_name: str               # Human-readable name
    metric_group: str              # Grouping (cohort, molecular, rai, ...)
    canonical_sql: str             # Exact SQL to compute value on MotherDuck
    canonical_table: str           # Primary source table
    numerator_def: str             # What the numerator counts
    denominator_def: str           # What the denominator counts (or 'N/A')
    use_tier: str                  # primary | descriptive | sensitivity | prohibited
    source_limitation: str         # '' if none, else brief description
    # Populated at runtime:
    canonical_value: int | float | None = None
    environment: str = ""
    last_verified_at: str = ""
    git_sha: str = ""


# ──────────────────────────────────────────────────────────────────────────
# Define all metrics.  SQL must be a single-value SELECT.
# ──────────────────────────────────────────────────────────────────────────
METRIC_DEFS: list[MetricDef] = [
    # ── Cohort denominators ───────────────────────────────────────────────
    MetricDef(
        "total_surgical_patients", "Total Surgical Patients", "cohort",
        "SELECT COUNT(DISTINCT research_id) FROM path_synoptics",
        "path_synoptics", "Distinct patients with at least one path_synoptic row",
        "N/A — universe", "primary", "",
    ),
    MetricDef(
        "master_cohort_rows", "Master Cohort Rows", "cohort",
        "SELECT COUNT(*) FROM master_cohort",
        "master_cohort", "Total rows in master_cohort (one per patient-source join)",
        "N/A", "descriptive", "",
    ),
    MetricDef(
        "manuscript_cohort_size", "Manuscript Cohort Size", "cohort",
        "SELECT COUNT(*) FROM manuscript_cohort_v1",
        "manuscript_cohort_v1", "Patients in frozen manuscript cohort",
        "N/A — frozen", "primary", "",
    ),
    MetricDef(
        "cancer_cohort_size", "Analysis-Eligible Cancer Cohort", "cohort",
        "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",
        "analysis_cancer_cohort_v1", "Patients with confirmed cancer + scoring eligibility",
        "manuscript_cohort_v1", "primary", "",
    ),
    MetricDef(
        "dedup_episodes", "Deduplicated Episodes", "cohort",
        "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",
        "episode_analysis_resolved_v1_dedup", "Surgery episodes after multi-path dedup",
        "N/A", "primary", "",
    ),
    MetricDef(
        "multi_surgery_patients", "Multi-Surgery Patients", "cohort",
        "SELECT COUNT(DISTINCT research_id) FROM tumor_episode_master_v2 WHERE surgery_episode_id > 1",
        "tumor_episode_master_v2", "Patients with 2+ surgeries",
        "total_surgical_patients", "primary", "",
    ),
    MetricDef(
        "multi_surgery_episodes", "Multi-Surgery Episodes", "cohort",
        "SELECT COUNT(*) FROM tumor_episode_master_v2 WHERE surgery_episode_id > 1",
        "tumor_episode_master_v2", "Episode rows for patients with 2+ surgeries",
        "N/A", "descriptive", "",
    ),
    MetricDef(
        "survival_cohort_enriched", "Survival Cohort (Enriched)", "cohort",
        "SELECT COUNT(*) FROM survival_cohort_enriched",
        "survival_cohort_enriched", "All patients in survival analytic table",
        "N/A", "descriptive", "",
    ),
    MetricDef(
        "master_clinical_v12_rows", "Master Clinical V12 Rows", "cohort",
        "SELECT COUNT(*) FROM patient_refined_master_clinical_v12",
        "patient_refined_master_clinical_v12",
        "Patients in final master clinical table", "N/A", "descriptive", "",
    ),

    # ── Molecular markers ─────────────────────────────────────────────────
    MetricDef(
        "braf_positive", "BRAF Positive Patients", "molecular",
        "SELECT COUNT(*) FROM patient_refined_master_clinical_v12 WHERE braf_positive_final IS TRUE",
        "patient_refined_master_clinical_v12",
        "Patients with confirmed BRAF V600E", "molecular_tested", "primary",
        "NLP false-positive correction applied (659→546); see reviewer_defense/06",
    ),
    MetricDef(
        "ras_positive", "RAS Positive Patients", "molecular",
        "SELECT COUNT(*) FROM patient_refined_master_clinical_v12 WHERE ras_positive_final IS TRUE",
        "patient_refined_master_clinical_v12",
        "Patients with confirmed RAS (NRAS/HRAS/KRAS)", "molecular_tested", "primary",
        "Includes Phase 11 recovery + Phase 13 subtype resolution",
    ),
    MetricDef(
        "tert_positive", "TERT Positive Patients", "molecular",
        "SELECT COUNT(*) FROM patient_refined_master_clinical_v12 WHERE tert_positive_v9 IS TRUE",
        "patient_refined_master_clinical_v12",
        "Patients with TERT promoter mutation", "molecular_tested", "primary", "",
    ),
    MetricDef(
        "molecular_tested_patients", "Molecular-Tested Patients", "molecular",
        "SELECT COUNT(DISTINCT research_id) FROM extracted_fna_bethesda_v1",
        "extracted_fna_bethesda_v1",
        "Patients with any molecular/Bethesda testing", "total_surgical_patients",
        "descriptive", "",
    ),

    # ── Recurrence ────────────────────────────────────────────────────────
    MetricDef(
        "recurrence_flagged", "Recurrence Any Flagged", "recurrence",
        "SELECT COUNT(*) FROM extracted_recurrence_refined_v1 WHERE recurrence_any IS TRUE",
        "extracted_recurrence_refined_v1",
        "Patients with any recurrence flag (structural or biochemical)",
        "total_surgical_patients", "primary", "",
    ),
    MetricDef(
        "recurrence_exact_date", "Recurrence Exact-Date Tier", "recurrence",
        "SELECT COUNT(*) FROM extracted_recurrence_refined_v1 WHERE recurrence_date_status = 'exact_source_date' AND recurrence_any IS TRUE",
        "extracted_recurrence_refined_v1",
        "Recurrence patients with day-level exact source date",
        "recurrence_flagged", "primary",
        "Only 2-3% of recurrence cases; rest unresolved — source limitation",
    ),
    MetricDef(
        "recurrence_biochem_inferred", "Recurrence Biochemical-Inferred", "recurrence",
        "SELECT COUNT(*) FROM extracted_recurrence_refined_v1 WHERE recurrence_date_status = 'biochemical_inflection_inferred' AND recurrence_any IS TRUE",
        "extracted_recurrence_refined_v1",
        "Recurrence patients with biochemical Tg inflection date",
        "recurrence_flagged", "sensitivity",
        "Rising Tg > 1.0 and > 2x nadir; conditional for TTE analysis",
    ),
    MetricDef(
        "recurrence_unresolved", "Recurrence Unresolved Date", "recurrence",
        "SELECT COUNT(*) FROM extracted_recurrence_refined_v1 WHERE recurrence_date_status = 'unresolved_date' AND recurrence_any IS TRUE",
        "extracted_recurrence_refined_v1",
        "Recurrence patients with no day-level date",
        "recurrence_flagged", "descriptive",
        "88.8% — requires manual chart review; prioritized queue deployed",
    ),

    # ── RAI ───────────────────────────────────────────────────────────────
    MetricDef(
        "rai_episodes", "RAI Treatment Episodes", "rai",
        "SELECT COUNT(*) FROM rai_treatment_episode_v2",
        "rai_treatment_episode_v2",
        "Total RAI treatment episode rows", "N/A", "primary", "",
    ),
    MetricDef(
        "rai_patients", "RAI Patients", "rai",
        "SELECT COUNT(DISTINCT research_id) FROM rai_treatment_episode_v2",
        "rai_treatment_episode_v2",
        "Distinct patients with any RAI episode", "total_surgical_patients",
        "primary", "",
    ),
    MetricDef(
        "rai_with_dose", "RAI Episodes With Dose", "rai",
        "SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL AND dose_mci > 0",
        "rai_treatment_episode_v2",
        "RAI episodes with quantified dose", "rai_episodes", "primary",
        "41% coverage — capped by absence of nuclear medicine notes (SL-02)",
    ),

    # ── Complications ─────────────────────────────────────────────────────
    MetricDef(
        "complication_any_patients", "Patients With Any Refined Complication", "complications",
        "SELECT COUNT(DISTINCT research_id) FROM extracted_complications_refined_v5",
        "extracted_complications_refined_v5",
        "Patients with at least one refined complication entity",
        "total_surgical_patients", "primary", "",
    ),
    MetricDef(
        "rln_injury_total", "RLN Injury Total (Incl Suspected)", "complications",
        "SELECT COUNT(*) FROM extracted_rln_injury_refined_v2",
        "extracted_rln_injury_refined_v2",
        "RLN injury patients (all tiers: confirmed + suspected)",
        "total_surgical_patients", "primary", "",
    ),

    # ── TIRADS & Imaging ──────────────────────────────────────────────────
    MetricDef(
        "tirads_patients", "TIRADS-Scored Patients", "imaging",
        "SELECT COUNT(*) FROM extracted_tirads_validated_v1",
        "extracted_tirads_validated_v1",
        "Patients with validated TIRADS score", "total_surgical_patients",
        "primary", "32.5% fill rate; improved from 4.2% via Phase 12",
    ),
    MetricDef(
        "imaging_fna_linkage_rows", "Imaging-FNA Linkage Rows", "linkage",
        "SELECT COUNT(*) FROM imaging_fna_linkage_v3",
        "imaging_fna_linkage_v3",
        "Cross-domain imaging→FNA linkages", "N/A", "descriptive", "",
    ),

    # ── Labs ──────────────────────────────────────────────────────────────
    MetricDef(
        "lab_canonical_rows", "Canonical Lab Rows", "labs",
        "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1",
        "longitudinal_lab_canonical_v1",
        "Total rows in unified lab table (5 analytes)",
        "N/A", "primary", "",
    ),
    MetricDef(
        "lab_canonical_patients", "Canonical Lab Patients", "labs",
        "SELECT COUNT(DISTINCT research_id) FROM longitudinal_lab_canonical_v1",
        "longitudinal_lab_canonical_v1",
        "Distinct patients with any canonical lab value",
        "total_surgical_patients", "primary", "",
    ),

    # ── Operative NLP ─────────────────────────────────────────────────────
    MetricDef(
        "operative_episodes", "Operative Episodes", "operative",
        "SELECT COUNT(*) FROM operative_episode_detail_v2",
        "operative_episode_detail_v2",
        "Total operative episode rows", "N/A", "descriptive", "",
    ),
    MetricDef(
        "operative_rln_monitoring", "Operative RLN Monitoring Flag", "operative",
        "SELECT SUM(CASE WHEN rln_monitoring_flag IS TRUE THEN 1 ELSE 0 END) FROM operative_episode_detail_v2",
        "operative_episode_detail_v2",
        "Episodes with RLN monitoring documented", "operative_episodes",
        "descriptive", "FALSE = NOT_PARSED, not confirmed-negative",
    ),

    # ── Linkage ───────────────────────────────────────────────────────────
    MetricDef(
        "surgery_pathology_linkage", "Surgery→Pathology Linkage", "linkage",
        "SELECT COUNT(*) FROM surgery_pathology_linkage_v3",
        "surgery_pathology_linkage_v3",
        "Cross-domain surgery→pathology linkages", "N/A", "descriptive", "",
    ),
    MetricDef(
        "fna_molecular_linkage", "FNA→Molecular Linkage", "linkage",
        "SELECT COUNT(*) FROM fna_molecular_linkage_v3",
        "fna_molecular_linkage_v3",
        "Cross-domain FNA→molecular test linkages", "N/A", "descriptive", "",
    ),

    # ── Scoring ───────────────────────────────────────────────────────────
    MetricDef(
        "ajcc8_calculable", "AJCC8 Calculable Patients", "scoring",
        "SELECT COUNT(*) FROM thyroid_scoring_py_v1 WHERE ajcc8_stage_group IS NOT NULL",
        "thyroid_scoring_py_v1",
        "Patients with calculable AJCC 8th edition staging",
        "total_surgical_patients", "primary",
        "37.6% full-cohort; ~96.6% among cancer-eligible",
    ),
    MetricDef(
        "ata_calculable", "ATA 2015 Calculable Patients", "scoring",
        "SELECT COUNT(*) FROM thyroid_scoring_py_v1 WHERE ata_initial_risk IS NOT NULL",
        "thyroid_scoring_py_v1",
        "Patients with calculable ATA 2015 initial risk",
        "total_surgical_patients", "primary",
        "28.9% full-cohort; ~76.0% among cancer-eligible",
    ),

    # ── Manual review ─────────────────────────────────────────────────────
    MetricDef(
        "adjudication_decisions", "Adjudication Decisions", "review",
        "SELECT COUNT(*) FROM adjudication_decisions",
        "adjudication_decisions",
        "Total reviewer adjudication decisions", "N/A", "descriptive", "",
    ),

    # ── Source-limited registry ───────────────────────────────────────────
    MetricDef(
        "motherduck_table_count", "MotherDuck Table Count", "infrastructure",
        "SELECT COUNT(DISTINCT table_name) FROM information_schema.tables WHERE table_schema='main'",
        "information_schema.tables",
        "Distinct tables in prod main schema", "N/A", "descriptive", "",
    ),
    MetricDef(
        "demographics_age_coverage_pct", "Demographics Age Coverage %", "demographics",
        "SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE age_at_surgery IS NOT NULL) / NULLIF(COUNT(*), 0), 1) FROM demographics_harmonized_v2",
        "demographics_harmonized_v2",
        "Percentage of patients with non-null age", "demographics_harmonized_v2 rows",
        "descriptive", "",
    ),
    MetricDef(
        "provenance_events", "Provenance Events", "provenance",
        "SELECT COUNT(*) FROM provenance_enriched_events_v1",
        "provenance_enriched_events_v1",
        "Total provenance-enriched event rows", "N/A", "descriptive", "",
    ),
]


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def get_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(ROOT), text=True,
        ).strip()
    except Exception:
        return "unknown"


def query_metric(con: Any, m: MetricDef, env: str) -> MetricDef:
    """Run the canonical SQL and populate runtime fields."""
    try:
        row = con.execute(m.canonical_sql).fetchone()
        m.canonical_value = row[0] if row else None
        if m.canonical_value is not None:
            # Try to keep as int when possible
            try:
                if float(m.canonical_value) == int(float(m.canonical_value)):
                    m.canonical_value = int(float(m.canonical_value))
                else:
                    m.canonical_value = round(float(m.canonical_value), 2)
            except (ValueError, TypeError):
                pass
    except Exception as e:
        m.canonical_value = None
        m.source_limitation = f"QUERY ERROR: {e}"
    m.environment = env
    m.last_verified_at = NOW.isoformat()
    m.git_sha = get_git_sha()
    return m


def metrics_to_dicts(metrics: list[MetricDef]) -> list[dict[str, Any]]:
    return [asdict(m) for m in metrics]


# ═══════════════════════════════════════════════════════════════════════════
# Materialization
# ═══════════════════════════════════════════════════════════════════════════

def materialize_table(con: Any, metrics: list[MetricDef], dry_run: bool = False) -> int:
    """Write canonical_metrics_registry_v1 TABLE in current database."""
    if dry_run:
        print("  [DRY-RUN] Skipping table materialization")
        return 0

    # Build INSERT values
    rows = []
    for m in metrics:
        val = "NULL" if m.canonical_value is None else (
            str(m.canonical_value) if isinstance(m.canonical_value, (int, float))
            else f"'{m.canonical_value}'"
        )
        rows.append(
            f"('{m.metric_id}','{m.metric_name}','{m.metric_group}',"
            f"$metric_sql${m.canonical_sql}$metric_sql$,"
            f"'{m.canonical_table}',"
            f"$num${m.numerator_def}$num$,"
            f"$den${m.denominator_def}$den$,"
            f"'{m.use_tier}',{val},"
            f"$sl${m.source_limitation}$sl$,"
            f"'{m.environment}','{m.last_verified_at}','{m.git_sha}')"
        )

    ddl = """
    CREATE OR REPLACE TABLE canonical_metrics_registry_v1 (
        metric_id               VARCHAR NOT NULL,
        metric_name             VARCHAR NOT NULL,
        metric_group            VARCHAR NOT NULL,
        canonical_sql           VARCHAR,
        canonical_table         VARCHAR,
        numerator_def           VARCHAR,
        denominator_def         VARCHAR,
        use_tier                VARCHAR,
        canonical_value         DOUBLE,
        source_limitation       VARCHAR,
        environment             VARCHAR,
        last_verified_at        VARCHAR,
        git_sha                 VARCHAR
    );
    """
    con.execute(ddl)
    for row_sql in rows:
        con.execute(f"INSERT INTO canonical_metrics_registry_v1 VALUES {row_sql}")

    n = con.execute("SELECT COUNT(*) FROM canonical_metrics_registry_v1").fetchone()[0]
    print(f"  Materialized canonical_metrics_registry_v1: {n} metrics")
    return int(n)


# ═══════════════════════════════════════════════════════════════════════════
# Export
# ═══════════════════════════════════════════════════════════════════════════

def export_registry(metrics: list[MetricDef], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # CSV
    import csv
    csv_path = out_dir / "canonical_metrics_registry_v1.csv"
    with csv_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(asdict(metrics[0]).keys()))
        w.writeheader()
        for m in metrics:
            w.writerow(asdict(m))
    print(f"  Exported CSV:  {csv_path}")

    # JSON
    json_path = out_dir / "canonical_metrics_registry_v1.json"
    blob = {
        "registry_version": "v1",
        "generated_at": NOW.isoformat(),
        "git_sha": get_git_sha(),
        "metric_count": len(metrics),
        "metrics": metrics_to_dicts(metrics),
    }
    json_path.write_text(json.dumps(blob, indent=2, default=str))
    print(f"  Exported JSON: {json_path}")

    # Manifest
    manifest = {
        "artifact": "canonical_metrics_registry",
        "version": "v1",
        "generated_at": NOW.isoformat(),
        "git_sha": get_git_sha(),
        "metric_count": len(metrics),
        "environment": metrics[0].environment if metrics else "unknown",
        "files": [
            str(csv_path.relative_to(ROOT)),
            str(json_path.relative_to(ROOT)),
        ],
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  Exported manifest: {manifest_path}")


def generate_markdown(metrics: list[MetricDef], out_path: Path) -> None:
    """Generate docs/canonical_metrics_registry_YYYYMMDD.md."""
    lines = [
        f"# Canonical Metrics Registry — {DATESTAMP}",
        "",
        f"**Generated:** {NOW.isoformat()}  ",
        f"**Git SHA:** `{get_git_sha()}`  ",
        f"**Environment:** {metrics[0].environment if metrics else 'unknown'}  ",
        f"**Total metrics:** {len(metrics)}",
        "",
        "## Governance Rules",
        "",
        "1. **All manuscript-facing counts** must reference this registry by `metric_id`.",
        "2. **Stale metrics** (>7 days since `last_verified_at`) trigger warnings in release gates.",
        "3. **Unregistered metrics** in manuscript docs trigger CI warnings.",
        "4. **Use tiers** control where a metric may appear:",
        "   - `primary` — manuscript text, abstract, tables",
        "   - `descriptive` — supplement, methods, internal dashboards",
        "   - `sensitivity` — sensitivity analyses only (conditional use)",
        "   - `prohibited` — known-bad value, must not be cited",
        "",
        "## Metrics",
        "",
    ]

    # Group by metric_group
    groups: dict[str, list[MetricDef]] = {}
    for m in metrics:
        groups.setdefault(m.metric_group, []).append(m)

    for group, group_metrics in groups.items():
        lines.append(f"### {group.replace('_', ' ').title()}")
        lines.append("")
        lines.append("| ID | Name | Value | Use Tier | Source Table | Limitation |")
        lines.append("|---|---|---|---|---|---|")
        for m in group_metrics:
            val = f"{m.canonical_value:,}" if isinstance(m.canonical_value, int) else (
                f"{m.canonical_value}" if m.canonical_value is not None else "—"
            )
            lim = m.source_limitation[:60] + "…" if len(m.source_limitation) > 60 else m.source_limitation
            lines.append(
                f"| `{m.metric_id}` | {m.metric_name} | **{val}** | {m.use_tier} | "
                f"`{m.canonical_table}` | {lim or '—'} |"
            )
        lines.append("")

    # Cross-source discrepancy notes
    lines.extend([
        "## Cross-Source Discrepancy Resolution",
        "",
        "| Metric | Old Registry (v1.csv) | Canonical (this registry) | Root Cause |",
        "|---|---|---|---|",
        "| BRAF+ | 376 (extracted_braf_recovery_v1) | **Uses patient_refined_master_clinical_v12** | NLP FP correction + multi-source recovery |",
        "| RAS+ | 292 (extracted_molecular_refined_v1) | **Uses patient_refined_master_clinical_v12** | Phase 11+13 subtype resolution |",
        "| TERT+ | 96 (extracted_molecular_refined_v1) | **Uses patient_refined_master_clinical_v12** | Subtype propagation from mol_test_episode_v2 |",
        "",
        "## Staleness Policy",
        "",
        "- Registry must be re-verified within **7 days** of any manuscript submission.",
        "- Release manifests auto-check registry freshness; stale registry → WARN gate.",
        "- Dashboard surfaces link to this registry; they must not hard-code counts.",
        "",
        f"*Registry hash: `{hashlib.sha256(json.dumps(metrics_to_dicts(metrics), default=str).encode()).hexdigest()[:12]}`*",
    ])

    out_path.write_text("\n".join(lines))
    print(f"  Generated markdown: {out_path}")


# ═══════════════════════════════════════════════════════════════════════════
# Drift detection helpers (importable by other scripts)
# ═══════════════════════════════════════════════════════════════════════════

def load_registry_from_table(con: Any) -> list[dict[str, Any]]:
    """Load canonical_metrics_registry_v1 from MotherDuck."""
    try:
        rows = con.execute(
            "SELECT * FROM canonical_metrics_registry_v1"
        ).fetchall()
        cols = [d[0] for d in con.description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []


def check_metric_drift(
    con: Any,
    metric_ids: list[str] | None = None,
    tolerance_pct: float = 1.0,
) -> list[dict[str, Any]]:
    """Check live values against registry; return drifted metrics.

    Returns list of dicts with: metric_id, registry_value, live_value, drift_pct, status.
    Useful for release gates and CI checks.
    """
    registry = load_registry_from_table(con)
    if not registry:
        return [{"metric_id": "REGISTRY_MISSING", "status": "FAIL",
                 "detail": "canonical_metrics_registry_v1 not found"}]

    results = []
    for rec in registry:
        mid = rec["metric_id"]
        if metric_ids and mid not in metric_ids:
            continue
        sql = rec.get("canonical_sql", "")
        reg_val = rec.get("canonical_value")
        if reg_val is None or not sql:
            continue
        try:
            live_val = con.execute(sql).fetchone()[0]
            if live_val is None:
                results.append({"metric_id": mid, "registry_value": reg_val,
                                "live_value": None, "drift_pct": None, "status": "WARN"})
                continue
            live_val = float(live_val)
            reg_val_f = float(reg_val)
            if reg_val_f == 0:
                drift = 0.0 if live_val == 0 else 100.0
            else:
                drift = abs(live_val - reg_val_f) / reg_val_f * 100.0
            status = "PASS" if drift <= tolerance_pct else "DRIFT"
            results.append({"metric_id": mid, "registry_value": reg_val_f,
                            "live_value": live_val, "drift_pct": round(drift, 2),
                            "status": status})
        except Exception as e:
            results.append({"metric_id": mid, "registry_value": reg_val,
                            "live_value": None, "drift_pct": None,
                            "status": "ERROR", "detail": str(e)})
    return results


def check_staleness_days(con: Any, max_days: int = 7) -> dict[str, Any]:
    """Check if registry is stale (oldest verification > max_days ago)."""
    registry = load_registry_from_table(con)
    if not registry:
        return {"status": "FAIL", "detail": "Registry not found", "oldest_days": None}

    oldest = None
    for rec in registry:
        ts = rec.get("last_verified_at", "")
        if ts:
            try:
                dt = datetime.fromisoformat(ts)
                age = (NOW - dt).days
                if oldest is None or age > oldest:
                    oldest = age
            except Exception:
                pass
    if oldest is None:
        return {"status": "WARN", "detail": "No valid timestamps", "oldest_days": None}
    status = "PASS" if oldest <= max_days else "STALE"
    return {"status": status, "oldest_days": oldest,
            "detail": f"Oldest metric verified {oldest}d ago (limit: {max_days}d)"}


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "prod"],
                        help="Target MotherDuck environment (default: dev)")
    parser.add_argument("--write", action="store_true",
                        help="Write canonical_metrics_registry_v1 TABLE in target env")
    parser.add_argument("--dry-run", action="store_true",
                        help="Query metrics but don't write table or exports")
    parser.add_argument("--sa", action="store_true",
                        help="Use service-account token (MD_SA_TOKEN)")
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  Canonical Metrics Registry — Phase 1 Governance Lock")
    print(f"  Environment: {args.env.upper()}")
    print(f"  Git SHA: {get_git_sha()}")
    print(f"  Timestamp: {NOW.isoformat()}")
    print(f"{'='*70}\n")

    # ── Connect ───────────────────────────────────────────────────────────
    try:
        client = MotherDuckClient.for_env(args.env, use_service_account=args.sa)
        con = client.connect_rw()
        print(f"  Connected to: {client.config.database}")
    except Exception as e:
        print(f"  ERROR: Cannot connect to {args.env}: {e}")
        sys.exit(2)

    # ── Query all metrics ─────────────────────────────────────────────────
    print(f"\n  Querying {len(METRIC_DEFS)} metrics...\n")
    t0 = time.time()
    errors = 0
    for m in METRIC_DEFS:
        query_metric(con, m, args.env)
        val_str = f"{m.canonical_value:,}" if isinstance(m.canonical_value, int) else str(m.canonical_value)
        status = "OK" if m.canonical_value is not None else "ERROR"
        if status == "ERROR":
            errors += 1
        icon = "✓" if status == "OK" else "✗"
        print(f"    {icon} {m.metric_id:<40} = {val_str}")

    elapsed = time.time() - t0
    print(f"\n  Queried {len(METRIC_DEFS)} metrics in {elapsed:.1f}s ({errors} errors)\n")

    if errors > 0:
        print(f"  WARNING: {errors} metrics returned NULL/ERROR — review before prod materialization\n")

    # ── Materialize table ─────────────────────────────────────────────────
    if args.write:
        materialize_table(con, METRIC_DEFS, dry_run=args.dry_run)
    elif not args.dry_run:
        print("  (use --write to materialize table in MotherDuck)")

    # ── Run drift check against itself (sanity) ──────────────────────────
    if args.write and not args.dry_run:
        print("\n  Running self-drift check...")
        drifts = check_metric_drift(con, tolerance_pct=0.0)
        drift_count = sum(1 for d in drifts if d["status"] == "DRIFT")
        if drift_count == 0:
            print(f"  ✓ Self-drift check: 0 drifted metrics (table matches live values)")
        else:
            print(f"  ✗ Self-drift check: {drift_count} drifted (stale immediately after write?)")

    con.close()

    # ── Export ────────────────────────────────────────────────────────────
    if not args.dry_run:
        export_dir = ROOT / "exports" / f"canonical_metrics_registry_{TIMESTAMP}"
        export_registry(METRIC_DEFS, export_dir)

        md_path = ROOT / "docs" / f"canonical_metrics_registry_{DATESTAMP}.md"
        generate_markdown(METRIC_DEFS, md_path)

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"  Metrics queried:   {len(METRIC_DEFS)}")
    print(f"  Errors:            {errors}")
    if args.write and not args.dry_run:
        print(f"  Table written:     canonical_metrics_registry_v1 ({args.env})")
    if not args.dry_run:
        print(f"  Exports:           exports/canonical_metrics_registry_{TIMESTAMP}/")
        print(f"  Docs:              docs/canonical_metrics_registry_{DATESTAMP}.md")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()

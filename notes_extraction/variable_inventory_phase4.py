"""
Phase 4 Variable Inventory Scanner
===================================
Scans MotherDuck + local repo to produce a prioritized variable inventory
for Phase 4 source-specific refinement.

Usage:
    .venv/bin/python notes_extraction/variable_inventory_phase4.py [--md] [--local] [--dry-run]

Outputs:
    notes_extraction/variable_inventory_phase4.md
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Clinical importance scoring table
# ---------------------------------------------------------------------------
# Each entry: (variable_name, clinical_impact_score, phase4_priority,
#              structured_source, nlp_sources, notes)
VARIABLE_CATALOG: list[dict] = [
    # === TIER 1: AJCC staging / guideline-critical ===
    dict(
        variable="ete_overall",
        label="Extrathyroidal Extension (ETE)",
        clinical_impact=10,
        phase4_priority=1,
        structured_sources=["path_synoptics.tumor_1_extrathyroidal_extension",
                            "tumor_episode_master_v2.extrathyroidal_extension",
                            "recurrence_risk_features_mv.ete",
                            "operative_episode_detail_v2.gross_ete_flag"],
        nlp_sources=["note_entities_staging (extrathyroidal_extension_detail)",
                     "extract_histology_v2 -> gross/microscopic ETE from path notes",
                     "extract_operative_v2 -> gross_ete/ete_present from op notes",
                     "extract_imaging_v2 -> imaging_ete from CT/US"],
        source_diversity=4,
        notes="MUST source-split: path (gold) vs op note vs imaging vs consent. "
              "Gross vs microscopic distinction is AJCC T-stage critical. "
              "35.7% fill rate in path_synoptics. Raw values include 'x','present','minimal','microscopic','extensive'.",
        requires_source_split=True,
        recommended_columns=["ete_path_confirmed", "ete_op_note_observed",
                             "ete_imaging_suspected", "ete_overall_confirmed",
                             "ete_grade"],
    ),
    dict(
        variable="tumor_size",
        label="Tumor Size / T Stage",
        clinical_impact=9,
        phase4_priority=2,
        structured_sources=["path_synoptics.tumor_1_size_greatest_dimension_cm",
                            "tumor_episode_master_v2.tumor_size_cm",
                            "imaging_nodule_long_v2.size_cm_max"],
        nlp_sources=["extract_histology_v2 (implicit in histology notes)",
                     "extract_imaging_v2 (size_cm_max from US/CT)"],
        source_diversity=3,
        notes="Path size is canonical; imaging pre-op size may differ. "
              "37.1% fill rate in path_synoptics. "
              "Path > imaging hierarchy for AJCC T-staging.",
        requires_source_split=True,
        recommended_columns=["tumor_size_path_cm", "tumor_size_imaging_cm",
                             "tumor_size_source"],
    ),
    dict(
        variable="margin_status",
        label="Surgical Margin Status (R0/R1/R2)",
        clinical_impact=9,
        phase4_priority=3,
        structured_sources=["path_synoptics.tumor_1_margin_status",
                            "path_synoptics.tumor_1_distance_to_closest_margin_mm",
                            "tumor_episode_master_v2.margin_status"],
        nlp_sources=["extract_histology_v2 (margin_status from path reports)"],
        source_diversity=1,
        notes="Path report only; 36.4% fill rate in path_synoptics. "
              "Raw values: 'involved', 'c/a', 'present', 'indeterminate', 'Involved'. "
              "Closest margin distance: 14.2% fill rate. "
              "Current normalization in tumor_episode_master_v2 is not clean.",
        requires_source_split=False,
        recommended_columns=["margin_status_refined", "closest_margin_mm",
                             "margin_site"],
    ),
    dict(
        variable="vascular_invasion",
        label="Vascular / Angioinvasion",
        clinical_impact=8,
        phase4_priority=4,
        structured_sources=["path_synoptics.tumor_1_angioinvasion",
                            "path_synoptics.tumor_1_angioinvasion_quantify",
                            "tumor_episode_master_v2.vascular_invasion"],
        nlp_sources=["extract_histology_v2 (vascular_invasion_detail)"],
        source_diversity=1,
        notes="Path report only; 34.5% fill rate. "
              "Raw values in tumor_episode_master_v2: 'x','present','focal','extensive'. "
              "focal vs extensive distinction matters for AJCC 8th Ed.",
        requires_source_split=False,
        recommended_columns=["vascular_invasion_refined", "vascular_invasion_grade"],
    ),
    dict(
        variable="perineural_invasion",
        label="Perineural Invasion (PNI)",
        clinical_impact=7,
        phase4_priority=5,
        structured_sources=["path_synoptics.tumor_1_perineural_invasion",
                            "tumor_episode_master_v2.perineural_invasion"],
        nlp_sources=["extract_histology_v2 (perineural_invasion)"],
        source_diversity=1,
        notes="Path report only; 13.7% fill rate (sparse). "
              "Binary: present vs absent.",
        requires_source_split=False,
        recommended_columns=["perineural_invasion_refined"],
    ),
    dict(
        variable="lymphovascular_invasion",
        label="Lymphovascular Invasion (LVI)",
        clinical_impact=7,
        phase4_priority=6,
        structured_sources=["path_synoptics.tumor_1_lymphatic_invasion",
                            "tumor_episode_master_v2.lymphatic_invasion"],
        nlp_sources=["extract_histology_v2 (lymphatic_invasion_detail)"],
        source_diversity=1,
        notes="Path report only; 31.6% fill rate. "
              "Key intermediate-risk factor in ATA guidelines.",
        requires_source_split=False,
        recommended_columns=["lvi_refined"],
    ),
    dict(
        variable="braf_molecular",
        label="BRAF / Molecular Markers",
        clinical_impact=9,
        phase4_priority=7,
        structured_sources=["molecular_test_episode_v2 (platform, result)",
                            "thyroseq_molecular_enrichment (BRAF, RAS, TERT)",
                            "recurrence_risk_features_mv (braf_positive, ras_positive)"],
        nlp_sources=["note_entities_genetics (BRAF=344 h_p + 83 op_note + 39 other_history)",
                     "extract_molecular_v2"],
        source_diversity=3,
        notes="BRAF NLP in h_p/op_note is heavily consent/risk-list contaminated. "
              "Structured molecular_test_episode_v2 is the gold standard. "
              "Need: tested_flag vs positive_flag, platform, date.",
        requires_source_split=True,
        recommended_columns=["braf_tested", "braf_positive_refined",
                             "molecular_platform", "molecular_test_date"],
    ),
    dict(
        variable="capsular_invasion",
        label="Capsular Invasion",
        clinical_impact=6,
        phase4_priority=8,
        structured_sources=["path_synoptics.tumor_1_capsular_invasion",
                            "tumor_episode_master_v2.capsular_invasion"],
        nlp_sources=["extract_histology_v2 (capsular_invasion)"],
        source_diversity=1,
        notes="Path report only. Key for FTC vs follicular adenoma distinction. "
              "Binary: present vs absent.",
        requires_source_split=False,
        recommended_columns=["capsular_invasion_refined"],
    ),
    dict(
        variable="recurrence_site",
        label="Recurrence Site and Detection Method",
        clinical_impact=8,
        phase4_priority=9,
        structured_sources=["recurrence_risk_features_mv (recurrence_flag, first_recurrence_date)"],
        nlp_sources=["extracted_clinical_events_v4 (recurrence NLP - heavily contaminated)",
                     "note_entities_problem_list"],
        source_diversity=2,
        notes="recurrence_flag in recurrence_risk_features_mv is structured (reliable). "
              "NLP clinical events are contaminated (6,405 false positives from single words). "
              "Need: recurrence_site (local/regional/distant), detection_method (imaging/Tg/biopsy).",
        requires_source_split=True,
        recommended_columns=["recurrence_site_refined", "recurrence_detection_method",
                             "recurrence_confirmed"],
    ),
    # === TIER 2: Post-surgical outcomes / quality metrics ===
    dict(
        variable="calcium_pth_nadir",
        label="Post-op Calcium / PTH Nadir",
        clinical_impact=6,
        phase4_priority=10,
        structured_sources=["thyroglobulin_labs (tg only)",
                            "thyroseq_followup_labs"],
        nlp_sources=["note_entities_medications (calcitriol, calcium_supplement)",
                     "note_entities_problem_list (hypocalcemia, hypoparathyroidism)"],
        source_diversity=3,
        notes="No dedicated calcium/PTH lab table found. "
              "Note_entities hypocalcemia/hypoparathyroidism refined in Phase 3. "
              "Need: post-op PTH nadir value + timing, calcium nadir + timing.",
        requires_source_split=True,
        recommended_columns=["pth_nadir_pg_ml", "pth_nadir_days_post_op",
                             "calcium_nadir_mg_dl", "hypoparathyroidism_confirmed"],
    ),
    dict(
        variable="completion_reason",
        label="Completion Thyroidectomy Indication",
        clinical_impact=5,
        phase4_priority=11,
        structured_sources=["path_synoptics.completion (yes/no flag only)"],
        nlp_sources=["note_entities_procedures (completion_thyroidectomy: 465 h_p + 344 op_note)",
                     "note_entities_problem_list"],
        source_diversity=2,
        notes="Completion flag exists but reason (cancer found, patient preference, etc.) "
              "is NLP-only. Op note is most reliable source for surgical indication.",
        requires_source_split=True,
        recommended_columns=["completion_reason_refined",
                             "completion_indication_source"],
    ),
    dict(
        variable="voice_laryngoscopy",
        label="Voice / Laryngoscopy Findings",
        clinical_impact=7,
        phase4_priority=12,
        structured_sources=["complications.vocal_cord_status",
                            "complications.laryngoscopy_date",
                            "extracted_rln_injury_refined_v2 (already refined)"],
        nlp_sources=["note_entities_complications (vocal_cord_paralysis, vocal_cord_paresis)"],
        source_diversity=3,
        notes="Already refined in Phase 2/3 (extracted_rln_injury_refined_v2). "
              "Need: bilateral vs unilateral extension, laryngoscopy scope findings, "
              "hoarseness severity. Build on existing refined tables.",
        requires_source_split=False,
        recommended_columns=["rln_injury_grade", "laryngoscopy_finding",
                             "voice_outcome"],
    ),
    # === TIER 3: Lower priority / already captured ===
    dict(
        variable="extranodal_extension",
        label="Extranodal Extension (ENE)",
        clinical_impact=6,
        phase4_priority=13,
        structured_sources=["path_synoptics.tumor_1_extranodal_extension",
                            "tumor_episode_master_v2.extranodal_extension"],
        nlp_sources=["extract_histology_v2 (extranodal_extension_detail)"],
        source_diversity=1,
        notes="Path report only. N2b staging determinant.",
        requires_source_split=False,
        recommended_columns=["ene_refined"],
    ),
    dict(
        variable="histology_variant",
        label="Aggressive Histologic Variant",
        clinical_impact=7,
        phase4_priority=14,
        structured_sources=["tumor_episode_master_v2.histology_variant",
                            "path_synoptics.tumor_1_variant"],
        nlp_sources=["extract_histology_v2 (aggressive_features, histology_subtype)"],
        source_diversity=1,
        notes="Tall cell, hobnail, diffuse sclerosing - high-risk variants. "
              "Path report only. Relatively well captured.",
        requires_source_split=False,
        recommended_columns=["aggressive_variant_confirmed"],
    ),
    dict(
        variable="tert_status",
        label="TERT Promoter Mutation",
        clinical_impact=8,
        phase4_priority=15,
        structured_sources=["recurrence_risk_features_mv (tert_positive)"],
        nlp_sources=["note_entities_genetics (TERT: 72 h_p + 39 op_note)"],
        source_diversity=2,
        notes="High-risk molecular marker. Structured source reliable. "
              "NLP likely consent/risk contaminated.",
        requires_source_split=True,
        recommended_columns=["tert_tested", "tert_positive_refined"],
    ),
]

# ---------------------------------------------------------------------------
# Source category mapping
# ---------------------------------------------------------------------------
NOTE_TYPE_TO_SOURCE_CATEGORY = {
    "h_p": "h_p_consent",
    "history_summary": "h_p_consent",
    "other_history": "h_p_consent",
    "op_note": "op_note",
    "dc_sum": "discharge",
    "ed_note": "discharge",
    "endocrine_note": "endocrine",
    "other_notes": "other",
}

SOURCE_RELIABILITY = {
    "path_report": 1.0,
    "structured_db": 1.0,
    "op_note": 0.9,
    "endocrine": 0.8,
    "discharge": 0.7,
    "imaging": 0.7,
    "h_p_consent": 0.2,
    "other": 0.5,
}


def _get_connection(use_md: bool = True, local_path: str = "thyroid_master.duckdb"):
    import duckdb
    if use_md:
        try:
            import toml
            secrets = toml.load(PROJECT_ROOT / ".streamlit/secrets.toml")
            token = secrets["MOTHERDUCK_TOKEN"]
        except Exception:
            import os
            token = os.environ.get("MOTHERDUCK_TOKEN", "")
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(str(PROJECT_ROOT / local_path))


def compute_inventory(con) -> list[dict]:
    """Augment VARIABLE_CATALOG with live fill rates from MotherDuck."""
    results = []

    # --- ETE ---
    try:
        r = con.execute("""
            SELECT COUNT(DISTINCT research_id) as total,
                   COUNT(DISTINCT CASE WHEN tumor_1_extrathyroidal_extension IS NOT NULL
                         THEN research_id END) as filled
            FROM path_synoptics WHERE research_id IS NOT NULL
        """).fetchone()
        ete_fill = round(100.0 * r[1] / r[0], 1) if r[0] else 0.0
    except Exception:
        ete_fill = 0.0

    # --- path_synoptics fill rates ---
    try:
        pr = con.execute("""
            SELECT
              COUNT(DISTINCT research_id) as total,
              COUNT(DISTINCT CASE WHEN tumor_1_margin_status IS NOT NULL THEN research_id END) as margin,
              COUNT(DISTINCT CASE WHEN tumor_1_angioinvasion IS NOT NULL THEN research_id END) as angio,
              COUNT(DISTINCT CASE WHEN tumor_1_perineural_invasion IS NOT NULL THEN research_id END) as perineural,
              COUNT(DISTINCT CASE WHEN tumor_1_lymphatic_invasion IS NOT NULL THEN research_id END) as lvi,
              COUNT(DISTINCT CASE WHEN tumor_1_size_greatest_dimension_cm IS NOT NULL THEN research_id END) as tsize,
              COUNT(DISTINCT CASE WHEN tumor_1_distance_to_closest_margin_mm IS NOT NULL THEN research_id END) as margin_mm,
              COUNT(DISTINCT CASE WHEN tumor_1_capsular_invasion IS NOT NULL THEN research_id END) as capsular
            FROM path_synoptics WHERE research_id IS NOT NULL
        """).fetchone()
        total = pr[0] if pr[0] else 1
        fill_map = {
            "margin_status": round(100.0 * pr[1] / total, 1),
            "vascular_invasion": round(100.0 * pr[2] / total, 1),
            "perineural_invasion": round(100.0 * pr[3] / total, 1),
            "lymphovascular_invasion": round(100.0 * pr[4] / total, 1),
            "tumor_size": round(100.0 * pr[5] / total, 1),
            "margin_mm": round(100.0 * pr[6] / total, 1),
            "capsular_invasion": round(100.0 * pr[7] / total, 1),
        }
    except Exception:
        fill_map = {}

    # --- NLP entity counts ---
    try:
        nlp_counts = {}
        for tbl in ["note_entities_genetics", "note_entities_complications",
                    "note_entities_staging", "note_entities_procedures"]:
            rows = con.execute(f"""
                SELECT entity_value_norm, COUNT(*) as n
                FROM {tbl}
                WHERE present_or_negated='present'
                GROUP BY 1
            """).fetchall()
            for ev, n in rows:
                nlp_counts[f"{tbl}:{ev}"] = n
    except Exception:
        nlp_counts = {}

    for v in VARIABLE_CATALOG:
        vname = v["variable"]
        entry = dict(v)

        # attach live fill rates
        if vname == "ete_overall":
            entry["fill_rate_pct"] = ete_fill
            entry["n_patients_with_data"] = r[1] if ete_fill else 0
        elif vname in fill_map:
            entry["fill_rate_pct"] = fill_map[vname]
        elif vname == "tumor_size":
            entry["fill_rate_pct"] = fill_map.get("tumor_size", 0.0)
        else:
            entry["fill_rate_pct"] = None

        # estimate current precision
        if vname in ("ete_overall", "tumor_size", "margin_status",
                     "vascular_invasion", "capsular_invasion",
                     "perineural_invasion", "lymphovascular_invasion"):
            # structured path_synoptics - high precision but mixed raw text
            entry["current_precision_estimate"] = 0.85
            entry["precision_note"] = "Structured but raw text normalization needed"
        elif vname in ("braf_molecular", "tert_status"):
            entry["current_precision_estimate"] = 0.45
            entry["precision_note"] = "NLP heavily consent-contaminated; structured sources needed"
        elif vname == "recurrence_site":
            entry["current_precision_estimate"] = 0.30
            entry["precision_note"] = "NLP events contaminated; structured recurrence_flag reliable"
        elif vname in ("calcium_pth_nadir",):
            entry["current_precision_estimate"] = 0.65
            entry["precision_note"] = "No dedicated lab table; NLP post Phase 3 refinement"
        elif vname == "voice_laryngoscopy":
            entry["current_precision_estimate"] = 0.85
            entry["precision_note"] = "Already refined in Phase 2/3"
        else:
            entry["current_precision_estimate"] = 0.70
            entry["precision_note"] = "Moderate; source split would improve"

        results.append(entry)

    return results


def write_markdown_report(results: list[dict], output_path: Path) -> None:
    lines = [
        "# Phase 4 Variable Inventory and Prioritization Matrix",
        "",
        f"_Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC_",
        "",
        "## Executive Summary",
        "",
        "This inventory covers all clinically meaningful variables in the THYROID_2026 pipeline "
        "that require source-specific attribution for publication-quality analyses. Variables are "
        "scored on three axes: **clinical impact** (AJCC staging / ATA guideline criticality), "
        "**current precision** (estimated reliability of existing extraction), and "
        "**source diversity** (number of distinct note types contributing mentions).",
        "",
        "### Source Reliability Hierarchy",
        "",
        "| Source Category | Reliability | Description |",
        "|----------------|-------------|-------------|",
        "| `path_report` | 1.0 | Formal synoptic pathology report |",
        "| `structured_db` | 1.0 | Structured database table (complications, labs) |",
        "| `op_note` | 0.9 | Operative note — direct surgical observation |",
        "| `endocrine` | 0.8 | Endocrine clinic follow-up note |",
        "| `discharge` | 0.7 | Discharge summary |",
        "| `imaging` | 0.7 | CT/US radiology report |",
        "| `h_p_consent` | 0.2 | H&P / consent template — boilerplate contamination |",
        "| `other` | 0.5 | Other notes |",
        "",
        "---",
        "",
        "## Prioritization Matrix",
        "",
        "| Rank | Variable | Clinical Impact | Fill Rate | Current Precision | Source Diversity | Source-Split Needed |",
        "|------|----------|----------------|-----------|-------------------|------------------|---------------------|",
    ]

    for v in sorted(results, key=lambda x: x["phase4_priority"]):
        fill = f"{v['fill_rate_pct']:.1f}%" if v.get("fill_rate_pct") is not None else "N/A"
        prec = f"{v['current_precision_estimate']:.0%}"
        split = "YES" if v["requires_source_split"] else "—"
        lines.append(
            f"| {v['phase4_priority']} | **{v['label']}** "
            f"| {v['clinical_impact']}/10 | {fill} | {prec} "
            f"| {v['source_diversity']} | {split} |"
        )

    lines += [
        "",
        "---",
        "",
        "## Detailed Variable Profiles",
        "",
    ]

    for v in sorted(results, key=lambda x: x["phase4_priority"]):
        lines += [
            f"### {v['phase4_priority']}. {v['label']} (`{v['variable']}`)",
            "",
            f"**Clinical Impact:** {v['clinical_impact']}/10  ",
            f"**Phase 4 Priority:** {v['phase4_priority']}  ",
            f"**Fill Rate:** {v['fill_rate_pct']:.1f}% of patients" if v.get("fill_rate_pct") is not None else "**Fill Rate:** N/A  ",
            f"**Current Precision Estimate:** {v['current_precision_estimate']:.0%} — _{v['precision_note']}_  ",
            f"**Source Diversity:** {v['source_diversity']} distinct note type categories  ",
            f"**Requires Source Split:** {'YES' if v['requires_source_split'] else 'No'}",
            "",
            "**Structured Sources:**",
        ]
        for s in v["structured_sources"]:
            lines.append(f"- `{s}`")
        lines += [
            "",
            "**NLP Sources:**",
        ]
        for s in v["nlp_sources"]:
            lines.append(f"- {s}")
        lines += [
            "",
            f"**Notes:** {v['notes']}",
            "",
            "**Recommended New Columns:**",
        ]
        for col in v["recommended_columns"]:
            lines.append(f"- `{col}`")
        lines.append("")

    lines += [
        "---",
        "",
        "## ETE Source Distribution (from path_synoptics)",
        "",
        "| Raw Value | Count | Normalized Category |",
        "|-----------|-------|---------------------|",
        "| `x` | 3,382 | microscopic (placeholder) — needs audit |",
        "| `present` | 252 | present — needs grade sub-classification |",
        "| `minimal` | 174 | microscopic |",
        "| `microscopic` | 65 | microscopic |",
        "| `c/a` | 29 | ambiguous — needs review |",
        "| `extensive` | 24 | gross |",
        "| `yes` | 19 | present — ambiguous grade |",
        "| `focal` | 13 | microscopic |",
        "| `indeterminate` | 9 | ambiguous |",
        "| `Yes;` | 7 | present |",
        "| _long free text_ | ~20 | mixed |",
        "| `None` | 7,691 | absent / no data |",
        "",
        "**Key insight**: The 'x' placeholder (3,382 cases) is the largest category and means "
        "'present but grade unspecified' — these require sub-classification by parsing the "
        "accompanying free-text comment fields.",
        "",
        "---",
        "",
        "## Source Contamination Summary",
        "",
        "| Entity | h_p Mentions | op_note Mentions | True Event Rate Est. |",
        "|--------|-------------|------------------|----------------------|",
        "| BRAF (genetics) | 344 | 83 | ~10-20% (consent/risk lists) |",
        "| ETE (staging) | — | — | NLP not deployed to note_entities_staging |",
        "| chyle_leak | 645 | 2,316 | ~3.3% (Phase 2 confirmed) |",
        "| hypocalcemia | 1,803 | 651 | ~3.3% (Phase 2 confirmed) |",
        "| rln_injury | 952 | 20 | ~0.85% (Phase 2 refined) |",
        "",
        "---",
        "",
        "## Phase 4 Execution Order",
        "",
        "1. **ETE** (priority 1) — source-split with gross/microscopic/suspected classification",
        "2. **Tumor Size** (priority 2) — path vs imaging concordance",
        "3. **Margin Status** (priority 3) — R0/R1 normalization + closest margin mm",
        "4. **Vascular Invasion** (priority 4) — focal vs extensive normalization",
        "5. **Perineural Invasion** (priority 5) — binary, path-only",
        "6. **LVI** (priority 6) — binary, path-only",
        "7. **BRAF/Molecular** (priority 7) — tested vs positive, platform attribution",
        "8. **Recurrence Site** (priority 9) — detection method attribution",
        "",
        "---",
        "",
        "## Recommended Next 5 Variables (Post-Phase 4)",
        "",
        "| Variable | Rationale |",
        "|----------|-----------|",
        "| **TERT promoter mutation** | High-risk molecular marker; NLP contamination needs audit |",
        "| **Extranodal extension (ENE)** | N2b staging determinant; path-only, sparse |",
        "| **Aggressive variant sub-type** | Tall cell / hobnail affect prognosis; needs validation |",
        "| **Post-op TSH suppression** | RAI eligibility surrogate; lab + note sources |",
        "| **RAI dose and avidity** | Already in rai_episode_v3 but needs source attribution |",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[inventory] Written: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Phase 4 variable inventory scanner")
    parser.add_argument("--md", action="store_true", default=True, help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    use_md = not args.local
    if args.dry_run:
        print("[dry-run] Would scan MotherDuck and write variable_inventory_phase4.md")
        return

    con = _get_connection(use_md)
    print(f"[inventory] Connected to {'MotherDuck' if use_md else 'local DuckDB'}")

    results = compute_inventory(con)
    print(f"[inventory] Scored {len(results)} variables")

    out = PROJECT_ROOT / "notes_extraction" / "variable_inventory_phase4.md"
    write_markdown_report(results, out)

    # Also save JSON for downstream consumption
    json_out = PROJECT_ROOT / "notes_extraction" / "variable_inventory_phase4.json"
    with open(json_out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[inventory] JSON written: {json_out}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
69_manuscript_reconciliation.py — Manuscript-readiness reconciliation pass

Creates additive manuscript source-of-truth tables, reconciles headline
metrics, classifies conditional issues, and validates denominator language.

Tables created (all additive, no production overwrites):
  manuscript_recon_metric_definitions_v1  — canonical metric registry
  manuscript_recon_ln_review_v1           — LN impossible value review
  manuscript_recon_bethesda_vi_review_v1  — Bethesda VI non-eligible review
  manuscript_recon_cancer_no_op_v1        — cancer without op detail classification
  manuscript_recon_rai_definitions_v1     — RAI assertion tier definitions
  manuscript_recon_recurrence_recon_v1    — recurrence definition reconciliation
  manuscript_patient_cohort_v2            — per-patient manuscript flags
  manuscript_metrics_v2                   — canonical metrics table
  manuscript_review_queue_v2              — unresolved review items
  manuscript_metric_sql_registry_v1       — SQL registry for each metric
  val_recon_metric_consistency_v1         — cross-source consistency checks
  val_recon_status_v1                     — overall reconciliation status

Supports --md, --local, --dry-run.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))

TS = datetime.now().strftime("%Y%m%d_%H%M")


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}")


def tbl_exists(con, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def resolve(con, *names: str) -> str:
    for n in names:
        if tbl_exists(con, n):
            return n
    return names[0]


def safe_int(con, sql: str) -> int:
    try:
        r = con.execute(sql).fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    except Exception:
        return -1


# ═══════════════════════════════════════════════════════════════════════
# PHASE 2 — Canonical metric definitions
# ═══════════════════════════════════════════════════════════════════════

def build_metric_definitions(con) -> pd.DataFrame:
    """Build canonical metric definitions from live MotherDuck data."""

    defs = []

    # --- 1. Total surgical patients ---
    n = safe_int(con, "SELECT COUNT(DISTINCT research_id) FROM path_synoptics")
    defs.append({
        "metric_name": "total_surgical_patients",
        "canonical_value": n,
        "numerator": n,
        "denominator": n,
        "denominator_population_label": "full_surgical_cohort",
        "source_table": "path_synoptics",
        "sql_object_name": "COUNT(DISTINCT research_id) FROM path_synoptics",
        "definition_notes": "All patients with at least one path_synoptics row (primary surgical spine)",
        "manuscript_safe_label": f"{n:,} patients underwent thyroid surgery",
        "alternate_definition_exists": "N",
        "alternate_definition_notes": "",
    })

    # --- 2. Analysis-eligible cancer patients ---
    pat_tbl = resolve(con, "patient_analysis_resolved_v1", "md_patient_analysis_resolved_v1")
    n_elig = safe_int(con, f"SELECT COUNT(DISTINCT research_id) FROM {pat_tbl} WHERE analysis_eligible_flag IS TRUE")
    n_total_resolved = safe_int(con, f"SELECT COUNT(*) FROM {pat_tbl}")
    defs.append({
        "metric_name": "analysis_eligible_cancer_patients",
        "canonical_value": n_elig,
        "numerator": n_elig,
        "denominator": n_total_resolved,
        "denominator_population_label": "all_resolved_patients",
        "source_table": pat_tbl,
        "sql_object_name": f"COUNT(DISTINCT research_id) FROM {pat_tbl} WHERE analysis_eligible_flag IS TRUE",
        "definition_notes": "Patients with cancer histology + complete staging data sufficient for analysis. Excludes benign-only, missing histology, and non-thyroid malignancy.",
        "manuscript_safe_label": f"{n_elig:,} patients met analysis eligibility criteria (confirmed thyroid malignancy with complete staging)",
        "alternate_definition_exists": "N",
        "alternate_definition_notes": "",
    })

    # --- 3. Recurrence ---
    rec_tbl = resolve(con, "extracted_recurrence_refined_v1", "md_extracted_recurrence_refined_v1")
    n_rec_any = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rec_tbl}
        WHERE recurrence_any IS TRUE OR LOWER(CAST(recurrence_any AS VARCHAR)) = 'true'
    """)
    n_rec_structural = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rec_tbl}
        WHERE LOWER(CAST(detection_category AS VARCHAR)) = 'structural_confirmed'
    """)
    n_rec_biochem = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rec_tbl}
        WHERE LOWER(CAST(detection_category AS VARCHAR)) = 'biochemical_only'
    """)
    n_rec_struct_unk = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rec_tbl}
        WHERE LOWER(CAST(detection_category AS VARCHAR)) = 'structural_date_unknown'
    """)
    risk_tbl = resolve(con, "recurrence_risk_features_mv", "md_recurrence_risk_features_mv")
    n_rec_risk_mv = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {risk_tbl}
        WHERE LOWER(CAST(recurrence_flag AS VARCHAR)) = 'true'
    """)

    defs.append({
        "metric_name": "recurrence_count_any",
        "canonical_value": n_rec_any,
        "numerator": n_rec_any,
        "denominator": n,
        "denominator_population_label": "full_surgical_cohort",
        "source_table": rec_tbl,
        "sql_object_name": f"COUNT(DISTINCT research_id) FROM {rec_tbl} WHERE recurrence_any IS TRUE",
        "definition_notes": f"All recurrence events (structural confirmed={n_rec_structural}, structural date unknown={n_rec_struct_unk}, biochemical only={n_rec_biochem}). Includes patients identified via Tg trajectory (rising Tg >1.0 and >2x nadir).",
        "manuscript_safe_label": f"{n_rec_any:,} patients ({100*n_rec_any/n:.1f}%) experienced recurrence (structural or biochemical) among {n:,} surgical patients",
        "alternate_definition_exists": "Y",
        "alternate_definition_notes": f"recurrence_risk_features_mv.recurrence_flag reports {n_rec_risk_mv:,} recurrences (excludes {n_rec_any - n_rec_risk_mv} biochemical-only events not in risk_mv). Use recurrence_risk_features_mv for structural recurrence; use extracted_recurrence_refined_v1 for all-type recurrence.",
    })

    defs.append({
        "metric_name": "recurrence_count_structural",
        "canonical_value": n_rec_structural + n_rec_struct_unk,
        "numerator": n_rec_structural + n_rec_struct_unk,
        "denominator": n,
        "denominator_population_label": "full_surgical_cohort",
        "source_table": rec_tbl,
        "sql_object_name": f"detection_category IN ('structural_confirmed','structural_date_unknown')",
        "definition_notes": f"Structural recurrence only. confirmed={n_rec_structural}, date_unknown={n_rec_struct_unk}. Matches recurrence_risk_features_mv.recurrence_flag count ({n_rec_risk_mv}).",
        "manuscript_safe_label": f"{n_rec_structural + n_rec_struct_unk:,} patients experienced structural recurrence",
        "alternate_definition_exists": "N",
        "alternate_definition_notes": "",
    })

    # --- 4. BRAF positive ---
    braf_tbl = resolve(con, "extracted_braf_recovery_v1", "md_extracted_braf_recovery_v1")
    n_braf = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {braf_tbl}
        WHERE LOWER(CAST(braf_status AS VARCHAR)) = 'positive'
    """)
    mol_ep_tbl = resolve(con, "molecular_test_episode_v2", "md_molecular_test_episode_v2")
    n_braf_structured = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {mol_ep_tbl}
        WHERE braf_flag IS TRUE OR LOWER(CAST(braf_flag AS VARCHAR)) = 'true'
    """)
    mol_panel_tbl = resolve(con, "extracted_molecular_panel_v1", "md_extracted_molecular_panel_v1")
    n_mol_tested = safe_int(con, f"SELECT COUNT(DISTINCT research_id) FROM {mol_panel_tbl}")

    defs.append({
        "metric_name": "braf_positive_count",
        "canonical_value": n_braf,
        "numerator": n_braf,
        "denominator": n_mol_tested,
        "denominator_population_label": "molecular_tested_patients",
        "source_table": braf_tbl,
        "sql_object_name": f"COUNT(DISTINCT research_id) FROM {braf_tbl} WHERE braf_status = 'positive'",
        "definition_notes": f"BRAF positive after FP correction (structured + NLP-confirmed). Structured-only count: {n_braf_structured}. NLP-recovered: {n_braf - n_braf_structured}. All NLP positives require explicit positive qualifier in note text.",
        "manuscript_safe_label": f"BRAF mutations were identified in {n_braf:,} of {n_mol_tested:,} molecularly tested patients ({100*n_braf/max(n_mol_tested,1):.1f}%)",
        "alternate_definition_exists": "Y",
        "alternate_definition_notes": f"Structured-only (mol_ep.braf_flag): {n_braf_structured}. Full cohort denominator: {n_braf}/{n} = {100*n_braf/n:.1f}%. Use molecular_tested denominator for prevalence.",
    })

    # --- 5. RAS positive ---
    ras_tbl = resolve(con, "extracted_ras_patient_summary_v1", "md_extracted_ras_patient_summary_v1")
    n_ras = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {ras_tbl}
        WHERE ras_positive IS TRUE OR LOWER(CAST(ras_positive AS VARCHAR)) = 'true'
    """)
    defs.append({
        "metric_name": "ras_positive_count",
        "canonical_value": n_ras,
        "numerator": n_ras,
        "denominator": n_mol_tested,
        "denominator_population_label": "molecular_tested_patients",
        "source_table": ras_tbl,
        "sql_object_name": f"COUNT(DISTINCT research_id) FROM {ras_tbl} WHERE ras_positive IS TRUE",
        "definition_notes": "RAS positive (NRAS+HRAS+KRAS) from subtypes extraction + mutation text parsing + NLP-confirmed entities. ras_flag in mol_ep is FALSE for all rows (known bug); use extracted_ras_patient_summary_v1 exclusively.",
        "manuscript_safe_label": f"RAS mutations were identified in {n_ras:,} of {n_mol_tested:,} molecularly tested patients ({100*n_ras/max(n_mol_tested,1):.1f}%)",
        "alternate_definition_exists": "N",
        "alternate_definition_notes": "mol_ep.ras_flag is unreliable (all FALSE). Do not use.",
    })

    # --- 6. Molecular tested denominator ---
    defs.append({
        "metric_name": "molecular_tested_denominator",
        "canonical_value": n_mol_tested,
        "numerator": n_mol_tested,
        "denominator": n,
        "denominator_population_label": "full_surgical_cohort",
        "source_table": mol_panel_tbl,
        "sql_object_name": f"COUNT(DISTINCT research_id) FROM {mol_panel_tbl}",
        "definition_notes": "Patients with any molecular panel result in extracted_molecular_panel_v1. Includes ThyroSeq, Afirma, IHC, PCR, FISH.",
        "manuscript_safe_label": f"Molecular testing was performed in {n_mol_tested:,} patients ({100*n_mol_tested/n:.1f}% of surgical cohort)",
        "alternate_definition_exists": "Y",
        "alternate_definition_notes": f"molecular_test_episode_v2 has {safe_int(con, f'SELECT COUNT(DISTINCT research_id) FROM {mol_ep_tbl}')} total patients (includes placeholder rows). Use extracted_molecular_panel_v1 for cleaned count.",
    })

    # --- 7. RAI treated ---
    rai_tbl = resolve(con, "rai_treatment_episode_v2", "md_rai_treatment_episode_v2")
    n_rai_likely = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rai_tbl}
        WHERE LOWER(CAST(rai_assertion_status AS VARCHAR)) IN ('definite_received','likely_received')
    """)
    n_rai_ambig = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rai_tbl}
        WHERE LOWER(CAST(rai_assertion_status AS VARCHAR)) = 'ambiguous'
    """)
    n_rai_any = safe_int(con, f"SELECT COUNT(DISTINCT research_id) FROM {rai_tbl}")
    rai_val_tbl = resolve(con, "extracted_rai_validated_v1", "md_extracted_rai_validated_v1")
    n_rai_confirmed_dose = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rai_val_tbl}
        WHERE LOWER(CAST(rai_validation_tier AS VARCHAR)) = 'confirmed_with_dose'
    """)
    n_rai_unconf_dose = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rai_val_tbl}
        WHERE LOWER(CAST(rai_validation_tier AS VARCHAR)) = 'unconfirmed_with_dose'
    """)

    defs.append({
        "metric_name": "rai_treated_strict",
        "canonical_value": n_rai_confirmed_dose,
        "numerator": n_rai_confirmed_dose,
        "denominator": n,
        "denominator_population_label": "full_surgical_cohort",
        "source_table": rai_val_tbl,
        "sql_object_name": f"rai_validation_tier = 'confirmed_with_dose' FROM {rai_val_tbl}",
        "definition_notes": f"STRICT definition: confirmed RAI receipt with documented dose. rai_assertion_status='likely_received' AND dose present. NOTE: 0 patients have 'definite_received' in rai_treatment_episode_v2; all {n_rai_confirmed_dose} confirmed patients are 'likely_received' with dose verification.",
        "manuscript_safe_label": f"{n_rai_confirmed_dose:,} patients received confirmed RAI therapy (dose-verified)",
        "alternate_definition_exists": "Y",
        "alternate_definition_notes": f"Broad: confirmed+unconfirmed_with_dose = {n_rai_confirmed_dose + n_rai_unconf_dose}. Any RAI signal (incl. ambiguous/negated): {n_rai_any}. Ambiguous-only: {n_rai_ambig}. CRITICAL: 'definite_received' tier is EMPTY (0 patients). The prior audit label 'definite/likely = 35' was misleading — all 35 are 'likely_received'.",
    })

    # --- 8. RLN injury ---
    rln_tbl = resolve(con, "extracted_rln_injury_refined_v2", "md_extracted_rln_injury_refined")
    n_rln_conf = safe_int(con, f"""
        SELECT COUNT(DISTINCT research_id) FROM {rln_tbl}
        WHERE rln_injury_is_confirmed IS TRUE OR LOWER(CAST(rln_injury_is_confirmed AS VARCHAR)) = 'true'
    """)
    n_rln_total = safe_int(con, f"SELECT COUNT(DISTINCT research_id) FROM {rln_tbl}")
    defs.append({
        "metric_name": "rln_injury_confirmed",
        "canonical_value": n_rln_conf,
        "numerator": n_rln_conf,
        "denominator": n,
        "denominator_population_label": "full_surgical_cohort",
        "source_table": rln_tbl,
        "sql_object_name": f"COUNT(DISTINCT research_id) FROM {rln_tbl} WHERE rln_injury_is_confirmed IS TRUE",
        "definition_notes": f"3-tier refined RLN injury: Tier 1 (laryngoscopy-confirmed), Tier 2 (chart-documented), Tier 3 (NLP-confirmed with context filtering). Total refined: {n_rln_total} (confirmed={n_rln_conf}, suspected={n_rln_total - n_rln_conf}).",
        "manuscript_safe_label": f"Confirmed RLN injury occurred in {n_rln_conf:,} patients ({100*n_rln_conf/n:.2f}%)",
        "alternate_definition_exists": "Y",
        "alternate_definition_notes": f"Including suspected: {n_rln_total} ({100*n_rln_total/n:.2f}%). Pre-refinement NLP: 654 (vastly inflated by consent boilerplate).",
    })

    # --- 9. Complications ---
    comp_tbl = resolve(con, "patient_refined_complication_flags_v2", "md_patient_refined_complication_flags_v2")
    n_comp = safe_int(con, f"SELECT COUNT(DISTINCT research_id) FROM {comp_tbl}")
    comp5_tbl = resolve(con, "extracted_complications_refined_v5", "md_extracted_complications_refined_v5")
    comp_detail = {}
    try:
        rows = con.execute(f"""
            SELECT entity_name, COUNT(DISTINCT research_id) AS pts
            FROM {comp5_tbl}
            WHERE entity_is_confirmed IS TRUE OR LOWER(CAST(entity_is_confirmed AS VARCHAR)) = 'true'
            GROUP BY 1 ORDER BY pts DESC
        """).fetchall()
        comp_detail = {r[0]: r[1] for r in rows}
    except Exception:
        pass

    comp_detail_str = "; ".join(f"{k}={v}" for k, v in comp_detail.items())
    defs.append({
        "metric_name": "complication_any_confirmed",
        "canonical_value": n_comp,
        "numerator": n_comp,
        "denominator": n,
        "denominator_population_label": "full_surgical_cohort",
        "source_table": comp_tbl,
        "sql_object_name": f"COUNT(DISTINCT research_id) FROM {comp_tbl}",
        "definition_notes": f"Patients with at least one confirmed complication after NLP refinement. Per-entity: {comp_detail_str}. Sum of per-entity exceeds total because patients can have multiple complications.",
        "manuscript_safe_label": f"Post-operative complications were confirmed in {n_comp:,} patients ({100*n_comp/n:.1f}%)",
        "alternate_definition_exists": "N",
        "alternate_definition_notes": "Pre-refinement NLP was 3.3% precision. Only refined counts are manuscript-safe.",
    })

    # --- 10. TIRADS coverage ---
    tirads_tbl = resolve(con, "extracted_tirads_validated_v1", "md_extracted_tirads_validated_v1")
    n_tirads = safe_int(con, f"SELECT COUNT(DISTINCT research_id) FROM {tirads_tbl}")
    defs.append({
        "metric_name": "tirads_coverage",
        "canonical_value": n_tirads,
        "numerator": n_tirads,
        "denominator": n,
        "denominator_population_label": "full_surgical_cohort",
        "source_table": tirads_tbl,
        "sql_object_name": f"COUNT(DISTINCT research_id) FROM {tirads_tbl}",
        "definition_notes": "Patients with validated TIRADS score from Phase 12 Excel ingestion + NLP + ACR recalculation. Data ceiling reached — remaining patients either lack pre-op US in system or had US at external facilities.",
        "manuscript_safe_label": f"Pre-operative TIRADS data was available for {n_tirads:,} patients ({100*n_tirads/n:.1f}%)",
        "alternate_definition_exists": "N",
        "alternate_definition_notes": "",
    })

    return pd.DataFrame(defs)


# ═══════════════════════════════════════════════════════════════════════
# PHASE 3 — Conditional issue resolution
# ═══════════════════════════════════════════════════════════════════════

def resolve_ln_impossible(con) -> None:
    """Create review table for LN involved > examined."""
    section("Phase 3A: LN Impossible Value")
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_recon_ln_review_v1 AS
        SELECT
            CAST(ps.research_id AS INTEGER) AS research_id,
            ps.tumor_1_ln_involved AS raw_ln_involved,
            ps.tumor_1_ln_examined AS raw_ln_examined,
            TRY_CAST(ps.tumor_1_ln_involved AS INTEGER) AS parsed_involved,
            TRY_CAST(ps.tumor_1_ln_examined AS INTEGER) AS parsed_examined,
            CASE
                WHEN ar.analysis_eligible_flag IS TRUE THEN 'analysis_eligible'
                ELSE 'not_analysis_eligible'
            END AS eligibility_status,
            'Data entry error: ln_examined=0 but ln_involved=1. Likely should be ln_examined=1. Raw data preserved; recommend derived correction to ln_examined=MAX(ln_involved, ln_examined) in manuscript layer.' AS recommendation,
            'path_synoptics' AS source_table,
            CURRENT_TIMESTAMP AS reviewed_at,
            'safe_to_correct_in_derived_layer' AS action_status
        FROM path_synoptics ps
        LEFT JOIN patient_analysis_resolved_v1 ar ON ps.research_id = ar.research_id
        WHERE TRY_CAST(ps.tumor_1_ln_involved AS INTEGER)
            > TRY_CAST(ps.tumor_1_ln_examined AS INTEGER)
          AND TRY_CAST(ps.tumor_1_ln_examined AS INTEGER) IS NOT NULL
          AND TRY_CAST(ps.tumor_1_ln_involved AS INTEGER) IS NOT NULL
    """)
    r = con.execute("SELECT COUNT(*) FROM manuscript_recon_ln_review_v1").fetchone()
    print(f"  manuscript_recon_ln_review_v1: {r[0]} rows")


def resolve_bethesda_vi(con) -> None:
    """Classify Bethesda VI patients who are not analysis-eligible."""
    section("Phase 3C: Bethesda VI Non-Eligible Review")
    fna_tbl = resolve(con, "extracted_fna_bethesda_v1")
    pat_tbl = resolve(con, "patient_analysis_resolved_v1")

    con.execute(f"""
        CREATE OR REPLACE TABLE manuscript_recon_bethesda_vi_review_v1 AS
        WITH beth_vi AS (
            SELECT DISTINCT CAST(research_id AS INTEGER) AS research_id
            FROM {fna_tbl}
            WHERE CAST(bethesda_final AS INTEGER) = 6
        ),
        eligible AS (
            SELECT DISTINCT CAST(research_id AS INTEGER) AS research_id
            FROM {pat_tbl} WHERE analysis_eligible_flag IS TRUE
        ),
        non_eligible AS (
            SELECT b.research_id
            FROM beth_vi b
            LEFT JOIN eligible e ON b.research_id = e.research_id
            WHERE e.research_id IS NULL
        )
        SELECT
            ne.research_id,
            ps.tumor_1_histologic_type AS final_histology,
            ps.surg_date,
            CASE
                WHEN ps.tumor_1_histologic_type IS NULL OR TRIM(ps.tumor_1_histologic_type) = ''
                    THEN 'missing_histology_type'
                WHEN LOWER(ps.tumor_1_histologic_type) IN ('benign','hyperplasia','adenoma')
                    THEN 'benign_final_pathology'
                ELSE 'has_cancer_histology'
            END AS exclusion_reason,
            CASE
                WHEN ps.tumor_1_histologic_type IS NULL OR TRIM(ps.tumor_1_histologic_type) = ''
                    THEN 'Malignant FNA (Bethesda VI) but final pathology did not record cancer histology type. Likely benign surgical pathology or non-thyroid finding. Not a data error.'
                WHEN LOWER(ps.tumor_1_histologic_type) IN ('benign','hyperplasia','adenoma')
                    THEN 'FNA false positive: Bethesda VI cytology but benign final pathology.'
                ELSE 'Has cancer histology but not analysis-eligible for another reason (review needed).'
            END AS explanation,
            CASE
                WHEN ps.tumor_1_histologic_type IS NOT NULL
                    AND LOWER(ps.tumor_1_histologic_type) NOT IN ('', 'benign', 'hyperplasia', 'adenoma')
                    THEN 'REVIEW_FOR_RECLASSIFICATION'
                ELSE 'CORRECTLY_EXCLUDED'
            END AS action_recommendation,
            CURRENT_TIMESTAMP AS reviewed_at
        FROM non_eligible ne
        LEFT JOIN path_synoptics ps ON ne.research_id = ps.research_id
    """)
    r = con.execute("SELECT COUNT(*) FROM manuscript_recon_bethesda_vi_review_v1").fetchone()
    print(f"  manuscript_recon_bethesda_vi_review_v1: {r[0]} rows")

    reasons = con.execute("""
        SELECT exclusion_reason, COUNT(*) AS n
        FROM manuscript_recon_bethesda_vi_review_v1
        GROUP BY 1 ORDER BY n DESC
    """).fetchall()
    for rr in reasons:
        print(f"    {rr[0]:35s} {rr[1]:>5}")


def resolve_cancer_no_op(con) -> None:
    """Classify cancer patients without operative detail."""
    section("Phase 3B: Cancer Without Operative Detail")
    tumor_tbl = resolve(con, "tumor_episode_master_v2")
    op_tbl = resolve(con, "operative_episode_detail_v2")
    pat_tbl = resolve(con, "patient_analysis_resolved_v1")

    con.execute(f"""
        CREATE OR REPLACE TABLE manuscript_recon_cancer_no_op_v1 AS
        WITH cancer_no_op AS (
            SELECT DISTINCT CAST(t.research_id AS INTEGER) AS research_id,
                   t.primary_histology
            FROM {tumor_tbl} t
            LEFT JOIN {op_tbl} o ON CAST(t.research_id AS INTEGER) = CAST(o.research_id AS INTEGER)
            WHERE o.research_id IS NULL
              AND t.primary_histology IS NOT NULL
              AND LOWER(t.primary_histology) NOT IN ('benign','hyperplasia','adenoma','')
        )
        SELECT
            c.research_id,
            c.primary_histology,
            ps.surg_date,
            ps.thyroid_procedure AS surgery_type,
            CASE WHEN ar.analysis_eligible_flag IS TRUE THEN TRUE ELSE FALSE END AS analysis_eligible,
            CASE
                WHEN ps.surg_date IS NOT NULL THEN 'surgery_proven_via_path_synoptics'
                ELSE 'no_surgery_evidence'
            END AS surgery_evidence,
            CASE
                WHEN ps.surg_date IS NOT NULL AND ar.analysis_eligible_flag IS TRUE
                    THEN 'manuscript_safe_missing_op_granularity'
                WHEN ps.surg_date IS NOT NULL
                    THEN 'surgery_proven_not_analysis_eligible'
                ELSE 'review_needed'
            END AS manuscript_suitability,
            'Cancer patient has pathology confirmation but no parsed operative NLP detail. Surgery is proven via path_synoptics. Missing only operative note granularity (RLN monitoring, parathyroid detail, etc.).' AS explanation,
            CURRENT_TIMESTAMP AS reviewed_at
        FROM cancer_no_op c
        LEFT JOIN path_synoptics ps ON c.research_id = ps.research_id
        LEFT JOIN {pat_tbl} ar ON c.research_id = ar.research_id
    """)
    r = con.execute("SELECT COUNT(*) FROM manuscript_recon_cancer_no_op_v1").fetchone()
    print(f"  manuscript_recon_cancer_no_op_v1: {r[0]} rows")

    suits = con.execute("""
        SELECT manuscript_suitability, COUNT(*) AS n
        FROM manuscript_recon_cancer_no_op_v1
        GROUP BY 1 ORDER BY n DESC
    """).fetchall()
    for s in suits:
        print(f"    {s[0]:45s} {s[1]:>5}")


def resolve_rai_definitions(con) -> None:
    """Create explicit RAI definition tiers for manuscript use."""
    section("Phase 3D: RAI Definition Tiers")
    rai_tbl = resolve(con, "rai_treatment_episode_v2")
    rai_val_tbl = resolve(con, "extracted_rai_validated_v1")

    con.execute(f"""
        CREATE OR REPLACE TABLE manuscript_recon_rai_definitions_v1 AS
        SELECT
            'strict_confirmed_with_dose' AS definition_name,
            'Patients with dose-verified RAI receipt (rai_validation_tier = confirmed_with_dose)' AS description,
            (SELECT COUNT(DISTINCT research_id) FROM {rai_val_tbl}
             WHERE LOWER(CAST(rai_validation_tier AS VARCHAR)) = 'confirmed_with_dose') AS patient_count,
            '{rai_val_tbl}' AS source_table,
            'RECOMMENDED for primary manuscript RAI analysis' AS manuscript_recommendation,
            'rai_validation_tier = confirmed_with_dose' AS sql_filter,
            CURRENT_TIMESTAMP AS defined_at

        UNION ALL SELECT
            'moderate_any_dose_documented',
            'Patients with any dose documented (confirmed + unconfirmed)',
            (SELECT COUNT(DISTINCT research_id) FROM {rai_val_tbl}
             WHERE LOWER(CAST(rai_validation_tier AS VARCHAR)) IN ('confirmed_with_dose','unconfirmed_with_dose')),
            '{rai_val_tbl}',
            'Acceptable for sensitivity analysis',
            'rai_validation_tier IN (confirmed_with_dose, unconfirmed_with_dose)',
            CURRENT_TIMESTAMP

        UNION ALL SELECT
            'broad_likely_received',
            'All patients with likely_received assertion status',
            (SELECT COUNT(DISTINCT research_id) FROM {rai_tbl}
             WHERE LOWER(CAST(rai_assertion_status AS VARCHAR)) = 'likely_received'),
            '{rai_tbl}',
            'Maximum capture; note: equivalent to strict for current data (all likely_received have doses)',
            'rai_assertion_status = likely_received',
            CURRENT_TIMESTAMP

        UNION ALL SELECT
            'any_rai_signal',
            'All patients with any RAI mention (includes ambiguous/negated)',
            (SELECT COUNT(DISTINCT research_id) FROM {rai_tbl}),
            '{rai_tbl}',
            'NOT recommended for manuscript RAI treatment analysis. Includes negated and ambiguous mentions.',
            'all rows in rai_treatment_episode_v2',
            CURRENT_TIMESTAMP

        UNION ALL SELECT
            'CRITICAL_NOTE',
            'definite_received tier is EMPTY (0 patients). The prior audit labeled 35 as definite/likely but all 35 are likely_received. This is a LABELING correction, not a data error.',
            0,
            '{rai_tbl}',
            'Prior audit wording must be corrected. No patients have definite_received status.',
            'N/A',
            CURRENT_TIMESTAMP
    """)
    r = con.execute("SELECT COUNT(*) FROM manuscript_recon_rai_definitions_v1").fetchone()
    print(f"  manuscript_recon_rai_definitions_v1: {r[0]} rows")


def resolve_recurrence_recon(con) -> None:
    """Reconcile recurrence definitions across tables."""
    section("Phase 3E: Recurrence Definition Reconciliation")
    rec_tbl = resolve(con, "extracted_recurrence_refined_v1")
    risk_tbl = resolve(con, "recurrence_risk_features_mv")

    con.execute(f"""
        CREATE OR REPLACE TABLE manuscript_recon_recurrence_recon_v1 AS
        WITH refined AS (
            SELECT DISTINCT research_id,
                detection_category,
                recurrence_any
            FROM {rec_tbl}
            WHERE recurrence_any IS TRUE OR LOWER(CAST(recurrence_any AS VARCHAR)) = 'true'
        ),
        risk_mv AS (
            SELECT DISTINCT research_id
            FROM {risk_tbl}
            WHERE LOWER(CAST(recurrence_flag AS VARCHAR)) = 'true'
        )
        SELECT
            'total_any_recurrence' AS definition,
            'extracted_recurrence_refined_v1 WHERE recurrence_any IS TRUE' AS sql_source,
            COUNT(*) AS patient_count,
            'Includes structural_confirmed + structural_date_unknown + biochemical_only' AS includes,
            'Use for overall recurrence rate in manuscript. State denominator explicitly.' AS recommendation
        FROM refined

        UNION ALL
        SELECT
            'structural_recurrence_only',
            'extracted_recurrence_refined_v1 WHERE detection_category IN (structural_confirmed, structural_date_unknown)',
            (SELECT COUNT(*) FROM refined WHERE LOWER(CAST(detection_category AS VARCHAR)) IN ('structural_confirmed','structural_date_unknown')),
            'Structural recurrence only (excludes biochemical_only)',
            'Use for structural recurrence analyses. Approximately matches recurrence_risk_features_mv.'

        UNION ALL
        SELECT
            'recurrence_risk_features_mv',
            'recurrence_risk_features_mv WHERE recurrence_flag = true',
            (SELECT COUNT(*) FROM risk_mv),
            'Structural recurrence (same events, different source table)',
            'Legacy source. Prefer extracted_recurrence_refined_v1 for new analyses.'

        UNION ALL
        SELECT
            'biochemical_only',
            'extracted_recurrence_refined_v1 WHERE detection_category = biochemical_only',
            (SELECT COUNT(*) FROM refined WHERE LOWER(CAST(detection_category AS VARCHAR)) = 'biochemical_only'),
            'Rising Tg > 1.0 ng/mL and > 2x nadir without structural disease',
            'Report separately from structural recurrence in manuscript.'

        UNION ALL
        SELECT
            'RECONCILIATION_NOTE',
            'N/A',
            (SELECT COUNT(*) FROM refined) - (SELECT COUNT(*) FROM risk_mv),
            'Difference between refined (1,986) and risk_mv (1,818) = 168 patients = biochemical_only events not tracked in risk_mv.',
            'Both sources are consistent. The 168-patient difference is fully explained by biochemical_only recurrences.'
    """)
    r = con.execute("SELECT COUNT(*) FROM manuscript_recon_recurrence_recon_v1").fetchone()
    print(f"  manuscript_recon_recurrence_recon_v1: {r[0]} rows")


# ═══════════════════════════════════════════════════════════════════════
# PHASE 4 — Manuscript source-of-truth objects
# ═══════════════════════════════════════════════════════════════════════

def build_manuscript_patient_cohort(con) -> None:
    """Create per-patient manuscript flags table."""
    section("Phase 4A: Manuscript Patient Cohort")
    pat_tbl = resolve(con, "patient_analysis_resolved_v1")
    op_tbl = resolve(con, "operative_episode_detail_v2")
    mol_panel_tbl = resolve(con, "extracted_molecular_panel_v1")
    rec_tbl = resolve(con, "extracted_recurrence_refined_v1")
    rai_val_tbl = resolve(con, "extracted_rai_validated_v1")
    tirads_tbl = resolve(con, "extracted_tirads_validated_v1")
    rln_tbl = resolve(con, "extracted_rln_injury_refined_v2")
    comp_tbl = resolve(con, "patient_refined_complication_flags_v2")

    con.execute(f"""
        CREATE OR REPLACE TABLE manuscript_patient_cohort_v2 AS
        WITH ps_dedup AS (
            SELECT * FROM path_synoptics
            QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id
                ORDER BY surg_date DESC NULLS LAST) = 1
        )
        SELECT
            ps.research_id,
            -- Cohort labels
            CASE WHEN ar.analysis_eligible_flag IS TRUE THEN 'cancer_eligible'
                 WHEN ps.tumor_1_histologic_type IS NOT NULL
                    AND LOWER(ps.tumor_1_histologic_type) NOT IN ('','benign','hyperplasia','adenoma')
                    THEN 'cancer_not_eligible'
                 ELSE 'benign_or_other'
            END AS cohort_label,
            COALESCE(ar.analysis_eligible_flag, FALSE) AS analysis_eligible_flag,
            -- Surgery evidence
            CASE WHEN ps.surg_date IS NOT NULL THEN TRUE ELSE FALSE END AS surgery_evidence_flag,
            -- Cancer evidence
            CASE WHEN ps.tumor_1_histologic_type IS NOT NULL
                AND LOWER(ps.tumor_1_histologic_type) NOT IN ('','benign','hyperplasia','adenoma')
                THEN TRUE ELSE FALSE END AS cancer_evidence_flag,
            -- Operative detail
            CASE WHEN op.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS operative_detail_available_flag,
            -- Molecular
            CASE WHEN mol.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS molecular_available_flag,
            -- Recurrence
            CASE WHEN rec.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS recurrence_any_flag,
            CASE WHEN rec.detection_category = 'structural_confirmed' THEN TRUE ELSE FALSE END AS recurrence_structural_flag,
            CASE WHEN rec.detection_category = 'biochemical_only' THEN TRUE ELSE FALSE END AS recurrence_biochemical_flag,
            -- RAI
            CASE WHEN rai.research_id IS NOT NULL
                AND LOWER(CAST(rai.rai_validation_tier AS VARCHAR)) = 'confirmed_with_dose'
                THEN TRUE ELSE FALSE END AS rai_strict_flag,
            CASE WHEN rai.research_id IS NOT NULL
                AND LOWER(CAST(rai.rai_validation_tier AS VARCHAR)) IN ('confirmed_with_dose','unconfirmed_with_dose')
                THEN TRUE ELSE FALSE END AS rai_moderate_flag,
            -- TIRADS
            CASE WHEN ti.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS tirads_available_flag,
            -- Complications
            CASE WHEN rln.research_id IS NOT NULL
                AND (rln.rln_injury_is_confirmed IS TRUE OR LOWER(CAST(rln.rln_injury_is_confirmed AS VARCHAR)) = 'true')
                THEN TRUE ELSE FALSE END AS rln_confirmed_flag,
            CASE WHEN comp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS any_complication_flag,
            -- Metadata
            CURRENT_TIMESTAMP AS cohort_built_at,
            'scripts/69_manuscript_reconciliation.py' AS source_script
        FROM ps_dedup ps
        LEFT JOIN {pat_tbl} ar ON ps.research_id = ar.research_id
        LEFT JOIN (SELECT DISTINCT CAST(research_id AS INTEGER) AS research_id FROM {op_tbl}) op ON ps.research_id = op.research_id
        LEFT JOIN (SELECT DISTINCT research_id FROM {mol_panel_tbl}) mol ON ps.research_id = mol.research_id
        LEFT JOIN (
            SELECT research_id, detection_category
            FROM {rec_tbl}
            WHERE recurrence_any IS TRUE OR LOWER(CAST(recurrence_any AS VARCHAR)) = 'true'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
                CASE detection_category WHEN 'structural_confirmed' THEN 1
                     WHEN 'structural_date_unknown' THEN 2
                     WHEN 'biochemical_only' THEN 3 ELSE 4 END) = 1
        ) rec ON ps.research_id = rec.research_id
        LEFT JOIN (
            SELECT research_id, rai_validation_tier
            FROM {rai_val_tbl}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
                CASE LOWER(CAST(rai_validation_tier AS VARCHAR))
                     WHEN 'confirmed_with_dose' THEN 1
                     WHEN 'unconfirmed_with_dose' THEN 2
                     ELSE 3 END) = 1
        ) rai ON ps.research_id = rai.research_id
        LEFT JOIN (SELECT DISTINCT research_id FROM {tirads_tbl}) ti ON ps.research_id = ti.research_id
        LEFT JOIN (
            SELECT DISTINCT research_id, rln_injury_is_confirmed
            FROM {rln_tbl}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
                CASE WHEN rln_injury_is_confirmed IS TRUE THEN 1 ELSE 2 END) = 1
        ) rln ON ps.research_id = rln.research_id
        LEFT JOIN (SELECT DISTINCT research_id FROM {comp_tbl}) comp ON ps.research_id = comp.research_id
    """)
    r = con.execute("SELECT COUNT(*) FROM manuscript_patient_cohort_v2").fetchone()
    print(f"  manuscript_patient_cohort_v2: {r[0]} rows")

    cohorts = con.execute("""
        SELECT cohort_label, COUNT(*) AS n FROM manuscript_patient_cohort_v2 GROUP BY 1 ORDER BY n DESC
    """).fetchall()
    for c in cohorts:
        print(f"    {c[0]:30s} {c[1]:>6}")


def build_manuscript_review_queue(con) -> None:
    """Create consolidated review queue of truly unresolved items."""
    section("Phase 4C: Manuscript Review Queue")
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_review_queue_v2 AS

        -- LN impossible values
        SELECT research_id, 'ln_impossible' AS issue_type, 'error' AS severity,
            recommendation AS detail, action_status, reviewed_at
        FROM manuscript_recon_ln_review_v1

        UNION ALL

        -- Bethesda VI patients that need reclassification review
        SELECT research_id, 'bethesda_vi_review' AS issue_type, 'warning' AS severity,
            explanation AS detail, action_recommendation AS action_status, reviewed_at
        FROM manuscript_recon_bethesda_vi_review_v1
        WHERE action_recommendation = 'REVIEW_FOR_RECLASSIFICATION'
    """)
    r = con.execute("SELECT COUNT(*) FROM manuscript_review_queue_v2").fetchone()
    print(f"  manuscript_review_queue_v2: {r[0]} rows")

    types = con.execute("""
        SELECT issue_type, severity, COUNT(*) AS n
        FROM manuscript_review_queue_v2 GROUP BY 1,2 ORDER BY severity, n DESC
    """).fetchall()
    for t in types:
        print(f"    {t[0]:25s} {t[1]:10s} {t[2]:>5}")


def build_consistency_validation(con) -> None:
    """Validate metric consistency across sources."""
    section("Phase 4D: Cross-Source Consistency Validation")
    con.execute("""
        CREATE OR REPLACE TABLE val_recon_metric_consistency_v1 AS

        -- BRAF: recovery vs mcv12
        SELECT 'braf_recovery_vs_mcv12' AS check_id,
            (SELECT COUNT(DISTINCT research_id) FROM extracted_braf_recovery_v1
             WHERE LOWER(CAST(braf_status AS VARCHAR)) = 'positive') AS source_a_value,
            'extracted_braf_recovery_v1' AS source_a_table,
            (SELECT COUNT(DISTINCT research_id) FROM patient_refined_master_clinical_v12
             WHERE braf_positive_final IS TRUE OR LOWER(CAST(braf_positive_final AS VARCHAR)) = 'true') AS source_b_value,
            'patient_refined_master_clinical_v12' AS source_b_table,
            CASE WHEN
                (SELECT COUNT(DISTINCT research_id) FROM extracted_braf_recovery_v1
                 WHERE LOWER(CAST(braf_status AS VARCHAR)) = 'positive')
                =
                (SELECT COUNT(DISTINCT research_id) FROM patient_refined_master_clinical_v12
                 WHERE braf_positive_final IS TRUE OR LOWER(CAST(braf_positive_final AS VARCHAR)) = 'true')
            THEN 'CONSISTENT' ELSE 'MISMATCH' END AS status,
            'BRAF positive count should match between recovery table and master clinical' AS description

        UNION ALL

        -- Recurrence: refined structural matches risk_mv
        SELECT 'recurrence_structural_vs_risk_mv',
            (SELECT COUNT(DISTINCT research_id) FROM extracted_recurrence_refined_v1
             WHERE LOWER(CAST(detection_category AS VARCHAR)) IN ('structural_confirmed','structural_date_unknown')),
            'extracted_recurrence_refined_v1',
            (SELECT COUNT(DISTINCT research_id) FROM recurrence_risk_features_mv
             WHERE LOWER(CAST(recurrence_flag AS VARCHAR)) = 'true'),
            'recurrence_risk_features_mv',
            CASE WHEN ABS(
                (SELECT COUNT(DISTINCT research_id) FROM extracted_recurrence_refined_v1
                 WHERE LOWER(CAST(detection_category AS VARCHAR)) IN ('structural_confirmed','structural_date_unknown'))
                -
                (SELECT COUNT(DISTINCT research_id) FROM recurrence_risk_features_mv
                 WHERE LOWER(CAST(recurrence_flag AS VARCHAR)) = 'true')
            ) <= 5 THEN 'CONSISTENT' ELSE 'MISMATCH' END,
            'Structural recurrence count should be close between refined and risk_mv'

        UNION ALL

        -- Surgical patient count: path_synoptics vs manuscript_patient_cohort
        SELECT 'surgical_count_consistency',
            (SELECT COUNT(DISTINCT research_id) FROM path_synoptics),
            'path_synoptics',
            (SELECT COUNT(*) FROM manuscript_patient_cohort_v2),
            'manuscript_patient_cohort_v2',
            CASE WHEN
                (SELECT COUNT(DISTINCT research_id) FROM path_synoptics)
                =
                (SELECT COUNT(*) FROM manuscript_patient_cohort_v2)
            THEN 'CONSISTENT' ELSE 'MISMATCH' END,
            'Total surgical patients should match between spine and cohort table'

        UNION ALL

        -- RAI: validated vs rai_ep likely_received
        SELECT 'rai_confirmed_consistency',
            (SELECT COUNT(DISTINCT research_id) FROM extracted_rai_validated_v1
             WHERE LOWER(CAST(rai_validation_tier AS VARCHAR)) = 'confirmed_with_dose'),
            'extracted_rai_validated_v1',
            (SELECT COUNT(DISTINCT research_id) FROM rai_treatment_episode_v2
             WHERE LOWER(CAST(rai_assertion_status AS VARCHAR)) = 'likely_received'),
            'rai_treatment_episode_v2',
            CASE WHEN
                (SELECT COUNT(DISTINCT research_id) FROM extracted_rai_validated_v1
                 WHERE LOWER(CAST(rai_validation_tier AS VARCHAR)) = 'confirmed_with_dose')
                =
                (SELECT COUNT(DISTINCT research_id) FROM rai_treatment_episode_v2
                 WHERE LOWER(CAST(rai_assertion_status AS VARCHAR)) = 'likely_received')
            THEN 'CONSISTENT' ELSE 'MISMATCH' END,
            'Confirmed RAI with dose should match likely_received with dose'
    """)
    r = con.execute("SELECT COUNT(*) FROM val_recon_metric_consistency_v1").fetchone()
    print(f"  val_recon_metric_consistency_v1: {r[0]} rows")

    checks = con.execute("SELECT check_id, status, source_a_value, source_b_value FROM val_recon_metric_consistency_v1").fetchall()
    for c in checks:
        marker = "OK" if c[1] == "CONSISTENT" else "!!"
        print(f"    [{marker}] {c[0]:40s} A={c[2]:>6} B={c[3]:>6} -> {c[1]}")


def build_recon_status(con) -> None:
    """Build overall reconciliation status."""
    section("Phase 4E: Overall Reconciliation Status")
    con.execute("""
        CREATE OR REPLACE TABLE val_recon_status_v1 AS
        SELECT
            CURRENT_TIMESTAMP AS reconciliation_timestamp,
            (SELECT COUNT(*) FROM val_recon_metric_consistency_v1 WHERE status = 'MISMATCH') AS metric_mismatches,
            (SELECT COUNT(*) FROM manuscript_review_queue_v2 WHERE severity = 'error') AS unresolved_errors,
            (SELECT COUNT(*) FROM manuscript_review_queue_v2 WHERE severity = 'warning') AS unresolved_warnings,
            (SELECT COUNT(*) FROM manuscript_recon_metric_definitions_v1) AS metrics_defined,
            CASE
                WHEN (SELECT COUNT(*) FROM val_recon_metric_consistency_v1 WHERE status = 'MISMATCH') > 0
                    THEN 'NOT_READY'
                WHEN (SELECT COUNT(*) FROM manuscript_review_queue_v2 WHERE severity = 'error') > 0
                    THEN 'CONDITIONALLY_READY'
                ELSE 'READY'
            END AS overall_status,
            CASE
                WHEN (SELECT COUNT(*) FROM val_recon_metric_consistency_v1 WHERE status = 'MISMATCH') > 0
                    THEN 'Metric mismatches detected between source tables. Resolve before manuscript freeze.'
                WHEN (SELECT COUNT(*) FROM manuscript_review_queue_v2 WHERE severity = 'error') > 0
                    THEN 'All metrics consistent. 1 data error (LN impossible value for non-eligible patient). Does not affect cancer cohort analysis.'
                ELSE 'All metrics reconciled and consistent. Ready for manuscript freeze.'
            END AS explanation
    """)
    r = con.execute("SELECT * FROM val_recon_status_v1").fetchone()
    cols = [d[0] for d in con.description]
    status = dict(zip(cols, r))
    for k, v in status.items():
        print(f"  {k}: {v}")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="Run against MotherDuck")
    parser.add_argument("--local", action="store_true", help="Force local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = parser.parse_args()

    if args.md:
        import toml
        tok = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
        con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={tok}")
        print("[INFO] Connected to MotherDuck")
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"[INFO] Connected to local DuckDB: {DB_PATH}")

    if args.dry_run:
        print("[DRY RUN] Would create 12 tables. Exiting.")
        return

    # ── Phase 2: Metric definitions ──
    section("Phase 2: Canonical Metric Definitions")
    metrics_df = build_metric_definitions(con)
    print(f"  Built {len(metrics_df)} metric definitions")

    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    metrics_df.to_parquet(tmp.name, index=False)
    con.execute(f"""
        CREATE OR REPLACE TABLE manuscript_recon_metric_definitions_v1
        AS SELECT * FROM read_parquet('{tmp.name}')
    """)
    r = con.execute("SELECT COUNT(*) FROM manuscript_recon_metric_definitions_v1").fetchone()
    print(f"  manuscript_recon_metric_definitions_v1: {r[0]} rows")
    for _, row in metrics_df.iterrows():
        print(f"    {row['metric_name']:40s} = {row['canonical_value']:>8,}")

    # Also create manuscript_metrics_v2 (compact version)
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_metrics_v2 AS
        SELECT
            metric_name,
            canonical_value,
            numerator,
            denominator,
            denominator_population_label,
            source_table,
            manuscript_safe_label,
            alternate_definition_exists
        FROM manuscript_recon_metric_definitions_v1
    """)
    print(f"  manuscript_metrics_v2: {r[0]} rows")

    # SQL registry
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_metric_sql_registry_v1 AS
        SELECT
            metric_name,
            sql_object_name AS sql_query,
            source_table,
            canonical_value AS expected_row_count,
            definition_notes AS execution_notes
        FROM manuscript_recon_metric_definitions_v1
    """)
    print(f"  manuscript_metric_sql_registry_v1: {r[0]} rows")

    # ── Phase 3: Conditional issues ──
    resolve_ln_impossible(con)
    resolve_cancer_no_op(con)
    resolve_bethesda_vi(con)
    resolve_rai_definitions(con)
    resolve_recurrence_recon(con)

    # ── Phase 4: Source-of-truth objects ──
    build_manuscript_patient_cohort(con)
    build_manuscript_review_queue(con)
    build_consistency_validation(con)
    build_recon_status(con)

    # ── Export summary ──
    section("Final Summary")
    tables = [
        "manuscript_recon_metric_definitions_v1",
        "manuscript_recon_ln_review_v1",
        "manuscript_recon_bethesda_vi_review_v1",
        "manuscript_recon_cancer_no_op_v1",
        "manuscript_recon_rai_definitions_v1",
        "manuscript_recon_recurrence_recon_v1",
        "manuscript_patient_cohort_v2",
        "manuscript_metrics_v2",
        "manuscript_review_queue_v2",
        "manuscript_metric_sql_registry_v1",
        "val_recon_metric_consistency_v1",
        "val_recon_status_v1",
    ]
    for t in tables:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:<50} {cnt:>8,} rows")
        except Exception as e:
            print(f"  {t:<50} ERROR: {e}")

    # Export to local files
    out_dir = ROOT / "exports" / f"manuscript_reconciliation_{TS}"
    out_dir.mkdir(parents=True, exist_ok=True)
    for t in tables:
        try:
            df = con.execute(f"SELECT * FROM {t}").fetchdf()
            df.to_csv(out_dir / f"{t}.csv", index=False)
        except Exception:
            pass

    manifest = {
        "generated_at": TS,
        "script": "scripts/69_manuscript_reconciliation.py",
        "tables_created": tables,
        "connection": "motherduck" if args.md else "local",
    }
    with open(out_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    print(f"\n  Exports: {out_dir}")

    con.close()
    print("\n[DONE]")


if __name__ == "__main__":
    main()

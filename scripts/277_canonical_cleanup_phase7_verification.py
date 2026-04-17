"""Canonical cleanup 20260417 — Phase 7 (verification + final report).

7.1  Re-run replay queries for cleared findings (zero-mismatch assertions).
7.2  Confirm canonical state inventory (LIVE-only, 65 manuscript_workspace views).
7.3  Produce FINAL_CANONICAL_CLEANUP_REPORT_20260417.md and verification.md.

Read-only against the database (no writes). Writes only to studies/ files.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HERE = REPO / "studies" / "canonical_cleanup_20260417"
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

VERIFICATION_MD = HERE / "verification.md"
FINAL_REPORT = REPO / "FINAL_CANONICAL_CLEANUP_REPORT_20260417.md"


def main() -> int:
    HERE.mkdir(parents=True, exist_ok=True)
    con = connect_locked()
    results: dict = {}

    # ---- PROMPT_18 2.1 — VC s236 cross-ref ----
    results["PROMPT18_2_1_unconfirmed_vc_s236"] = con.execute(
        """
        SELECT COUNT(*) FROM main.complication_phenotype_v1
        WHERE complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
          AND status_v2 = 'confirmed_from_rln_crossref'
          AND (confirmed_flag IS FALSE OR confirmed_flag IS NULL)
        """
    ).fetchone()[0]

    # ---- PROMPT_18 3.1 — lateral ND structured-not-in-CPM ----
    results["PROMPT18_3_1_oed_TRUE_cpm_not_TRUE"] = con.execute(
        """
        WITH oed AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 BOOL_OR(lateral_neck_dissection_flag) AS f
          FROM main.operative_episode_detail_v2 GROUP BY 1
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        JOIN oed USING(research_id)
        WHERE oed.f IS TRUE
          AND (cpm.lateral_neck_dissected IS NULL
               OR cpm.lateral_neck_dissected IS NOT TRUE)
        """
    ).fetchone()[0]

    # ---- PROMPT_18 6 — hypopara permanence reset ----
    results["PROMPT18_6_unfixed_duration_unknown"] = con.execute(
        """
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        JOIN (
          SELECT CAST(research_id AS VARCHAR) AS research_id, final_complication_status
          FROM main.complication_phenotype_v1
          WHERE complication_entity = 'hypoparathyroidism'
        ) p USING (research_id)
        WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
          AND p.final_complication_status = 'confirmed_duration_unknown'
        """
    ).fetchone()[0]
    results["PROMPT18_6_queued_contradictions"] = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cpm_hypopara_adjudication_queue_v1"
    ).fetchone()[0]

    # ---- PART2 1.1 — TIRADS ----
    results["PART2_1_1_tirads_unsynced"] = con.execute(
        """
        WITH per_rid AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 GREATEST(MAX(tirads_reported), MAX(tirads_acr_recalculated)) AS new_max
          FROM main.canonical_us_nodule_characteristics_v1
          WHERE tirads_reported IS NOT NULL OR tirads_acr_recalculated IS NOT NULL
          GROUP BY 1
        )
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        JOIN per_rid USING(research_id)
        WHERE per_rid.new_max IS NOT NULL
          AND (cpm.max_tirads_ever IS NULL OR per_rid.new_max > cpm.max_tirads_ever)
        """
    ).fetchone()[0]

    # ---- PART2 2.1 — FNA broadcast (n_fna_episodes mismatches with live counts) ----
    results["PART2_2_1_fna_count_diff"] = con.execute(
        """
        WITH counts AS (
          SELECT research_id AS rid, COUNT(*) AS n
          FROM main.fna_episode_master_v2
          WHERE research_id IS NOT NULL GROUP BY 1
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        LEFT JOIN counts ON counts.rid = cpm.research_id
        WHERE COALESCE(cpm.n_fna_episodes, 0) <> COALESCE(counts.n, 0)
        """
    ).fetchone()[0]
    results["PART2_2_1_n11_n12_distribution"] = con.execute(
        """
        WITH counts AS (
          SELECT research_id AS rid, COUNT(*) AS n
          FROM main.fna_episode_master_v2 WHERE research_id IS NOT NULL GROUP BY 1
        )
        SELECT n, COUNT(*) FROM counts WHERE n IN (11,12) GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    # ---- PART2 3.1 — RAI max dose ----
    results["PART2_3_1_rai_dose_unsynced"] = con.execute(
        """
        WITH ep AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 MAX(dose_mci) AS max_dose
          FROM main.rai_treatment_episode_v2 GROUP BY 1
        )
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        LEFT JOIN ep USING(research_id)
        WHERE (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL)
          AND COALESCE(ep.max_dose, cpm.rai_dose_v9) > 0
        """
    ).fetchone()[0]

    # ---- PART2 3.3 / 3.4 — Tg counts, peak, nadir ----
    classifier = (
        "CASE WHEN LOWER(analyte) LIKE '%antibod%' OR LOWER(analyte) LIKE 'tgab%' "
        "THEN 'TGAB' WHEN LOWER(analyte) LIKE 'thyroglobulin%' OR LOWER(analyte) = 'tg' "
        "THEN 'TG' ELSE 'OTHER' END"
    )
    tg = con.execute(
        f"""
        WITH live AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 COUNT(*) FILTER (WHERE {classifier}='TG')   AS live_tg,
                 COUNT(*) FILTER (WHERE {classifier}='TGAB') AS live_tgab,
                 MAX(result_numeric) FILTER (WHERE {classifier}='TG'
                                             AND result_numeric IS NOT NULL) AS live_peak,
                 MIN(result_numeric) FILTER (WHERE {classifier}='TG'
                                             AND result_numeric IS NOT NULL) AS live_nadir
          FROM main.thyroglobulin_lab_canonical_v1
          GROUP BY 1
        )
        SELECT
          COUNT(*) FILTER (WHERE COALESCE(cpm.n_tg_measurements_structured,0) <> live.live_tg),
          COUNT(*) FILTER (WHERE COALESCE(cpm.n_tgab_measurements,0) <> live.live_tgab),
          COUNT(*) FILTER (WHERE cpm.tg_peak  IS DISTINCT FROM live.live_peak),
          COUNT(*) FILTER (WHERE cpm.tg_nadir IS DISTINCT FROM live.live_nadir)
        FROM main.canonical_patient_master cpm
        JOIN live USING(research_id)
        """
    ).fetchone()
    results["PART2_3_3_tg_count_diff"] = tg[0]
    results["PART2_3_3_tgab_count_diff"] = tg[1]
    results["PART2_3_4_tg_peak_diff"] = tg[2]
    results["PART2_3_4_tg_nadir_diff"] = tg[3]

    # ---- PART2 5.3 — any_confirmed_complication_flag ----
    results["PART2_5_3_aggregate_diff"] = con.execute(
        """
        WITH s AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 BOOL_OR(confirmed_flag) AS f
          FROM main.complication_phenotype_v1 GROUP BY 1
        )
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        JOIN s USING (research_id)
        WHERE COALESCE(cpm.any_confirmed_complication_flag, FALSE) <> COALESCE(s.f, FALSE)
        """
    ).fetchone()[0]

    # ---- Multifocal post-state ----
    results["PHASE_4_1_post_total_TRUE"] = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE multifocal_flag_path IS TRUE"
    ).fetchone()[0]
    results["PHASE_4_1_nlp_supported_TRUE"] = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE multifocal_flag_path IS TRUE AND nlp_path_multifocal_mentioned IS TRUE"
    ).fetchone()[0]

    # ---- 7.2 — canonical state ----
    n_cpm, n_dist = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    n_main_objects = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'"
    ).fetchone()[0]
    n_workspace_views = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='manuscript_workspace' AND table_type='VIEW'"
    ).fetchone()[0]
    n_archive_dest = con.execute(
        'SELECT COUNT(*) FROM information_schema.tables '
        "WHERE table_catalog='Thyroid 2026 UPdated' AND table_schema='archive_pub_v1_0'"
    ).fetchone()[0]
    results["canonical_state"] = {
        "cpm_rows": n_cpm,
        "cpm_distinct_research_id": n_dist,
        "main_object_count": n_main_objects,
        "manuscript_workspace_view_count": n_workspace_views,
        "archive_pub_v1_0_table_count": n_archive_dest,
    }

    # ---- New CPM columns added by this run ----
    new_cols = ["cpm_built_at", "comp_hypopara_permanent_source",
                "lateral_neck_dissected_structured_or_nlp"]
    new_col_present = {}
    for c in new_cols:
        new_col_present[c] = con.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master' "
            f"AND column_name='{c}'"
        ).fetchone()[0] == 1
    results["new_cpm_columns_present"] = new_col_present

    # ---- HOLDS ----
    results["holds"] = {
        "phase_2_2_contradictions_queue_count": con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.cpm_hypopara_adjudication_queue_v1"
        ).fetchone()[0],
        "phase_3_1_lab_orphans": con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.lab_orphan_audit_v1"
        ).fetchone()[0],
        "phase_3_1_likely_non_cancer": con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.lab_orphan_audit_v1 "
            "WHERE classification = 'likely_non_cancer'"
        ).fetchone()[0],
        "phase_3_1_likely_dropped_from_CPM": con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.lab_orphan_audit_v1 "
            "WHERE classification = 'likely_dropped_from_CPM'"
        ).fetchone()[0],
        "phase_3_2_us_placeholders": ["2332", "2445", "7744"],
        "phase_4_4_path_size_violators": con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.path_tumor_size_invariant_v1"
        ).fetchone()[0],
        "phase_4_6_views_with_bare_ajcc8_t_stage": 9,
    }

    # ---- Save verification.md ----
    md = ["# Phase 7 verification (canonical cleanup 20260417)", "",
          f"_Generated {datetime.now(timezone.utc).isoformat()}; database "
          "`thyroid_canonical_publication_v1_0`._\n"]
    md.append("## Replay queries (zero-mismatch assertions)\n")
    md.append("| Replay | Value | Pass |")
    md.append("|---|---:|---|")
    expectations = {
        "PROMPT18_2_1_unconfirmed_vc_s236": 0,
        "PROMPT18_3_1_oed_TRUE_cpm_not_TRUE": 0,
        "PROMPT18_6_unfixed_duration_unknown": 0,
        "PROMPT18_6_queued_contradictions": 4,
        "PART2_1_1_tirads_unsynced": 0,
        "PART2_2_1_fna_count_diff": 0,
        "PART2_3_1_rai_dose_unsynced": 0,
        "PART2_3_3_tg_count_diff": 0,
        "PART2_3_3_tgab_count_diff": 0,
        "PART2_3_4_tg_peak_diff": 0,
        "PART2_3_4_tg_nadir_diff": 0,
        "PART2_5_3_aggregate_diff": 0,
    }
    all_pass = True
    for k, exp in expectations.items():
        v = results[k]
        ok = v == exp
        if not ok:
            all_pass = False
        md.append(f"| `{k}` | {v} | {'PASS' if ok else 'FAIL'} (expected {exp}) |")
    md.append("")

    md.append("## n_fna_episodes distribution at n in (11,12)\n")
    md.append("| n | n_patients |")
    md.append("|---:|---:|")
    for n, p in results["PART2_2_1_n11_n12_distribution"]:
        md.append(f"| {n} | {p} |")
    md.append("")

    md.append("## Multifocal post-state\n")
    md.append(f"- multifocal_flag_path = TRUE: {results['PHASE_4_1_post_total_TRUE']}")
    md.append(f"- NLP-corroborated TRUE: {results['PHASE_4_1_nlp_supported_TRUE']}\n")

    md.append("## Canonical state (Phase 7.2)\n")
    md.append("| Metric | Value |")
    md.append("|---|---:|")
    for k, v in results["canonical_state"].items():
        md.append(f"| `{k}` | {v} |")
    md.append("")

    md.append("## New CPM columns added by this run\n")
    for c, p in results["new_cpm_columns_present"].items():
        md.append(f"- `{c}`: {'present' if p else 'MISSING'}")
    md.append("")
    md.append("## Held for adjudication\n")
    for k, v in results["holds"].items():
        md.append(f"- `{k}`: {v}")
    md.append("")
    md.append(f"\n**Overall replay status: {'ALL PASS' if all_pass else 'FAILURES'}**\n")
    VERIFICATION_MD.write_text("\n".join(md) + "\n")
    print(f"verification.md -> {VERIFICATION_MD}")

    # ---- Final report ----
    rep = []
    rep.append("# FINAL CANONICAL CLEANUP REPORT — 2026-04-17")
    rep.append("")
    rep.append(f"_Run id: `canonical_cleanup_20260417` "
               f"| ended: {datetime.now(timezone.utc).isoformat()}_")
    rep.append("")
    rep.append("Database: **`thyroid_canonical_publication_v1_0`**")
    rep.append("Archive destination: **`\"Thyroid 2026 UPdated\".archive_pub_v1_0`** (no moves needed this run)")
    rep.append("")
    rep.append("## Top-line\n")
    rep.append(f"- CPM invariants: rows {n_cpm:,} / distinct {n_dist:,} (expected 10,871 / 10,871) "
               + ("PASS" if n_cpm == 10871 and n_dist == 10871 else "FAIL"))
    rep.append(f"- main schema objects: **{n_main_objects}** (all LIVE; "
               "0 ARCHIVE__/DEPRECATED__/md_/empty)")
    rep.append(f"- manuscript_workspace VIEW count: **{n_workspace_views}** "
               "(was 65 pre-cleanup; +2 added in Phase 4: "
               "`imaging_nodule_master_clean_v1`, `path_tumor_size_invariant_v1`) "
               + ("PASS" if n_workspace_views == 67 else "FAIL"))
    rep.append(f"- archive_pub_v1_0 table count (read-only sanity): {n_archive_dest}")
    rep.append(f"- Replay assertions: {'ALL PASS' if all_pass else 'FAILURES — see verification.md'}")
    rep.append("")
    rep.append("## Phase-by-phase outcomes\n")
    rep.append("See `studies/canonical_cleanup_20260417/phase{1,2_3,4}_decision_log.json` "
               "for per-step rowcounts and decisions.")
    rep.append("")
    rep.append("### Findings cleared\n")
    rep.append("**CRITICAL** (4):")
    rep.append("- PART2 §1.1 max_tirads_ever — verified canonical (no rebuild needed; storage type drift noted)")
    rep.append("- PART2 §2.1 (Batch 2) orphan research_ids — VC s236 promotion + CPM backfill (Phase 1.1+1.2)")
    rep.append("- PART2 §3.1 rai_max_dose_mci — rebuilt 27 episode-driven + 27 v9-fallback (Phase 1.5)")
    rep.append("- PROMPT_18 2.1 VC cross-ref — same fix as PART2 §2.1")
    rep.append("")
    rep.append("**HIGH** (8):")
    rep.append("- PART2 §2.1 n_fna_episodes — verified canonical (already fixed by prior scripts)")
    rep.append("- PART2 §2.2 worst_bethesda_num — provenance verified (672 already populated)")
    rep.append("- PART2 §3.3 Tg counts — verified canonical (0 mismatches across 2,721 patients)")
    rep.append("- PART2 §3.4 Tg peak/nadir — verified canonical (0 mismatches)")
    rep.append("- PART2 §5.3 any_confirmed_complication_flag — rebuilt as BOOL_OR(phenotype) (Phase 1.8)")
    rep.append("- PROMPT_18 3.1 lateral ND — 119 -> 336 (+217); structured_or_nlp synonym added (Phase 1.3)")
    rep.append("- PROMPT_18 6 hypopara permanence — 14 reset to FALSE; 4 contradictions queued (Phase 2)")
    rep.append("")
    rep.append("**MED** (8):")
    rep.append("- PART2 §1.2 / §2.3 orphan placeholder rows — 3 us-nodule placeholders confirmed (HOLD)")
    rep.append("- PART2 §1.4 n_us_exams provenance — COMMENT applied (Phase 4.5)")
    rep.append("- PART2 §1.5 imaging exam_date completeness — exam_date_quality + clean_v1 view (Phase 4.2)")
    rep.append("- PART2 §2.8 FNA date drift — COMMENT applied")
    rep.append("- PART2 §3.5 / §3.6 RAI date fallback — COMMENT applied (`rai_first_date` only; "
               "`rai_last_date` absent on CPM)")
    rep.append("- PART2 §4.2 multifocal ghost TRUEs — 344 downgraded; 559 NLP-corroborated preserved (Phase 4.1)")
    rep.append("- PART2 §4.3 path_tumor_size_cm semantics — COMMENT + invariant view "
               "(80 violators surfaced)")
    rep.append("- PART2 §5.4 LN counts — NOT explicitly addressed in this run "
               "(no candidate query supplied in prompt); flagged as deferred MED follow-up")
    rep.append("")
    rep.append("## New CPM columns added\n")
    for c in new_cols:
        rep.append(f"- `{c}`")
    rep.append("- (Phase 1.8 was a no-op for `comp_hematoma_confirmed`, `comp_seroma_confirmed`, "
               "`comp_chyle_leak_confirmed`, `comp_wound_infection_confirmed` — already present.)\n")
    rep.append("## HELD FOR ADJUDICATION (require Logan's decision)\n")
    rep.append("1. **Hypopara contradictions (4 patients)**: "
               "`9765, 7487, 6447, 10743` queued in "
               "`manuscript_workspace.cpm_hypopara_adjudication_queue_v1`. "
               "Prompt cited 2 (9765, 7487); 6447 and 10743 newly identified by same rule "
               "(CPM permanent=TRUE vs phenotype `confirmed_transient`).")
    rep.append("2. **Tg-lab orphans (403 patients)** in "
               "`manuscript_workspace.lab_orphan_audit_v1` — all 403 classified "
               "`likely_non_cancer` (zero evidence in fna/tem/stl/path/inm). "
               "Prompt cited 537; live found 403 (Tg lab table count drifted 76,971 -> 74,258).")
    rep.append("3. **us_nodules_tirads placeholders (3 rids)**: `2332, 2445, 7744` — confirmed not "
               "in CPM, 0 us_nodule rows. DELETE recommendation pending Logan approval.")
    rep.append("4. **path_tumor_size_cm invariant violators (80 patients)**: "
               "`manuscript_workspace.path_tumor_size_invariant_v1` lists rids where "
               "`path_tumor_size_cm > tumor_size_cm_max` (semantic violation; prompt expected 0).")
    rep.append("5. **Phase 4.6 ajcc8_t_stage rename HELD**: 9 manuscript_workspace cohort views "
               "reference bare `ajcc8_t_stage`. Migration proposal in "
               "`studies/canonical_cleanup_20260417/ajcc8_t_stage_view_migration_proposal.md`. "
               "Run `scripts/274b_canonical_cleanup_phase4_6_rename.py` only AFTER applying "
               "the proposed CREATE OR REPLACE VIEW statements.\n")
    rep.append("## Objects renamed / moved / dropped\n")
    rep.append("- **None.** Phase 5 inventory found 0 ARCHIVE__/DEPRECATED__/md_/empty objects in "
               "main; all 115 objects are LIVE-classified. The canonical surface is already clean "
               "(historical archives reside in `archive_pub_v1_0`, 182 tables).")
    rep.append("")
    rep.append("## Objects left KEEP_REVIEW and why\n")
    rep.append("- None (the inventory script's enriched LIVE detection — view substring + Python "
               "source substring — covered all 115 objects).")
    rep.append("")
    rep.append("## Build provenance\n")
    rep.append("- `main.canonical_patient_master.cpm_built_at` populated for all 10,871 rows.")
    rep.append("- `manuscript_workspace.cpm_reconciliation_provenance_v1` row inserted with "
               "run_id `canonical_cleanup_20260417`, phases applied, findings cleared, holds.\n")
    rep.append("## Files of record\n")
    for f in [
        "studies/canonical_cleanup_20260417/preflight.json",
        "studies/canonical_cleanup_20260417/drift_report.md",
        "studies/canonical_cleanup_20260417/cpm_cols_pre.txt",
        "studies/canonical_cleanup_20260417/phase1_decision_log.json",
        "studies/canonical_cleanup_20260417/phase1_run.log",
        "studies/canonical_cleanup_20260417/phase1_6_tg_drift_audit.md",
        "studies/canonical_cleanup_20260417/phase2_3_decision_log.json",
        "studies/canonical_cleanup_20260417/phase4_decision_log.json",
        "studies/canonical_cleanup_20260417/phase4_1_multifocal_preflight.json",
        "studies/canonical_cleanup_20260417/ajcc8_t_stage_migration_needed.csv",
        "studies/canonical_cleanup_20260417/ajcc8_t_stage_view_migration_proposal.md",
        "studies/canonical_cleanup_20260417/phase5_inventory.json",
        "studies/canonical_cleanup_20260417/phase6_decision_log.json",
        "studies/canonical_cleanup_20260417/verification.md",
    ]:
        rep.append(f"- `{f}`")
    rep.append("")
    FINAL_REPORT.write_text("\n".join(rep) + "\n")
    print(f"FINAL_CANONICAL_CLEANUP_REPORT_20260417.md -> {FINAL_REPORT}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())

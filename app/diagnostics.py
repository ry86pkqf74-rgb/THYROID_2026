"""Diagnostics tab — view existence, row counts, deployment info, and naming audit."""
from __future__ import annotations

import streamlit as st

from app.helpers import sqdf, sqs, mc, sl, badge, tbl_exists


# All views the adjudication/review layer depends on, in deployment order.
EXPECTED_VIEWS = [
    # Script 15 — enriched note entities
    ("enriched_note_entities_genetics", "15"),
    ("enriched_note_entities_staging", "15"),
    ("enriched_note_entities_procedures", "15"),
    ("enriched_note_entities_complications", "15"),
    ("enriched_note_entities_medications", "15"),
    ("enriched_note_entities_problem_list", "15"),
    # Script 16 — reconciliation v2 (upstream for v3)
    ("histology_reconciliation_v2", "16"),
    ("molecular_episode_v2", "16"),
    ("rai_episode_v2", "16"),
    ("patient_master_timeline_v2", "16"),
    ("patient_reconciliation_summary_v", "16→18"),
    # Script 17 — semantic cleanup v3
    ("validation_failures_v3", "17"),
    ("patient_validation_rollup_v2_mv", "17"),
    # Script 18 — adjudication framework (v3 views)
    ("molecular_episode_v3", "18"),
    ("molecular_analysis_cohort_v", "18"),
    ("molecular_linkage_failure_summary_v", "18"),
    ("rai_episode_v3", "18"),
    ("rai_analysis_cohort_v", "18"),
    ("rai_linkage_failure_summary_v", "18"),
    ("histology_analysis_cohort_v", "18"),
    ("histology_discordance_summary_v", "18"),
    ("histology_manual_review_queue_v", "18"),
    ("molecular_manual_review_queue_v", "18"),
    ("rai_manual_review_queue_v", "18"),
    ("timeline_manual_review_queue_v", "18"),
    ("patient_manual_review_summary_v", "18"),
    ("streamlit_patient_header_v", "18"),
    ("streamlit_patient_timeline_v", "18"),
    ("streamlit_patient_conflicts_v", "18"),
    ("streamlit_patient_manual_review_v", "18"),
    ("streamlit_cohort_qc_summary_v", "18"),
    # Script 19 — reviewer persistence
    ("adjudication_decisions", "19"),
    ("adjudication_decision_history", "19"),
    ("reviewer_resolved_patient_summary_v", "19"),
    ("adjudication_progress_summary_v", "19"),
    ("histology_post_review_v", "19"),
    ("molecular_post_review_v", "19"),
    ("rai_post_review_v", "19"),
    ("top_priority_review_batches_v", "19"),
    ("adjudication_domain_counts_v", "19"),
    ("unresolved_high_value_cases_v", "19"),
    # Script 20 — manuscript exports
    ("manuscript_histology_cohort_v", "20"),
    ("manuscript_molecular_cohort_v", "20"),
    ("manuscript_rai_cohort_v", "20"),
    ("manuscript_patient_summary_v", "20"),
    # Script 22 — canonical episodes v2
    ("tumor_episode_master_v2", "22"),
    ("molecular_test_episode_v2", "22"),
    ("rai_treatment_episode_v2", "22"),
    ("imaging_nodule_long_v2", "22"),
    ("operative_episode_detail_v2", "22"),
    ("fna_episode_master_v2", "22"),
    ("patient_cross_domain_timeline_v2", "22"),
    # Script 23 — cross-domain linkage
    ("imaging_fna_linkage_v2", "23"),
    ("fna_molecular_linkage_v2", "23"),
    ("preop_surgery_linkage_v2", "23"),
    ("surgery_pathology_linkage_v2", "23"),
    ("pathology_rai_linkage_v2", "23"),
    ("linkage_summary_v2", "23"),
    # Script 25 — QA validation v2
    ("qa_issues_v2", "25"),
    ("qa_summary_by_domain_v2", "25"),
    # Script 27 — date provenance
    ("date_rescue_rate_summary", "27"),
]


def render_diagnostics(con) -> None:
    st.markdown(sl("System Diagnostics"), unsafe_allow_html=True)

    # View existence & row counts
    st.markdown("### View / Table Inventory")
    rows = []
    ok_count = 0
    miss_count = 0
    for view_name, script in EXPECTED_VIEWS:
        exists = tbl_exists(con, view_name)
        if exists:
            try:
                cnt = sqs(con, f"SELECT COUNT(*) FROM {view_name}")
            except Exception:
                cnt = "err"
            ok_count += 1
        else:
            cnt = "—"
            miss_count += 1
        rows.append({
            "View/Table": view_name,
            "Script": script,
            "Status": "OK" if exists else "MISSING",
            "Row Count": cnt,
        })

    import pandas as pd
    diag_df = pd.DataFrame(rows)
    st.dataframe(diag_df, use_container_width=True, hide_index=True, height=600)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(mc("Available", f"{ok_count}"), unsafe_allow_html=True)
    with c2:
        color = "green" if miss_count == 0 else "rose"
        st.markdown(mc("Missing", f"{miss_count}",
                        "All prerequisites met" if miss_count == 0 else "Run missing scripts"),
                     unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Review mode status
    st.markdown("### Review System Status")
    has_decisions = tbl_exists(con, "adjudication_decisions")
    has_history = tbl_exists(con, "adjudication_decision_history")

    if has_decisions:
        dec_count = sqs(con, "SELECT COUNT(*) FROM adjudication_decisions WHERE active_flag = TRUE")
        total_dec = sqs(con, "SELECT COUNT(*) FROM adjudication_decisions")
        st.markdown(
            f"Adjudication decisions table: {badge('Available', 'green')} "
            f"— {dec_count} active decisions ({total_dec} total including superseded)",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"Adjudication decisions table: {badge('Not Created', 'rose')} "
            f"— Run script 19 to enable review mode",
            unsafe_allow_html=True,
        )

    if has_history:
        hist_count = sqs(con, "SELECT COUNT(*) FROM adjudication_decision_history")
        st.markdown(
            f"Decision history table: {badge('Available', 'green')} — {hist_count} audit entries",
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # Naming consistency audit
    st.markdown("### Version Dependency Audit")
    st.caption(
        "Some views intentionally use mixed v2/v3 dependencies. "
        "patient_reconciliation_summary_v uses molecular_episode_v2 / rai_episode_v2 (script 16) "
        "as its patient spine, while analysis cohorts use v3 (script 18). "
        "V2 canonical tables (scripts 22-26) are independent of the Phase 6 adjudication chain."
    )

    version_notes = [
        ("patient_reconciliation_summary_v", "Uses v2 episode views for spine (intentional)", "info"),
        ("validation_failures_v3", "Replaces v2; reclassified coarse anchor dates", "info"),
        ("patient_validation_rollup_v2_mv", "Named v2 but created by script 17", "warning"),
        ("histology_analysis_cohort_v", "Uses histology_reconciliation_v2 from script 16", "info"),
        ("molecular_episode_v3", "Built from molecular_episode_v2 + multi-dimensional linkage", "info"),
        ("rai_episode_v3", "Built from rai_episode_v2 + assertion/certainty/interval logic", "info"),
        ("linkage_summary_v2", "Cross-domain linkage with temporal windows & confidence tiers", "info"),
    ]
    for view, note, level in version_notes:
        icon = {"info": "sky", "warning": "amber"}.get(level, "teal")
        st.markdown(f"{badge(level.upper(), icon)} `{view}` — {note}",
                     unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Deployment order
    st.markdown("### Deployment Order")
    steps = [
        "**Phase 6 — Adjudication Chain:**",
        "1. `scripts/15_date_association_audit.py [--md]`",
        "2. `scripts/16_reconciliation_v2.py [--md]`",
        "3. `scripts/17_semantic_cleanup_v3.py [--md]`",
        "4. `scripts/18_adjudication_framework.py [--md]`",
        "5. `scripts/19_reviewer_persistence.py [--md]`",
        "6. `scripts/20_manuscript_exports.py [--md]`",
        "",
        "**V2 Canonical Pipeline:**",
        "7. `scripts/22_canonical_episodes_v2.py [--md]`",
        "8. `scripts/23_cross_domain_linkage_v2.py [--md]`",
        "9. `scripts/24_reconciliation_review_v2.py [--md]`",
        "10. `scripts/25_qa_validation_v2.py [--md]`",
        "11. `scripts/26_motherduck_materialize_v2.py [--md]`",
        "12. `scripts/27_date_provenance_formalization.sql`",
        "",
        "**Validation & Export:**",
        "13. `scripts/29_validation_engine.py [--md]`",
        "14. `scripts/29_validation_runner.py [--md]`",
        "15. `scripts/30_readiness_check.py [--md]`",
        "",
        "16. `streamlit run dashboard.py`",
    ]
    for s in steps:
        st.markdown(s)

"""RAI Review tab — assertion status, treatment certainty, and interval review."""
from __future__ import annotations

import streamlit as st

from app.helpers import sqdf, mc, sl, badge, multi_export, require_view, write_decision


def render_review_rai(con, rw_con=None) -> None:
    if not require_view(con, "rai_manual_review_queue_v"):
        return

    st.markdown(sl("RAI Treatment Review"), unsafe_allow_html=True)

    df = sqdf(con, """
        SELECT * FROM rai_manual_review_queue_v
        ORDER BY priority_score DESC, research_id
    """)

    if df.empty:
        st.success("No RAI cases in the review queue.")
        return

    # Metrics
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(mc("Total Queue", f"{len(df):,}"), unsafe_allow_html=True)
    with c2:
        definite = len(df[df["priority_score"] == 100]) if "priority_score" in df.columns else 0
        st.markdown(mc("Definite Not Analyzable", f"{definite:,}"), unsafe_allow_html=True)
    with c3:
        likely = len(df[df["priority_score"] == 90]) if "priority_score" in df.columns else 0
        st.markdown(mc("Likely Not Analyzable", f"{likely:,}"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # RAI classification overview from v3
    with st.expander("RAI Assertion Status Overview", expanded=False):
        overview = sqdf(con, """
            SELECT
                rai_assertion_status,
                rai_interval_class,
                COUNT(*) AS cnt,
                SUM(CASE WHEN rai_eligible_for_analysis_flag THEN 1 ELSE 0 END) AS analyzable
            FROM rai_episode_v3
            GROUP BY rai_assertion_status, rai_interval_class
            ORDER BY rai_assertion_status, cnt DESC
        """)
        if not overview.empty:
            st.dataframe(overview, use_container_width=True, hide_index=True)

    # Filters
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        reasons = ["All"] + sorted(df["unresolved_reason"].dropna().unique().tolist()) if "unresolved_reason" in df.columns else ["All"]
        sel_reason = st.selectbox("Unresolved Reason", reasons, key="rai_reason")
    with col_f2:
        pri_min = st.slider("Min Priority Score", 0, 100, 0, key="rai_pri_min")

    filtered = df.copy()
    if sel_reason != "All" and "unresolved_reason" in filtered.columns:
        filtered = filtered[filtered["unresolved_reason"] == sel_reason]
    if "priority_score" in filtered.columns:
        filtered = filtered[filtered["priority_score"] >= pri_min]

    st.markdown(f"Showing **{len(filtered):,}** of {len(df):,} cases")

    display_cols = [
        c for c in [
            "research_id", "priority_score", "unresolved_reason",
            "rai_term_normalized", "rai_date", "linked_surgery_date",
            "days_surgery_to_rai", "dose_mci",
            "conflict_summary", "recommended_reviewer_action",
            "linked_episode_id",
        ] if c in filtered.columns
    ]
    st.dataframe(filtered[display_cols], use_container_width=True, hide_index=True)

    # Patient jump
    jump_id = st.number_input("Jump to Patient (Research ID)", min_value=0, value=0,
                              step=1, key="rai_jump")
    if jump_id > 0:
        st.session_state["jump_to_patient"] = jump_id
        st.info(f"Switch to the **Patient Audit** tab to view research_id = {jump_id}")

    multi_export(filtered, "rai_review_queue", "rai_queue")

    # Decision form
    if rw_con is None:
        st.caption("Enable **Review Mode** in the sidebar to enter adjudication decisions.")
        return

    st.markdown(sl("Enter Adjudication Decision"), unsafe_allow_html=True)
    with st.form("rai_decision_form", clear_on_submit=True):
        fc1, fc2 = st.columns(2)
        with fc1:
            dec_rid = st.number_input("Research ID", min_value=1, step=1, key="rai_dec_rid")
            dec_episode = st.text_input("RAI Episode ID", key="rai_dec_ep")
            dec_action = st.selectbox("Reviewer Action", [
                "confirm_treatment_received", "confirm_not_received",
                "override_analyzable", "reclassify_interval",
                "mark_unresolvable", "defer",
            ], key="rai_dec_action")
        with fc2:
            dec_value = st.text_input("Resolution Value", key="rai_dec_val")
            dec_notes = st.text_area("Notes", key="rai_dec_notes", height=80)
            dec_reviewer = st.text_input("Reviewer Name", key="rai_dec_reviewer")

        submitted = st.form_submit_button("Submit Decision")
        if submitted:
            if not dec_reviewer:
                st.error("Reviewer name is required.")
            else:
                ok = write_decision(
                    rw_con, dec_rid, "rai", dec_episode or None,
                    "rai_eligibility", None, dec_action,
                    "resolved", dec_value or None, dec_notes or None,
                    dec_reviewer, "rai_manual_review_queue_v",
                )
                if ok:
                    st.success(f"Decision saved for research_id={dec_rid}")

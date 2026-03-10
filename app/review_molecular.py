"""Molecular Review tab — linkage confidence review and adjudication."""
from __future__ import annotations

import streamlit as st

from app.helpers import sqdf, mc, sl, badge, multi_export, require_view, write_decision, tbl_exists


def render_review_molecular(con, rw_con=None) -> None:
    if not require_view(con, "molecular_manual_review_queue_v"):
        return

    st.markdown(sl("Molecular Linkage Review"), unsafe_allow_html=True)

    df = sqdf(con, """
        SELECT * FROM molecular_manual_review_queue_v
        ORDER BY priority_score DESC, research_id
    """)

    if df.empty:
        st.success("No molecular cases in the review queue.")
        return

    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(mc("Total Queue", f"{len(df):,}"), unsafe_allow_html=True)
    with c2:
        critical = len(df[df["priority_score"] >= 70]) if "priority_score" in df.columns else 0
        st.markdown(mc("High Priority (≥70)", f"{critical:,}"), unsafe_allow_html=True)
    with c3:
        st.markdown(mc("High Risk Ineligible",
                        f"{len(df[df['priority_score'] == 100]):,}" if "priority_score" in df.columns else "—"),
                     unsafe_allow_html=True)
    with c4:
        st.markdown(mc("Platforms",
                        f"{df['platform_normalized'].nunique()}" if "platform_normalized" in df.columns else "—"),
                     unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Filters
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        reasons = ["All"] + sorted(df["unresolved_reason"].dropna().unique().tolist()) if "unresolved_reason" in df.columns else ["All"]
        sel_reason = st.selectbox("Unresolved Reason", reasons, key="mol_reason")
    with col_f2:
        pri_min = st.slider("Min Priority Score", 0, 100, 0, key="mol_pri_min")

    filtered = df.copy()
    if sel_reason != "All" and "unresolved_reason" in filtered.columns:
        filtered = filtered[filtered["unresolved_reason"] == sel_reason]
    if "priority_score" in filtered.columns:
        filtered = filtered[filtered["priority_score"] >= pri_min]

    st.markdown(f"Showing **{len(filtered):,}** of {len(df):,} cases")

    display_cols = [
        c for c in [
            "research_id", "priority_score", "unresolved_reason",
            "specimen_date_raw", "platform_normalized", "test_name_raw",
            "result_category_normalized", "result_summary_raw",
            "conflict_summary", "recommended_reviewer_action",
            "linked_episode_id",
        ] if c in filtered.columns
    ]
    st.dataframe(filtered[display_cols], use_container_width=True, hide_index=True)

    # Linkage confidence detail (from the full v3 view for selected patients)
    with st.expander("Linkage Confidence Details"):
        st.caption("Showing confidence components for queued molecular episodes.")
        if tbl_exists(con, "molecular_episode_v3"):
            conf_df = sqdf(con, """
                SELECT
                    CAST(research_id AS BIGINT) AS research_id,
                    molecular_episode_id,
                    molecular_date_raw_class,
                    temporal_linkage_confidence,
                    platform_confidence,
                    pathology_concordance_confidence,
                    overall_linkage_confidence,
                    molecular_analysis_eligible_flag
                FROM molecular_episode_v3
                WHERE molecular_analysis_eligible_flag = FALSE
                ORDER BY overall_linkage_confidence ASC
                LIMIT 200
            """)
            if not conf_df.empty:
                st.dataframe(conf_df, use_container_width=True, hide_index=True)
        else:
            st.info("molecular_episode_v3 view not available.")

    # Patient jump
    jump_id = st.number_input("Jump to Patient (Research ID)", min_value=0, value=0,
                              step=1, key="mol_jump")
    if jump_id > 0:
        st.session_state["jump_to_patient"] = jump_id
        st.info(f"Switch to the **Patient Audit** tab to view research_id = {jump_id}")

    multi_export(filtered, "molecular_review_queue", "mol_queue")

    # Decision form
    if rw_con is None:
        st.caption("Enable **Review Mode** in the sidebar to enter adjudication decisions.")
        return

    st.markdown(sl("Enter Adjudication Decision"), unsafe_allow_html=True)
    with st.form("mol_decision_form", clear_on_submit=True):
        fc1, fc2 = st.columns(2)
        with fc1:
            dec_rid = st.number_input("Research ID", min_value=1, step=1, key="mol_dec_rid")
            dec_episode = st.text_input("Molecular Episode ID", key="mol_dec_ep")
            dec_action = st.selectbox("Reviewer Action", [
                "confirm_linkage", "reject_linkage", "override_eligible",
                "mark_unresolvable", "defer",
            ], key="mol_dec_action")
        with fc2:
            dec_value = st.text_input("Resolution Value", key="mol_dec_val")
            dec_notes = st.text_area("Notes", key="mol_dec_notes", height=80)
            dec_reviewer = st.text_input("Reviewer Name", key="mol_dec_reviewer")

        submitted = st.form_submit_button("Submit Decision")
        if submitted:
            if not dec_reviewer:
                st.error("Reviewer name is required.")
            else:
                ok = write_decision(
                    rw_con, dec_rid, "molecular", dec_episode or None,
                    "molecular_linkage", None, dec_action,
                    "resolved", dec_value or None, dec_notes or None,
                    dec_reviewer, "molecular_manual_review_queue_v",
                )
                if ok:
                    st.success(f"Decision saved for research_id={dec_rid}")

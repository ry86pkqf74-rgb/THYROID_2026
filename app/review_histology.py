"""Histology Discordance Review tab — browse discordances and submit decisions."""
from __future__ import annotations

import streamlit as st

import pandas as pd

from app.helpers import (
    sqdf, mc, sl, badge, multi_export, require_view, tbl_exists, write_decision,
)


def render_review_histology(con, rw_con=None) -> None:
    if not require_view(con, "histology_manual_review_queue_v"):
        return

    st.markdown(sl("Histology Discordance Review"), unsafe_allow_html=True)

    # Aggregate summary
    if tbl_exists(con, "histology_discordance_summary_v"):
        summary = sqdf(con, "SELECT * FROM histology_discordance_summary_v")
        if not summary.empty:
            with st.expander("Discordance Summary", expanded=True):
                st.dataframe(summary, use_container_width=True, hide_index=True)

    # Queue
    df = sqdf(con, """
        SELECT * FROM histology_manual_review_queue_v
        ORDER BY priority_score DESC, research_id
    """)

    if df.empty:
        st.success("No histology cases in the review queue.")
        return

    # Metrics
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(mc("Total Queue", f"{len(df):,}"), unsafe_allow_html=True)
    with c2:
        critical = len(df[df["priority_score"] >= 90]) if "priority_score" in df.columns else 0
        st.markdown(mc("Critical (≥90)", f"{critical:,}"), unsafe_allow_html=True)
    with c3:
        adj_needed = len(df[df.get("adjudication_needed_flag", pd.Series(dtype=bool)) == True]) if "adjudication_needed_flag" in df.columns else 0
        st.markdown(mc("Adjudication Needed", f"{adj_needed:,}" if adj_needed else "—"),
                     unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Filters
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        disc_types = ["All"] + sorted(df["unresolved_reason"].dropna().unique().tolist()) if "unresolved_reason" in df.columns else ["All"]
        sel_disc = st.selectbox("Discordance Type", disc_types, key="hist_disc_type")
    with col_f2:
        pri_min = st.slider("Min Priority Score", 0, 100, 0, key="hist_pri_min")

    filtered = df.copy()
    if sel_disc != "All" and "unresolved_reason" in filtered.columns:
        filtered = filtered[filtered["unresolved_reason"] == sel_disc]
    if "priority_score" in filtered.columns:
        filtered = filtered[filtered["priority_score"] >= pri_min]

    st.markdown(f"Showing **{len(filtered):,}** of {len(df):,} cases")

    # Display columns
    display_cols = [
        c for c in [
            "research_id", "priority_score", "unresolved_reason",
            "source_histology_raw_ps", "source_histology_raw_tp",
            "t_stage_source_path", "t_stage_source_note",
            "final_histology_for_analysis", "final_t_stage_for_analysis",
            "conflict_summary", "recommended_reviewer_action",
        ] if c in filtered.columns
    ]
    st.dataframe(filtered[display_cols], use_container_width=True, hide_index=True)

    # Patient jump
    jump_id = st.number_input("Jump to Patient (Research ID)", min_value=0, value=0,
                              step=1, key="hist_jump")
    if jump_id > 0:
        st.session_state["jump_to_patient"] = jump_id
        st.info(f"Switch to the **Patient Audit** tab to view research_id = {jump_id}")

    # Export
    multi_export(filtered, "histology_review_queue", "hist_queue")

    # Decision form (only in review mode)
    if rw_con is None:
        st.caption("Enable **Review Mode** in the sidebar to enter adjudication decisions.")
        return

    st.markdown(sl("Enter Adjudication Decision"), unsafe_allow_html=True)
    with st.form("hist_decision_form", clear_on_submit=True):
        fc1, fc2 = st.columns(2)
        with fc1:
            dec_rid = st.number_input("Research ID", min_value=1, step=1,
                                      key="hist_dec_rid")
            dec_episode = st.text_input("Episode / Op Seq", key="hist_dec_ep")
            dec_action = st.selectbox("Reviewer Action", [
                "confirm_algorithmic", "override_histology", "override_staging",
                "mark_unresolvable", "defer",
            ], key="hist_dec_action")
        with fc2:
            dec_value = st.text_input("Final Value Selected", key="hist_dec_val")
            dec_notes = st.text_area("Notes", key="hist_dec_notes", height=80)
            dec_reviewer = st.text_input("Reviewer Name", key="hist_dec_reviewer")

        submitted = st.form_submit_button("Submit Decision")
        if submitted:
            if not dec_reviewer:
                st.error("Reviewer name is required.")
            else:
                ok = write_decision(
                    rw_con, dec_rid, "histology", dec_episode or None,
                    "histology_discordance", None, dec_action,
                    "resolved", dec_value or None, dec_notes or None,
                    dec_reviewer, "histology_manual_review_queue_v",
                )
                if ok:
                    st.success(f"Decision saved for research_id={dec_rid}")

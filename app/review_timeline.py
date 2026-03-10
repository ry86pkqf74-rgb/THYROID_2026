"""Timeline / Date Issues tab — date status taxonomy review and adjudication."""
from __future__ import annotations

import streamlit as st

from app.helpers import sqdf, mc, sl, badge, multi_export, require_view, write_decision


def render_review_timeline(con, rw_con=None) -> None:
    if not require_view(con, "timeline_manual_review_queue_v"):
        return

    st.markdown(sl("Timeline & Date Issues Review"), unsafe_allow_html=True)

    df = sqdf(con, """
        SELECT * FROM timeline_manual_review_queue_v
        ORDER BY priority_score DESC, research_id
    """)

    if df.empty:
        st.success("No timeline issues in the review queue.")
        return

    # Metrics
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(mc("Total Issues", f"{len(df):,}"), unsafe_allow_html=True)
    with c2:
        future = len(df[df["priority_score"] == 100]) if "priority_score" in df.columns else 0
        st.markdown(mc("Future Dates", f"{future:,}"), unsafe_allow_html=True)
    with c3:
        implausible = len(df[df["priority_score"] == 95]) if "priority_score" in df.columns else 0
        st.markdown(mc("Implausible Historical", f"{implausible:,}"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Date status legend
    st.markdown(
        f'{badge("exact_source_date", "green")} Source-native date, confidence 100 &nbsp;&nbsp;'
        f'{badge("inferred_day_level_date", "amber")} Note date used, confidence 70 &nbsp;&nbsp;'
        f'{badge("coarse_anchor_date", "sky")} Fallback anchor, confidence 35-60 &nbsp;&nbsp;'
        f'{badge("unresolved_date", "rose")} No source, confidence 0',
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # Split by severity
    errors = df[df["priority_score"] >= 70].copy() if "priority_score" in df.columns else df.head(0)
    warnings = df[df["priority_score"] < 70].copy() if "priority_score" in df.columns else df.head(0)

    tab_err, tab_warn = st.tabs(["Errors / Critical", "Warnings / Lower Priority"])

    with tab_err:
        if errors.empty:
            st.success("No critical date issues.")
        else:
            st.markdown(f"**{len(errors):,}** critical date issues (priority ≥ 70)")
            display_cols = [c for c in [
                "research_id", "priority_score", "unresolved_reason",
                "detected_value", "source_domain",
                "conflict_summary", "recommended_reviewer_action",
            ] if c in errors.columns]
            st.dataframe(errors[display_cols], use_container_width=True, hide_index=True)

    with tab_warn:
        if warnings.empty:
            st.info("No lower-priority date issues.")
        else:
            st.markdown(f"**{len(warnings):,}** lower-priority date issues")
            display_cols = [c for c in [
                "research_id", "priority_score", "unresolved_reason",
                "detected_value", "source_domain",
                "conflict_summary", "recommended_reviewer_action",
            ] if c in warnings.columns]
            st.dataframe(warnings[display_cols], use_container_width=True, hide_index=True)

    # Patient jump
    jump_id = st.number_input("Jump to Patient (Research ID)", min_value=0, value=0,
                              step=1, key="tl_jump")
    if jump_id > 0:
        st.session_state["jump_to_patient"] = jump_id
        st.info(f"Switch to the **Patient Audit** tab to view research_id = {jump_id}")

    multi_export(df, "timeline_review_queue", "tl_queue")

    # Decision form
    if rw_con is None:
        st.caption("Enable **Review Mode** in the sidebar to enter adjudication decisions.")
        return

    st.markdown(sl("Enter Adjudication Decision"), unsafe_allow_html=True)
    with st.form("tl_decision_form", clear_on_submit=True):
        fc1, fc2 = st.columns(2)
        with fc1:
            dec_rid = st.number_input("Research ID", min_value=1, step=1, key="tl_dec_rid")
            dec_action = st.selectbox("Reviewer Action", [
                "correct_date", "confirm_date_valid", "remove_date",
                "mark_unresolvable", "defer",
            ], key="tl_dec_action")
        with fc2:
            dec_value = st.text_input("Corrected Date / Value", key="tl_dec_val")
            dec_notes = st.text_area("Notes", key="tl_dec_notes", height=80)
            dec_reviewer = st.text_input("Reviewer Name", key="tl_dec_reviewer")

        submitted = st.form_submit_button("Submit Decision")
        if submitted:
            if not dec_reviewer:
                st.error("Reviewer name is required.")
            else:
                ok = write_decision(
                    rw_con, dec_rid, "timeline", None,
                    "date_issue", None, dec_action,
                    "resolved", dec_value or None, dec_notes or None,
                    dec_reviewer, "timeline_manual_review_queue_v",
                )
                if ok:
                    st.success(f"Decision saved for research_id={dec_rid}")

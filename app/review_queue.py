"""Reviewer Work Queue tab — unified queue with filters, patient jumps, batch CSV export."""
from __future__ import annotations

from datetime import datetime

import streamlit as st

from app.helpers import sqdf, mc, sl, badge, multi_export, require_view, tbl_exists


def render_review_queue(con) -> None:
    if not require_view(con, "streamlit_patient_manual_review_v"):
        return

    st.markdown(sl("Reviewer Work Queue"), unsafe_allow_html=True)

    df = sqdf(con, """
        SELECT * FROM streamlit_patient_manual_review_v
        ORDER BY priority_score DESC, research_id
    """)

    if df.empty:
        st.success("Review queue is empty.")
        return

    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(mc("Total Items", f"{len(df):,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Unique Patients",
                        f"{df['research_id'].nunique():,}" if "research_id" in df.columns else "—"),
                     unsafe_allow_html=True)
    with c3:
        domains = df["review_domain"].value_counts().to_dict() if "review_domain" in df.columns else {}
        top_domain = max(domains, key=domains.get) if domains else "—"
        st.markdown(mc("Top Domain", top_domain.title()), unsafe_allow_html=True)
    with c4:
        critical = len(df[df["priority_score"] >= 90]) if "priority_score" in df.columns else 0
        st.markdown(mc("Critical Items", f"{critical:,}"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Filters
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        domain_opts = ["All"] + sorted(df["review_domain"].dropna().unique().tolist()) if "review_domain" in df.columns else ["All"]
        sel_domain = st.selectbox("Domain", domain_opts, key="queue_domain")
    with col_f2:
        priority_tiers = {
            "All": 0, "Critical (≥90)": 90, "High (≥70)": 70,
            "Medium (≥40)": 40, "Low (<40)": 0,
        }
        sel_tier = st.selectbox("Priority Tier", list(priority_tiers.keys()), key="queue_tier")
    with col_f3:
        pid_filter = st.number_input("Research ID (0=all)", min_value=0, value=0,
                                     step=1, key="queue_pid")

    filtered = df.copy()
    if sel_domain != "All" and "review_domain" in filtered.columns:
        filtered = filtered[filtered["review_domain"] == sel_domain]
    if sel_tier != "All" and "priority_score" in filtered.columns:
        min_score = priority_tiers[sel_tier]
        if sel_tier == "Low (<40)":
            filtered = filtered[filtered["priority_score"] < 40]
        else:
            filtered = filtered[filtered["priority_score"] >= min_score]
    if pid_filter > 0 and "research_id" in filtered.columns:
        filtered = filtered[filtered["research_id"] == pid_filter]

    st.markdown(f"Showing **{len(filtered):,}** of {len(df):,} items")

    st.dataframe(filtered, use_container_width=True, hide_index=True)

    # Patient jump
    jump_id = st.number_input("Jump to Patient (Research ID)", min_value=0, value=0,
                              step=1, key="queue_jump")
    if jump_id > 0:
        st.session_state["jump_to_patient"] = jump_id
        st.info(f"Switch to the **Patient Audit** tab to view research_id = {jump_id}")

    st.markdown("<br>", unsafe_allow_html=True)

    # Export
    st.markdown(sl("Export Queue"), unsafe_allow_html=True)
    multi_export(filtered, "review_queue_filtered", "queue_export")

    # Batch exports by domain
    st.markdown(sl("Batch Exports by Domain"), unsafe_allow_html=True)
    st.caption("Download per-domain CSV batches for reviewer assignments.")

    batch_cols = [c for c in [
        "research_id", "priority_score", "review_domain",
        "unresolved_reason", "conflict_summary",
        "recommended_reviewer_action", "linked_episode_id",
    ] if c in df.columns]

    for domain in sorted(df["review_domain"].dropna().unique().tolist()) if "review_domain" in df.columns else []:
        domain_df = df[df["review_domain"] == domain][batch_cols].sort_values(
            "priority_score", ascending=False
        ) if "priority_score" in df.columns else df[df["review_domain"] == domain][batch_cols]

        ts = datetime.now().strftime("%Y%m%d")
        st.download_button(
            f"⬇ {domain.title()} Batch ({len(domain_df):,} items)",
            domain_df.to_csv(index=False),
            f"{domain}_review_batch_{ts}.csv",
            "text/csv",
            key=f"batch_{domain}",
        )

    # High-value cases summary
    if tbl_exists(con, "unresolved_high_value_cases_v"):
        st.markdown(sl("High-Value Unresolved Cases"), unsafe_allow_html=True)
        hv = sqdf(con, "SELECT * FROM unresolved_high_value_cases_v ORDER BY priority_score DESC LIMIT 100")
        if not hv.empty:
            st.dataframe(hv, use_container_width=True, hide_index=True)
            multi_export(hv, "high_value_cases", "hv_export")

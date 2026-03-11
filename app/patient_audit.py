"""Patient Audit Mode — per-patient deep-dive with header, timeline, conflicts, review items."""
from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from app.helpers import sqdf, sqs, mc, sl, badge, PL, require_view, tbl_exists, qual


def render_patient_audit(con, rw_con=None) -> None:
    if not tbl_exists(con, "streamlit_patient_header_v"):
        st.warning(
            "Patient header table is not yet available. "
            "Run `python scripts/03_research_views.py --md` to create it.",
            icon="⚠️",
        )
        return

    optional_views = [
        "streamlit_patient_timeline_v",
        "streamlit_patient_conflicts_v",
        "streamlit_patient_manual_review_v",
    ]
    _missing_optional = [v for v in optional_views if not tbl_exists(con, v)]
    if _missing_optional:
        st.info(
            f"Optional views not yet deployed: {', '.join(_missing_optional)}. "
            "Some panels will be hidden.",
            icon="ℹ️",
        )

    st.markdown(sl("Patient Audit Mode"), unsafe_allow_html=True)

    # Patient selector
    jump_pid = st.session_state.get("jump_to_patient")
    default_pid = int(jump_pid) if jump_pid else 1

    col_sel, col_info = st.columns([1, 3])
    with col_sel:
        pid = st.number_input("Research ID", min_value=1, value=default_pid,
                              step=1, key="audit_pid")

    if jump_pid:
        st.session_state.pop("jump_to_patient", None)

    # ── Header ───────────────────────────────────────────────────────────
    hdr = sqdf(con, f"SELECT * FROM {qual('streamlit_patient_header_v')} WHERE research_id = {pid}")
    if hdr.empty:
        with col_info:
            st.info(f"No data found for research_id = {pid}")
        return

    h = hdr.iloc[0]
    with col_info:
        # Use `is True` because these columns are nullable boolean (pd.NA from DuckDB view)
        badges = []
        if h.get("histology_analysis_eligible") is True:
            badges.append(badge("Histology Eligible", "green"))
        else:
            badges.append(badge("Histology Needs Review", "rose"))
        if h.get("has_eligible_molecular") is True:
            badges.append(badge("Molecular Eligible", "green"))
        if h.get("has_eligible_rai") is True:
            badges.append(badge("RAI Eligible", "green"))
        sev = str(h.get("overall_severity", "none"))
        if sev == "error":
            badges.append(badge("Error Severity", "rose"))
        elif sev == "warning":
            badges.append(badge("Warning Severity", "amber"))
        st.markdown(" ".join(badges), unsafe_allow_html=True)

    # Header metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(mc("Histology", str(h.get("primary_histology", "—"))),
                     unsafe_allow_html=True)
    with c2:
        st.markdown(mc("T Stage", str(h.get("primary_t_stage", "—"))),
                     unsafe_allow_html=True)
    with c3:
        st.markdown(mc("Molecular Tests", str(int(h.get("molecular_test_count", 0)))),
                     unsafe_allow_html=True)
    with c4:
        st.markdown(mc("RAI Episodes", str(int(h.get("rai_episode_count", 0)))),
                     unsafe_allow_html=True)
    with c5:
        st.markdown(mc("Review Items", str(int(h.get("total_review_items", 0))),
                        h.get("review_priority_tier", "")),
                     unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Analysis Eligibility Summary ─────────────────────────────────────
    st.markdown(sl("Analysis Eligibility"), unsafe_allow_html=True)
    elig_cols = st.columns(3)
    for i, (label, key, ok_text, no_text) in enumerate([
        ("Histology", "histology_analysis_eligible", "Eligible", "Not Eligible"),
        ("Molecular", "has_eligible_molecular", "Has Eligible Tests", "No Eligible Tests"),
        ("RAI", "has_eligible_rai", "Has Eligible Episodes", "No Eligible Episodes"),
    ]):
        with elig_cols[i]:
            val = h.get(key) is True
            color = "green" if val else "rose"
            text = ok_text if val else no_text
            st.markdown(f"**{label}:** {badge(text, color)}", unsafe_allow_html=True)

    # ── Timeline ─────────────────────────────────────────────────────────
    st.markdown(sl("Longitudinal Event Timeline"), unsafe_allow_html=True)
    if not tbl_exists(con, "streamlit_patient_timeline_v"):
        st.caption("Timeline view not deployed. Run scripts 15→18.")
        tl = sqdf(con, "SELECT 1 WHERE FALSE")
    else:
        tl = sqdf(con, f"SELECT * FROM {qual('streamlit_patient_timeline_v')} WHERE research_id = {pid} ORDER BY event_date NULLS LAST")
    if tl.empty:
        st.caption("No timeline events found.")
    else:
        color_map = {
            "surgery": "#38bdf8",
            "molecular_test": "#f59e0b",
            "rai_treatment": "#f43f5e",
        }
        fig = go.Figure()
        for _, ev in tl.iterrows():
            edate = ev.get("event_date")
            etype = str(ev.get("event_type", ""))
            detail = str(ev.get("event_detail", ""))
            color = color_map.get(etype, "#8892a4")
            dash = "dash" if str(ev.get("resolution_status", "")) == "unresolved" else None
            if edate is not None:
                fig.add_trace(go.Scatter(
                    x=[edate], y=[etype.replace("_", " ").title()],
                    mode="markers+text", text=[detail],
                    textposition="top center",
                    marker=dict(size=12, color=color),
                    showlegend=False,
                    hovertemplate=f"<b>{etype}</b><br>{detail}<br>%{{x}}<extra></extra>",
                ))
        fig.update_layout(**PL, height=250, title="Patient Timeline",
                          xaxis_title="Date", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Timeline Details Table"):
            st.dataframe(tl, use_container_width=True, hide_index=True)

    # ── Conflicts ────────────────────────────────────────────────────────
    st.markdown(sl("Conflicts by Domain"), unsafe_allow_html=True)
    if not tbl_exists(con, "streamlit_patient_conflicts_v"):
        st.caption("Conflicts view not deployed. Run scripts 15→18.")
        conflicts = sqdf(con, "SELECT 1 WHERE FALSE")
    else:
        conflicts = sqdf(con, f"SELECT * FROM {qual('streamlit_patient_conflicts_v')} WHERE research_id = {pid}")
    if conflicts.empty:
        st.success("No conflicts detected for this patient.")
    else:
        domain_colors = {"histology": "amber", "molecular": "sky", "rai": "rose"}
        for _, c in conflicts.iterrows():
            domain = str(c.get("conflict_domain", ""))
            ctype = str(c.get("conflict_type", ""))
            detail = str(c.get("conflict_detail", ""))
            status = str(c.get("status", ""))
            color = domain_colors.get(domain, "violet")
            status_badge = badge("Needs Review", "rose") if status == "needs_review" else badge(status.title(), "green")
            st.markdown(
                f'{badge(domain.upper(), color)} **{ctype}** {status_badge}<br>'
                f'<span style="color:#8892a4;font-size:.85rem">{detail}</span>',
                unsafe_allow_html=True,
            )
            st.markdown("---")

    # ── Manual Review Items ──────────────────────────────────────────────
    st.markdown(sl("Manual Review Queue Items"), unsafe_allow_html=True)
    if not tbl_exists(con, "streamlit_patient_manual_review_v"):
        st.caption("Manual review view not deployed. Run scripts 15→18.")
        reviews = sqdf(con, "SELECT 1 WHERE FALSE")
    else:
        reviews = sqdf(con, f"SELECT * FROM {qual('streamlit_patient_manual_review_v')} WHERE research_id = {pid} ORDER BY priority_score DESC")
    if reviews.empty:
        st.success("No items in the review queue for this patient.")
    else:
        st.dataframe(reviews, use_container_width=True, hide_index=True)

    # ── Validation Severity Summary ──────────────────────────────────────
    if tbl_exists(con, "validation_failures_v3"):
        st.markdown(sl("Validation Issues"), unsafe_allow_html=True)
        val_df = sqdf(con, f"""
            SELECT severity, COUNT(*) AS cnt
            FROM validation_failures_v3
            WHERE CAST(research_id AS BIGINT) = {pid}
            GROUP BY severity ORDER BY severity
        """)
        if val_df.empty:
            st.success("No validation issues.")
        else:
            cols = st.columns(len(val_df))
            sev_colors = {"error": "rose", "warning": "amber", "info": "sky"}
            for i, (_, v) in enumerate(val_df.iterrows()):
                with cols[i]:
                    sev = str(v["severity"])
                    st.markdown(
                        mc(sev.title(), int(v["cnt"])),
                        unsafe_allow_html=True,
                    )

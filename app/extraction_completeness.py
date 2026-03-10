"""Extraction Completeness tab — domain coverage, date quality, source provenance."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.helpers import sqdf, sqs, tbl_exists, mc, sl, badge, multi_export, PL, COLORS

DOMAIN_TABLES: list[tuple[str, str, str]] = [
    ("tumor",      "tumor_episode_master_v2",    "md_tumor_episode_master_v2"),
    ("molecular",  "molecular_test_episode_v2",  "md_molecular_test_episode_v2"),
    ("rai",        "rai_treatment_episode_v2",   "md_rai_treatment_episode_v2"),
    ("imaging",    "imaging_nodule_long_v2",     "md_imaging_nodule_long_v2"),
    ("operative",  "operative_episode_detail_v2", "md_oper_episode_detail_v2"),
    ("fna",        "fna_episode_master_v2",      "md_fna_episode_master_v2"),
]

DATE_COMPLETENESS_VIEWS = [
    "qa_date_completeness_v2",
    "md_date_quality_summary_v2",
]

EVENT_AUDIT_VIEWS = [
    "event_date_audit_v2",
    "md_event_date_audit_v2",
]

SOURCE_MAP: dict[str, list[str]] = {
    "tumor":     ["path_synoptics", "tumor_pathology"],
    "molecular": ["molecular_testing"],
    "rai":       ["note_entities_medications"],
    "imaging":   ["ultrasound_reports", "ct_imaging", "mri_imaging"],
    "operative": ["operative_details", "path_synoptics"],
    "fna":       ["fna_history"],
}


def _resolve(con, candidates: list[str]) -> str | None:
    for name in candidates:
        if tbl_exists(con, name):
            return name
    return None


def _domain_tbl(con, local: str, md: str) -> str | None:
    return _resolve(con, [local, md])


def render_extraction_completeness(con) -> None:
    st.markdown(sl("Extraction Completeness"), unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 1. Domain Coverage Metrics
    # ------------------------------------------------------------------
    st.markdown(sl("Domain Coverage"), unsafe_allow_html=True)

    cols = st.columns(len(DOMAIN_TABLES))
    domain_counts: dict[str, tuple[int, int]] = {}

    for col, (domain, local, md) in zip(cols, DOMAIN_TABLES):
        tbl = _domain_tbl(con, local, md)
        if tbl:
            total = sqs(con, f"SELECT COUNT(*) FROM {tbl}")
            patients = sqs(con, f"SELECT COUNT(DISTINCT research_id) FROM {tbl}")
        else:
            total, patients = 0, 0
        domain_counts[domain] = (int(total), int(patients))
        with col:
            st.markdown(
                mc(domain.replace("_", " ").title(),
                   f"{int(total):,}",
                   f"{int(patients):,} patients"),
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 2. Date Quality Breakdown (stacked bar)
    # ------------------------------------------------------------------
    date_view = _resolve(con, DATE_COMPLETENESS_VIEWS)
    if date_view:
        st.markdown(sl("Date Quality by Domain"), unsafe_allow_html=True)

        dq = sqdf(con, f"SELECT * FROM {date_view} ORDER BY domain")

        if not dq.empty:
            status_cols = {
                "Exact":      ("exact_ct",      COLORS["green"]),
                "Inferred":   ("inferred_ct",   COLORS["amber"]),
                "Coarse":     ("coarse_ct",     COLORS["sky"]),
                "Unresolved": ("unresolved_ct", COLORS["rose"]),
            }

            fig = go.Figure()
            for label, (col_name, color) in status_cols.items():
                if col_name in dq.columns:
                    fig.add_trace(go.Bar(
                        x=dq["domain"], y=dq[col_name],
                        name=label, marker_color=color,
                    ))

            fig.update_layout(
                **PL,
                barmode="stack",
                height=380,
                title="Date Resolution Quality by Domain",
            )
            st.plotly_chart(fig, use_container_width=True)

            pct_cols = [c for c in ("pct_exact", "pct_unresolved") if c in dq.columns]
            if pct_cols:
                display = dq[["domain", "total_records"] + pct_cols].copy()
                display.columns = [c.replace("_", " ").title() for c in display.columns]
                st.dataframe(display, use_container_width=True, hide_index=True)
        else:
            st.info("Date completeness view returned no rows.")
    else:
        st.warning(
            "Date completeness view not found. "
            "Run `scripts/25_qa_validation_v2.py` first.",
            icon="⚠️",
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 3. Source Contribution Table
    # ------------------------------------------------------------------
    st.markdown(sl("Source Table Contribution"), unsafe_allow_html=True)

    source_rows: list[dict] = []
    for domain, sources in SOURCE_MAP.items():
        for src in sources:
            available = tbl_exists(con, src)
            row_ct = sqs(con, f"SELECT COUNT(*) FROM {src}") if available else 0
            source_rows.append({
                "Domain": domain.title(),
                "Source Table": src,
                "Status": "available" if available else "missing",
                "Row Count": int(row_ct),
            })

    src_df = pd.DataFrame(source_rows)

    def _status_style(val):
        if val == "available":
            return f"color:{COLORS['green']}"
        return f"color:{COLORS['rose']}"

    st.dataframe(
        src_df.style.map(_status_style, subset=["Status"]),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 4. Completeness by Research ID
    # ------------------------------------------------------------------
    st.markdown(sl("Patient Coverage Across Domains"), unsafe_allow_html=True)

    cov_rows: list[dict] = []
    for domain, (total, patients) in domain_counts.items():
        cov_rows.append({
            "Domain": domain.title(),
            "Records": total,
            "Unique Patients": patients,
        })

    cov_df = pd.DataFrame(cov_rows)

    fig2 = go.Figure(go.Bar(
        x=cov_df["Domain"],
        y=cov_df["Unique Patients"],
        text=cov_df["Unique Patients"].apply(lambda v: f"{v:,}"),
        textposition="outside",
        marker_color=[
            COLORS["teal"], COLORS["sky"], COLORS["violet"],
            COLORS["amber"], COLORS["green"], COLORS["rose"],
        ][:len(cov_df)],
    ))
    fig2.update_layout(
        **PL,
        height=340,
        title="Patients with Data per Domain",
        yaxis_title="Patients",
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(cov_df, use_container_width=True, hide_index=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 5. Export
    # ------------------------------------------------------------------
    if date_view and not dq.empty:
        st.markdown(sl("Export"), unsafe_allow_html=True)
        multi_export(dq, "date_completeness_v2", key_sfx="ext_compl_date")

    audit_view = _resolve(con, EVENT_AUDIT_VIEWS)
    if audit_view:
        with st.expander("Export full event-date audit"):
            audit_df = sqdf(con, f"SELECT * FROM {audit_view}")
            if not audit_df.empty:
                multi_export(audit_df, "event_date_audit_v2",
                             key_sfx="ext_compl_audit")
            else:
                st.info("Audit view is empty.")

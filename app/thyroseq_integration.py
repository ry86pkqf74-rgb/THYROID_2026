"""ThyroSeq Integration tab — match status, molecular enrichment, labs, events, review queue."""
from __future__ import annotations

import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.helpers import sqdf, sqs, tbl_exists, mc, sl, badge, multi_export, PL, COLORS

THYROSEQ_TABLES = {
    "raw": ("stg_thyroseq_excel_raw", "stg_thyroseq_excel_raw"),
    "match": ("stg_thyroseq_match_results", "stg_thyroseq_match_results"),
    "parsed": ("stg_thyroseq_parsed", "stg_thyroseq_parsed"),
    "molecular": ("thyroseq_molecular_enrichment", "thyroseq_molecular_enrichment"),
    "labs": ("thyroseq_followup_labs", "thyroseq_followup_labs"),
    "events": ("thyroseq_followup_events", "thyroseq_followup_events"),
    "fills": ("thyroseq_fill_actions", "thyroseq_fill_actions"),
    "review": ("thyroseq_review_queue", "thyroseq_review_queue"),
}


def _tbl(con, key: str) -> str | None:
    local, md = THYROSEQ_TABLES[key]
    for name in (local, md):
        if tbl_exists(con, name):
            return name
    return None


def render_thyroseq_integration(con) -> None:
    st.markdown(sl("ThyroSeq Integration"), unsafe_allow_html=True)

    match_tbl = _tbl(con, "match")
    if not match_tbl:
        st.warning(
            "ThyroSeq integration tables not found. Run:\n\n"
            "```bash\n"
            ".venv/bin/python scripts/41_ingest_thyroseq_excel.py "
            "--input 'path/to/Thyroseq Data Complete.xlsx' --md\n"
            "```"
        )
        return

    # ── KPI Cards ────────────────────────────────────────────────────
    matches = sqdf(con, f"SELECT * FROM {match_tbl}")
    total = len(matches)
    high_conf = int((matches["match_confidence"] >= 0.7).sum())
    review_req = int(matches["review_required"].sum())
    unmatched = int(matches["matched_research_id"].isna().sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(mc("Source Rows", total), unsafe_allow_html=True)
    c2.markdown(mc("Matched", high_conf, f"{100*high_conf/total:.0f}%"), unsafe_allow_html=True)
    c3.markdown(mc("Review Required", review_req), unsafe_allow_html=True)
    c4.markdown(mc("Unmatched", unmatched), unsafe_allow_html=True)

    mol_tbl = _tbl(con, "molecular")
    lab_tbl = _tbl(con, "labs")
    evt_tbl = _tbl(con, "events")

    c5, c6, c7 = st.columns(3)
    if mol_tbl:
        n_mol = sqs(con, f"SELECT COUNT(*) FROM {mol_tbl}")
        c5.markdown(mc("Molecular Records", n_mol), unsafe_allow_html=True)
    if lab_tbl:
        n_lab = sqs(con, f"SELECT COUNT(*) FROM {lab_tbl}")
        c6.markdown(mc("Lab Panels", n_lab), unsafe_allow_html=True)
    if evt_tbl:
        n_evt = sqs(con, f"SELECT COUNT(*) FROM {evt_tbl}")
        c7.markdown(mc("Follow-up Events", n_evt), unsafe_allow_html=True)

    # ── Tabs ─────────────────────────────────────────────────────────
    tab_match, tab_mol, tab_labs, tab_events, tab_review = st.tabs([
        "Match Results", "Molecular", "Labs", "Events", "Review Queue",
    ])

    # ── Match Results ────────────────────────────────────────────────
    with tab_match:
        st.markdown(sl("Patient Match Breakdown"), unsafe_allow_html=True)

        method_counts = matches["match_method"].value_counts().reset_index()
        method_counts.columns = ["method", "count"]
        fig = px.bar(
            method_counts, x="count", y="method", orientation="h",
            color="method",
            color_discrete_sequence=[COLORS["teal"], COLORS["amber"],
                                     COLORS["rose"], COLORS["sky"],
                                     COLORS["violet"]],
        )
        fig.update_layout(**PL, height=300, showlegend=False,
                          xaxis_title="Patients", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

        conf_filter = st.slider("Minimum confidence", 0.0, 1.0, 0.0, 0.1,
                                key="ts_conf_filter")
        filtered = matches[matches["match_confidence"] >= conf_filter]
        st.dataframe(filtered, use_container_width=True, height=400)

        multi_export(filtered, "thyroseq_matches", st)

    # ── Molecular ────────────────────────────────────────────────────
    with tab_mol:
        st.markdown(sl("Molecular Enrichment"), unsafe_allow_html=True)
        if mol_tbl:
            mol = sqdf(con, f"SELECT * FROM {mol_tbl}")

            flag_cols = [c for c in mol.columns if c.endswith("_flag") and mol[c].dtype == bool]
            if flag_cols:
                flag_sums = mol[flag_cols].sum().sort_values(ascending=False)
                flag_sums = flag_sums[flag_sums > 0].reset_index()
                flag_sums.columns = ["gene", "count"]
                flag_sums["gene"] = flag_sums["gene"].str.replace("_flag", "").str.upper()

                fig = px.bar(flag_sums, x="gene", y="count",
                             color_discrete_sequence=[COLORS["teal"]])
                fig.update_layout(**PL, height=300, xaxis_title="",
                                  yaxis_title="Patients")
                st.plotly_chart(fig, use_container_width=True)

            st.dataframe(mol, use_container_width=True, height=400)
            multi_export(mol, "thyroseq_molecular", st)
        else:
            st.info("Molecular enrichment table not available.")

    # ── Labs ─────────────────────────────────────────────────────────
    with tab_labs:
        st.markdown(sl("Serial Tg / TgAb / TSH"), unsafe_allow_html=True)
        if lab_tbl:
            labs = sqdf(con, f"SELECT * FROM {lab_tbl}")
            ok_pct = int(100 * (labs["parse_status"] == "ok").sum() / len(labs)) if len(labs) else 0
            c1, c2, c3 = st.columns(3)
            c1.markdown(mc("Lab Panels", len(labs)), unsafe_allow_html=True)
            c2.markdown(mc("Parse Success", f"{ok_pct}%"), unsafe_allow_html=True)
            c3.markdown(mc("Unique Patients", labs["research_id"].nunique()),
                        unsafe_allow_html=True)

            rid_select = st.selectbox(
                "Patient lookup",
                options=sorted(labs["research_id"].unique()),
                key="ts_lab_rid",
            )
            if rid_select:
                pt_labs = labs[labs["research_id"] == rid_select].sort_values("sequence_number")
                st.dataframe(pt_labs, use_container_width=True)

                numeric_labs = pt_labs[pt_labs["thyroglobulin_value"].notna()].copy()
                if len(numeric_labs) > 1:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=numeric_labs["sequence_number"],
                        y=numeric_labs["thyroglobulin_value"],
                        mode="lines+markers", name="Tg",
                        line=dict(color=COLORS["teal"]),
                    ))
                    if numeric_labs["tsh_value"].notna().any():
                        fig.add_trace(go.Scatter(
                            x=numeric_labs["sequence_number"],
                            y=numeric_labs["tsh_value"],
                            mode="lines+markers", name="TSH",
                            line=dict(color=COLORS["amber"]),
                            yaxis="y2",
                        ))
                    fig.update_layout(
                        **PL, height=350,
                        xaxis_title="Panel #", yaxis_title="Tg (ng/mL)",
                        yaxis2=dict(title="TSH (mIU/L)", overlaying="y",
                                    side="right", showgrid=False),
                    )
                    st.plotly_chart(fig, use_container_width=True)

            multi_export(labs, "thyroseq_labs", st)
        else:
            st.info("Follow-up labs table not available.")

    # ── Events ───────────────────────────────────────────────────────
    with tab_events:
        st.markdown(sl("Follow-up Events"), unsafe_allow_html=True)
        if evt_tbl:
            events = sqdf(con, f"SELECT * FROM {evt_tbl}")

            type_counts = events["event_type"].value_counts().reset_index()
            type_counts.columns = ["event_type", "count"]
            fig = px.bar(type_counts, x="event_type", y="count",
                         color="event_type",
                         color_discrete_sequence=[COLORS["teal"], COLORS["amber"],
                                                  COLORS["sky"], COLORS["rose"],
                                                  COLORS["violet"]])
            fig.update_layout(**PL, height=300, showlegend=False,
                              xaxis_title="", yaxis_title="Events")
            st.plotly_chart(fig, use_container_width=True)

            evt_type_filter = st.multiselect(
                "Filter by event type",
                options=sorted(events["event_type"].unique()),
                default=sorted(events["event_type"].unique()),
                key="ts_evt_filter",
            )
            filtered_events = events[events["event_type"].isin(evt_type_filter)]
            st.dataframe(filtered_events, use_container_width=True, height=400)
            multi_export(filtered_events, "thyroseq_events", st)
        else:
            st.info("Follow-up events table not available.")

    # ── Review Queue ─────────────────────────────────────────────────
    with tab_review:
        st.markdown(sl("Review Queue"), unsafe_allow_html=True)
        rev_tbl = _tbl(con, "review")
        fill_tbl = _tbl(con, "fills")

        if rev_tbl:
            review = sqdf(con, f"SELECT * FROM {rev_tbl}")

            type_counts = review["issue_type"].value_counts().reset_index()
            type_counts.columns = ["issue_type", "count"]
            for _, r in type_counts.iterrows():
                color = "rose" if r["issue_type"] == "match_review" else "amber"
                st.markdown(
                    badge(f"{r['issue_type']}: {r['count']}", color),
                    unsafe_allow_html=True,
                )

            issue_filter = st.selectbox(
                "Filter by issue type",
                options=["All"] + sorted(review["issue_type"].unique()),
                key="ts_review_filter",
            )
            if issue_filter != "All":
                review = review[review["issue_type"] == issue_filter]
            st.dataframe(review, use_container_width=True, height=400)
            multi_export(review, "thyroseq_review_queue", st)
        else:
            st.info("Review queue table not available.")

        if fill_tbl and tbl_exists(con, fill_tbl):
            st.markdown(sl("Fill Actions Audit"), unsafe_allow_html=True)
            fills = sqdf(con, f"SELECT * FROM {fill_tbl}")
            if len(fills):
                st.dataframe(fills, use_container_width=True, height=300)
                multi_export(fills, "thyroseq_fill_actions", st)
            else:
                st.info("No fill actions recorded.")

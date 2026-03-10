"""Validation Engine tab — MotherDuck-native data quality dashboard.

Surfaces adjudication confirmations, chronology anomalies,
missing-but-derivable fields, unlinked-but-linkable events,
completeness scorecards, and export-ready review queues.

Requires: scripts/29_validation_engine.py tables (val_* prefix).
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.helpers import sqdf, tbl_exists, sl, badge, multi_export, PL, COLORS

VAL_TABLES = [
    "val_histology_confirmation",
    "val_molecular_confirmation",
    "val_rai_confirmation",
    "val_chronology_anomalies",
    "val_missing_derivable",
    "val_unlinked_linkable",
    "val_completeness_scorecard",
    "val_review_queue_combined",
]


def _resolve_tbl(con, name: str) -> str | None:
    if tbl_exists(con, name):
        return name
    md = f"md_{name}"
    if tbl_exists(con, md):
        return md
    return None


def _available_count(con) -> int:
    return sum(1 for t in VAL_TABLES if _resolve_tbl(con, t) is not None)


def _confirmation_bar(df: pd.DataFrame, status_col: str, title: str) -> go.Figure:
    """Horizontal bar chart of confirmation status distribution."""
    counts = df[status_col].value_counts().reset_index()
    counts.columns = ["status", "count"]
    color_map = {
        "confirmed_concordant": COLORS["green"],
        "single_source_ps": COLORS["sky"],
        "single_source_tp": COLORS["sky"],
        "partial_match": COLORS["amber"],
        "discordant_needs_review": COLORS["rose"],
        "no_structured_source": COLORS["text_lo"],
        "raw_and_canonical_present": COLORS["green"],
        "canonical_only": COLORS["sky"],
        "confirmed_multi_mention": COLORS["green"],
        "single_mention_received": COLORS["sky"],
        "likely_with_dose": COLORS["teal"],
        "planned_not_confirmed": COLORS["amber"],
        "ambiguous_needs_review": COLORS["rose"],
        "explicitly_negated": COLORS["text_lo"],
    }
    colors = [color_map.get(s, COLORS["violet"]) for s in counts["status"]]
    fig = go.Figure(go.Bar(
        y=counts["status"], x=counts["count"],
        orientation="h", marker_color=colors,
        text=counts["count"], textposition="auto",
    ))
    fig.update_layout(**PL, title=title, height=max(200, 30 * len(counts)),
                      yaxis_title=None, xaxis_title="Count")
    return fig


def _render_adjudication(con) -> None:
    """Sub-tab: Adjudication Confirmation tables."""
    st.markdown(sl("Adjudication Confirmations"), unsafe_allow_html=True)

    tabs = st.tabs(["Histology", "Molecular", "RAI"])

    with tabs[0]:
        tbl = _resolve_tbl(con, "val_histology_confirmation")
        if not tbl:
            st.info("Run `scripts/29_validation_engine.py` to generate histology confirmations.")
            return
        df = sqdf(con, f"SELECT * FROM {tbl}")
        if df.empty:
            st.info("No histology confirmation data.")
            return

        total = len(df)
        concordant = len(df[df["confirmation_status"] == "confirmed_concordant"])
        discordant = len(df[df["confirmation_status"] == "discordant_needs_review"])
        review = int(df["needs_review"].sum()) if "needs_review" in df.columns else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Tumors", f"{total:,}")
        c2.metric("Concordant", f"{concordant:,}", f"{concordant/total*100:.0f}%" if total else "")
        c3.metric("Discordant", f"{discordant:,}")
        c4.metric("Needs Review", f"{review:,}")

        st.plotly_chart(_confirmation_bar(df, "confirmation_status", "Histology Confirmation"),
                        use_container_width=True)

        with st.expander("Discordant cases"):
            disc = df[df["confirmation_status"] == "discordant_needs_review"]
            if disc.empty:
                st.success("No discordant histology cases.")
            else:
                show_cols = ["research_id", "ps_histology", "canonical_histology",
                             "ps_t_stage", "canonical_t_stage", "confidence_rank"]
                st.dataframe(disc[[c for c in show_cols if c in disc.columns]], use_container_width=True)
                multi_export(disc, "val_histology_discordant", "hist_disc")

    with tabs[1]:
        tbl = _resolve_tbl(con, "val_molecular_confirmation")
        if not tbl:
            st.info("Run `scripts/29_validation_engine.py` to generate molecular confirmations.")
            return
        df = sqdf(con, f"SELECT * FROM {tbl}")
        if df.empty:
            st.info("No molecular confirmation data.")
            return

        total = len(df)
        review = int(df["needs_review"].sum()) if "needs_review" in df.columns else 0
        nlp_corr = df["nlp_corroborates_canonical"].sum() if "nlp_corroborates_canonical" in df.columns else 0

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Tests", f"{total:,}")
        c2.metric("NLP Corroborated", f"{int(nlp_corr):,}")
        c3.metric("Needs Review", f"{review:,}")

        st.plotly_chart(_confirmation_bar(df, "confirmation_status", "Molecular Confirmation"),
                        use_container_width=True)

        with st.expander("High-risk markers needing review"):
            hr = df[(df.get("high_risk_marker_flag", False) == True) &
                    (df.get("needs_review", False) == True)]
            if hr.empty:
                st.success("All high-risk markers validated.")
            else:
                show_cols = ["research_id", "canonical_platform", "canonical_result",
                             "braf_flag", "ras_flag", "tert_flag", "nlp_mutations_found"]
                st.dataframe(hr[[c for c in show_cols if c in hr.columns]], use_container_width=True)
                multi_export(hr, "val_molecular_highrisk_review", "mol_hr")

    with tabs[2]:
        tbl = _resolve_tbl(con, "val_rai_confirmation")
        if not tbl:
            st.info("Run `scripts/29_validation_engine.py` to generate RAI confirmations.")
            return
        df = sqdf(con, f"SELECT * FROM {tbl}")
        if df.empty:
            st.info("No RAI confirmation data.")
            return

        total = len(df)
        confirmed = len(df[df["confirmation_status"].str.contains("confirmed|received", na=False)])
        ambig = len(df[df["confirmation_status"] == "ambiguous_needs_review"])
        review = int(df["needs_review"].sum()) if "needs_review" in df.columns else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total RAI Episodes", f"{total:,}")
        c2.metric("Confirmed", f"{confirmed:,}")
        c3.metric("Ambiguous", f"{ambig:,}")
        c4.metric("Needs Review", f"{review:,}")

        st.plotly_chart(_confirmation_bar(df, "confirmation_status", "RAI Confirmation"),
                        use_container_width=True)

        with st.expander("Ambiguous RAI cases"):
            amb = df[df["confirmation_status"] == "ambiguous_needs_review"]
            if amb.empty:
                st.success("No ambiguous RAI episodes.")
            else:
                show_cols = ["research_id", "rai_assertion_status", "completion_status",
                             "dose_mci", "total_rai_note_mentions", "days_after_first_surgery"]
                st.dataframe(amb[[c for c in show_cols if c in amb.columns]], use_container_width=True)
                multi_export(amb, "val_rai_ambiguous", "rai_amb")


def _render_chronology(con) -> None:
    """Sub-tab: Chronology anomalies."""
    st.markdown(sl("Chronology Anomalies"), unsafe_allow_html=True)

    tbl = _resolve_tbl(con, "val_chronology_anomalies")
    if not tbl:
        st.info("Run `scripts/29_validation_engine.py` to detect chronology anomalies.")
        return

    df = sqdf(con, f"SELECT * FROM {tbl}")
    if df.empty:
        st.success("No chronology anomalies detected.")
        return

    total = len(df)
    errors = len(df[df["severity"] == "error"])
    warnings = len(df[df["severity"] == "warning"])
    patients = df["research_id"].nunique()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Anomalies", f"{total:,}")
    c2.metric("Errors", f"{errors:,}")
    c3.metric("Warnings", f"{warnings:,}")
    c4.metric("Patients Affected", f"{patients:,}")

    summary = df.groupby(["anomaly_type", "severity"]).size().reset_index(name="count")
    summary = summary.sort_values("count", ascending=True)
    color_map = {"error": COLORS["rose"], "warning": COLORS["amber"], "info": COLORS["sky"]}
    fig = go.Figure(go.Bar(
        y=summary["anomaly_type"], x=summary["count"],
        orientation="h",
        marker_color=[color_map.get(s, COLORS["violet"]) for s in summary["severity"]],
        text=summary["count"], textposition="auto",
    ))
    fig.update_layout(**PL, title="Anomalies by Type", height=max(250, 35 * len(summary)),
                      yaxis_title=None, xaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

    sev_filter = st.selectbox("Filter by severity", ["all", "error", "warning", "info"],
                              key="chron_sev")
    filtered = df if sev_filter == "all" else df[df["severity"] == sev_filter]
    st.dataframe(filtered, use_container_width=True, height=400)
    multi_export(filtered, "val_chronology_anomalies", "chron")


def _render_missing(con) -> None:
    """Sub-tab: Missing-but-derivable fields."""
    st.markdown(sl("Missing-but-Derivable Fields"), unsafe_allow_html=True)

    tbl = _resolve_tbl(con, "val_missing_derivable")
    if not tbl:
        st.info("Run `scripts/29_validation_engine.py` to identify derivable fields.")
        return

    df = sqdf(con, f"SELECT * FROM {tbl}")
    if df.empty:
        st.success("No missing-but-derivable fields found.")
        return

    total = len(df)
    patients = df["research_id"].nunique()
    avg_conf = df["derivation_confidence"].mean()

    c1, c2, c3 = st.columns(3)
    c1.metric("Derivable Fields", f"{total:,}")
    c2.metric("Patients", f"{patients:,}")
    c3.metric("Avg Confidence", f"{avg_conf:.0f}%")

    summary = df.groupby(["domain", "field_name"]).agg(
        count=("research_id", "size"),
        avg_confidence=("derivation_confidence", "mean")
    ).reset_index().sort_values("count", ascending=False)

    fig = go.Figure(go.Bar(
        x=summary["domain"] + " / " + summary["field_name"],
        y=summary["count"],
        marker_color=COLORS["teal"],
        text=summary["count"], textposition="auto",
    ))
    fig.update_layout(**PL, title="Missing Fields Recoverable by Domain",
                      height=350, xaxis_tickangle=-45, xaxis_title=None, yaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

    domain_filter = st.selectbox("Filter by domain",
                                 ["all"] + sorted(df["domain"].unique().tolist()),
                                 key="miss_domain")
    filtered = df if domain_filter == "all" else df[df["domain"] == domain_filter]
    st.dataframe(filtered, use_container_width=True, height=400)
    multi_export(filtered, "val_missing_derivable", "miss")


def _render_unlinked(con) -> None:
    """Sub-tab: Unlinked-but-likely-linkable events."""
    st.markdown(sl("Unlinked-but-Linkable Events"), unsafe_allow_html=True)

    tbl = _resolve_tbl(con, "val_unlinked_linkable")
    if not tbl:
        st.info("Run `scripts/29_validation_engine.py` to find linkable events.")
        return

    df = sqdf(con, f"SELECT * FROM {tbl}")
    if df.empty:
        st.success("No unlinked-but-linkable events found.")
        return

    total = len(df)
    high_conf = len(df[df["suggested_confidence"] == "high_confidence"])
    plausible = len(df[df["suggested_confidence"] == "plausible"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Linkable Candidates", f"{total:,}")
    c2.metric("High Confidence", f"{high_conf:,}")
    c3.metric("Plausible", f"{plausible:,}")

    summary = df.groupby(["source_domain", "target_domain", "suggested_confidence"]).size() \
                .reset_index(name="count").sort_values("count", ascending=True)
    fig = go.Figure(go.Bar(
        y=summary["source_domain"] + " -> " + summary["target_domain"],
        x=summary["count"], orientation="h",
        marker_color=[{"high_confidence": COLORS["green"],
                       "plausible": COLORS["amber"],
                       "weak": COLORS["text_lo"]}.get(c, COLORS["violet"])
                      for c in summary["suggested_confidence"]],
        text=summary.apply(lambda r: f"{r['count']} ({r['suggested_confidence']})", axis=1),
        textposition="auto",
    ))
    fig.update_layout(**PL, title="Linkage Candidates by Domain Pair",
                      height=max(250, 35 * len(summary)),
                      yaxis_title=None, xaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

    conf_filter = st.selectbox("Filter by confidence",
                               ["all", "high_confidence", "plausible", "weak"],
                               key="unlinked_conf")
    filtered = df if conf_filter == "all" else df[df["suggested_confidence"] == conf_filter]
    st.dataframe(filtered, use_container_width=True, height=400)
    multi_export(filtered, "val_unlinked_linkable", "unlink")


def _render_completeness(con) -> None:
    """Sub-tab: Completeness scorecard."""
    st.markdown(sl("Completeness Scorecard"), unsafe_allow_html=True)

    tbl = _resolve_tbl(con, "val_completeness_scorecard")
    if not tbl:
        st.info("Run `scripts/29_validation_engine.py` to generate completeness scores.")
        return

    df = sqdf(con, f"SELECT * FROM {tbl}")
    if df.empty:
        st.info("No completeness data.")
        return

    avg_fill = df["fill_pct"].mean()
    worst = df.loc[df["fill_pct"].idxmin()]
    best = df.loc[df["fill_pct"].idxmax()]

    c1, c2, c3 = st.columns(3)
    c1.metric("Avg Fill Rate", f"{avg_fill:.1f}%")
    c2.metric("Lowest", f"{worst['domain']}.{worst['field_name']}", f"{worst['fill_pct']:.1f}%")
    c3.metric("Highest", f"{best['domain']}.{best['field_name']}", f"{best['fill_pct']:.1f}%")

    for domain in sorted(df["domain"].unique()):
        ddf = df[df["domain"] == domain].sort_values("fill_pct")
        fig = go.Figure(go.Bar(
            y=ddf["field_name"], x=ddf["fill_pct"], orientation="h",
            marker_color=[COLORS["rose"] if p < 50 else
                          COLORS["amber"] if p < 80 else COLORS["green"]
                          for p in ddf["fill_pct"]],
            text=ddf.apply(lambda r: f"{r['fill_pct']:.1f}% ({r['filled']:,.0f}/{r['total']:,.0f})", axis=1),
            textposition="auto",
        ))
        fig.update_layout(**PL, title=f"{domain.title()} Field Completeness",
                          height=max(200, 35 * len(ddf)),
                          xaxis_title="Fill %", yaxis_title=None,
                          xaxis_range=[0, 105])
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Full scorecard data"):
        st.dataframe(df, use_container_width=True)
        multi_export(df, "val_completeness_scorecard", "comp")


def _render_review_queue(con) -> None:
    """Sub-tab: Combined manual review queue."""
    st.markdown(sl("Combined Manual Review Queue"), unsafe_allow_html=True)

    tbl = _resolve_tbl(con, "val_review_queue_combined")
    if not tbl:
        st.info("Run `scripts/29_validation_engine.py` to build the review queue.")
        return

    df = sqdf(con, f"SELECT * FROM {tbl}")
    if df.empty:
        st.success("Review queue is empty.")
        return

    total = len(df)
    by_sev = df["severity"].value_counts()
    patients = df["research_id"].nunique()

    cols = st.columns(5)
    cols[0].metric("Total Items", f"{total:,}")
    cols[1].metric("Errors", f"{by_sev.get('error', 0):,}")
    cols[2].metric("Warnings", f"{by_sev.get('warning', 0):,}")
    cols[3].metric("Info", f"{by_sev.get('info', 0):,}")
    cols[4].metric("Patients", f"{patients:,}")

    by_domain = df.groupby(["domain", "severity"]).size().reset_index(name="count")
    fig = go.Figure()
    for sev, color in [("error", COLORS["rose"]), ("warning", COLORS["amber"]), ("info", COLORS["sky"])]:
        sub = by_domain[by_domain["severity"] == sev]
        if not sub.empty:
            fig.add_trace(go.Bar(name=sev, x=sub["domain"], y=sub["count"], marker_color=color))
    fig.update_layout(**PL, barmode="stack", title="Review Items by Domain & Severity",
                      height=350, xaxis_title=None, yaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

    fc1, fc2, fc3 = st.columns(3)
    sev_f = fc1.selectbox("Severity", ["all", "error", "warning", "info"], key="rq_sev")
    dom_f = fc2.selectbox("Domain", ["all"] + sorted(df["domain"].unique().tolist()), key="rq_dom")
    pri_f = fc3.selectbox("Priority", ["all"] + sorted(df["review_priority"].unique().tolist()), key="rq_pri")

    filtered = df.copy()
    if sev_f != "all":
        filtered = filtered[filtered["severity"] == sev_f]
    if dom_f != "all":
        filtered = filtered[filtered["domain"] == dom_f]
    if pri_f != "all":
        filtered = filtered[filtered["review_priority"] == pri_f]

    st.dataframe(filtered, use_container_width=True, height=500)
    multi_export(filtered, "val_review_queue", "rq")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def render_validation_engine(con) -> None:
    st.markdown(sl("Validation Engine"), unsafe_allow_html=True)

    available = _available_count(con)
    total_tables = len(VAL_TABLES)

    if available == 0:
        st.warning(
            "No validation tables found. Run `scripts/29_validation_engine.py --md` "
            "to create them, then refresh.",
            icon="⚠️"
        )
        return

    st.markdown(
        f"Validation coverage: {badge(f'{available}/{total_tables} tables', 'teal')}",
        unsafe_allow_html=True,
    )

    sub_tabs = st.tabs([
        "Adjudication",
        "Chronology",
        "Missing Derivable",
        "Unlinked Linkable",
        "Completeness",
        "Review Queue",
    ])

    with sub_tabs[0]:
        _render_adjudication(con)
    with sub_tabs[1]:
        _render_chronology(con)
    with sub_tabs[2]:
        _render_missing(con)
    with sub_tabs[3]:
        _render_unlinked(con)
    with sub_tabs[4]:
        _render_completeness(con)
    with sub_tabs[5]:
        _render_review_queue(con)

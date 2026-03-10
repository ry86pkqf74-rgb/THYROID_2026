"""Overall Adjudication / Review Summary dashboard tab."""
from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from app.helpers import sqdf, sqs, tbl_exists, mc, sl, badge, multi_export, PL, COLORS


def _resolve_view(con, local: str, md: str) -> str | None:
    if tbl_exists(con, local):
        return local
    if tbl_exists(con, md):
        return md
    return None


def render_adjudication_summary(con) -> None:
    qa_view = _resolve_view(con, "qa_summary_by_domain_v2", "md_qa_summary_v2")
    hp_view = _resolve_view(con, "qa_high_priority_review_v2", "md_qa_high_priority_v2")
    date_view = _resolve_view(con, "qa_date_completeness_v2", "md_date_quality_summary_v2")
    linkage_view = _resolve_view(con, "linkage_summary_v2", "md_linkage_summary_v2")
    mrq_view = _resolve_view(con, "manual_review_queue_summary_v2", "md_manual_review_queue_summary_v2")

    if not qa_view:
        st.warning(
            "Required view `qa_summary_by_domain_v2` is not available. "
            "Run the prerequisite deployment scripts first.",
            icon="⚠️",
        )
        return

    st.markdown(sl("Adjudication / Review Summary"), unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 1. Overall QA Summary
    # ------------------------------------------------------------------
    st.markdown(sl("Overall QA Summary"), unsafe_allow_html=True)

    qa_df = sqdf(con, f"SELECT * FROM {qa_view}")

    if not qa_df.empty and "severity" in qa_df.columns and "issue_count" in qa_df.columns:
        sev_totals = qa_df.groupby("severity")["issue_count"].sum()
        err_total = int(sev_totals.get("error", 0))
        warn_total = int(sev_totals.get("warning", 0))
        info_total = int(sev_totals.get("info", 0))

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(
                mc("Errors", f"{err_total:,}", badge("manual review", "rose")),
                unsafe_allow_html=True,
            )
        with c2:
            st.markdown(
                mc("Warnings", f"{warn_total:,}", badge("verify", "amber")),
                unsafe_allow_html=True,
            )
        with c3:
            st.markdown(
                mc("Info", f"{info_total:,}", badge("monitor", "sky")),
                unsafe_allow_html=True,
            )
        with c4:
            grand = err_total + warn_total + info_total
            st.markdown(mc("Total Issues", f"{grand:,}"), unsafe_allow_html=True)
    elif not qa_df.empty:
        st.dataframe(qa_df, use_container_width=True, hide_index=True)
    else:
        st.info("No QA summary data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 2. Issues by Check Type
    # ------------------------------------------------------------------
    st.markdown(sl("Issues by Check Type"), unsafe_allow_html=True)

    if not qa_df.empty and "check_id" in qa_df.columns and "issue_count" in qa_df.columns:
        check_df = qa_df.groupby("check_id", as_index=False)["issue_count"].sum()
        check_df = check_df.sort_values("issue_count", ascending=False)

        if not check_df.empty:
            fig_chk = go.Figure(go.Bar(
                x=check_df["check_id"],
                y=check_df["issue_count"],
                text=check_df["issue_count"].apply(lambda v: f"{v:,}"),
                textposition="outside",
                marker_color=PL["colorway"][:len(check_df)] if len(check_df) <= len(PL["colorway"]) else COLORS["teal"],
            ))
            fig_chk.update_layout(**PL, height=400, title="Issue Count by Check Type")
            st.plotly_chart(fig_chk, use_container_width=True)
    else:
        st.info("No check-type breakdown available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 3. Manual Review Queue Summary
    # ------------------------------------------------------------------
    st.markdown(sl("Manual Review Queue Summary"), unsafe_allow_html=True)

    if mrq_view:
        mrq_df = sqdf(con, f"SELECT * FROM {mrq_view} ORDER BY domain")
        if not mrq_df.empty:
            st.dataframe(mrq_df, use_container_width=True, hide_index=True)
            multi_export(mrq_df, "manual_review_queue_summary", key_sfx="adj_mrq")
        else:
            st.success("Manual review queue is empty.")
    else:
        st.info("Manual review queue summary view not available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 4. High Priority Review
    # ------------------------------------------------------------------
    st.markdown(sl("High Priority Review (Errors Only)"), unsafe_allow_html=True)

    if hp_view:
        hp_df = sqdf(con, f"""
            SELECT * FROM {hp_view}
            WHERE severity = 'error'
            ORDER BY research_id
        """)

        if hp_df.empty:
            st.success("No high-priority error-severity issues found.")
        else:
            st.markdown(f"**{len(hp_df):,}** error-severity issues requiring review")
            st.dataframe(hp_df, use_container_width=True, hide_index=True)
            multi_export(hp_df, "qa_high_priority", key_sfx="adj_hp")
    else:
        st.info("High priority review view not available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 5. Date Completeness
    # ------------------------------------------------------------------
    st.markdown(sl("Date Completeness"), unsafe_allow_html=True)

    if date_view:
        date_df = sqdf(con, f"SELECT * FROM {date_view}")
        if not date_df.empty:
            numeric_cols = date_df.select_dtypes(include="number").columns
            if len(numeric_cols) >= 2:
                labels = date_df.iloc[:, 0].astype(str).tolist() if date_df.shape[0] > 0 else []
                values = date_df[numeric_cols[0]].tolist() if labels else []

                if labels and values:
                    fig_date = go.Figure(go.Bar(
                        x=labels,
                        y=values,
                        text=[f"{v:,}" if isinstance(v, (int, float)) else str(v) for v in values],
                        textposition="outside",
                        marker_color=[COLORS["green"], COLORS["amber"], COLORS["sky"], COLORS["rose"]][:len(labels)],
                    ))
                    fig_date.update_layout(**PL, height=340, title="Date Quality Summary")
                    st.plotly_chart(fig_date, use_container_width=True)
                else:
                    st.dataframe(date_df, use_container_width=True, hide_index=True)
            else:
                st.dataframe(date_df, use_container_width=True, hide_index=True)
        else:
            st.info("No date completeness data available.")
    else:
        st.info("Date completeness view not available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 6. Linkage Summary
    # ------------------------------------------------------------------
    st.markdown(sl("Cross-Domain Linkage Summary"), unsafe_allow_html=True)

    if linkage_view:
        link_df = sqdf(con, f"SELECT * FROM {linkage_view}")
        if not link_df.empty:
            st.dataframe(link_df, use_container_width=True, hide_index=True)
            multi_export(link_df, "linkage_summary", key_sfx="adj_linkage")
        else:
            st.info("No linkage summary data available.")
    else:
        st.info("Linkage summary view not available. Run `scripts/23_cross_domain_linkage_v2.py`.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 7. Granular Linkage Quality (from materialized tables)
    # ------------------------------------------------------------------
    st.markdown(sl("Linkage Quality by Domain Pair"), unsafe_allow_html=True)

    linkage_pairs = [
        ("imaging_fna_linkage_v2", "md_imaging_fna_linkage_v2", "Imaging → FNA"),
        ("fna_molecular_linkage_v2", "md_fna_molecular_linkage_v2", "FNA → Molecular"),
        ("preop_surgery_linkage_v2", "md_preop_surgery_linkage_v2", "Pre-op → Surgery"),
        ("surgery_pathology_linkage_v2", "md_surgery_pathology_linkage_v2", "Surgery → Pathology"),
        ("pathology_rai_linkage_v2", "md_pathology_rai_linkage_v2", "Pathology → RAI"),
    ]
    linkage_rows = []
    for local_name, md_name, label in linkage_pairs:
        lv = _resolve_view(con, local_name, md_name)
        if lv:
            total = sqs(con, f"SELECT COUNT(*) FROM {lv}")
            try:
                weak = sqs(con, f"SELECT COUNT(*) FROM {lv} WHERE confidence_tier = 'weak'")
            except Exception:
                weak = 0
            try:
                exact = sqs(con, f"SELECT COUNT(*) FROM {lv} WHERE confidence_tier = 'exact_match'")
            except Exception:
                exact = 0
            linkage_rows.append({
                "Pair": label, "Total Links": total,
                "Exact Match": exact, "Weak": weak,
                "Weak %": f"{weak/total*100:.1f}%" if total else "—",
            })
    if linkage_rows:
        import pandas as pd
        lq_df = pd.DataFrame(linkage_rows)
        st.dataframe(lq_df, use_container_width=True, hide_index=True)
    else:
        st.info("Granular linkage tables not yet materialized. Run `scripts/26_motherduck_materialize_v2.py`.")

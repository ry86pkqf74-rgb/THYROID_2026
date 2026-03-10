"""Operative Detail Analytics dashboard tab."""
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


def render_operative_dashboard(con) -> None:
    ep_view = _resolve_view(con, "operative_episode_detail_v2", "md_oper_episode_detail_v2")
    review_view = _resolve_view(
        con, "operative_pathology_reconciliation_review_v2", "md_op_path_recon_review_v2",
    )

    if not ep_view:
        st.warning(
            "Required view `operative_episode_detail_v2` is not available. "
            "Run the prerequisite deployment scripts first.",
            icon="⚠️",
        )
        return

    st.markdown(sl("Operative Detail Analytics"), unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 1. Top Metrics
    # ------------------------------------------------------------------
    total = sqs(con, f"SELECT COUNT(*) FROM {ep_view}")
    patients = sqs(con, f"SELECT COUNT(DISTINCT research_id) FROM {ep_view}")

    proc_df = sqdf(con, f"""
        SELECT
            COALESCE(procedure_normalized, 'Unknown') AS procedure,
            COUNT(*) AS cnt
        FROM {ep_view}
        GROUP BY procedure_normalized
        ORDER BY cnt DESC
    """)

    top_proc = proc_df.iloc[0]["procedure"] if not proc_df.empty else "—"
    top_proc_ct = int(proc_df.iloc[0]["cnt"]) if not proc_df.empty else 0
    n_proc_types = len(proc_df) if not proc_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(mc("Total Surgeries", f"{int(total):,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Unique Patients", f"{int(patients):,}"), unsafe_allow_html=True)
    with c3:
        st.markdown(
            mc("Most Common", f"{top_proc_ct:,}", top_proc),
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(mc("Procedure Types", f"{n_proc_types}"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 2. Procedure Types
    # ------------------------------------------------------------------
    st.markdown(sl("Procedure Type Distribution"), unsafe_allow_html=True)

    if not proc_df.empty:
        fig_proc = go.Figure(go.Bar(
            x=proc_df["procedure"],
            y=proc_df["cnt"],
            text=proc_df["cnt"].apply(lambda v: f"{v:,}"),
            textposition="outside",
            marker_color=PL["colorway"][:len(proc_df)],
        ))
        fig_proc.update_layout(**PL, height=400, title="Procedure Type Distribution")
        st.plotly_chart(fig_proc, use_container_width=True)
    else:
        st.info("No procedure type data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 3. Neck Dissection
    # ------------------------------------------------------------------
    st.markdown(sl("Neck Dissection Rates"), unsafe_allow_html=True)

    cnd = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE cnd_performed = TRUE")
    lnd = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE lnd_performed = TRUE")
    cnd_pct = (int(cnd) / int(total) * 100) if int(total) > 0 else 0.0
    lnd_pct = (int(lnd) / int(total) * 100) if int(total) > 0 else 0.0

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            mc("Central Neck Dissection", f"{int(cnd):,}", f"{cnd_pct:.1f}%"),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            mc("Lateral Neck Dissection", f"{int(lnd):,}", f"{lnd_pct:.1f}%"),
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 4. Parathyroid Autograft
    # ------------------------------------------------------------------
    st.markdown(sl("Parathyroid Autograft"), unsafe_allow_html=True)

    autograft = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE parathyroid_autograft = TRUE")
    auto_pct = (int(autograft) / int(total) * 100) if int(total) > 0 else 0.0

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            mc("Autograft Performed", f"{int(autograft):,}", badge("documented", "green")),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            mc("Rate", f"{auto_pct:.1f}%", f"of {int(total):,} surgeries"),
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 5. Gross Invasion Findings
    # ------------------------------------------------------------------
    st.markdown(sl("Gross Invasion Findings"), unsafe_allow_html=True)

    ete_ct = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE ete_gross = TRUE")
    strap_ct = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE strap_muscle_invasion = TRUE")
    tracheal_ct = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE tracheal_invasion = TRUE")
    esoph_ct = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE esophageal_invasion = TRUE")

    inv_labels = ["ETE (Gross)", "Strap Muscle", "Tracheal", "Esophageal"]
    inv_counts = [int(ete_ct), int(strap_ct), int(tracheal_ct), int(esoph_ct)]
    inv_colors = [COLORS["rose"], COLORS["amber"], COLORS["violet"], COLORS["sky"]]

    fig_inv = go.Figure(go.Bar(
        x=inv_labels,
        y=inv_counts,
        text=[f"{c:,}" for c in inv_counts],
        textposition="outside",
        marker_color=inv_colors,
    ))
    fig_inv.update_layout(**PL, height=360, title="Gross Invasion by Type")
    st.plotly_chart(fig_inv, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 6. EBL Distribution
    # ------------------------------------------------------------------
    st.markdown(sl("Estimated Blood Loss Distribution"), unsafe_allow_html=True)

    ebl_df = sqdf(con, f"""
        SELECT ebl_ml
        FROM {ep_view}
        WHERE ebl_ml IS NOT NULL AND ebl_ml > 0
    """)

    if not ebl_df.empty:
        fig_ebl = go.Figure(go.Histogram(
            x=ebl_df["ebl_ml"],
            nbinsx=25,
            marker_color=COLORS["amber"],
            marker_line=dict(color=COLORS["border"], width=1),
        ))
        fig_ebl.update_layout(
            **PL, height=380,
            title="EBL Distribution (mL)",
            xaxis_title="EBL (mL)",
            yaxis_title="Count",
        )
        st.plotly_chart(fig_ebl, use_container_width=True)
    else:
        st.info("No EBL data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 7. Op-Path Mismatches
    # ------------------------------------------------------------------
    if review_view:
        st.markdown(sl("Operative-Pathology Reconciliation Review"), unsafe_allow_html=True)

        review_df = sqdf(con, f"""
            SELECT * FROM {review_view}
            ORDER BY research_id
        """)

        if review_df.empty:
            st.success("No operative-pathology reconciliation issues found.")
        else:
            if "severity" in review_df.columns:
                sev_counts = review_df["severity"].value_counts()
                err_ct = int(sev_counts.get("error", 0))
                warn_ct = int(sev_counts.get("warning", 0))

                c1, c2 = st.columns(2)
                with c1:
                    st.markdown(mc("Errors", f"{err_ct:,}", badge("needs review", "rose")), unsafe_allow_html=True)
                with c2:
                    st.markdown(mc("Warnings", f"{warn_ct:,}", badge("verify", "amber")), unsafe_allow_html=True)

                sev_filter = st.selectbox(
                    "Filter Severity", ["All", "error", "warning"], key="op_dash_sev",
                )
                filtered = review_df if sev_filter == "All" else review_df[review_df["severity"] == sev_filter]
            else:
                filtered = review_df

            st.markdown(f"Showing **{len(filtered):,}** of {len(review_df):,} records")
            st.dataframe(filtered, use_container_width=True, hide_index=True)
            multi_export(filtered, "op_path_reconciliation", key_sfx="op_dash_review")
    else:
        st.info("Operative-pathology reconciliation review view not available.")

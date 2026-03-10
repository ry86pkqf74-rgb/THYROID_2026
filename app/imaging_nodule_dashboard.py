"""Imaging / Nodule Analytics dashboard tab."""
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


def render_imaging_nodule_dashboard(con) -> None:
    nodule_view = _resolve_view(con, "imaging_nodule_long_v2", "md_imaging_nodule_long_v2")
    exam_view = _resolve_view(con, "imaging_exam_summary_v2", "md_imaging_exam_summary_v2")
    concordance_view = _resolve_view(
        con, "imaging_pathology_concordance_review_v2", "md_imaging_path_concordance_v2",
    )

    if not nodule_view:
        st.warning(
            "Required view `imaging_nodule_long_v2` is not available. "
            "Run the prerequisite deployment scripts first.",
            icon="⚠️",
        )
        return

    st.markdown(sl("Imaging / Nodule Analytics"), unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 1. Top Metrics
    # ------------------------------------------------------------------
    total_nodules = sqs(con, f"SELECT COUNT(*) FROM {nodule_view}")
    patients = sqs(con, f"SELECT COUNT(DISTINCT research_id) FROM {nodule_view}")

    modality_df = sqdf(con, f"""
        SELECT
            COALESCE(modality, 'Unknown') AS modality,
            COUNT(*) AS cnt
        FROM {nodule_view}
        GROUP BY modality
        ORDER BY cnt DESC
    """)

    us_ct = int(modality_df.loc[modality_df["modality"].str.contains("US|Ultrasound", case=False, na=False), "cnt"].sum()) if not modality_df.empty else 0
    ct_ct = int(modality_df.loc[modality_df["modality"].str.contains("CT", case=False, na=False), "cnt"].sum()) if not modality_df.empty else 0
    mri_ct = int(modality_df.loc[modality_df["modality"].str.contains("MRI|MR", case=False, na=False), "cnt"].sum()) if not modality_df.empty else 0

    mean_per_pt = (int(total_nodules) / int(patients)) if int(patients) > 0 else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(mc("Total Nodules", f"{int(total_nodules):,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Unique Patients", f"{int(patients):,}"), unsafe_allow_html=True)
    with c3:
        st.markdown(
            mc("Modality", f"{us_ct:,} / {ct_ct:,} / {mri_ct:,}", "US / CT / MRI"),
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(mc("Mean / Patient", f"{mean_per_pt:.1f}"), unsafe_allow_html=True)
    with c5:
        if exam_view:
            total_exams = sqs(con, f"SELECT COUNT(*) FROM {exam_view}")
            st.markdown(mc("Total Exams", f"{int(total_exams):,}"), unsafe_allow_html=True)
        else:
            st.markdown(mc("Exams View", "—", badge("unavailable", "text_lo")), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 2. TI-RADS Distribution
    # ------------------------------------------------------------------
    st.markdown(sl("TI-RADS Distribution"), unsafe_allow_html=True)

    tirads_df = sqdf(con, f"""
        SELECT
            COALESCE(CAST(tirads_score AS VARCHAR), 'null') AS tirads,
            COUNT(*) AS cnt
        FROM {nodule_view}
        GROUP BY tirads
        ORDER BY tirads
    """)

    if not tirads_df.empty:
        tirads_colors = {
            "1": COLORS["green"], "2": COLORS["teal"], "3": COLORS["sky"],
            "4": COLORS["amber"], "5": COLORS["rose"], "null": COLORS["text_lo"],
        }
        bar_colors = [tirads_colors.get(str(t), COLORS["teal_dim"]) for t in tirads_df["tirads"]]

        fig_tirads = go.Figure(go.Bar(
            x=tirads_df["tirads"],
            y=tirads_df["cnt"],
            text=tirads_df["cnt"].apply(lambda v: f"{v:,}"),
            textposition="outside",
            marker_color=bar_colors,
        ))
        fig_tirads.update_layout(**PL, height=380, title="TI-RADS Score Distribution")
        st.plotly_chart(fig_tirads, use_container_width=True)
    else:
        st.info("No TI-RADS data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 3. Nodule Size Distribution
    # ------------------------------------------------------------------
    st.markdown(sl("Nodule Size Distribution"), unsafe_allow_html=True)

    size_df = sqdf(con, f"""
        SELECT size_cm_max
        FROM {nodule_view}
        WHERE size_cm_max IS NOT NULL AND size_cm_max > 0
    """)

    if not size_df.empty:
        fig_size = go.Figure(go.Histogram(
            x=size_df["size_cm_max"],
            nbinsx=30,
            marker_color=COLORS["sky"],
            marker_line=dict(color=COLORS["border"], width=1),
        ))
        fig_size.update_layout(
            **PL, height=380,
            title="Nodule Maximum Size (cm)",
            xaxis_title="Size (cm)",
            yaxis_title="Count",
        )
        st.plotly_chart(fig_size, use_container_width=True)
    else:
        st.info("No nodule size data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 4. Laterality Distribution
    # ------------------------------------------------------------------
    st.markdown(sl("Laterality Distribution"), unsafe_allow_html=True)

    lat_df = sqdf(con, f"""
        SELECT
            COALESCE(laterality, 'Unknown') AS laterality,
            COUNT(*) AS cnt
        FROM {nodule_view}
        GROUP BY laterality
        ORDER BY cnt DESC
    """)

    if not lat_df.empty:
        fig_lat = go.Figure(go.Pie(
            labels=lat_df["laterality"],
            values=lat_df["cnt"],
            hole=0.4,
            marker=dict(colors=PL["colorway"][:len(lat_df)]),
            textinfo="label+percent",
            textfont=dict(color=COLORS["text_hi"]),
        ))
        fig_lat.update_layout(**PL, height=380, title="Nodule Laterality")
        st.plotly_chart(fig_lat, use_container_width=True)
    else:
        st.info("No laterality data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 5. Suspicious Lymph Nodes
    # ------------------------------------------------------------------
    st.markdown(sl("Suspicious Lymph Nodes"), unsafe_allow_html=True)

    susp_ln = sqs(con, f"""
        SELECT COUNT(*) FROM {nodule_view}
        WHERE suspicious_lymph_node_flag = TRUE
    """)
    susp_pct = (int(susp_ln) / int(total_nodules) * 100) if int(total_nodules) > 0 else 0.0

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            mc("Suspicious LN", f"{int(susp_ln):,}", badge("flagged", "rose")),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            mc("Prevalence", f"{susp_pct:.1f}%", f"of {int(total_nodules):,} nodules"),
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 6. Linkage Status
    # ------------------------------------------------------------------
    st.markdown(sl("FNA Linkage Status"), unsafe_allow_html=True)

    linked = sqs(con, f"""
        SELECT COUNT(*) FROM {nodule_view}
        WHERE linked_fna_episode_id IS NOT NULL
    """)
    unlinked = int(total_nodules) - int(linked)
    link_pct = (int(linked) / int(total_nodules) * 100) if int(total_nodules) > 0 else 0.0

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            mc("Linked to FNA", f"{int(linked):,}", f"{link_pct:.1f}%"),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            mc("Unlinked", f"{unlinked:,}", f"{100 - link_pct:.1f}%"),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(mc("Total", f"{int(total_nodules):,}"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 7. Imaging-Pathology Concordance Review
    # ------------------------------------------------------------------
    if concordance_view:
        st.markdown(sl("Imaging-Pathology Concordance Review"), unsafe_allow_html=True)

        conc_df = sqdf(con, f"SELECT * FROM {concordance_view} ORDER BY research_id")

        if conc_df.empty:
            st.success("No imaging-pathology concordance issues found.")
        else:
            st.markdown(f"Showing **{len(conc_df):,}** concordance review records")
            st.dataframe(conc_df, use_container_width=True, hide_index=True)
            multi_export(conc_df, "imaging_path_concordance", key_sfx="img_conc_review")
    else:
        st.info("Imaging-pathology concordance review view not available.")

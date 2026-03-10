"""Molecular Testing Analytics dashboard tab."""
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


def render_molecular_dashboard(con) -> None:
    ep_view = _resolve_view(con, "molecular_test_episode_v2", "md_molecular_test_episode_v2")
    review_view = _resolve_view(con, "molecular_linkage_review_v2", "md_molecular_linkage_review_v2")

    if not ep_view:
        st.warning(
            "Required view `molecular_test_episode_v2` is not available. "
            "Run the prerequisite deployment scripts first.",
            icon="⚠️",
        )
        return

    st.markdown(sl("Molecular Testing Analytics"), unsafe_allow_html=True)
    st.info("Showing pre-adjudication data. Adjudicated values are available in manuscript exports (script 20).", icon="ℹ️")

    # ------------------------------------------------------------------
    # 1. Top Metrics
    # ------------------------------------------------------------------
    total = sqs(con, f"SELECT COUNT(*) FROM {ep_view}")
    patients = sqs(con, f"SELECT COUNT(DISTINCT research_id) FROM {ep_view}")

    platform_df = sqdf(con, f"""
        SELECT
            COALESCE(platform_normalized, 'Unknown') AS platform,
            COUNT(*) AS cnt
        FROM {ep_view}
        GROUP BY platform_normalized
        ORDER BY cnt DESC
    """)

    thyroseq_ct = int(platform_df.loc[platform_df["platform"].str.contains("ThyroSeq", case=False, na=False), "cnt"].sum()) if not platform_df.empty else 0
    afirma_ct = int(platform_df.loc[platform_df["platform"].str.contains("Afirma", case=False, na=False), "cnt"].sum()) if not platform_df.empty else 0
    other_ct = int(total) - thyroseq_ct - afirma_ct

    linked = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE linked_fna_episode_id IS NOT NULL")
    unlinked = int(total) - int(linked)

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(mc("Total Tests", f"{int(total):,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Unique Patients", f"{int(patients):,}"), unsafe_allow_html=True)
    with c3:
        st.markdown(
            mc("Platforms", f"{thyroseq_ct:,} / {afirma_ct:,} / {other_ct:,}",
               "ThyroSeq / Afirma / Other"),
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(mc("Linked", f"{int(linked):,}", badge("FNA-linked", "green")), unsafe_allow_html=True)
    with c5:
        st.markdown(mc("Unlinked", f"{unlinked:,}", badge("no FNA", "rose")), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 2. Mutation Frequencies
    # ------------------------------------------------------------------
    st.markdown(sl("Mutation Frequencies"), unsafe_allow_html=True)

    markers = ["BRAF", "RAS", "RET", "TERT", "NTRK", "EIF1AX", "TP53", "ALK", "fusion", "CNA"]
    marker_counts: list[dict[str, object]] = []
    for m in markers:
        ct = sqs(con, f"""
            SELECT COUNT(*) FROM {ep_view}
            WHERE result_summary_raw ILIKE '%{m}%'
               OR test_name_raw ILIKE '%{m}%'
        """)
        marker_counts.append({"Marker": m, "Count": int(ct)})

    if marker_counts:
        fig_mut = go.Figure(go.Bar(
            x=[r["Marker"] for r in marker_counts],
            y=[r["Count"] for r in marker_counts],
            text=[f"{r['Count']:,}" for r in marker_counts],
            textposition="outside",
            marker_color=[
                COLORS["teal"], COLORS["sky"], COLORS["violet"], COLORS["amber"],
                COLORS["rose"], COLORS["green"], COLORS["teal_dim"], COLORS["sky"],
                COLORS["violet"], COLORS["amber"],
            ],
        ))
        fig_mut.update_layout(**PL, height=380, title="Marker Mention Prevalence")
        st.plotly_chart(fig_mut, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 3. Platform Distribution (pie)
    # ------------------------------------------------------------------
    st.markdown(sl("Platform Distribution"), unsafe_allow_html=True)

    if not platform_df.empty:
        fig_plat = go.Figure(go.Pie(
            labels=platform_df["platform"],
            values=platform_df["cnt"],
            hole=0.4,
            marker=dict(colors=PL["colorway"][:len(platform_df)]),
            textinfo="label+percent",
            textfont=dict(color=COLORS["text_hi"]),
        ))
        fig_plat.update_layout(**PL, height=380, title="Platform Distribution")
        st.plotly_chart(fig_plat, use_container_width=True)
    else:
        st.info("No platform data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 4. Result Classification
    # ------------------------------------------------------------------
    st.markdown(sl("Result Classification"), unsafe_allow_html=True)

    result_df = sqdf(con, f"""
        SELECT
            COALESCE(result_category_normalized, 'unknown') AS result,
            COUNT(*) AS cnt
        FROM {ep_view}
        GROUP BY result_category_normalized
        ORDER BY cnt DESC
    """)

    if not result_df.empty:
        color_map = {
            "positive": COLORS["rose"],
            "negative": COLORS["green"],
            "suspicious": COLORS["amber"],
            "indeterminate": COLORS["sky"],
            "non_diagnostic": COLORS["violet"],
            "cancelled": COLORS["text_lo"],
        }
        bar_colors = [color_map.get(r, COLORS["teal_dim"]) for r in result_df["result"]]

        fig_res = go.Figure(go.Bar(
            x=result_df["result"],
            y=result_df["cnt"],
            text=result_df["cnt"].apply(lambda v: f"{v:,}"),
            textposition="outside",
            marker_color=bar_colors,
        ))
        fig_res.update_layout(**PL, height=360, title="Result Classification Distribution")
        st.plotly_chart(fig_res, use_container_width=True)
    else:
        st.info("No result classification data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 5. High-Risk Markers
    # ------------------------------------------------------------------
    st.markdown(sl("High-Risk Markers"), unsafe_allow_html=True)

    hr_ct = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE high_risk_marker_flag = TRUE")
    hr_pct = (int(hr_ct) / int(total) * 100) if int(total) > 0 else 0.0

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(mc("High-Risk Flagged", f"{int(hr_ct):,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Prevalence", f"{hr_pct:.1f}%", f"of {int(total):,} tests"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 6. Linkage Status
    # ------------------------------------------------------------------
    st.markdown(sl("FNA Linkage Status"), unsafe_allow_html=True)

    link_pct = (int(linked) / int(total) * 100) if int(total) > 0 else 0.0
    unlink_pct = 100.0 - link_pct

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            mc("Linked to FNA", f"{int(linked):,}", f"{link_pct:.1f}%"),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            mc("Unlinked", f"{unlinked:,}", f"{unlink_pct:.1f}%"),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(mc("Total", f"{int(total):,}"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 7. Review Queue
    # ------------------------------------------------------------------
    if review_view:
        st.markdown(sl("Molecular Linkage Review Queue"), unsafe_allow_html=True)

        review_df = sqdf(con, f"""
            SELECT * FROM {review_view}
            WHERE severity IN ('warning', 'error')
            ORDER BY severity DESC, research_id
        """)

        if review_df.empty:
            st.success("No warnings or errors in the molecular linkage review queue.")
        else:
            sev_counts = review_df["severity"].value_counts() if "severity" in review_df.columns else {}
            err_ct = int(sev_counts.get("error", 0))
            warn_ct = int(sev_counts.get("warning", 0))

            c1, c2 = st.columns(2)
            with c1:
                st.markdown(mc("Errors", f"{err_ct:,}", badge("needs review", "rose")), unsafe_allow_html=True)
            with c2:
                st.markdown(mc("Warnings", f"{warn_ct:,}", badge("verify", "amber")), unsafe_allow_html=True)

            sev_filter = st.selectbox(
                "Filter Severity", ["All", "error", "warning"], key="mol_dash_sev",
            )
            filtered = review_df if sev_filter == "All" else review_df[review_df["severity"] == sev_filter]

            st.markdown(f"Showing **{len(filtered):,}** of {len(review_df):,} issues")
            st.dataframe(filtered, use_container_width=True, hide_index=True)
            multi_export(filtered, "molecular_linkage_review", key_sfx="mol_dash_review")
    else:
        st.info("Molecular linkage review view not available.")

    # ------------------------------------------------------------------
    # Linkage Quality
    # ------------------------------------------------------------------
    link_view = _resolve_view(con, "fna_molecular_linkage_v2", "md_fna_molecular_linkage_v2")
    if link_view:
        st.markdown("### Linkage Quality")
        link_df = sqdf(con, f"""
            SELECT linkage_confidence, COUNT(*) AS cnt
            FROM {link_view}
            GROUP BY linkage_confidence
            ORDER BY CASE linkage_confidence
                WHEN 'exact_match' THEN 1 WHEN 'high_confidence' THEN 2
                WHEN 'plausible' THEN 3 WHEN 'weak' THEN 4 ELSE 5
            END
        """)
        if link_df is not None and len(link_df) > 0:
            st.dataframe(link_df, use_container_width=True, hide_index=True)
            weak_ct = link_df.loc[link_df["linkage_confidence"] == "weak", "cnt"].sum() if "weak" in link_df["linkage_confidence"].values else 0
            if weak_ct > 0:
                st.warning(f"{int(weak_ct)} weak FNA-molecular linkages require manual review.", icon="⚠️")

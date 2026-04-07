"""Molecular Testing Analytics — unified normalized layer (contract views) + episode linkage v2."""
from __future__ import annotations

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.helpers import sqdf, sqs, tbl_exists, mc, sl, badge, multi_export, PL, COLORS

# Contract slice (scripts/sql/133_molecular_contract_views_ddl.sql)
RESULTS_CONTRACT_V = "molecular_results_contract_v"
VARIANT_CONTRACT_V = "molecular_variant_contract_v"
QC_SUMMARY_V = "molecular_qc_summary_v"
PATIENT_ROLLUP_V = "molecular_patient_rollup_v"


def _resolve_view(con, local: str, md: str) -> str | None:
    if tbl_exists(con, local):
        return local
    if tbl_exists(con, md):
        return md
    return None


def _has_object(con, name: str) -> bool:
    return tbl_exists(con, name)


def _assay_family_expr(alias: str = "r") -> str:
    """Classify rows as ThyroSeq / Afirma / Other from platform, assay, vendor."""
    blob = (
        f"lower(coalesce({alias}.platform,'') || ' ' || coalesce({alias}.assay_name,'') "
        f"|| ' ' || coalesce({alias}.vendor,''))"
    )
    return f"""CASE
        WHEN {blob} LIKE '%afirma%' THEN 'Afirma'
        WHEN {blob} LIKE '%thyroseq%' THEN 'ThyroSeq'
        ELSE 'Other'
    END"""


def render_unified_molecular_contract(con) -> None:
    """Dashboard sections backed by main.* contract views only (no raw staging)."""
    st.markdown(sl("Unified molecular layer (contract)"), unsafe_allow_html=True)
    st.caption(
        "Sourced from `molecular_*_contract_v` and `molecular_qc_summary_v`. "
        "ThyroSeq workbook integration remains under Manuscript → ThyroSeq Integration."
    )

    if not _has_object(con, RESULTS_CONTRACT_V):
        st.warning(
            "Contract view `molecular_results_contract_v` not found. "
            "Run `scripts/117_md_contract_views.py` after the molecular results layer is populated.",
            icon="⚠️",
        )
        return

    n_res = int(sqs(con, f"SELECT COUNT(*) FROM {RESULTS_CONTRACT_V}"))
    if n_res == 0:
        st.info(
            "Normalized molecular contract slice is empty (no live `molecular_results` rows). "
            "Episode linkage or ThyroSeq-only data may still be available in the **Episode linkage (v2)** tab."
        )

    total = n_res
    patients = int(sqs(con, f"SELECT COUNT(DISTINCT research_id) FROM {RESULTS_CONTRACT_V}"))

    fam_sql = f"""
        SELECT {_assay_family_expr('r')} AS assay_family, COUNT(*)::BIGINT AS cnt
        FROM {RESULTS_CONTRACT_V} r
        GROUP BY 1 ORDER BY cnt DESC
    """
    fam_df = sqdf(con, fam_sql)
    thy_ct = int(fam_df.loc[fam_df["assay_family"] == "ThyroSeq", "cnt"].sum()) if not fam_df.empty else 0
    afi_ct = int(fam_df.loc[fam_df["assay_family"] == "Afirma", "cnt"].sum()) if not fam_df.empty else 0
    oth_ct = max(0, total - thy_ct - afi_ct)

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(mc("Live results", f"{total:,}"), unsafe_allow_html=True)
    c2.markdown(mc("Patients", f"{patients:,}"), unsafe_allow_html=True)
    c3.markdown(
        mc("ThyroSeq / Afirma / Other", f"{thy_ct:,} / {afi_ct:,} / {oth_ct:,}"),
        unsafe_allow_html=True,
    )
    st_alts = int(
        sqs(
            con,
            f"SELECT COUNT(DISTINCT assay_name) FROM {RESULTS_CONTRACT_V} "
            "WHERE COALESCE(TRIM(assay_name),'') <> ''",
        )
    )
    c4.markdown(mc("Distinct assays", f"{st_alts:,}"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Assay / platform distribution ---
    st.markdown(sl("Assay & platform distribution"), unsafe_allow_html=True)
    plat_df = sqdf(
        con,
        f"""
        SELECT COALESCE(platform, '(unknown)') AS platform, COUNT(*)::BIGINT AS cnt
        FROM {RESULTS_CONTRACT_V}
        GROUP BY 1 ORDER BY cnt DESC
        """,
    )
    assay_df = sqdf(
        con,
        f"""
        SELECT COALESCE(assay_name, '(unknown)') AS assay_name, COUNT(*)::BIGINT AS cnt
        FROM {RESULTS_CONTRACT_V}
        GROUP BY 1 ORDER BY cnt DESC LIMIT 25
        """,
    )
    pc1, pc2 = st.columns(2)
    with pc1:
        if not plat_df.empty and plat_df["cnt"].sum() > 0:
            fig_p = px.bar(
                plat_df,
                x="cnt",
                y="platform",
                orientation="h",
                color="platform",
                color_discrete_sequence=PL["colorway"],
            )
            fig_p.update_layout(**PL, height=320, showlegend=False, yaxis_title="", xaxis_title="Results")
            st.plotly_chart(fig_p, use_container_width=True)
        else:
            st.caption("No platform values recorded.")
    with pc2:
        if not assay_df.empty and assay_df["cnt"].sum() > 0:
            fig_a = px.bar(
                assay_df,
                x="cnt",
                y="assay_name",
                orientation="h",
                color="assay_name",
                color_discrete_sequence=[COLORS["teal"], COLORS["sky"], COLORS["violet"], COLORS["amber"]],
            )
            fig_a.update_layout(**PL, height=320, showlegend=False, yaxis_title="", xaxis_title="Results")
            st.plotly_chart(fig_a, use_container_width=True)
        else:
            st.caption("No assay_name values recorded.")

    # ThyroSeq vs Afirma pie
    if not fam_df.empty and fam_df["cnt"].sum() > 0:
        fig_fam = go.Figure(
            go.Pie(
                labels=fam_df["assay_family"],
                values=fam_df["cnt"],
                hole=0.4,
                marker=dict(colors=PL["colorway"][: len(fam_df)]),
                textinfo="label+percent",
                textfont=dict(color=COLORS["text_hi"]),
            )
        )
        fig_fam.update_layout(**PL, height=340, title="ThyroSeq vs Afirma (keyword roll-up)")
        st.plotly_chart(fig_fam, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Panel version distribution ---
    st.markdown(sl("Panel / version distribution"), unsafe_allow_html=True)
    ver_df = sqdf(
        con,
        f"""
        SELECT COALESCE(panel_version, '(unset)') AS panel_version, COUNT(*)::BIGINT AS cnt
        FROM {RESULTS_CONTRACT_V}
        GROUP BY 1 ORDER BY cnt DESC LIMIT 40
        """,
    )
    if not ver_df.empty and ver_df["cnt"].sum() > 0:
        fig_v = go.Figure(
            go.Bar(
                x=ver_df["panel_version"],
                y=ver_df["cnt"],
                marker_color=COLORS["teal"],
            )
        )
        fig_v.update_layout(**PL, height=340, title="panel_version", xaxis_title="", yaxis_title="Results")
        st.plotly_chart(fig_v, use_container_width=True)
        multi_export(ver_df, "molecular_panel_version_dist", key_sfx="mol_pver")
    else:
        st.caption("No panel_version data.")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Patient-level timeline ---
    st.markdown(sl("Patient-level assay timeline"), unsafe_allow_html=True)
    if not _has_object(con, PATIENT_ROLLUP_V):
        st.caption("`molecular_patient_rollup_v` not found — re-run contract DDL load.")
        rollup = None
    else:
        rollup = sqdf(con, f"SELECT * FROM {PATIENT_ROLLUP_V} ORDER BY n_molecular_results DESC NULLS LAST")
    if rollup is not None and not rollup.empty:
        st.dataframe(rollup, use_container_width=True, hide_index=True)
        multi_export(rollup, "molecular_patient_rollup_v", key_sfx="mol_rollup")

        rid_opts = [int(x) for x in rollup["research_id"].dropna().unique().tolist()[:500]]
        if rid_opts:
            pick = st.selectbox("Patient (research_id)", options=rid_opts, key="mol_contract_rid_tl")
            tl = sqdf(
                con,
                f"""
                SELECT
                    test_date_parsed,
                    assay_name,
                    platform,
                    panel_version,
                    parse_status,
                    normalization_status,
                    molecular_result_id
                FROM {RESULTS_CONTRACT_V}
                WHERE research_id = {int(pick)}
                ORDER BY test_date_parsed NULLS LAST, ingestion_ts NULLS LAST
                """,
            )
            if not tl.empty:
                st.dataframe(tl, use_container_width=True, hide_index=True)
                dates = tl[tl["test_date_parsed"].notna()]
                if len(dates) > 1:
                    fig_tl = px.scatter(
                        dates,
                        x="test_date_parsed",
                        y="assay_name",
                        color="platform",
                        color_discrete_sequence=PL["colorway"],
                    )
                    fig_tl.update_layout(**PL, height=280, title="Assays over time")
                    st.plotly_chart(fig_tl, use_container_width=True)
                multi_export(tl, f"molecular_timeline_patient_{pick}", key_sfx=f"mol_tl_{pick}")
    elif rollup is not None and rollup.empty:
        st.caption("No rows in molecular_patient_rollup_v.")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Variant frequency ---
    st.markdown(sl("Variant frequency (top genes)"), unsafe_allow_html=True)
    if _has_object(con, VARIANT_CONTRACT_V):
        vf = sqdf(
            con,
            f"""
            SELECT COALESCE(gene_symbol, '(unknown)') AS gene_symbol, COUNT(*)::BIGINT AS n_calls
            FROM {VARIANT_CONTRACT_V}
            GROUP BY 1 ORDER BY n_calls DESC LIMIT 60
            """,
        )
        if not vf.empty:
            fig_vf = px.bar(
                vf.head(25),
                x="gene_symbol",
                y="n_calls",
                color_discrete_sequence=[COLORS["sky"]],
            )
            fig_vf.update_layout(**PL, height=360, xaxis_title="", yaxis_title="Variant rows")
            st.plotly_chart(fig_vf, use_container_width=True)
            multi_export(vf, "molecular_variant_gene_frequency", key_sfx="mol_vfreq")
        else:
            st.caption("No variant rows in contract slice.")
    else:
        st.info("`molecular_variant_contract_v` not available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Fusion / CNV summaries ---
    st.markdown(sl("Fusion & CNV summaries"), unsafe_allow_html=True)
    if _has_object(con, VARIANT_CONTRACT_V):
        fc = sqdf(
            con,
            f"""
            SELECT
                variant_class,
                COUNT(*)::BIGINT AS n_rows,
                COUNT(DISTINCT molecular_result_id)::BIGINT AS n_results,
                COUNT(DISTINCT research_id)::BIGINT AS n_patients
            FROM {VARIANT_CONTRACT_V}
            WHERE lower(COALESCE(variant_class, '')) IN ('fusion', 'cnv')
               OR fusion_partner IS NOT NULL
               OR partner_gene_symbol IS NOT NULL
            GROUP BY variant_class ORDER BY n_rows DESC
            """,
        )
        if not fc.empty:
            st.dataframe(fc, use_container_width=True, hide_index=True)
            multi_export(fc, "molecular_fusion_cnv_summary", key_sfx="mol_fc_sum")
        else:
            st.caption("No fusion/CNV-classified variants in the contract slice.")

        detail = sqdf(
            con,
            f"""
            SELECT research_id, gene_symbol, partner_gene_symbol, fusion_partner,
                   variant_class, canonical_hgvs, molecular_result_id
            FROM {VARIANT_CONTRACT_V}
            WHERE lower(COALESCE(variant_class, '')) IN ('fusion', 'cnv')
               OR fusion_partner IS NOT NULL
            ORDER BY research_id
            LIMIT 5000
            """,
        )
        if detail is not None and not detail.empty:
            st.dataframe(detail, use_container_width=True, hide_index=True)
            multi_export(detail, "molecular_fusion_cnv_detail", key_sfx="mol_fc_det")
    st.markdown("<br>", unsafe_allow_html=True)

    # --- QC & normalization ---
    st.markdown(sl("QC, parse status, and normalization"), unsafe_allow_html=True)
    if _has_object(con, QC_SUMMARY_V):
        qc = sqdf(con, f"SELECT * FROM {QC_SUMMARY_V} ORDER BY n_results DESC")
        if not qc.empty:
            st.dataframe(qc, use_container_width=True, hide_index=True)
            multi_export(qc, "molecular_qc_summary_v", key_sfx="mol_qc_sum")
    else:
        st.caption("`molecular_qc_summary_v` not found — re-run contract DDL load.")

    flags_norm_sql = f"""
        SELECT molecular_result_id, research_id, assay_name, platform,
               parse_status, normalization_status, qc_flags, source_table
        FROM {RESULTS_CONTRACT_V}
        WHERE lower(COALESCE(parse_status, '')) IN ('partial', 'failed', 'pending')
           OR lower(COALESCE(normalization_status, '')) IN ('pending_review', 'quarantine', 'failed')
           OR qc_flags IS NOT NULL
        ORDER BY research_id, molecular_result_id
        LIMIT 15000
    """
    prob = sqdf(con, flags_norm_sql)
    if prob is not None and not prob.empty:
        st.warning(f"**{len(prob):,}** results with parse/QC/normalization attention flags (capped display).")
        st.dataframe(prob, use_container_width=True, hide_index=True)
        multi_export(prob, "molecular_parse_norm_qc_flags", key_sfx="mol_qc_flags")
    elif total > 0:
        st.success("No rows matched default parse/normalization/QC problem filters.")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Unresolved manual review (contract/QA view) ---
    st.markdown(sl("Unresolved molecular / genetics review queue"), unsafe_allow_html=True)
    if tbl_exists(con, "manual_review_queue"):
        pending = sqdf(
            con,
            """
            SELECT *
            FROM qa.manual_review_queue
            WHERE verification_status IS NULL
              AND (
                    lower(domain) LIKE '%molecular%'
                 OR lower(domain) LIKE '%genetics%'
                 OR domain IN ('molecular', 'genetics')
                  )
            ORDER BY run_label, research_id
            """,
        )
        if pending is not None and not pending.empty:
            st.markdown(
                mc("Pending items", f"{len(pending):,}", badge("manual review", "amber")),
                unsafe_allow_html=True,
            )
            st.dataframe(pending, use_container_width=True, hide_index=True)
            multi_export(pending, "molecular_manual_review_pending", key_sfx="mol_mrq")
        else:
            st.success("No pending molecular/genetics rows in `qa.manual_review_queue`.")
    else:
        st.caption("`qa.manual_review_queue` not present in this catalog.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(sl("Export normalized molecular data (contract slice)"), unsafe_allow_html=True)
    res_full = sqdf(con, f"SELECT * FROM {RESULTS_CONTRACT_V}")
    multi_export(res_full, "molecular_results_contract_v_export", key_sfx="mol_ex_res")
    if _has_object(con, VARIANT_CONTRACT_V):
        var_full = sqdf(con, f"SELECT * FROM {VARIANT_CONTRACT_V}")
        multi_export(var_full, "molecular_variant_contract_v_export", key_sfx="mol_ex_var")


def render_molecular_episode_v2_dashboard(con, ep_view: str) -> None:
    """Pre-adjudication episode / linkage analytics (legacy v2 freeze views)."""
    review_view = _resolve_view(con, "molecular_linkage_review_v2", "md_molecular_linkage_review_v2")

    st.markdown(sl("Molecular Testing Analytics (episode v2)"), unsafe_allow_html=True)
    st.info(
        "Showing pre-adjudication episode data. Normalized layer metrics live in **Unified molecular layer**.",
        icon="ℹ️",
    )

    total = sqs(con, f"SELECT COUNT(*) FROM {ep_view}")
    patients = sqs(con, f"SELECT COUNT(DISTINCT research_id) FROM {ep_view}")

    platform_df = sqdf(
        con,
        f"""
        SELECT
            COALESCE(platform_normalized, 'Unknown') AS platform,
            COUNT(*) AS cnt
        FROM {ep_view}
        GROUP BY platform_normalized
        ORDER BY cnt DESC
    """,
    )

    thyroseq_ct = (
        int(platform_df.loc[platform_df["platform"].str.contains("ThyroSeq", case=False, na=False), "cnt"].sum())
        if not platform_df.empty
        else 0
    )
    afirma_ct = (
        int(platform_df.loc[platform_df["platform"].str.contains("Afirma", case=False, na=False), "cnt"].sum())
        if not platform_df.empty
        else 0
    )
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
            mc(
                "Platforms",
                f"{thyroseq_ct:,} / {afirma_ct:,} / {other_ct:,}",
                "ThyroSeq / Afirma / Other",
            ),
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(mc("Linked", f"{int(linked):,}", badge("FNA-linked", "green")), unsafe_allow_html=True)
    with c5:
        st.markdown(mc("Unlinked", f"{unlinked:,}", badge("no FNA", "rose")), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(sl("Mutation Frequencies"), unsafe_allow_html=True)

    markers = ["BRAF", "RAS", "RET", "TERT", "NTRK", "EIF1AX", "TP53", "ALK", "fusion", "CNA"]
    marker_counts: list[dict[str, object]] = []
    for m in markers:
        ct = sqs(
            con,
            f"""
            SELECT COUNT(*) FROM {ep_view}
            WHERE result_summary_raw ILIKE '%{m}%'
               OR test_name_raw ILIKE '%{m}%'
        """,
        )
        marker_counts.append({"Marker": m, "Count": int(ct)})

    if marker_counts:
        fig_mut = go.Figure(
            go.Bar(
                x=[r["Marker"] for r in marker_counts],
                y=[r["Count"] for r in marker_counts],
                text=[f"{r['Count']:,}" for r in marker_counts],
                textposition="outside",
                marker_color=[
                    COLORS["teal"],
                    COLORS["sky"],
                    COLORS["violet"],
                    COLORS["amber"],
                    COLORS["rose"],
                    COLORS["green"],
                    COLORS["teal_dim"],
                    COLORS["sky"],
                    COLORS["violet"],
                    COLORS["amber"],
                ],
            )
        )
        fig_mut.update_layout(**PL, height=380, title="Marker Mention Prevalence")
        st.plotly_chart(fig_mut, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(sl("Platform Distribution"), unsafe_allow_html=True)

    if not platform_df.empty:
        fig_plat = go.Figure(
            go.Pie(
                labels=platform_df["platform"],
                values=platform_df["cnt"],
                hole=0.4,
                marker=dict(colors=PL["colorway"][: len(platform_df)]),
                textinfo="label+percent",
                textfont=dict(color=COLORS["text_hi"]),
            )
        )
        fig_plat.update_layout(**PL, height=380, title="Platform Distribution")
        st.plotly_chart(fig_plat, use_container_width=True)
    else:
        st.info("No platform data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(sl("Result Classification"), unsafe_allow_html=True)

    result_df = sqdf(
        con,
        f"""
        SELECT
            COALESCE(result_category_normalized, 'unknown') AS result,
            COUNT(*) AS cnt
        FROM {ep_view}
        GROUP BY result_category_normalized
        ORDER BY cnt DESC
    """,
    )

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

        fig_res = go.Figure(
            go.Bar(
                x=result_df["result"],
                y=result_df["cnt"],
                text=result_df["cnt"].apply(lambda v: f"{v:,}"),
                textposition="outside",
                marker_color=bar_colors,
            )
        )
        fig_res.update_layout(**PL, height=360, title="Result Classification Distribution")
        st.plotly_chart(fig_res, use_container_width=True)
    else:
        st.info("No result classification data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(sl("High-Risk Markers"), unsafe_allow_html=True)

    hr_ct = sqs(con, f"SELECT COUNT(*) FROM {ep_view} WHERE high_risk_marker_flag = TRUE")
    hr_pct = (int(hr_ct) / int(total) * 100) if int(total) > 0 else 0.0

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(mc("High-Risk Flagged", f"{int(hr_ct):,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Prevalence", f"{hr_pct:.1f}%", f"of {int(total):,} tests"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

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

    if review_view:
        st.markdown(sl("Molecular Linkage Review Queue"), unsafe_allow_html=True)

        review_df = sqdf(
            con,
            f"""
            SELECT * FROM {review_view}
            WHERE severity IN ('warning', 'error')
            ORDER BY severity DESC, research_id
        """,
        )

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
                "Filter Severity",
                ["All", "error", "warning"],
                key="mol_dash_sev",
            )
            filtered = review_df if sev_filter == "All" else review_df[review_df["severity"] == sev_filter]

            st.markdown(f"Showing **{len(filtered):,}** of {len(review_df):,} issues")
            st.dataframe(filtered, use_container_width=True, hide_index=True)
            multi_export(filtered, "molecular_linkage_review", key_sfx="mol_dash_review")
    else:
        st.info("Molecular linkage review view not available.")

    link_view = _resolve_view(con, "fna_molecular_linkage_v2", "md_fna_molecular_linkage_v2")
    if link_view:
        st.markdown("### Linkage Quality")
        link_df = sqdf(
            con,
            f"""
            SELECT linkage_confidence, COUNT(*) AS cnt
            FROM {link_view}
            GROUP BY linkage_confidence
            ORDER BY CASE linkage_confidence
                WHEN 'exact_match' THEN 1 WHEN 'high_confidence' THEN 2
                WHEN 'plausible' THEN 3 WHEN 'weak' THEN 4 ELSE 5
            END
        """,
        )
        if link_df is not None and len(link_df) > 0:
            st.dataframe(link_df, use_container_width=True, hide_index=True)
            weak_ct = (
                link_df.loc[link_df["linkage_confidence"] == "weak", "cnt"].sum()
                if "weak" in link_df["linkage_confidence"].values
                else 0
            )
            if weak_ct > 0:
                st.warning(f"{int(weak_ct)} weak FNA-molecular linkages require manual review.", icon="⚠️")


def render_molecular_dashboard(con) -> None:
    """Entry: prefer unified contract layer when deployed; always offer episode v2 when available."""
    ep_view = _resolve_view(con, "molecular_test_episode_v2", "md_molecular_test_episode_v2")
    has_contract = _has_object(con, RESULTS_CONTRACT_V)

    if has_contract and ep_view:
        u_tab, e_tab = st.tabs(["Unified molecular layer", "Episode linkage (v2)"])
        with u_tab:
            render_unified_molecular_contract(con)
        with e_tab:
            render_molecular_episode_v2_dashboard(con, ep_view)
        return

    if has_contract:
        render_unified_molecular_contract(con)
        if not ep_view:
            st.info(
                "`molecular_test_episode_v2` not found — episode linkage charts are unavailable; "
                "normalized contract data is shown above."
            )
        return

    if ep_view:
        st.info(
            "Normalized contract views are not deployed. Showing **episode linkage (v2)** only. "
            "Run the molecular results layer and `scripts/117_md_contract_views.py` for the unified tab.",
            icon="ℹ️",
        )
        render_molecular_episode_v2_dashboard(con, ep_view)
        return

    st.warning(
        "Neither `molecular_results_contract_v` nor `molecular_test_episode_v2` is available. "
        "Run ingest / contract deployment scripts.",
        icon="⚠️",
    )

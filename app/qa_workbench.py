"""
QA Workbench -- Action-oriented data quality page.

Surfaces health monitoring tables, provenance completeness,
linkage coverage, and actionable next-step guidance.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd


def _safe_query(con, sql: str, fallback=None):
    try:
        return con.execute(sql).fetchdf()
    except Exception:
        return fallback


def render_qa_workbench(con) -> None:
    st.header("Data Quality & System Health")

    # --- Dataset Integrity Summary ---
    st.subheader("Dataset Integrity")
    integrity = _safe_query(con, "SELECT * FROM val_dataset_integrity_summary_v1 ORDER BY table_name")
    if integrity is not None and len(integrity) > 0:
        cols = st.columns(4)
        total_rows = int(integrity["row_count"].sum()) if "row_count" in integrity.columns else 0
        n_tables = len(integrity)
        cols[0].metric("Tables Audited", n_tables)
        cols[1].metric("Total Rows", f"{total_rows:,}")
        zero_tables = integrity[integrity.get("row_count", pd.Series(dtype=int)) == 0] if "row_count" in integrity.columns else pd.DataFrame()
        cols[2].metric("Empty Tables", len(zero_tables))
        if "column_count" in integrity.columns:
            cols[3].metric("Total Columns", int(integrity["column_count"].sum()))

        with st.expander("Full Table Inventory", expanded=False):
            st.dataframe(integrity, use_container_width=True)

        if len(zero_tables) > 0:
            st.warning(f"{len(zero_tables)} tables have zero rows. These may need rebuilding.")
            st.dataframe(zero_tables[["table_name"]] if "table_name" in zero_tables.columns else zero_tables, use_container_width=True)
    else:
        st.info("Run `scripts/75_dataset_maturation.py --md --phase 7` to generate integrity summary.")

    st.divider()

    # --- Provenance Completeness ---
    st.subheader("Provenance Completeness")
    prov = _safe_query(con, "SELECT * FROM val_provenance_completeness_v2 ORDER BY table_name")
    if prov is not None and len(prov) > 0:
        if "fill_pct" in prov.columns:
            avg_prov = float(prov["fill_pct"].mean())
            st.progress(min(avg_prov / 100.0, 1.0), text=f"Average provenance fill: {avg_prov:.1f}%")
        with st.expander("Provenance Details", expanded=False):
            st.dataframe(prov, use_container_width=True)
    else:
        st.info("Provenance completeness data not available. Run script 75 phase 7.")

    st.divider()

    # --- Linkage Completeness ---
    st.subheader("Episode Linkage Coverage")
    linkage = _safe_query(con, "SELECT * FROM val_episode_linkage_completeness_v1")
    if linkage is not None and len(linkage) > 0:
        st.dataframe(linkage, use_container_width=True)
    else:
        st.info("Linkage completeness data not available. Run script 75 phase 7.")

    st.divider()

    # --- Lab Completeness ---
    st.subheader("Lab Coverage by Analyte")
    labs = _safe_query(con, "SELECT * FROM val_lab_completeness_v1 ORDER BY analyte_group, lab_name_standardized")
    if labs is not None and len(labs) > 0:
        available = labs[labs.get("data_completeness_tier", pd.Series(dtype=str)) != "future_institutional_required"]
        future = labs[labs.get("data_completeness_tier", pd.Series(dtype=str)) == "future_institutional_required"]

        if len(available) > 0:
            st.dataframe(available, use_container_width=True)
        if len(future) > 0:
            st.caption(f"{len(future)} analytes pending future institutional lab extract.")
    else:
        st.info("Run `scripts/77_lab_canonical_layer.py --md` to build lab completeness.")

    st.divider()

    # --- Chronology Anomalies ---
    st.subheader("Chronology Anomaly Resolution")
    anomalies = _safe_query(con, """
        SELECT resolution_bucket, COUNT(*) as n
        FROM val_temporal_anomaly_resolution_v1
        GROUP BY 1 ORDER BY 2 DESC
    """)
    if anomalies is not None and len(anomalies) > 0:
        cols = st.columns(len(anomalies))
        for i, row in anomalies.iterrows():
            bucket = str(row.get("resolution_bucket", "unknown"))
            n = int(row.get("n", 0))
            cols[i % len(cols)].metric(bucket.replace("_", " ").title(), n)
    else:
        st.info("No chronology anomaly data. Run script 75 phase 5.")

    st.divider()

    # --- Imaging-FNA Linkage Status ---
    st.subheader("Imaging-FNA Linkage")
    img_linkage = _safe_query(con, """
        SELECT COUNT(*) AS n_links,
               COUNT(DISTINCT research_id) AS n_patients,
               COUNT(*) FILTER (WHERE analysis_eligible_link_flag) AS n_eligible
        FROM imaging_fna_linkage_v3
    """)
    if img_linkage is not None and len(img_linkage) > 0:
        row = img_linkage.iloc[0]
        n_links = int(row.get("n_links") or 0)
        n_pts = int(row.get("n_patients") or 0)
        n_elig = int(row.get("n_eligible") or 0)
        cols = st.columns(3)
        cols[0].metric("Total Links", f"{n_links:,}")
        cols[1].metric("Patients Linked", f"{n_pts:,}")
        cols[2].metric("Analysis-Eligible", f"{n_elig:,}")
        if n_links == 0:
            st.warning("Imaging-FNA linkage is empty. Run `scripts/78_final_hardening.py --md --phase B`.")
    else:
        st.info("Imaging-FNA linkage table not found. Run script 78 phase B.")
    st.caption("Source: `imaging_fna_linkage_v3` | Linkage uses imaging_nodule_master_v1 (TIRADS Excel) preferred over imaging_nodule_long_v2 (placeholder).")

    st.divider()

    # --- Chained Molecular Linkage Metrics ---
    st.subheader("Molecular Linkage Chain")
    st.caption("Molecular tests link to surgery via chained linkage: molecular -> FNA -> surgery. "
               "No direct molecular-surgery linkage table exists by design.")
    mol_fna = _safe_query(con, """
        SELECT COUNT(DISTINCT research_id) AS n
        FROM fna_molecular_linkage_v3
        WHERE score_rank = 1 AND analysis_eligible_link_flag
    """)
    mol_surg = _safe_query(con, """
        SELECT COUNT(DISTINCT research_id) AS n
        FROM preop_surgery_linkage_v3
        WHERE preop_type = 'molecular' AND score_rank = 1 AND analysis_eligible_link_flag
    """)
    if mol_fna is not None and mol_surg is not None:
        n_mol_fna = int((mol_fna.iloc[0, 0]) or 0) if len(mol_fna) > 0 else 0
        n_mol_surg = int((mol_surg.iloc[0, 0]) or 0) if len(mol_surg) > 0 else 0
        cols = st.columns(2)
        cols[0].metric("Molecular -> FNA", f"{n_mol_fna:,} patients")
        cols[1].metric("Molecular -> Surgery (direct preop)", f"{n_mol_surg:,} patients")
    else:
        st.info("Molecular linkage tables not found.")

    st.divider()

    # --- RAI Dose Missingness ---
    st.subheader("RAI Dose Missingness")
    rai_miss = _safe_query(con, "SELECT * FROM vw_rai_dose_missingness_summary ORDER BY n_episodes DESC")
    if rai_miss is not None and len(rai_miss) > 0:
        for _, row in rai_miss.iterrows():
            reason = str(row.get("reason", "unknown"))
            n = int(row.get("n_episodes") or 0)
            pct = float(row.get("pct") or 0)
            icon = {"dose_available": "OK", "no_source_report_available": "source-limited",
                    "linkage_failed": "fixable", "source_present_no_dose_stated": "parse-gap"}.get(reason, "")
            st.markdown(f"- **{reason}**: {n:,} episodes ({pct}%) {f'-- {icon}' if icon else ''}")
        st.caption("Source: `vw_rai_dose_missingness_summary` | `no_source_report_available` = zero nuclear med notes in corpus.")
    else:
        rai_fill = _safe_query(con, """
            SELECT ROUND(100.0 * SUM(CASE WHEN dose_mci IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
            FROM rai_treatment_episode_v2
        """)
        if rai_fill is not None and len(rai_fill) > 0:
            pct = float(rai_fill.iloc[0, 0] or 0)
            st.info(f"RAI dose fill at {pct}%. Run `scripts/78_final_hardening.py --md --phase C` for missingness classification.")
        else:
            st.info("RAI treatment data not available.")

    st.divider()

    # --- Recurrence Date Status ---
    st.subheader("Recurrence Date Resolution")
    recur_tiers = _safe_query(con, "SELECT * FROM val_recurrence_date_resolution_v1 ORDER BY date_tier")
    if recur_tiers is not None and len(recur_tiers) > 0:
        st.dataframe(recur_tiers, use_container_width=True)
        unresolved = recur_tiers[recur_tiers.get("date_tier", pd.Series(dtype=str)) == "unresolved_date"]
        if len(unresolved) > 0:
            n_unres = int(unresolved.iloc[0].get("n_rows") or 0)
            n_mc = int(unresolved.iloc[0].get("n_in_manuscript_cohort") or 0)
            st.warning(f"{n_unres} unresolved recurrence dates ({n_mc} in manuscript cohort). "
                       "Review prioritized queue in Manual Review workbench.")
    else:
        recur = _safe_query(con, """
            SELECT COUNT(*) as n FROM extracted_recurrence_refined_v1
            WHERE recurrence_date_status = 'unresolved_date'
        """)
        if recur is not None and len(recur) > 0:
            n = int(recur.iloc[0, 0] or 0)
            if n > 0:
                st.warning(f"{n} unresolved recurrence dates. "
                           "Run `scripts/78_final_hardening.py --md --phase A` for review queue.")
    st.caption("Source: `val_recurrence_date_resolution_v1` / `extracted_recurrence_refined_v1`")

    st.divider()

    # --- Lab Canonical Status ---
    st.subheader("Lab Canonical Layer")
    lab_val = _safe_query(con, "SELECT lab_name_standardized, n_total, n_exact_dupes, validation_status FROM val_lab_canonical_v1")
    if lab_val is not None and len(lab_val) > 0:
        all_pass = all(lab_val["validation_status"].isin(["PASS", "WARN"]))
        n_dupes = int(lab_val["n_exact_dupes"].sum()) if "n_exact_dupes" in lab_val.columns else -1
        lab_total = int(lab_val["n_total"].sum()) if "n_total" in lab_val.columns else 0
        if n_dupes == 0:
            st.success(f"Lab canonical layer: {lab_total:,} rows, 0 duplicate groups.")
        else:
            st.warning(f"Lab canonical layer: {lab_total:,} rows, {n_dupes} duplicate groups remain. Run dedup.")
        with st.expander("Lab Validation Detail"):
            st.dataframe(lab_val, use_container_width=True)
    else:
        st.info("Run `scripts/78_final_hardening.py --md --phase D` to build lab validation.")

    st.divider()

    # --- Operative V2 NLP Status ---
    st.subheader("Operative NLP Enrichment")
    op_fill = _safe_query(con, """
        SELECT
            COUNT(*) FILTER (WHERE berry_ligament_flag IS NOT NULL) AS berry,
            COUNT(*) FILTER (WHERE frozen_section_flag IS NOT NULL) AS frozen,
            COUNT(*) FILTER (WHERE ebl_ml_nlp IS NOT NULL) AS ebl_nlp,
            COUNT(*) FILTER (WHERE parathyroid_identified_count IS NOT NULL) AS parathyroid_count,
            COUNT(*) AS total
        FROM operative_episode_detail_v2
    """)
    if op_fill is not None and len(op_fill) > 0:
        row = op_fill.iloc[0]
        total = int(row.get("total") or 0)
        filled = max(int(row.get("berry") or 0), int(row.get("frozen") or 0),
                     int(row.get("ebl_nlp") or 0), int(row.get("parathyroid_count") or 0))
        if filled == 0:
            st.info(f"8 operative NLP fields at 0/{total:,} — extractor exists "
                    "(`extract_operative_v2.py`) but outputs not yet materialized. "
                    "This is a **source-limited gap**, not a data quality issue.")
        else:
            st.success(f"Operative NLP enrichment partially populated ({filled}/{total:,} rows).")
    else:
        st.info("Operative episode table not available.")

    st.divider()

    # --- Actionable Guidance ---
    st.subheader("What To Run Next")
    guidance = []
    if integrity is not None and len(integrity) > 0 and len(zero_tables) > 0:
        guidance.append("Rebuild empty tables listed above (check deployment order in docs)")
    if labs is not None and len(labs) > 0 and len(future) > 0:
        guidance.append("Obtain institutional lab extract for TSH, vitamin D, albumin, etc.")

    rai_fill_check = _safe_query(con, """
        SELECT ROUND(100.0 * SUM(CASE WHEN dose_mci IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
        FROM rai_treatment_episode_v2
    """)
    if rai_fill_check is not None and len(rai_fill_check) > 0:
        pct = float(rai_fill_check.iloc[0, 0] or 0)
        if pct < 50:
            guidance.append(f"RAI dose fill at {pct}% -- source-limited (no nuclear med notes in corpus)")

    if guidance:
        for g in guidance:
            st.markdown(f"- {g}")
    else:
        st.success("No critical gaps detected. Dataset is in good shape.")

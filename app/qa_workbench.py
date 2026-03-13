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

    # --- Actionable Guidance ---
    st.subheader("What To Run Next")
    guidance = []
    if integrity is not None and len(integrity) > 0 and len(zero_tables) > 0:
        guidance.append("Rebuild empty tables listed above (check deployment order in docs)")
    if labs is not None and len(labs) > 0 and len(future) > 0:
        guidance.append("Obtain institutional lab extract for TSH, vitamin D, albumin, etc.")

    rai_fill = _safe_query(con, """
        SELECT ROUND(100.0 * SUM(CASE WHEN dose_mci IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
        FROM rai_treatment_episode_v2
    """)
    if rai_fill is not None and len(rai_fill) > 0:
        pct = float(rai_fill.iloc[0, 0] or 0)
        if pct < 50:
            guidance.append(f"RAI dose fill at {pct}% -- source-limited (no nuclear med notes in corpus)")

    recur = _safe_query(con, """
        SELECT COUNT(*) as n FROM extracted_recurrence_refined_v1
        WHERE recurrence_date_status = 'unresolved_date'
    """)
    if recur is not None and len(recur) > 0:
        n = int(recur.iloc[0, 0] or 0)
        if n > 0:
            guidance.append(f"{n} recurrence events with unresolved dates -- requires manual chart review")

    if guidance:
        for g in guidance:
            st.markdown(f"- {g}")
    else:
        st.success("No critical gaps detected. Dataset is in good shape.")

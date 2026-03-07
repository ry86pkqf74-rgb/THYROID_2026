#!/usr/bin/env python3
"""
Thyroid Cohort Explorer — Interactive Streamlit Dashboard
Powered by MotherDuck cloud data warehouse.

Run locally:
    export MOTHERDUCK_TOKEN='your_token'
    streamlit run dashboard.py

For Streamlit Cloud, add MOTHERDUCK_TOKEN to app secrets.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from motherduck_client import MotherDuckClient, MotherDuckConfig

# ── Page config ──────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Thyroid Cohort Explorer",
    page_icon="\U0001f52c",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ────────────────────────────────────────────────────────────

SHARE_PATH = (
    "md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c"
)
DATABASE = "thyroid_research_2026"

PALETTE = px.colors.qualitative.Set2

# ── Token resolution ─────────────────────────────────────────────────────


def _ensure_token() -> bool:
    """Bridge Streamlit secrets into env so MotherDuckClient can find it."""
    if os.getenv("MOTHERDUCK_TOKEN"):
        return True
    try:
        os.environ["MOTHERDUCK_TOKEN"] = st.secrets["MOTHERDUCK_TOKEN"]
        return True
    except (KeyError, FileNotFoundError):
        return False


# ── Connection ───────────────────────────────────────────────────────────


@st.cache_resource(show_spinner="Connecting to MotherDuck\u2026")
def _get_connection() -> duckdb.DuckDBPyConnection:
    config = MotherDuckConfig(database=DATABASE, share_path=SHARE_PATH)
    client = MotherDuckClient(config)
    try:
        return client.connect_ro_share()
    except Exception:
        return client.connect_rw()


# ── Cached query helpers ─────────────────────────────────────────────────


@st.cache_data(ttl=300, show_spinner=False)
def _query_df(_con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return _con.execute(sql).fetchdf()


@st.cache_data(ttl=300, show_spinner=False)
def _scalar(_con: duckdb.DuckDBPyConnection, sql: str):
    row = _con.execute(sql).fetchone()
    return row[0] if row else 0


def _safe_query_df(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    try:
        return _query_df(con, sql)
    except Exception as exc:
        st.warning(f"Query failed: {exc}")
        return pd.DataFrame()


def _safe_scalar(con: duckdb.DuckDBPyConnection, sql: str):
    try:
        return _scalar(con, sql)
    except Exception:
        return 0


# ── Data loaders ─────────────────────────────────────────────────────────


def _load_overview(con: duckdb.DuckDBPyConnection) -> dict:
    q = _safe_scalar
    return {
        "total": q(con, "SELECT COUNT(DISTINCT research_id) FROM master_cohort"),
        "tumor_path": q(con, "SELECT COUNT(*) FROM master_cohort WHERE has_tumor_pathology"),
        "benign_path": q(con, "SELECT COUNT(*) FROM master_cohort WHERE has_benign_pathology"),
        "fna": q(con, "SELECT COUNT(*) FROM master_cohort WHERE has_fna_cytology"),
        "braf": q(
            con,
            "SELECT COALESCE(SUM(CASE WHEN braf_mutation_mentioned THEN 1 ELSE 0 END), 0) "
            "FROM tumor_pathology",
        ),
        "parathyroid_adenoma": q(
            con,
            "SELECT COALESCE(SUM(CASE WHEN is_parathyroid_adenoma THEN 1 ELSE 0 END), 0) "
            "FROM parathyroid",
        ),
        "rai_pos": q(con, "SELECT COUNT(*) FROM nuclear_med WHERE rai_avid_flag = 'positive'"),
        "nuclear_med": q(con, "SELECT COUNT(*) FROM master_cohort WHERE has_nuclear_med"),
        "ultrasound": q(con, "SELECT COUNT(*) FROM master_cohort WHERE has_ultrasound_reports"),
        "ct": q(con, "SELECT COUNT(*) FROM master_cohort WHERE has_ct_imaging"),
        "tg_labs": q(con, "SELECT COUNT(*) FROM master_cohort WHERE has_thyroglobulin_labs"),
        "anti_tg_labs": q(con, "SELECT COUNT(*) FROM master_cohort WHERE has_anti_thyroglobulin_labs"),
    }


def _load_advanced_features(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return _safe_query_df(con, "SELECT * FROM advanced_features_view")


def _load_histology_dist(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return _safe_query_df(
        con,
        "SELECT COALESCE(histology_1_type, 'Not specified') AS histology, "
        "COUNT(*) AS n FROM tumor_pathology "
        "WHERE histology_1_type IS NOT NULL AND TRIM(histology_1_type) != '' "
        "GROUP BY 1 ORDER BY n DESC",
    )


def _load_stage_dist(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return _safe_query_df(
        con,
        "SELECT COALESCE(histology_1_overall_stage_ajcc8, 'Unknown') AS stage, "
        "COUNT(*) AS n FROM tumor_pathology "
        "WHERE histology_1_overall_stage_ajcc8 IS NOT NULL "
        "AND TRIM(histology_1_overall_stage_ajcc8) != '' "
        "GROUP BY 1 ORDER BY n DESC",
    )


def _load_mutation_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return _safe_query_df(
        con,
        "SELECT 'BRAF' AS mutation, COALESCE(SUM(CASE WHEN braf_mutation_mentioned THEN 1 ELSE 0 END),0) AS mentioned FROM tumor_pathology "
        "UNION ALL SELECT 'RAS',  COALESCE(SUM(CASE WHEN ras_mutation_mentioned  THEN 1 ELSE 0 END),0) FROM tumor_pathology "
        "UNION ALL SELECT 'RET',  COALESCE(SUM(CASE WHEN ret_mutation_mentioned  THEN 1 ELSE 0 END),0) FROM tumor_pathology "
        "UNION ALL SELECT 'TERT', COALESCE(SUM(CASE WHEN tert_mutation_mentioned THEN 1 ELSE 0 END),0) FROM tumor_pathology "
        "UNION ALL SELECT 'NTRK', COALESCE(SUM(CASE WHEN ntrk_mutation_mentioned THEN 1 ELSE 0 END),0) FROM tumor_pathology "
        "UNION ALL SELECT 'ALK',  COALESCE(SUM(CASE WHEN alk_mutation_mentioned  THEN 1 ELSE 0 END),0) FROM tumor_pathology",
    )


def _load_rai_breakdown(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return _safe_query_df(
        con,
        "SELECT COALESCE(rai_avid_flag, 'not assessed') AS status, COUNT(*) AS n "
        "FROM nuclear_med GROUP BY 1 ORDER BY n DESC",
    )


def _load_benign_phenotypes(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return _safe_query_df(
        con,
        "SELECT 'MNG' AS phenotype, COALESCE(SUM(CASE WHEN is_mng THEN 1 ELSE 0 END),0) AS n FROM benign_pathology "
        "UNION ALL SELECT 'Graves',              COALESCE(SUM(CASE WHEN is_graves THEN 1 ELSE 0 END),0)              FROM benign_pathology "
        "UNION ALL SELECT 'Follicular Adenoma',   COALESCE(SUM(CASE WHEN is_follicular_adenoma THEN 1 ELSE 0 END),0)  FROM benign_pathology "
        "UNION ALL SELECT 'Hurthle Adenoma',      COALESCE(SUM(CASE WHEN is_hurthle_adenoma THEN 1 ELSE 0 END),0)     FROM benign_pathology "
        "UNION ALL SELECT 'Hashimoto',            COALESCE(SUM(CASE WHEN is_hashimoto THEN 1 ELSE 0 END),0)           FROM benign_pathology "
        "UNION ALL SELECT 'TGDC',                 COALESCE(SUM(CASE WHEN is_tgdc THEN 1 ELSE 0 END),0)                FROM benign_pathology "
        "UNION ALL SELECT 'Hyalinizing Trabecular',COALESCE(SUM(CASE WHEN is_hyalinizing_trabecular THEN 1 ELSE 0 END),0) FROM benign_pathology",
    )


def _load_sex_dist(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return _safe_query_df(
        con,
        "SELECT COALESCE(sex, 'Unknown') AS sex, COUNT(*) AS n "
        "FROM master_cohort WHERE sex IS NOT NULL AND TRIM(sex) != '' "
        "GROUP BY 1 ORDER BY n DESC",
    )


# ── Sidebar filters ─────────────────────────────────────────────────────


def _build_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    with st.sidebar:
        st.header("Filters")
        st.caption("Applied to Data Explorer and filtered visualizations")

        histology_opts = sorted(df["histology_1_type"].dropna().unique().tolist())
        selected_histology = st.multiselect(
            "Histology type",
            options=histology_opts,
            default=[],
            placeholder="All histology types",
        )

        st.divider()

        braf_only = st.checkbox("BRAF mutation mentioned")
        parathyroid_only = st.checkbox("Has parathyroid data")
        tumor_path_only = st.checkbox("Tumor pathology only")

        st.divider()

        sex_opts = sorted(df["sex"].dropna().unique().tolist())
        selected_sex = st.multiselect("Sex", options=sex_opts, default=[])

        ages = df["age_at_surgery"].dropna()
        if not ages.empty:
            age_lo, age_hi = int(ages.min()), int(ages.max())
            if age_lo < age_hi:
                age_range = st.slider(
                    "Age at surgery", age_lo, age_hi, (age_lo, age_hi)
                )
            else:
                age_range = (age_lo, age_hi)
        else:
            age_range = (0, 120)

        st.divider()
        if st.button("Clear all filters"):
            st.rerun()

    filtered = df.copy()
    if selected_histology:
        filtered = filtered[filtered["histology_1_type"].isin(selected_histology)]
    if braf_only:
        filtered = filtered[filtered["braf_mutation_mentioned"] == True]  # noqa: E712
    if parathyroid_only:
        filtered = filtered[filtered["has_parathyroid"] == True]  # noqa: E712
    if tumor_path_only:
        filtered = filtered[filtered["has_tumor_pathology"] == True]  # noqa: E712
    if selected_sex:
        filtered = filtered[filtered["sex"].isin(selected_sex)]
    filtered = filtered[
        filtered["age_at_surgery"].isna()
        | (
            (filtered["age_at_surgery"] >= age_range[0])
            & (filtered["age_at_surgery"] <= age_range[1])
        )
    ]
    return filtered


# ── Tab renderers ────────────────────────────────────────────────────────


def _render_overview(con: duckdb.DuckDBPyConnection) -> None:
    m = _load_overview(con)

    r1 = st.columns(4)
    r1[0].metric("Total Patients", f"{m['total']:,}")
    r1[1].metric("Tumor Pathology", f"{m['tumor_path']:,}")
    r1[2].metric("Benign Pathology", f"{m['benign_path']:,}")
    r1[3].metric("FNA Cytology", f"{m['fna']:,}")

    r2 = st.columns(4)
    r2[0].metric("BRAF Mentioned", f"{m['braf']:,}")
    r2[1].metric("Parathyroid Adenomas", f"{m['parathyroid_adenoma']:,}")
    r2[2].metric("RAI Positive", f"{m['rai_pos']:,}")
    r2[3].metric("Nuclear Med Studies", f"{m['nuclear_med']:,}")

    r3 = st.columns(4)
    r3[0].metric("Ultrasound Reports", f"{m['ultrasound']:,}")
    r3[1].metric("CT Imaging", f"{m['ct']:,}")
    r3[2].metric("Tg Labs", f"{m['tg_labs']:,}")
    r3[3].metric("Anti-Tg Labs", f"{m['anti_tg_labs']:,}")

    st.divider()
    st.subheader("Data Completeness by Year")
    df_comp = _safe_query_df(
        con, "SELECT * FROM data_completeness_by_year ORDER BY surgery_year"
    )
    if not df_comp.empty:
        fig = px.bar(
            df_comp,
            x="surgery_year",
            y=[
                "n_tumor_pathology",
                "n_fna_cytology",
                "n_ultrasound_reports",
                "n_thyroglobulin_labs",
            ],
            barmode="group",
            labels={
                "value": "Patients",
                "surgery_year": "Surgery Year",
                "variable": "Domain",
            },
            color_discrete_sequence=PALETTE,
        )
        fig.update_layout(
            legend=dict(orientation="h", y=-0.2), height=420, margin=dict(t=10)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Completeness data not available.")


def _render_explorer(df: pd.DataFrame) -> None:
    st.subheader(f"Filtered Cohort \u2014 {len(df):,} patients")

    default_cols = [
        c
        for c in [
            "research_id",
            "age_at_surgery",
            "sex",
            "histology_1_type",
            "variant_standardized",
            "overall_stage_ajcc8",
            "largest_tumor_cm",
            "braf_mutation_mentioned",
            "has_parathyroid",
        ]
        if c in df.columns
    ]

    cols = st.multiselect(
        "Columns to display",
        options=df.columns.tolist(),
        default=default_cols,
    )
    display = df[cols] if cols else df
    st.dataframe(display, use_container_width=True, height=520)

    st.download_button(
        "\u2b07 Download filtered CSV",
        data=df.to_csv(index=False),
        file_name=f"thyroid_cohort_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )


def _render_visualizations(con: duckdb.DuckDBPyConnection) -> None:
    left, right = st.columns(2)

    with left:
        st.subheader("Histology Distribution")
        df_h = _load_histology_dist(con)
        if not df_h.empty:
            fig = px.bar(
                df_h.head(15),
                x="n",
                y="histology",
                orientation="h",
                color="n",
                color_continuous_scale="Blues",
                labels={"n": "Patients", "histology": ""},
            )
            fig.update_layout(
                showlegend=False,
                coloraxis_showscale=False,
                yaxis=dict(autorange="reversed"),
                height=460,
                margin=dict(l=10, t=10),
            )
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("AJCC 8th Edition Stage")
        df_s = _load_stage_dist(con)
        if not df_s.empty:
            fig = px.bar(
                df_s,
                x="stage",
                y="n",
                color="stage",
                color_discrete_sequence=PALETTE,
                labels={"n": "Patients", "stage": "Stage"},
            )
            fig.update_layout(showlegend=False, height=460, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    left2, right2 = st.columns(2)

    with left2:
        st.subheader("Sex Distribution")
        df_sex = _load_sex_dist(con)
        if not df_sex.empty:
            fig = px.pie(
                df_sex,
                values="n",
                names="sex",
                color_discrete_sequence=PALETTE,
                hole=0.4,
            )
            fig.update_layout(height=380, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    with right2:
        st.subheader("Parathyroid Findings")
        df_p = _safe_query_df(
            con,
            "SELECT CASE WHEN is_parathyroid_adenoma THEN 'Adenoma' "
            "ELSE 'Other / None' END AS finding, COUNT(*) AS n "
            "FROM parathyroid GROUP BY 1",
        )
        if not df_p.empty:
            fig = px.pie(
                df_p,
                values="n",
                names="finding",
                color_discrete_sequence=["#ff7f0e", "#1f77b4"],
                hole=0.4,
            )
            fig.update_layout(height=380, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)


def _render_advanced(con: duckdb.DuckDBPyConnection) -> None:
    left, right = st.columns(2)

    with left:
        st.subheader("Mutation Flags Summary")
        df_m = _load_mutation_summary(con)
        if not df_m.empty:
            fig = px.bar(
                df_m,
                x="mutation",
                y="mentioned",
                color="mutation",
                color_discrete_sequence=PALETTE,
                labels={"mentioned": "Reports mentioning", "mutation": ""},
            )
            fig.update_layout(showlegend=False, height=400, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("RAI Avidity Breakdown")
        df_r = _load_rai_breakdown(con)
        if not df_r.empty:
            fig = px.pie(
                df_r,
                values="n",
                names="status",
                color_discrete_sequence=PALETTE,
                hole=0.4,
            )
            fig.update_layout(height=400, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("Benign Pathology Phenotypes")
    df_b = _load_benign_phenotypes(con)
    if not df_b.empty:
        fig = px.bar(
            df_b,
            x="phenotype",
            y="n",
            color="phenotype",
            color_discrete_sequence=PALETTE,
            labels={"n": "Patients", "phenotype": ""},
        )
        fig.update_layout(showlegend=False, height=400, margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("Recurrence Risk Bands")
    df_rr = _safe_query_df(
        con,
        "SELECT recurrence_risk_band AS band, COUNT(*) AS n "
        "FROM recurrence_risk_cohort GROUP BY 1 ORDER BY n DESC",
    )
    if not df_rr.empty:
        c1, c2 = st.columns([1, 2])
        with c1:
            st.dataframe(df_rr, use_container_width=True)
        with c2:
            fig = px.pie(
                df_rr,
                values="n",
                names="band",
                color="band",
                color_discrete_map={
                    "low": "#2ca02c",
                    "intermediate": "#ff7f0e",
                    "high": "#d62728",
                },
                hole=0.4,
            )
            fig.update_layout(height=350, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)


# ── Main ─────────────────────────────────────────────────────────────────


def main() -> None:
    if not _ensure_token():
        st.title("\U0001f52c Thyroid Cohort Explorer")
        st.error(
            "**MotherDuck token not found.**\n\n"
            "Provide your token via one of:\n\n"
            "1. **Environment variable:**  \n"
            "   `export MOTHERDUCK_TOKEN='your_token'`\n\n"
            "2. **Streamlit secrets** (`.streamlit/secrets.toml`):  \n"
            '   `MOTHERDUCK_TOKEN = "your_token_here"`\n\n'
            "Get a token at [app.motherduck.com]"
            "(https://app.motherduck.com) \u2192 Settings \u2192 Access Tokens."
        )
        st.stop()

    try:
        con = _get_connection()
    except Exception as exc:
        st.error(f"Failed to connect to MotherDuck: {exc}")
        st.stop()

    st.title("\U0001f52c Thyroid Cohort Explorer (MotherDuck)")
    st.caption(
        "Interactive dashboard for the thyroid cancer research cohort "
        "\u2022 11,673 patients"
    )

    df_full = _load_advanced_features(con)
    if df_full.empty:
        st.error(
            "Could not load `advanced_features_view`. "
            "Verify the view exists in MotherDuck."
        )
        st.stop()

    df_filtered = _build_sidebar(df_full)

    tab_ov, tab_ex, tab_viz, tab_adv = st.tabs(
        [
            "\U0001f4ca Overview",
            "\U0001f5c2 Data Explorer",
            "\U0001f4c8 Visualizations",
            "\U0001f9ec Advanced",
        ]
    )

    with tab_ov:
        _render_overview(con)

    with tab_ex:
        _render_explorer(df_filtered)

    with tab_viz:
        _render_visualizations(con)

    with tab_adv:
        _render_advanced(con)

    st.divider()
    st.caption(
        f"**Data source:** MotherDuck `{DATABASE}` \u2022 "
        f"Read-only share: `{SHARE_PATH}` \u2022 "
        f"Last loaded: {datetime.now().strftime('%Y-%m-%d %H:%M')} \u2022 "
        "Built with Streamlit + DuckDB + Plotly"
    )


if __name__ == "__main__":
    main()

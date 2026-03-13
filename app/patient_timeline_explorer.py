"""
Patient Timeline Explorer tab.

Lets users pick a research_id and see the full enriched timeline
with date provenance, episodes (molecular / RAI / operative / imaging),
and a one-click Publication Snapshot export.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.helpers import (
    COLORS,
    PL,
    mc,
    multi_export,
    sl,
    sqdf,
    tbl_exists,
)

# ── Plotly layout tokens ──────────────────────────────────────────────────
_DATE_STATUS_COLORS = {
    "exact_source_date":        COLORS["teal"],
    "inferred_day_level_date":  COLORS["sky"],
    "note_text_inferred_date":  COLORS["violet"],
    "coarse_anchor_date":       COLORS["amber"],
    "unresolved_date":          COLORS["rose"],
}

_DATE_STATUS_LABELS = {
    "exact_source_date":        "Exact (entity_date)",
    "inferred_day_level_date":  "Inferred (note_date)",
    "note_text_inferred_date":  "Inferred (note body)",
    "coarse_anchor_date":       "Coarse anchor",
    "unresolved_date":          "Unresolved",
}


# ── Helper: resolve timeline table (v3_mv first, md_ prefix fallback) ─────
def _timeline_tbl(con) -> str | None:
    for name in ("timeline_rescue_v3_mv", "md_timeline_rescue_v3_mv",
                 "timeline_rescue_v2_mv",  "md_timeline_rescue_v2_mv"):
        if tbl_exists(con, name):
            return name
    return None


def _enriched_tbl(con) -> str | None:
    for name in ("enriched_patient_timeline_v3_mv",
                 "md_enriched_patient_timeline_v3_mv"):
        if tbl_exists(con, name):
            return name
    return None


# ── Rescue KPI (prominent progress bar) ──────────────────────────────────
def _render_rescue_kpi(con):
    """Large date rescue KPI card with progress bar pulled from
    timeline_unresolved_summary_v2_mv or date_rescue_rate_summary."""
    kpi_tbl = None
    for name in ("timeline_unresolved_summary_v2_mv",
                 "date_rescue_rate_summary",
                 "md_date_rescue_rate_summary"):
        if tbl_exists(con, name):
            kpi_tbl = name
            break

    if not kpi_tbl:
        st.info("Date rescue KPI table not yet materialized — run script 26 with --md.", icon="ℹ️")
        return

    try:
        if kpi_tbl == "timeline_unresolved_summary_v2_mv":
            df = sqdf(con, f"""
                SELECT
                    SUM(total_rows)    AS total_entities,
                    SUM(rescued_rows)  AS rescued,
                    ROUND(100.0 * SUM(rescued_rows) / NULLIF(SUM(total_rows), 0), 1)
                                       AS rescue_rate_pct
                FROM {kpi_tbl}
                WHERE entity_table != 'ALL_DOMAINS'
            """)
        else:
            df = sqdf(con, f"""
                SELECT total_entities, rescued, rescue_rate_pct
                FROM {kpi_tbl}
                WHERE entity_table = 'ALL_DOMAINS'
                LIMIT 1
            """)

        if df.empty:
            st.info("No rescue KPI data available yet.", icon="ℹ️")
            return

        row = df.iloc[0]
        rate = float(row.get("rescue_rate_pct", 0) or 0)
        rescued = int(row.get("rescued", 0) or 0)
        total = int(row.get("total_entities", 0) or 0)

        st.markdown(
            f'<div style="background:linear-gradient(135deg,#0a1a20,#0e1219);'
            f'border:1px solid #1e2535;border-left:3px solid #2dd4bf;'
            f'border-radius:12px;padding:1.1rem 1.5rem;margin-bottom:0.8rem">'
            f'<div style="font-family:\'DM Mono\',monospace;font-size:.62rem;'
            f'letter-spacing:.15em;color:#2dd4bf;text-transform:uppercase;'
            f'margin-bottom:6px">Date Rescue Rate — All Domains</div>'
            f'<div style="font-family:\'DM Serif Display\',serif;font-size:2.4rem;'
            f'color:#f0f4ff;line-height:1">{rate:.1f}%'
            f'<span style="font-size:.9rem;color:#8892a4;font-family:sans-serif;'
            f'margin-left:10px">{rescued:,} of {total:,} entity rows rescued</span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )
        st.progress(min(rate / 100.0, 1.0))

    except Exception as e:
        st.warning(f"Could not load rescue KPI: {e}", icon="⚠️")


# ── Per-domain rescue bar chart ───────────────────────────────────────────
def _render_rescue_by_type(con, tl_tbl: str):
    try:
        df = sqdf(con, f"""
            SELECT
                entity_type,
                COUNT(*)                                                AS total,
                SUM(CASE WHEN date_status != 'unresolved_date'
                         THEN 1 ELSE 0 END)                             AS rescued,
                ROUND(100.0 * SUM(CASE WHEN date_status != 'unresolved_date'
                                       THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 1)                         AS rescue_pct,
                SUM(CASE WHEN date_status = 'unresolved_date'
                         THEN 1 ELSE 0 END)                             AS unresolved
            FROM {tl_tbl}
            GROUP BY entity_type
            ORDER BY rescue_pct DESC
        """)
        if df.empty:
            return
        fig = go.Figure(go.Bar(
            y=df["entity_type"],
            x=df["rescue_pct"],
            orientation="h",
            marker_color=COLORS["teal"],
            text=df["rescue_pct"].apply(lambda v: f"{v:.1f}%"),
            textposition="auto",
            customdata=df[["rescued", "unresolved"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>Rescue rate: %{x:.1f}%<br>"
                "Rescued: %{customdata[0]:,}<br>"
                "Unresolved: %{customdata[1]:,}<extra></extra>"
            ),
        ))
        fig.update_layout(**PL, height=240, xaxis_title="% Rescued",
                          yaxis_autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.caption(f"Rescue-by-type chart unavailable: {e}")


# ── Patient picker ────────────────────────────────────────────────────────
def _render_patient_timeline(con, tl_tbl: str, enr_tbl: str | None):
    st.markdown(sl("Patient Timeline Explorer"), unsafe_allow_html=True)

    # Date status legend
    _legend_html = " ".join(
        f'<span style="background:{color}20;color:{color};padding:2px 8px;'
        f'border-radius:4px;font-size:.72rem;margin-right:6px">{label}</span>'
        for status, label in _DATE_STATUS_LABELS.items()
        if (color := _DATE_STATUS_COLORS.get(status, "#8892a4"))
    )
    st.markdown(f'<div style="margin-bottom:10px">{_legend_html}</div>', unsafe_allow_html=True)

    rid_input = st.text_input(
        "Research ID",
        placeholder="Enter research_id (integer)…",
        key="pte_research_id",
    )
    if not rid_input.strip():
        st.info("Enter a research_id above to load the patient timeline.", icon="🔍")
        return

    try:
        rid = int(rid_input.strip())
    except ValueError:
        st.error("research_id must be an integer.", icon="❌")
        return

    # ── Patient header metrics ────────────────────────────────────────────
    header_tbl = None
    for n in ("streamlit_patient_header_v", "md_streamlit_patient_header_v",
              "patient_level_summary_mv",   "md_patient_level_summary_mv"):
        if tbl_exists(con, n):
            header_tbl = n
            break

    if header_tbl:
        hdf = sqdf(con, f"SELECT * FROM {header_tbl} WHERE research_id = {rid} LIMIT 1")
        if hdf.empty:
            st.warning(f"No patient header found for research_id {rid}.", icon="⚠️")
        else:
            h = hdf.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.markdown(mc("Sex", str(h.get("sex", "—"))), unsafe_allow_html=True)
            with c2:
                age = h.get("age_at_surgery", h.get("age", "—"))
                st.markdown(mc("Age at Surgery", str(age)), unsafe_allow_html=True)
            with c3:
                hist = h.get("histology_1_type", h.get("histology", "—"))
                st.markdown(mc("Histology", str(hist)[:28]), unsafe_allow_html=True)
            with c4:
                stage = h.get("overall_stage_ajcc8", h.get("stage", "—"))
                st.markdown(mc("AJCC8 Stage", str(stage)), unsafe_allow_html=True)

            # Eligibility badges
            badges = []
            if h.get("histology_analysis_eligible") is True:
                badges.append("Histology")
            if h.get("has_eligible_molecular") is True:
                badges.append("Molecular")
            if h.get("has_eligible_rai") is True:
                badges.append("RAI")
            if badges:
                badge_html = " ".join(
                    f'<span style="background:#2dd4bf20;color:#2dd4bf;padding:2px 6px;'
                    f'border-radius:3px;font-size:.68rem;margin-right:4px">{b}</span>'
                    for b in badges
                )
                st.markdown(f"Eligible: {badge_html}", unsafe_allow_html=True)

            # Source table info
            st.caption(f"Source: `{header_tbl}` | research_id = {rid}")
            st.markdown("<br>", unsafe_allow_html=True)

    # ── Timeline rows ──────────────────────────────────────────────────────
    src = enr_tbl or tl_tbl
    tl_df = sqdf(con, f"""
        SELECT
            entity_type,
            inferred_event_date,
            date_status,
            date_is_source_native_flag,
            date_is_inferred_flag,
            date_requires_manual_review_flag,
            evidence_span,
            entity_date,
            note_date
        FROM {src}
        WHERE research_id = {rid}
        ORDER BY inferred_event_date NULLS LAST, entity_type
    """)

    if tl_df.empty:
        st.info(f"No timeline entities found for research_id {rid}.", icon="📭")
    else:
        st.caption(f"{len(tl_df):,} entity rows · "
                   f"{tl_df['date_status'].value_counts().get('unresolved_date', 0)} unresolved")

        # Color-coded status column
        def _status_badge(s):
            color = _DATE_STATUS_COLORS.get(s, "#8892a4")
            label = _DATE_STATUS_LABELS.get(s, s)
            return f'<span style="background:{color}20;color:{color};padding:1px 6px;border-radius:3px;font-size:.7rem">{label}</span>'

        tl_show = tl_df.copy()
        tl_show["evidence_span"] = tl_show["evidence_span"].astype(str).str[:80]
        st.dataframe(
            tl_show,
            use_container_width=True,
            height=420,
            column_config={
                "inferred_event_date": st.column_config.DateColumn("Inferred Date"),
                "entity_type": st.column_config.TextColumn("Entity Type"),
                "date_status": st.column_config.TextColumn("Date Status"),
                "date_is_source_native_flag": st.column_config.CheckboxColumn("Source Date"),
                "date_is_inferred_flag": st.column_config.CheckboxColumn("Inferred"),
                "date_requires_manual_review_flag": st.column_config.CheckboxColumn("Needs Review"),
                "evidence_span": st.column_config.TextColumn("Evidence (truncated)", width="large"),
            },
        )
        st.markdown(sl("Export This Patient's Timeline"), unsafe_allow_html=True)
        multi_export(tl_df, f"timeline_pid{rid}", key_sfx=f"pte_{rid}")

    # ── Episode tables ─────────────────────────────────────────────────────
    with st.expander("Molecular Episodes", expanded=False):
        for tbl in ("molecular_test_episode_v2", "md_molecular_test_episode_v2"):
            if tbl_exists(con, tbl):
                mdf = sqdf(con, f"SELECT * FROM {tbl} WHERE research_id = {rid}")
                if not mdf.empty:
                    st.dataframe(mdf, use_container_width=True)
                    multi_export(mdf, f"molecular_pid{rid}", key_sfx=f"mol_{rid}")
                else:
                    st.caption("No molecular episodes.")
                break
        else:
            st.caption("molecular_test_episode_v2 not available.")

    with st.expander("RAI Episodes", expanded=False):
        for tbl in ("rai_treatment_episode_v2", "md_rai_treatment_episode_v2"):
            if tbl_exists(con, tbl):
                rdf = sqdf(con, f"SELECT * FROM {tbl} WHERE research_id = {rid}")
                if not rdf.empty:
                    st.dataframe(rdf, use_container_width=True)
                    multi_export(rdf, f"rai_pid{rid}", key_sfx=f"rai_{rid}")
                else:
                    st.caption("No RAI episodes.")
                break
        else:
            st.caption("rai_treatment_episode_v2 not available.")

    with st.expander("Operative Episodes", expanded=False):
        for tbl in ("operative_episode_detail_v2", "md_operative_episode_detail_v2"):
            if tbl_exists(con, tbl):
                odf = sqdf(con, f"SELECT * FROM {tbl} WHERE research_id = {rid}")
                if not odf.empty:
                    st.dataframe(odf, use_container_width=True)
                    multi_export(odf, f"operative_pid{rid}", key_sfx=f"op_{rid}")
                else:
                    st.caption("No operative episodes.")
                break
        else:
            st.caption("operative_episode_detail_v2 not available.")

    with st.expander("Imaging / Nodule Episodes", expanded=False):
        for tbl in ("imaging_nodule_master_v1", "md_imaging_nodule_master_v1",
                     "imaging_nodule_long_v2", "md_imaging_nodule_long_v2"):
            if tbl_exists(con, tbl):
                idf = sqdf(con, f"SELECT * FROM {tbl} WHERE research_id = {rid}")
                if not idf.empty:
                    st.dataframe(idf, use_container_width=True)
                    multi_export(idf, f"imaging_pid{rid}", key_sfx=f"img_{rid}")
                else:
                    st.caption("No imaging episodes.")
                break
        else:
            st.caption("imaging_nodule_master_v1 not available.")


# ── Publication Snapshot ──────────────────────────────────────────────────
def _render_publication_snapshot(con, enr_tbl: str | None):
    st.markdown(sl("Publication Snapshot — Full Provenanced Timeline"), unsafe_allow_html=True)
    st.markdown(
        "Exports **enriched_patient_timeline_v3_mv** + **qa_summary_by_domain_v2** "
        "to `exports/` with a provenance manifest. "
        "Only available when the enriched timeline view is materialized.",
    )

    if not enr_tbl:
        st.warning(
            "enriched_patient_timeline_v3_mv is not yet materialized. "
            "Run `scripts/20_enriched_patient_timeline_v3.sql` in MotherDuck first.",
            icon="⚠️",
        )
        return

    if not st.button("⬇️ Export Publication Snapshot", key="pub_snap_btn"):
        return

    with st.spinner("Fetching enriched timeline (may take a moment for large cohorts)…"):
        try:
            tl_df = sqdf(con, f"SELECT * FROM {enr_tbl}")
            qa_tbl = next(
                (n for n in ("qa_summary_by_domain_v2", "md_qa_summary_by_domain_v2")
                 if tbl_exists(con, n)),
                None,
            )
            qa_df = sqdf(con, f"SELECT * FROM {qa_tbl}") if qa_tbl else pd.DataFrame()

            ts = datetime.now().strftime("%Y%m%d_%H%M")
            out_dir = Path("exports") / f"pub_snapshot_{ts}"
            out_dir.mkdir(parents=True, exist_ok=True)

            tl_path = out_dir / "enriched_timeline.parquet"
            tl_df.to_parquet(tl_path, index=False)

            if not qa_df.empty:
                qa_path = out_dir / "qa_summary.parquet"
                qa_df.to_parquet(qa_path, index=False)

            manifest = {
                "generated_at": datetime.now().isoformat(),
                "enriched_timeline_rows": len(tl_df),
                "qa_summary_rows": len(qa_df),
                "source_view": enr_tbl,
                "qa_source": qa_tbl,
                "output_dir": str(out_dir),
            }
            (out_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2), encoding="utf-8"
            )

            st.success(
                f"Snapshot saved to `{out_dir}/` — "
                f"{len(tl_df):,} timeline rows, {len(qa_df):,} QA rows.",
                icon="✅",
            )

            # In-browser download (CSV for convenience)
            st.markdown("**In-browser download:**")
            multi_export(tl_df, "enriched_timeline", key_sfx="pub_tl")

        except Exception as e:
            st.error(f"Snapshot export failed: {e}", icon="❌")


# ── Main render function ──────────────────────────────────────────────────
def render_patient_timeline_explorer(con):
    st.markdown("## Patient Timeline Explorer")
    st.markdown(
        "Explore per-patient enriched timelines with full date provenance. "
        "Uses `timeline_rescue_v3_mv` (or v2 fallback) joined with episode tables."
    )

    tl_tbl = _timeline_tbl(con)
    enr_tbl = _enriched_tbl(con)

    if tl_tbl is None:
        st.error(
            "No timeline rescue view found (timeline_rescue_v3_mv / v2_mv). "
            "Run scripts 17 → 39 with --md first.",
            icon="🚫",
        )
        return

    # ── Rescue KPI banner ─────────────────────────────────────────────────
    _render_rescue_kpi(con)

    st.markdown(sl("Rescue Rate by Entity Type"), unsafe_allow_html=True)
    _render_rescue_by_type(con, tl_tbl)

    st.markdown("---")

    # ── Tabs within the explorer ──────────────────────────────────────────
    sub_tl, sub_snap = st.tabs(["🔍 Patient Lookup", "📤 Publication Snapshot"])

    with sub_tl:
        _render_patient_timeline(con, tl_tbl, enr_tbl)

    with sub_snap:
        _render_publication_snapshot(con, enr_tbl)

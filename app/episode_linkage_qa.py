"""
Episode Linkage QA — Multi-surgery integrity dashboard.

Surfaces the 7 audit tables from scripts/97_episode_linkage_audit.py:
  - val_episode_linkage_summary_v1       (KPIs)
  - val_episode_linkage_integrity_v1     (per-patient grades)
  - val_episode_mislink_candidates_v1    (suspect links)
  - val_episode_ambiguity_review_v1      (ambiguous artifacts)
  - val_episode_key_propagation_v1       (canonical-key fill)
  - val_episode_artifact_assignment_v1   (per-artifact detail)
  - multi_surgery_episode_cohort_v1      (cohort spine)
"""
from __future__ import annotations

import streamlit as st
import pandas as pd


# ── helpers ────────────────────────────────────────────────────────────────
def _q(con, sql: str, fallback=None):
    """Safe query — returns DataFrame or *fallback* on any error."""
    try:
        return con.execute(sql).fetchdf()
    except Exception:
        return fallback


def _resolve(con, canonical: str) -> str | None:
    """Try canonical name, then md_ prefix."""
    for name in (canonical, f"md_{canonical}"):
        try:
            con.execute(f"SELECT 1 FROM {name} LIMIT 0")
            return name
        except Exception:
            continue
    return None


def _metric_or_dash(df, col, fmt="d"):
    """Safely extract a scalar from a 1-row summary frame."""
    if df is None or df.empty or col not in df.columns:
        return "—"
    val = df[col].iloc[0]
    if pd.isna(val):
        return "—"
    if fmt == "d":
        return f"{int(val):,}"
    if fmt == ".1f":
        return f"{float(val):.1f}"
    if fmt == ".1%":
        return f"{float(val):.1%}"
    return str(val)


# ── grade colors ───────────────────────────────────────────────────────────
_GRADE_COLORS = {
    "GREEN": "🟢",
    "YELLOW": "🟡",
    "RED": "🔴",
    "REVIEW_REQUIRED": "🟠",
    "NO_ARTIFACTS": "⚪",
}


# ── main render ────────────────────────────────────────────────────────────
def render_episode_linkage_qa(con) -> None:
    """Streamlit entrypoint for the Episode Linkage QA tab."""

    st.header("Multi-Surgery Episode Linkage QA")
    st.caption(
        "Audit of cross-domain linkage integrity for patients with ≥2 surgeries. "
        "Source: `scripts/97_episode_linkage_audit.py`."
    )

    # ── resolve table names ────────────────────────────────────────────
    tbl_summary   = _resolve(con, "val_episode_linkage_summary_v1")
    tbl_integrity = _resolve(con, "val_episode_linkage_integrity_v1")
    tbl_mislink   = _resolve(con, "val_episode_mislink_candidates_v1")
    tbl_ambiguity = _resolve(con, "val_episode_ambiguity_review_v1")
    tbl_keyprop   = _resolve(con, "val_episode_key_propagation_v1")
    tbl_artifact  = _resolve(con, "val_episode_artifact_assignment_v1")
    tbl_cohort    = _resolve(con, "multi_surgery_episode_cohort_v1")

    if tbl_summary is None:
        st.warning(
            "Episode linkage audit tables not found.  \n"
            "Run: `.venv/bin/python scripts/97_episode_linkage_audit.py --env dev --md`"
        )
        return

    # ── KPI row ────────────────────────────────────────────────────────
    summary = _q(con, f"SELECT * FROM {tbl_summary}")
    if summary is not None and not summary.empty:
        # pivot metric_name → metric_value into a dict
        kpis: dict[str, str] = {}
        if {"metric_name", "metric_value"}.issubset(summary.columns):
            for _, r in summary.iterrows():
                kpis[str(r["metric_name"])] = r["metric_value"]
        else:
            # wide format fallback
            for c in summary.columns:
                kpis[c] = summary[c].iloc[0]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Multi-Surgery Patients", kpis.get("multi_surgery_patients", "—"))
        c2.metric("Total Episodes", kpis.get("total_episodes", "—"))
        c3.metric("Artifacts Assigned", kpis.get("total_artifacts", "—"))
        c4.metric("Mislink Candidates", kpis.get("mislink_candidates", "—"))
        c5.metric("Ambiguous Artifacts", kpis.get("ambiguous_artifacts", "—"))

        c6, c7, c8, c9 = st.columns(4)
        c6.metric("🟢 GREEN", kpis.get("grade_GREEN", "—"))
        c7.metric("🟡 YELLOW", kpis.get("grade_YELLOW", "—"))
        c8.metric("🔴 RED", kpis.get("grade_RED", "—"))
        c9.metric("🟠 REVIEW_REQUIRED", kpis.get("grade_REVIEW_REQUIRED", "—"))
    else:
        st.info("Summary KPI table is empty.")

    st.divider()

    # ── Integrity grade distribution ───────────────────────────────────
    st.subheader("Per-Patient Integrity Grades")
    if tbl_integrity:
        integrity = _q(con, f"SELECT * FROM {tbl_integrity} ORDER BY linkage_grade, research_id")
        if integrity is not None and not integrity.empty:
            # distribution chart
            grade_counts = (
                integrity["linkage_grade"]
                .value_counts()
                .reindex(["GREEN", "YELLOW", "RED", "REVIEW_REQUIRED", "NO_ARTIFACTS"], fill_value=0)
            )
            st.bar_chart(grade_counts, color="#4a90d9")

            # filterable table
            grade_filter = st.multiselect(
                "Filter by grade",
                options=list(grade_counts.index),
                default=["RED", "REVIEW_REQUIRED"],
                key="elq_grade_filter",
            )
            if grade_filter:
                subset = integrity[integrity["linkage_grade"].isin(grade_filter)]
            else:
                subset = integrity
            st.dataframe(subset, use_container_width=True, height=350)
        else:
            st.info("No integrity data.")
    else:
        st.info("Integrity table not found.")

    st.divider()

    # ── Mislink candidates ─────────────────────────────────────────────
    st.subheader("High-Risk Mislink Candidates")
    if tbl_mislink:
        mislinks = _q(con, f"SELECT * FROM {tbl_mislink} ORDER BY mislink_verdict DESC, research_id")
        if mislinks is not None and not mislinks.empty:
            verdict_counts = mislinks["mislink_verdict"].value_counts()
            cols = st.columns(len(verdict_counts))
            for i, (v, n) in enumerate(verdict_counts.items()):
                cols[i].metric(str(v).replace("_", " ").title(), int(n))

            with st.expander(f"All mislink candidates ({len(mislinks):,})", expanded=False):
                st.dataframe(mislinks, use_container_width=True, height=400)
        else:
            st.success("No mislink candidates detected.")
    else:
        st.info("Mislink table not found.")

    st.divider()

    # ── Ambiguity review ───────────────────────────────────────────────
    st.subheader("Ambiguous Artifact Assignments")
    if tbl_ambiguity:
        ambig = _q(con, f"""
            SELECT artifact_domain, assignment_confidence,
                   COUNT(*) AS n
            FROM {tbl_ambiguity}
            GROUP BY 1, 2
            ORDER BY 1, 3 DESC
        """)
        if ambig is not None and not ambig.empty:
            st.dataframe(ambig, use_container_width=True)

            # Patient-level drill-down
            with st.expander("Patient-level ambiguity detail", expanded=False):
                detail = _q(con, f"SELECT * FROM {tbl_ambiguity} ORDER BY research_id, artifact_domain LIMIT 2000")
                if detail is not None:
                    st.dataframe(detail, use_container_width=True, height=350)
        else:
            st.success("No ambiguous artifact assignments.")
    else:
        st.info("Ambiguity table not found.")

    st.divider()

    # ── Key propagation ────────────────────────────────────────────────
    st.subheader("Canonical Episode-Key Propagation")
    if tbl_keyprop:
        kp = _q(con, f"SELECT * FROM {tbl_keyprop}")
        if kp is not None and not kp.empty:
            st.dataframe(kp, use_container_width=True)
        else:
            st.info("Key propagation data empty.")
    else:
        st.info("Key propagation table not found.")

    st.divider()

    # ── Cohort & artifact detail (collapsed) ───────────────────────────
    with st.expander("Multi-Surgery Cohort Detail", expanded=False):
        if tbl_cohort:
            cohort = _q(con, f"SELECT * FROM {tbl_cohort} ORDER BY research_id, surgery_episode_id LIMIT 3000")
            if cohort is not None and not cohort.empty:
                st.dataframe(cohort, use_container_width=True, height=300)
            else:
                st.info("Cohort table empty.")
        else:
            st.info("Cohort table not found.")

    with st.expander("Artifact Assignment Detail (first 2 000 rows)", expanded=False):
        if tbl_artifact:
            arts = _q(con, f"SELECT * FROM {tbl_artifact} ORDER BY research_id, surgery_episode_id LIMIT 2000")
            if arts is not None and not arts.empty:
                st.dataframe(arts, use_container_width=True, height=300)
            else:
                st.info("Artifact table empty.")
        else:
            st.info("Artifact table not found.")

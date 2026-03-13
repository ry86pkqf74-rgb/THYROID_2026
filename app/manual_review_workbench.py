"""
Manual Review / Exceptions Workbench

Aggregates high-value review targets for efficient triage:
- Chronology true conflicts
- Extraction errors
- Unresolved linkage ambiguities
- Unresolved recurrence dates
- High-value review queue items
"""
from __future__ import annotations

import streamlit as st


def _safe_query(con, sql: str, fallback=None):
    try:
        return con.execute(sql).fetchdf()
    except Exception:
        return fallback


def render_manual_review_workbench(con) -> None:
    st.header("Manual Review & Exceptions Workbench")
    st.caption("High-priority items requiring human review, organized for efficient triage.")

    tab_conflict, tab_extract, tab_linkage, tab_recur, tab_queue = st.tabs([
        "Chronology Conflicts",
        "Extraction Errors",
        "Linkage Ambiguities",
        "Unresolved Recurrence",
        "Review Queue",
    ])

    with tab_conflict:
        st.subheader("True Chronology Conflicts")
        conflicts = _safe_query(con, """
            SELECT * FROM val_temporal_anomaly_resolution_v1
            WHERE resolution_bucket = 'true_conflict'
            ORDER BY research_id
        """)
        if conflicts is not None and len(conflicts) > 0:
            st.metric("True Conflicts", len(conflicts))
            st.dataframe(conflicts, use_container_width=True, height=400)
        else:
            st.success("No true chronology conflicts found.")

    with tab_extract:
        st.subheader("Source Extraction Errors")
        errors = _safe_query(con, """
            SELECT * FROM val_temporal_anomaly_resolution_v1
            WHERE resolution_bucket = 'source_extraction_error'
            ORDER BY research_id
        """)
        if errors is not None and len(errors) > 0:
            st.metric("Extraction Errors", len(errors))
            st.dataframe(errors, use_container_width=True, height=400)
        else:
            st.success("No extraction errors flagged.")

    with tab_linkage:
        st.subheader("Linkage Ambiguities (Multi-Candidate)")
        ambig = _safe_query(con, """
            SELECT * FROM linkage_ambiguity_review_v1
            ORDER BY n_candidates DESC, research_id
            LIMIT 500
        """)
        if ambig is not None and len(ambig) > 0:
            st.metric("Ambiguous Linkages", len(ambig))
            st.dataframe(ambig, use_container_width=True, height=400)
        else:
            st.info("No linkage ambiguity review data available.")

    with tab_recur:
        st.subheader("Unresolved Recurrence Dates")
        recur = _safe_query(con, """
            SELECT research_id, detection_category, recurrence_site_inferred,
                   recurrence_date_status, recurrence_date_confidence,
                   n_recurrence_sources, recurrence_data_confidence
            FROM extracted_recurrence_refined_v1
            WHERE recurrence_date_status = 'unresolved_date'
            ORDER BY recurrence_data_confidence DESC, research_id
            LIMIT 500
        """)
        if recur is not None and len(recur) > 0:
            st.metric("Unresolved Recurrence Dates", len(recur))
            st.dataframe(recur, use_container_width=True, height=400)
        else:
            st.info("No unresolved recurrence dates.")

    with tab_queue:
        st.subheader("High-Value Review Queue")
        queue = _safe_query(con, """
            SELECT * FROM unresolved_high_value_cases_v
            ORDER BY research_id
            LIMIT 500
        """)
        if queue is not None and len(queue) > 0:
            st.metric("High-Value Cases", len(queue))
            st.dataframe(queue, use_container_width=True, height=400)
        else:
            existing = _safe_query(con, """
                SELECT * FROM streamlit_patient_manual_review_v
                LIMIT 200
            """)
            if existing is not None and len(existing) > 0:
                st.metric("Manual Review Items", len(existing))
                st.dataframe(existing, use_container_width=True, height=400)
            else:
                st.info("No review queue items found.")

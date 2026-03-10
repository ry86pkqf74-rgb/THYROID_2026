"""RAI Treatment Analytics dashboard tab."""
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


def render_rai_dashboard(con) -> None:
    ep_view = _resolve_view(con, "rai_treatment_episode_v2", "md_rai_treatment_episode_v2")
    review_view = _resolve_view(con, "rai_adjudication_review_v2", "md_rai_adjudication_review_v2")

    if not ep_view:
        st.warning(
            "Required view `rai_treatment_episode_v2` is not available. "
            "Run the prerequisite deployment scripts first.",
            icon="⚠️",
        )
        return

    st.markdown(sl("RAI Treatment Analytics"), unsafe_allow_html=True)
    st.info("Showing pre-adjudication data. Adjudicated values are available in manuscript exports (script 20).", icon="ℹ️")

    # ------------------------------------------------------------------
    # 1. Top Metrics
    # ------------------------------------------------------------------
    total = sqs(con, f"SELECT COUNT(*) FROM {ep_view}")
    patients = sqs(con, f"SELECT COUNT(DISTINCT research_id) FROM {ep_view}")

    completed = sqs(con, f"""
        SELECT COUNT(*) FROM {ep_view}
        WHERE rai_assertion_status IN ('definite_received', 'likely_received')
    """)
    recommended = sqs(con, f"""
        SELECT COUNT(*) FROM {ep_view}
        WHERE rai_assertion_status = 'planned'
    """)
    uncertain = int(total) - int(completed) - int(recommended)

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(mc("Total Episodes", f"{int(total):,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Unique Patients", f"{int(patients):,}"), unsafe_allow_html=True)
    with c3:
        st.markdown(
            mc("Completed", f"{int(completed):,}", badge("received", "green")),
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            mc("Recommended", f"{int(recommended):,}", badge("planned", "sky")),
            unsafe_allow_html=True,
        )
    with c5:
        st.markdown(
            mc("Uncertain", f"{uncertain:,}", badge("ambiguous", "amber")),
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 2. Dose Distribution (histogram)
    # ------------------------------------------------------------------
    st.markdown(sl("Dose Distribution (Completed Treatments)"), unsafe_allow_html=True)

    dose_df = sqdf(con, f"""
        SELECT dose_mci
        FROM {ep_view}
        WHERE dose_mci IS NOT NULL
          AND dose_mci > 0
          AND rai_assertion_status IN ('definite_received', 'likely_received')
    """)

    if not dose_df.empty:
        fig_dose = go.Figure(go.Histogram(
            x=dose_df["dose_mci"],
            nbinsx=30,
            marker_color=COLORS["teal"],
            marker_line=dict(color=COLORS["border"], width=1),
        ))
        fig_dose.update_layout(
            **PL, height=380,
            title="RAI Dose Distribution (mCi)",
            xaxis_title="Dose (mCi)",
            yaxis_title="Count",
        )
        st.plotly_chart(fig_dose, use_container_width=True)
    else:
        st.info("No dose data available for completed treatments.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 3. Treatment Intent
    # ------------------------------------------------------------------
    st.markdown(sl("Treatment Intent"), unsafe_allow_html=True)

    intent_df = sqdf(con, f"""
        SELECT
            COALESCE(rai_intent, 'unknown') AS intent,
            COUNT(*) AS cnt
        FROM {ep_view}
        GROUP BY rai_intent
        ORDER BY cnt DESC
    """)

    if not intent_df.empty:
        fig_intent = go.Figure(go.Bar(
            x=intent_df["intent"],
            y=intent_df["cnt"],
            text=intent_df["cnt"].apply(lambda v: f"{v:,}"),
            textposition="outside",
            marker_color=PL["colorway"][:len(intent_df)],
        ))
        fig_intent.update_layout(**PL, height=360, title="RAI Treatment Intent Distribution")
        st.plotly_chart(fig_intent, use_container_width=True)
    else:
        st.info("No treatment intent data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 4. Assertion Status
    # ------------------------------------------------------------------
    st.markdown(sl("Assertion Status"), unsafe_allow_html=True)

    assert_df = sqdf(con, f"""
        SELECT
            COALESCE(rai_assertion_status, 'unknown') AS status,
            COUNT(*) AS cnt
        FROM {ep_view}
        GROUP BY rai_assertion_status
        ORDER BY cnt DESC
    """)

    if not assert_df.empty:
        status_colors = {
            "definite_received": COLORS["green"],
            "likely_received": COLORS["teal"],
            "planned": COLORS["sky"],
            "historical": COLORS["violet"],
            "negated": COLORS["rose"],
            "ambiguous": COLORS["amber"],
        }
        bar_colors = [status_colors.get(s, COLORS["text_lo"]) for s in assert_df["status"]]

        fig_assert = go.Figure(go.Bar(
            x=assert_df["status"],
            y=assert_df["cnt"],
            text=assert_df["cnt"].apply(lambda v: f"{v:,}"),
            textposition="outside",
            marker_color=bar_colors,
        ))
        fig_assert.update_layout(**PL, height=360, title="RAI Assertion Status Distribution")
        st.plotly_chart(fig_assert, use_container_width=True)
    else:
        st.info("No assertion status data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 5. Chronology vs Surgery
    # ------------------------------------------------------------------
    st.markdown(sl("Chronology vs Surgery"), unsafe_allow_html=True)

    before_surgery = sqs(con, f"""
        SELECT COUNT(*) FROM {ep_view}
        WHERE days_surgery_to_rai IS NOT NULL
          AND days_surgery_to_rai < 0
    """)
    median_days = sqs(con, f"""
        SELECT MEDIAN(days_surgery_to_rai) FROM {ep_view}
        WHERE days_surgery_to_rai IS NOT NULL
          AND days_surgery_to_rai >= 0
    """)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            mc("RAI Before Surgery", f"{int(before_surgery):,}",
               badge("chronology flag", "rose") if int(before_surgery) > 0 else None),
            unsafe_allow_html=True,
        )
    with c2:
        med_val = f"{float(median_days):.0f}" if median_days else "—"
        st.markdown(
            mc("Median Days to RAI", med_val, "from surgery (post-op only)"),
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # 6. Review Queue
    # ------------------------------------------------------------------
    if review_view:
        st.markdown(sl("RAI Adjudication Review Queue"), unsafe_allow_html=True)

        review_df = sqdf(con, f"""
            SELECT * FROM {review_view}
            WHERE severity IN ('warning', 'error')
            ORDER BY severity DESC, research_id
        """)

        if review_df.empty:
            st.success("No warnings or errors in the RAI adjudication review queue.")
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
                "Filter Severity", ["All", "error", "warning"], key="rai_dash_sev",
            )
            filtered = review_df if sev_filter == "All" else review_df[review_df["severity"] == sev_filter]

            st.markdown(f"Showing **{len(filtered):,}** of {len(review_df):,} issues")
            st.dataframe(filtered, use_container_width=True, hide_index=True)
            multi_export(filtered, "rai_adjudication_review", key_sfx="rai_dash_review")
    else:
        st.info("RAI adjudication review view not available.")

    # ------------------------------------------------------------------
    # Linkage Quality
    # ------------------------------------------------------------------
    link_view = _resolve_view(con, "pathology_rai_linkage_v2", "md_pathology_rai_linkage_v2")
    if link_view:
        st.markdown("### Linkage Quality")
        link_df = sqdf(con, f"""
            SELECT linkage_confidence, COUNT(*) AS cnt
            FROM {link_view}
            GROUP BY linkage_confidence
            ORDER BY CASE linkage_confidence
                WHEN 'high_confidence' THEN 1
                WHEN 'plausible' THEN 2 WHEN 'weak' THEN 3 ELSE 4
            END
        """)
        if link_df is not None and len(link_df) > 0:
            st.dataframe(link_df, use_container_width=True, hide_index=True)
            weak_ct = link_df.loc[link_df["linkage_confidence"] == "weak", "cnt"].sum() if "weak" in link_df["linkage_confidence"].values else 0
            if weak_ct > 0:
                st.warning(f"{int(weak_ct)} weak pathology-RAI linkages require manual review.", icon="⚠️")

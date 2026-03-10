"""
Shared helpers for adjudication UI tabs.

Re-exports query/UI helpers so tab modules can import from one place
without creating circular dependencies with dashboard.py.
"""
from __future__ import annotations

import io
from datetime import datetime

import pandas as pd
import streamlit as st

try:
    import openpyxl  # noqa: F401
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# ── Color tokens (match dashboard.py theme) ──────────────────────────────
COLORS = {
    "bg": "#07090f",
    "surface": "#0e1219",
    "surface2": "#141923",
    "border": "#1e2535",
    "teal": "#2dd4bf",
    "teal_dim": "#1a8a7a",
    "amber": "#f59e0b",
    "rose": "#f43f5e",
    "sky": "#38bdf8",
    "violet": "#a78bfa",
    "green": "#34d399",
    "text_hi": "#f0f4ff",
    "text_mid": "#8892a4",
    "text_lo": "#4a5568",
}

PL = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(14,18,25,0.8)",
    font=dict(family="DM Sans", color="#8892a4", size=12),
    title_font=dict(family="DM Serif Display", color="#f0f4ff", size=15),
    xaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", zerolinecolor="#1e2535"),
    yaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", zerolinecolor="#1e2535"),
    legend=dict(bgcolor="rgba(14,18,25,0.8)", bordercolor="#1e2535", borderwidth=1),
    margin=dict(l=16, r=16, t=36, b=16),
    colorway=["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#34d399", "#fb923c"],
    hoverlabel=dict(bgcolor="#141923", bordercolor="#1e2535", font_color="#f0f4ff"),
)


# ── Query helpers ────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def qdf(_con, sql: str) -> pd.DataFrame:
    return _con.execute(sql).fetchdf()


@st.cache_data(ttl=300, show_spinner=False)
def qs(_con, sql: str):
    r = _con.execute(sql).fetchone()
    return r[0] if r else 0


def sqdf(con, sql: str) -> pd.DataFrame:
    try:
        return qdf(con, sql)
    except Exception as e:
        st.warning(f"Query failed: {e}", icon="⚠️")
        return pd.DataFrame()


def sqs(con, sql: str):
    try:
        return qs(con, sql)
    except Exception:
        return 0


def tbl_exists(con, name: str) -> bool:
    try:
        return bool(sqs(con, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{name}'"))
    except Exception:
        return False


# ── UI helpers ───────────────────────────────────────────────────────────

def mc(label: str, value, delta=None) -> str:
    d = f'<div class="metric-delta">{delta}</div>' if delta else ""
    return (
        f'<div class="metric-card">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>{d}</div>'
    )


def sl(t: str) -> str:
    return f'<span class="section-label">{t}</span>'


def badge(text: str, color_key: str = "teal") -> str:
    """Inline badge for status indicators."""
    c = COLORS.get(color_key, COLORS["teal"])
    return (
        f'<span style="background:{c}20;color:{c};padding:2px 8px;'
        f'border-radius:4px;font-size:.75rem;font-weight:600;'
        f'font-family:var(--font-m,monospace)">{text}</span>'
    )


def multi_export(df: pd.DataFrame, prefix: str, key_sfx: str = "") -> None:
    """Render CSV + Excel + Parquet download buttons in a 3-column row."""
    ts = datetime.now().strftime("%Y%m%d")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("⬇ CSV", df.to_csv(index=False),
                           f"{prefix}_{ts}.csv", "text/csv",
                           key=f"csv_{key_sfx}")
    with c2:
        if HAS_OPENPYXL:
            buf = io.BytesIO()
            df.to_excel(buf, index=False, engine="openpyxl")
            st.download_button(
                "⬇ Excel", buf.getvalue(), f"{prefix}_{ts}.xlsx",
                "application/vnd.openxmlformats-officedocument"
                ".spreadsheetml.sheet", key=f"xlsx_{key_sfx}")
        else:
            st.caption("Install openpyxl for Excel export")
    with c3:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        st.download_button("⬇ Parquet", buf.getvalue(),
                           f"{prefix}_{ts}.parquet",
                           "application/octet-stream",
                           key=f"pq_{key_sfx}")


def require_view(con, view_name: str) -> bool:
    """Check that a required view exists; show diagnostic if missing."""
    if tbl_exists(con, view_name):
        return True
    st.error(
        f"Required view `{view_name}` is not available. "
        f"Run the prerequisite deployment scripts (15→16→17→18→19) first.",
        icon="🚫"
    )
    return False


def write_decision(rw_con, research_id: int, domain: str,
                   linked_episode_id: str | None, conflict_type: str | None,
                   unresolved_reason: str | None, reviewer_action: str,
                   resolution_status: str, final_value: str | None,
                   notes: str | None, reviewer_name: str,
                   source_view: str | None) -> bool:
    """Insert an adjudication decision and copy to history. Returns True on success."""
    try:
        # Deactivate prior decisions for same entity
        rw_con.execute("""
            UPDATE adjudication_decisions
            SET active_flag = FALSE
            WHERE research_id = ?
              AND review_domain = ?
              AND COALESCE(linked_episode_id, '') = COALESCE(?, '')
              AND active_flag = TRUE
        """, [research_id, domain, linked_episode_id])

        rw_con.execute("""
            INSERT INTO adjudication_decisions (
                research_id, review_domain, linked_episode_id,
                conflict_type, unresolved_reason, reviewer_action,
                reviewer_resolution_status, final_value_selected,
                final_value_notes, reviewer_name, source_view, active_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
        """, [research_id, domain, linked_episode_id,
              conflict_type, unresolved_reason, reviewer_action,
              resolution_status, final_value, notes, reviewer_name,
              source_view])

        # Copy to history
        rw_con.execute("""
            INSERT INTO adjudication_decision_history (
                decision_id, research_id, review_domain, linked_episode_id,
                conflict_type, unresolved_reason, reviewer_action,
                reviewer_resolution_status, final_value_selected,
                final_value_notes, reviewer_name, reviewed_at,
                source_view, active_flag
            )
            SELECT decision_id, research_id, review_domain, linked_episode_id,
                   conflict_type, unresolved_reason, reviewer_action,
                   reviewer_resolution_status, final_value_selected,
                   final_value_notes, reviewer_name, reviewed_at,
                   source_view, active_flag
            FROM adjudication_decisions
            WHERE research_id = ?
              AND review_domain = ?
              AND active_flag = TRUE
            ORDER BY decision_id DESC LIMIT 1
        """, [research_id, domain])

        return True
    except Exception as e:
        st.error(f"Failed to save decision: {e}", icon="❌")
        return False

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

SHARE_CATALOG = "thyroid_share"
DATABASE = "thyroid_research_2026"

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


def qual(table: str) -> str:
    """Return fully-qualified table name for the active MotherDuck catalog.

    After _get_con() issues USE <catalog>, most unqualified names resolve
    automatically.  This helper is available for edge cases that need
    explicit qualification.
    """
    try:
        cat = st.session_state.get("_motherduck_catalog", DATABASE)
    except Exception:
        cat = DATABASE
    return f"{cat}.{table}"


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
    """Robust multi-format export — handles nullable Int64/boolean, tz-aware dates, object columns, and empty dataframes."""
    if df is None or df.empty:
        st.info("No data to export")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    df_export = df.copy()

    # 1. Datetime columns (tz-aware from MotherDuck)
    datetime_cols = [col for col in df_export.columns if pd.api.types.is_datetime64_any_dtype(df_export[col])]
    for col in datetime_cols:
        if hasattr(df_export[col].dt, "tz_localize"):
            df_export[col] = df_export[col].dt.tz_localize(None)
        df_export[col] = pd.to_datetime(df_export[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S").replace("NaT", "")

    # 2. Safe handling for nullable types (Int64, boolean)
    for col in df_export.columns:
        if col in datetime_cols:
            continue
        dtype_str = str(df_export[col].dtype)
        if "boolean" in dtype_str or pd.api.types.is_bool_dtype(df_export[col]):
            df_export[col] = df_export[col].map({True: "Yes", False: "No", pd.NA: "", None: ""})
        elif pd.api.types.is_numeric_dtype(df_export[col]) and "Int" in dtype_str:
            df_export[col] = df_export[col].fillna(0) if any(k in col.lower() for k in ["count", "age", "year"]) else df_export[col]
        else:
            df_export[col] = df_export[col].astype(str).replace(["nan", "None", "<NA>", "NaT"], "")

    # Final safety net — ensure all remaining object columns are plain strings
    df_export = df_export.astype(str).replace("nan", "")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("⬇️ CSV", df_export.to_csv(index=False),
                           f"{prefix}_{ts}.csv", "text/csv",
                           key=f"csv_{key_sfx}")
    with c2:
        if HAS_OPENPYXL:
            buf = io.BytesIO()
            df_export.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button(
                "⬇️ Excel", buf.getvalue(), f"{prefix}_{ts}.xlsx",
                "application/vnd.openxmlformats-officedocument"
                ".spreadsheetml.sheet", key=f"xlsx_{key_sfx}")
        else:
            st.caption("Install openpyxl for Excel export")
    with c3:
        buf = io.BytesIO()
        df_export.to_parquet(buf, index=False)
        buf.seek(0)
        st.download_button("⬇️ Parquet", buf.getvalue(),
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


# ── Runtime status helpers ─────────────────────────────────────────────

def get_runtime_info() -> dict:
    """Collect runtime status from session state for display."""
    return {
        "version": st.session_state.get("_app_version", "unknown"),
        "catalog": st.session_state.get("_motherduck_catalog", DATABASE),
        "connection_mode": st.session_state.get("_connection_mode", "unknown"),
        "connection_detail": st.session_state.get("_connection_detail", ""),
        "is_ro_share": st.session_state.get("_connection_mode") == "ro_share",
        "is_fallback": st.session_state.get("_connection_mode") == "rw_fallback",
        "loaded_at": st.session_state.get("_loaded_at", ""),
    }


def render_runtime_status_panel() -> None:
    """Compact runtime status panel for diagnostics / sidebar."""
    info = get_runtime_info()
    mode = info["connection_mode"]
    mode_labels = {
        "ro_share": ("Read-Only Share", "green"),
        "rw_fallback": ("Read-Write (fallback)", "amber"),
        "rw_review": ("Read-Write (review)", "sky"),
        "local": ("Local DuckDB", "violet"),
        "unknown": ("Unknown", "text_mid"),
    }
    label, color = mode_labels.get(mode, mode_labels["unknown"])

    st.markdown(
        f'<div style="background:{COLORS["surface"]};border:1px solid {COLORS["border"]};'
        f'border-radius:10px;padding:0.7rem 1rem;margin-bottom:0.6rem">'
        f'<div style="font-family:var(--font-m,monospace);font-size:.58rem;'
        f'letter-spacing:.12em;text-transform:uppercase;color:{COLORS["text_mid"]};'
        f'margin-bottom:6px">RUNTIME STATUS</div>'
        f'<div style="display:flex;gap:1rem;flex-wrap:wrap;align-items:center">'
        f'<span style="font-size:.75rem;color:{COLORS["text_hi"]}">'
        f'{info["version"]}</span>'
        f'<span>{badge(label, color)}</span>'
        f'<span style="font-size:.7rem;color:{COLORS["text_mid"]}">'
        f'Catalog: <code style="color:{COLORS["teal"]}">{info["catalog"]}</code></span>'
        f'</div>'
        f'{"<div style=" + chr(34) + "font-size:.65rem;color:" + COLORS["text_lo"] + ";margin-top:4px" + chr(34) + ">" + info["loaded_at"] + "</div>" if info["loaded_at"] else ""}'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_fallback_warning() -> None:
    """Show a prominent warning if the app fell back from RO share to RW."""
    if st.session_state.get("_connection_mode") == "rw_fallback":
        st.warning(
            "**Read-only share unavailable** — connected via read-write fallback. "
            "Data is live but writes are possible. This may occur if the RO share "
            "URL has changed or the share is temporarily inaccessible. "
            "Check the Connection Help expander in the sidebar for details.",
            icon="⚡",
        )


def render_health_kpis(con) -> None:
    """Compact health KPI row for the Overview or sidebar."""
    checks = [
        ("val_dataset_integrity_summary_v1", "Integrity"),
        ("val_provenance_completeness_v2", "Provenance"),
        ("val_episode_linkage_completeness_v1", "Linkage"),
        ("val_lab_completeness_v1", "Lab Coverage"),
    ]
    cols = st.columns(len(checks))
    for i, (tbl, label) in enumerate(checks):
        exists = tbl_exists(con, tbl)
        with cols[i]:
            if exists:
                try:
                    n = sqs(con, f"SELECT COUNT(*) FROM {tbl}")
                    st.markdown(
                        mc(label, f"{n}", "available"),
                        unsafe_allow_html=True,
                    )
                except Exception:
                    st.markdown(mc(label, "err", "query failed"), unsafe_allow_html=True)
            else:
                st.markdown(mc(label, "—", "not deployed"), unsafe_allow_html=True)


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

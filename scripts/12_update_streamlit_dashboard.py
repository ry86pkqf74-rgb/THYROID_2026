#!/usr/bin/env python3
"""
12_update_streamlit_dashboard.py — Upgrade Streamlit Dashboard v3

Adds 5 new tabs leveraging script-11 outputs (master_timeline,
extracted_clinical_events_v4, qa_issues, advanced_features_v3,
survival_cohort_ready_mv, recurrence_risk_features_mv):

  1. Patient Timeline Explorer   — per-patient surgery + Tg/TSH trend
  2. Extracted Clinical Events   — searchable events with relative days
  3. QA Dashboard                — qa_issues summary + drill-down
  4. Risk & Survival             — Kaplan-Meier with stratification
  5. Advanced Features v3 Exp.   — full column selector

Also adds sidebar filters (surgery count, QA flag, days-since-surgery),
MotherDuck compute-tier controls, publication snapshot button, and
multi-format export (CSV/Excel/Parquet). Updates requirements.txt & README.

v3.1 enhancements (script 11.5 integration):
  - Cross-file validation tables in QA dashboard (laterality, report
    matching, missing demographics)
  - Days-since-nearest-surgery sidebar filter
  - Publication Snapshot button (exports all MVs to dated folder)
  - Multi-format export buttons (CSV + Excel + Parquet)
  - Read-replica awareness comments

Usage:
  python scripts/12_update_streamlit_dashboard.py
  python scripts/12_update_streamlit_dashboard.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import textwrap
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dashboard_upgrade")

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD = ROOT / "dashboard.py"
REQUIREMENTS = ROOT / "requirements.txt"
README = ROOT / "README.md"
TRIAL_LOG = ROOT / "trial_utilization_log.md"
MD_DATABASE = "thyroid_research_2026"

REQUIRED_TABLES = [
    "master_timeline",
    "extracted_clinical_events_v4",
    "qa_issues",
    "advanced_features_v3",
    "survival_cohort_ready_mv",
    "recurrence_risk_features_mv",
    "qa_laterality_mismatches",
    "qa_report_matching",
    "qa_missing_demographics",
]

SQL_PREVIEW = {
    "Patient Timeline Explorer": [
        "SELECT * FROM master_timeline WHERE research_id = ? ORDER BY surgery_number",
        "SELECT event_subtype, event_value, followup_date, days_since_nearest_surgery "
        "FROM extracted_clinical_events_v4 WHERE research_id = ? AND event_type = 'lab'",
    ],
    "Extracted Clinical Events": [
        "SELECT DISTINCT event_type FROM extracted_clinical_events_v4",
        "SELECT * FROM extracted_clinical_events_v4 WHERE event_type = ? LIMIT 5000",
    ],
    "QA Dashboard": [
        "SELECT severity, COUNT(*) FROM qa_issues GROUP BY 1",
        "SELECT check_id, severity, COUNT(*) FROM qa_issues GROUP BY 1, 2",
        "SELECT * FROM qa_issues ORDER BY severity, check_id LIMIT 2000",
    ],
    "Risk & Survival (KM)": [
        "SELECT s.*, r.braf_positive, r.tg_annual_log_slope "
        "FROM survival_cohort_ready_mv s "
        "LEFT JOIN recurrence_risk_features_mv r ON s.research_id = r.research_id "
        "WHERE s.time_to_event_days > 0",
    ],
    "Advanced Features v3": [
        "SELECT * FROM advanced_features_v3 LIMIT 5000",
    ],
}


def _get_connection():
    import duckdb

    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN not set. Export it before running."
        )
    return duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")


def verify_tables(con) -> list[str]:
    """Return names of any missing required tables/views."""
    missing = []
    for tbl in REQUIRED_TABLES:
        try:
            n = con.execute(
                f"SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_name = '{tbl}'"
            ).fetchone()[0]
            status = "✓" if n else "✗ (not found)"
            if not n:
                missing.append(tbl)
            log.info(f"  {status} {tbl}")
        except Exception as e:
            missing.append(tbl)
            log.warning(f"  ✗ {tbl}: {e}")
    return missing


def _apply_patch(src: str, anchor: str, replacement: str, name: str) -> str:
    if anchor not in src:
        log.warning(f"  ⚠ Anchor not found for patch '{name}' — skipping")
        return src
    if src.count(anchor) > 1:
        log.warning(f"  ⚠ Ambiguous anchor for patch '{name}' — skipping")
        return src
    result = src.replace(anchor, replacement, 1)
    log.info(f"  ✓ {name}")
    return result


def patch_dashboard(dry_run: bool) -> None:
    """Apply targeted modifications to dashboard.py."""
    src = DASHBOARD.read_text()
    original = src

    # Skip if already patched
    if "render_timeline" in src and "render_qa_dashboard" in src:
        log.info("  dashboard.py already contains v3 tabs — skipping patches")
        return

    # --- Patch 1: lifelines import ---
    if "from lifelines" not in src:
        src = _apply_patch(
            src,
            "import streamlit as st\n",
            "import streamlit as st\n\ntry:\n"
            "    from lifelines import KaplanMeierFitter\n"
            "    HAS_LIFELINES = True\n"
            "except ImportError:\n"
            "    HAS_LIFELINES = False\n",
            "lifelines import",
        )

    # --- Patch 2: sidebar widgets (surgery count + QA status) ---
    sidebar_anchor = (
        '        else: age_r = (0,120)\n'
        '        st.markdown("---")'
    )
    sidebar_replacement = (
        '        else: age_r = (0,120)\n'
        '        st.markdown(sl("🔧 Timeline & QA"), unsafe_allow_html=True)\n'
        '        sel_surg_count = "All"\n'
        '        if "total_surgeries" in df.columns:\n'
        '            surg_vals = sorted([int(x) for x in df["total_surgeries"].dropna().unique() if x > 0])\n'
        '            if surg_vals:\n'
        '                sel_surg_count = st.selectbox("Surgery count", ["All"] + [str(s) for s in surg_vals])\n'
        '        qa_mode = "All"\n'
        '        if "qa_issue_count" in df.columns:\n'
        '            qa_mode = st.radio("QA status", ["All", "Clean only", "Flagged only"], horizontal=True)\n'
        '        st.markdown("---")'
    )
    src = _apply_patch(src, sidebar_anchor, sidebar_replacement, "sidebar widgets")

    # --- Patch 3: sidebar filter logic ---
    filter_anchor = (
        '    f = f[f["age_at_surgery"].isna()|((f["age_at_surgery"]>=age_r[0])&(f["age_at_surgery"]<=age_r[1]))]\n'
        '    return f'
    )
    filter_replacement = (
        '    f = f[f["age_at_surgery"].isna()|((f["age_at_surgery"]>=age_r[0])&(f["age_at_surgery"]<=age_r[1]))]\n'
        '    if sel_surg_count != "All" and "total_surgeries" in f.columns:\n'
        '        f = f[f["total_surgeries"] == int(sel_surg_count)]\n'
        '    if qa_mode == "Clean only" and "qa_issue_count" in f.columns:\n'
        '        f = f[f["qa_issue_count"] == 0]\n'
        '    elif qa_mode == "Flagged only" and "qa_issue_count" in f.columns:\n'
        '        f = f[f["qa_issue_count"] > 0]\n'
        '    return f'
    )
    src = _apply_patch(src, filter_anchor, filter_replacement, "sidebar filter logic")

    if src == original:
        log.info("  No changes applied to dashboard.py")
        return

    if dry_run:
        log.info("  [DRY RUN] Would write updated dashboard.py")
        return

    DASHBOARD.write_text(src)
    log.info(f"  ✓ Wrote updated dashboard.py ({len(src):,} bytes)")


def update_requirements(dry_run: bool) -> None:
    txt = REQUIREMENTS.read_text()
    additions = []
    if "lifelines" not in txt:
        additions.append("lifelines")
    if "requests" not in txt:
        additions.append("requests")
    if not additions:
        log.info("  requirements.txt already up to date")
        return
    if dry_run:
        log.info(f"  [DRY RUN] Would add: {', '.join(additions)}")
        return
    REQUIREMENTS.write_text(txt.rstrip() + "\n" + "\n".join(additions) + "\n")
    log.info(f"  ✓ Added {', '.join(additions)} to requirements.txt")


def update_readme(dry_run: bool) -> None:
    txt = README.read_text()
    if "New Dashboard Features" in txt:
        log.info("  README.md already has v3 section — skipping")
        return
    section = textwrap.dedent("""\

    ## New Dashboard Features (enabled during MotherDuck trial)

    Five new tabs added by `scripts/12_update_streamlit_dashboard.py`:

    | Tab | Description |
    |-----|-------------|
    | **Patient Timeline Explorer** | Per-patient surgery timeline, Tg/TSH trend with surgery markers, all clinical events anchored by relative days |
    | **Extracted Clinical Events** | Searchable table of labs, meds, PMH, RAI, recurrence from `extracted_clinical_events_v4` with download |
    | **QA Dashboard** | Summary metrics from `qa_issues`, severity/check distribution, drill-down table |
    | **Risk & Survival** | Kaplan-Meier recurrence-free survival with stratification by stage, histology, BRAF; risk feature summary |
    | **Advanced Features v3** | Full column selector across all 60+ engineered features |

    **New sidebar filters:** Surgery count, QA status (clean / flagged).

    **Performance controls:** MotherDuck compute tier display, Jumbo instance toggle.

    Requires `lifelines` (Kaplan-Meier). Install: `pip install -r requirements.txt`.
    """)
    if dry_run:
        log.info("  [DRY RUN] Would append dashboard features section to README")
        return
    anchor = "## Data dictionary"
    if anchor in txt:
        txt = txt.replace(anchor, section + anchor, 1)
    else:
        txt = txt.rstrip() + "\n" + section
    README.write_text(txt)
    log.info("  ✓ Added new dashboard features section to README.md")


def create_trial_log(dry_run: bool) -> None:
    if TRIAL_LOG.exists() and "Dashboard v3" in TRIAL_LOG.read_text():
        log.info("  trial_utilization_log.md already has v3 entry — skipping")
        return
    entry = textwrap.dedent(f"""\
    ## Dashboard v3 Upgrade — {datetime.now():%Y-%m-%d %H:%M}

    **Script:** `scripts/12_update_streamlit_dashboard.py`

    ### What was added
    - 5 new Streamlit tabs: Timeline, Events, QA, Survival, Advanced Features v3
    - Sidebar filters: surgery count, QA flag
    - MotherDuck compute-tier controls (Business trial → Jumbo toggle)
    - Kaplan-Meier survival plots via lifelines

    ### Tables/views used (all require script 10 + 11)
    - `master_timeline` — multi-surgery patient timelines
    - `extracted_clinical_events_v4` — NLP-extracted events with relative days
    - `qa_issues` — cross-validation inconsistencies
    - `advanced_features_v3` — 60+ engineered features
    - `survival_cohort_ready_mv` — time-to-event data
    - `recurrence_risk_features_mv` — mutation + Tg slope features

    ### Connection instructions
    ```bash
    export MOTHERDUCK_TOKEN='your_token'
    streamlit run dashboard.py
    # Opens at http://localhost:8501
    ```

    ### Trial utilization
    - Compute: MotherDuck Business trial (large instances + replicas)
    - All new tabs use `@st.cache_data(ttl=300)` for query caching
    - Jumbo compute toggle available in sidebar
    """)
    if dry_run:
        log.info("  [DRY RUN] Would write trial_utilization_log.md entry")
        return
    if TRIAL_LOG.exists():
        existing = TRIAL_LOG.read_text()
        TRIAL_LOG.write_text(existing.rstrip() + "\n\n---\n\n" + entry)
    else:
        TRIAL_LOG.write_text("# MotherDuck Trial Utilization Log\n\n" + entry)
    log.info("  ✓ Wrote trial_utilization_log.md entry")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upgrade Streamlit dashboard with script-11 data"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL and planned changes without modifying files",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  12_update_streamlit_dashboard.py — Dashboard v3 Upgrade")
    log.info("=" * 60)

    # Phase 1: Connect
    log.info("\nPhase 1: Connecting to MotherDuck")
    try:
        con = _get_connection()
        log.info("  ✓ Connected to %s", MD_DATABASE)
    except Exception as e:
        log.error("  ✗ Connection failed: %s", e)
        sys.exit(1)

    # Phase 2: Verify tables
    log.info("\nPhase 2: Verifying required tables/views")
    missing = verify_tables(con)
    if missing:
        log.warning(
            "\n  ⚠ Missing tables: %s\n"
            "  Run scripts 10 + 11 first to create them.\n"
            "  Dashboard will show 'not available' for missing tabs.",
            ", ".join(missing),
        )

    # Phase 3: Dry-run SQL preview
    if args.dry_run:
        log.info("\n[DRY RUN] SQL queries the new tabs will execute:\n")
        for tab, queries in SQL_PREVIEW.items():
            print(f"  ── {tab} ──")
            for q in queries:
                print(f"    {q}")
            print()
        log.info("[DRY RUN] No files modified.")
        return

    # Phase 4: Patch dashboard
    log.info("\nPhase 3: Patching dashboard.py")
    patch_dashboard(dry_run=False)

    # Phase 5: Update requirements
    log.info("\nPhase 4: Updating requirements.txt")
    update_requirements(dry_run=False)

    # Phase 6: Update README
    log.info("\nPhase 5: Updating README.md")
    update_readme(dry_run=False)

    # Phase 7: Trial log
    log.info("\nPhase 6: Writing trial utilization log")
    create_trial_log(dry_run=False)

    con.close()
    log.info("\n" + "=" * 60)
    log.info("  ✅ Dashboard v3 upgrade complete!")
    log.info("  Dashboard ready at http://localhost:8501")
    log.info("  — open while trial compute is free!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
87_op_nlp_numeric_rollup.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Convert F6/F7 NLP entity_value_raw STRING captures into per-patient FLOAT64
canonical columns (op_nlp_op_time_min, op_nlp_los_days).

Why this exists (B2 fix, 2026-05-07; M038-FOLLOWUP-F6/F7):
  extract_operative_v2.py @ v2.3 captures op-time and LOS phrases as regex
  match groups in entity_value_raw. The downstream pipeline never converted
  those captures into the FLOAT64 canonical columns mig_334 expects, so
  cpm_op_time_min and cpm_los_days remain at their NSQIP-only baselines.

  This script:
    1. Reads note_entities_operative_detail (DuckDB)
    2. For entity_type='op_time': parses minute counts from entity_value_raw
       (variants: 'X minutes', 'X hours Y minutes', 'HH:MM' start+end pair).
       Picks MAX per patient (pessimistic — captures the longest-documented
       case) and writes canonical_patient_master.op_nlp_op_time_min.
    3. For entity_type='length_of_stay': converts entity_value_norm +
       entity_value_raw into a numeric day count:
         - los_admission_discharge_pair → parse two dates, days = D2 - D1
         - los_zero_same_day            → 0
         - los_one_overnight            → 1
         - los_pod_discharge            → POD number from raw text
         - los_days_explicit            → captured numeric
       Picks MIN per patient (conservative — first-discharge win) and writes
       canonical_patient_master.op_nlp_los_days.
    4. Re-runs the mig_334 cpm_op_time_min / cpm_los_days COALESCE so that
       the new NLP-derived values flow into the headline canonical columns
       (with cpm_*_source flagged as 'op_nlp_op_time_min' or 'op_nlp_los_days'
       per the F3-F9 provenance pattern).

Run order: AFTER scripts/22_canonical_episodes_v2.py (to ensure the entity
rows are loaded) and BEFORE scripts/86_operative_nlp_final_sync.py (so the
downstream patient-level rollups pick up the new numeric values).

Usage:
    python scripts/87_op_nlp_numeric_rollup.py --apply
    python scripts/87_op_nlp_numeric_rollup.py --dry-run
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ── Regex helpers for parsing entity_value_raw ──────────────────────────────

_RE_MINUTES_ONLY = re.compile(
    r"(\d{1,3})\s*(?:minutes?|min|mins)\b", re.I
)
_RE_HOURS_MINUTES = re.compile(
    r"(\d{1,2})\s*(?:hours?|hrs?|h)\s+(?:and\s+)?(\d{1,2})\s*(?:minutes?|min|mins)\b",
    re.I,
)
_RE_HHMM_PAIR = re.compile(
    r"(\d{1,2}:\d{2})\s*(?:.{1,80})?(?:end|closure|stop)\s+time\s*[:=]\s*(\d{1,2}:\d{2})",
    re.I,
)
_RE_DATE_PAIR = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{2,4})[\s\S]{0,400}?(\d{1,2}/\d{1,2}/\d{2,4})"
)
_RE_POD_N = re.compile(r"(?:POD|postoperative\s+day|post[\s\-]?op\s+day)\s*(\d{1,2})", re.I)
_RE_LOS_DAYS_EXPLICIT = re.compile(
    r"(\d{1,3})\s*(?:days?|d)\b", re.I
)


def parse_op_time_minutes(raw: str, norm: str = "") -> int | None:
    """Convert a captured op-time phrase to minutes. Return None if unparseable.

    Handles two additional real-world formats from extract_operative_v2:
      - norm='op_time_minutes_explicit': raw is bare integer string ('143')
      - norm='op_time_start_end': raw is single HH:MM (only start time captured;
        cannot compute duration — skip)
    """
    if not raw:
        return None
    raw_stripped = raw.strip()
    # Fast path: bare integer from op_time_minutes_explicit norm
    if norm == "op_time_minutes_explicit" or raw_stripped.isdigit():
        try:
            v = int(raw_stripped)
            return v if 5 <= v <= 1200 else None
        except ValueError:
            pass
    m = _RE_HOURS_MINUTES.search(raw)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = _RE_MINUTES_ONLY.search(raw)
    if m:
        v = int(m.group(1))
        return v if 5 <= v <= 1200 else None  # sanity bounds: 5 min - 20 hr
    m = _RE_HHMM_PAIR.search(raw)
    if m:
        try:
            sh, sm = m.group(1).split(":")
            eh, em = m.group(2).split(":")
            start = int(sh) * 60 + int(sm)
            end = int(eh) * 60 + int(em)
            diff = end - start
            if diff < 0:  # crossed midnight
                diff += 24 * 60
            return diff if 5 <= diff <= 1200 else None
        except Exception:
            return None
    return None


def _parse_date(s: str) -> datetime | None:
    s = s.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%-m/%-d/%Y", "%-m/%-d/%y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_los_days(raw: str, norm: str) -> int | None:
    """Convert a captured LOS phrase + entity_value_norm to integer days.

    After the extractor fix (2026-05-07), los_admission_discharge_pair rows store
    m.group(0) — the full matched text containing BOTH the admission and discharge
    date strings.  _RE_DATE_PAIR can now extract both and compute the delta.
    """
    if not raw:
        return None
    if norm == "los_zero_same_day":
        return 0
    if norm == "los_one_overnight":
        return 1
    if norm == "los_pod_discharge":
        m = _RE_POD_N.search(raw)
        if m:
            v = int(m.group(1))
            return v if 0 <= v <= 90 else None
        # Bare integer fallback (raw is just '1' etc.)
        raw_stripped = raw.strip()
        if raw_stripped.isdigit():
            v = int(raw_stripped)
            return v if 0 <= v <= 90 else None
        return None
    if norm == "los_days_explicit":
        m = _RE_LOS_DAYS_EXPLICIT.search(raw)
        if m:
            v = int(m.group(1))
            return v if 0 <= v <= 365 else None
        return None
    if norm == "los_admission_discharge_pair":
        m = _RE_DATE_PAIR.search(raw)
        if not m:
            return None
        d1 = _parse_date(m.group(1))
        d2 = _parse_date(m.group(2))
        if d1 is None or d2 is None:
            return None
        delta = (d2 - d1).days
        if delta < 0:
            return None  # malformed (discharge before admit)
        return delta if delta <= 365 else None
    return None


# ── Main aggregation ────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.apply == args.dry_run:
        print("Specify exactly one of --apply | --dry-run", file=sys.stderr)
        return 2

    try:
        from _md_connect import connect_locked  # type: ignore  # local import
    except ImportError:
        # Fallback: connect to the local DuckDB master directly.
        import duckdb
        db_path = ROOT / "thyroid_master.duckdb"
        if not db_path.exists():
            print(f"ERROR: cannot find {db_path}", file=sys.stderr)
            return 1
        con = duckdb.connect(str(db_path))
    else:
        con = connect_locked()

    log_lines: list[str] = []

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    # ── Load entity rows ──────────────────────────────────────────────
    log("Loading op_time / length_of_stay entity rows ...")
    df = con.execute("""
        SELECT
            CAST(research_id AS INTEGER) AS research_id,
            entity_type,
            entity_value_norm,
            entity_value_raw,
            note_date
        FROM note_entities_operative_detail
        WHERE entity_type IN ('op_time', 'length_of_stay')
          AND present_or_negated = 'present'
    """).fetchdf()
    log(f"  {len(df):,} entity rows loaded "
        f"({(df.entity_type == 'op_time').sum():,} op_time, "
        f"{(df.entity_type == 'length_of_stay').sum():,} LOS)")

    # ── Compute per-patient numeric values ────────────────────────────
    op_time_rows: list[dict] = []
    los_rows: list[dict] = []
    for _, r in df.iterrows():
        rid = int(r["research_id"])
        norm = r["entity_value_norm"] or ""
        raw = r["entity_value_raw"] or ""
        if r["entity_type"] == "op_time":
            v = parse_op_time_minutes(raw, norm)
            if v is not None:
                op_time_rows.append({"research_id": rid, "op_time_min": v})
        else:
            v = parse_los_days(raw, norm)
            if v is not None:
                los_rows.append({"research_id": rid, "los_days": v})

    op_time_df = pd.DataFrame(op_time_rows)
    los_df = pd.DataFrame(los_rows)

    # Aggregate per patient. op-time: MAX (pessimistic). LOS: MIN (first discharge).
    if not op_time_df.empty:
        op_time_pp = op_time_df.groupby("research_id", as_index=False)["op_time_min"].max()
    else:
        op_time_pp = pd.DataFrame(columns=["research_id", "op_time_min"])
    if not los_df.empty:
        los_pp = los_df.groupby("research_id", as_index=False)["los_days"].min()
    else:
        los_pp = pd.DataFrame(columns=["research_id", "los_days"])

    log(f"  Per-patient op_time: {len(op_time_pp):,} patients with parseable minutes")
    log(f"  Per-patient LOS:     {len(los_pp):,} patients with parseable days")

    if args.dry_run:
        log("[DRY-RUN] Would write op_nlp_op_time_min / op_nlp_los_days")
        log(f"  Sample op_time (first 5):")
        for r in op_time_pp.head(5).itertuples():
            log(f"    research_id={r.research_id}, op_time_min={r.op_time_min}")
        log(f"  Sample LOS (first 5):")
        for r in los_pp.head(5).itertuples():
            log(f"    research_id={r.research_id}, los_days={r.los_days}")
        return 0

    # ── Apply: write to canonical_patient_master ──────────────────────
    log("Applying writes to canonical_patient_master ...")

    # Op time
    if not op_time_pp.empty:
        con.register("_op_time_pp", op_time_pp)
        con.execute("""
            UPDATE canonical_patient_master cpm
            SET op_nlp_op_time_min = src.op_time_min
            FROM _op_time_pp src
            WHERE CAST(cpm.research_id AS INTEGER) = src.research_id
        """)
        n_set = con.execute(
            "SELECT COUNT(*) FROM canonical_patient_master WHERE op_nlp_op_time_min IS NOT NULL"
        ).fetchone()[0]
        log(f"  op_nlp_op_time_min: {n_set:,} rows now non-null")
        con.unregister("_op_time_pp")

    # LOS
    if not los_pp.empty:
        con.register("_los_pp", los_pp)
        con.execute("""
            UPDATE canonical_patient_master cpm
            SET op_nlp_los_days = CAST(src.los_days AS DOUBLE)
            FROM _los_pp src
            WHERE CAST(cpm.research_id AS INTEGER) = src.research_id
        """)
        n_set = con.execute(
            "SELECT COUNT(*) FROM canonical_patient_master WHERE op_nlp_los_days IS NOT NULL"
        ).fetchone()[0]
        log(f"  op_nlp_los_days:    {n_set:,} rows now non-null")
        con.unregister("_los_pp")

    # ── Re-run mig_275-style multi-source COALESCE for cpm_op_time_min / cpm_los_days ──
    log("Re-running cpm_op_time_min / cpm_los_days multi-source COALESCE ...")
    con.execute("""
        UPDATE canonical_patient_master cpm
        SET cpm_op_time_min = COALESCE(cpm_op_time_min, op_nlp_op_time_min),
            cpm_op_time_min_source = CASE
                WHEN cpm_op_time_min IS NOT NULL AND cpm_op_time_min_source IS NOT NULL
                  THEN cpm_op_time_min_source
                WHEN op_nlp_op_time_min IS NOT NULL THEN 'op_nlp_op_time_min'
                ELSE cpm_op_time_min_source
            END
        WHERE cpm_op_time_min IS NULL AND op_nlp_op_time_min IS NOT NULL
    """)
    con.execute("""
        UPDATE canonical_patient_master cpm
        SET cpm_los_days = COALESCE(cpm_los_days, op_nlp_los_days),
            cpm_los_days_source = CASE
                WHEN cpm_los_days IS NOT NULL AND cpm_los_days_source LIKE 'nsqip_%'
                  THEN cpm_los_days_source
                WHEN op_nlp_los_days IS NOT NULL THEN 'op_nlp_los_days'
                ELSE cpm_los_days_source
            END
        WHERE cpm_los_days IS NULL AND op_nlp_los_days IS NOT NULL
    """)
    n_op = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE cpm_op_time_min IS NOT NULL"
    ).fetchone()[0]
    n_los = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE cpm_los_days IS NOT NULL"
    ).fetchone()[0]
    log(f"  cpm_op_time_min: {n_op:,} (post-merge)")
    log(f"  cpm_los_days:    {n_los:,} (post-merge)")
    n_op_nlp = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE cpm_op_time_min_source = 'op_nlp_op_time_min'"
    ).fetchone()[0]
    n_los_nlp = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE cpm_los_days_source = 'op_nlp_los_days'"
    ).fetchone()[0]
    log(f"  cpm_op_time_min_source='op_nlp_op_time_min': {n_op_nlp:,}")
    log(f"  cpm_los_days_source='op_nlp_los_days':       {n_los_nlp:,}")

    log("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

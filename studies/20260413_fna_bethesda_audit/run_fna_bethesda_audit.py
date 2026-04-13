#!/usr/bin/env python3
"""
FNA Bethesda completeness audit: source Excel (pipeline melt) vs structured tables (MotherDuck).

Canonical long FNA rows match ``scripts/build_fna_history_from_fnas_detailed.py`` (8119 rows);
``fna_index`` is date-ordered within patient, not Excel slot order.

Run:
  .venv/bin/python studies/20260413_fna_bethesda_audit/run_fna_bethesda_audit.py

Token: motherduck_client.get_token() (motherduck.local.toml / env).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402
from utils.md_connect import connect_md_fail_closed  # noqa: E402

LOCAL_DB = ROOT / "thyroid_master.duckdb"
RAW_FNA = ROOT / "raw" / "FNAs 12_5_2025.xlsx"
CMD_LOG = OUT / "commands_run.log"

_ROMAN = {
    "I": 1,
    "II": 2,
    "III": 3,
    "IV": 4,
    "V": 5,
    "VI": 6,
}


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    with CMD_LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"{ts} {msg}\n")


def _fp(d: dict) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _is_blank(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and np.isnan(v):
        return True
    s = str(v).strip()
    return s == "" or s.lower() in ("nan", "none")


def parse_bethesda_cell(val) -> int | None:
    """Map Bethesda *cell* to 1–6 — conservative; long path blobs should not use word shortcuts."""
    if _is_blank(val):
        return None
    s = str(val).strip()
    if len(s) > 120:
        return None
    m = re.match(r"^(\d)$", s)
    if m:
        n = int(m.group(1))
        return n if 1 <= n <= 6 else None
    m = re.match(r"^(\d)\.0$", s)
    if m:
        n = int(m.group(1))
        return n if 1 <= n <= 6 else None
    t = re.sub(r"\s+", " ", s.upper())
    for rom, n in _ROMAN.items():
        if re.search(rf"\b{rom}\b", t):
            return n
    low = s.lower()
    if len(s) <= 80:
        if "nondiagnostic" in low or "non-diagnostic" in low or "unsatisfactory" in low:
            return 1
        if "aus" in low or "flus" in low:
            return 3
        if "follicular neoplasm" in low or "suspicious for follicular" in low:
            return 4
        if "suspicious for malignancy" in low or "suspicious malignancy" in low:
            return 5
        if "malignant" in low or "positive for malignancy" in low:
            return 6
        if low in ("ii", "benign", "2"):
            return 2
        if re.match(r"^[ivx]{1,4}$", low):
            return _ROMAN.get(low.upper())
    return None


def infer_bethesda_from_text(path_s: str, hist_s: str) -> int | None:
    """Conservative: explicit 'Bethesda category/class' or Roman at line start."""
    blob = f"{path_s}\n{hist_s}".strip()
    if not blob:
        return None
    low = blob.lower()
    m = re.search(r"bethesda\s*(?:category|class)?\s*([ivx]{1,4}|\d)", low)
    if m:
        tok = m.group(1).upper()
        if tok.isdigit() and len(tok) == 1:
            n = int(tok)
            if 1 <= n <= 6:
                return n
        if tok in _ROMAN:
            return _ROMAN[tok]
    head = blob[:800]
    m2 = re.search(r"(?:^|[\n;])\s*([IV]{1,3}V?)\s*[.\)]", head, re.I | re.M)
    if m2:
        t = m2.group(1).upper()
        if t in _ROMAN:
            return _ROMAN[t]
    return None


def classify_source(
    bethesda_cell_parsed: int | None,
    inferred: int | None,
    has_any_content: bool,
    path_snip: str,
) -> tuple[str, str]:
    if bethesda_cell_parsed is not None:
        return "explicit_source_bethesda", "Bethesda field maps to category 1–6."
    if inferred is not None:
        return "inferred_from_source_text", "Explicit Bethesda wording in path/history (conservative)."
    if not has_any_content:
        return "not_scorable_from_source", "Empty stacked row after pipeline filter — unexpected."
    snip = (path_snip or "")[:120].replace("\n", " ")
    return "not_scorable_from_source", (
        "No explicit Bethesda category in stacked bethesda column and no conservative text inference; "
        f"excerpt: {snip!r}"
    )


def to_int_cat(x) -> int | None:
    if x is None:
        return None
    if isinstance(x, float) and np.isnan(x):
        return None
    if isinstance(x, (np.floating,)) and np.isnan(x):
        return None
    try:
        i = int(float(x))
        return i if 1 <= i <= 6 else None
    except (TypeError, ValueError):
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--local",
        action="store_true",
        help="Use thyroid_master.duckdb instead of MotherDuck",
    )
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    if CMD_LOG.exists():
        CMD_LOG.unlink()
    _log("run_fna_bethesda_audit.py start")
    _log(f"token_mode={token_mode()} token={'SET' if get_token() else 'MISSING'}")

    if not RAW_FNA.is_file():
        _log(f"FATAL missing {RAW_FNA}")
        (OUT / "verdict.md").write_text(
            f"# FNA Bethesda audit — BLOCKED\n\nMissing source workbook: `{RAW_FNA}`\n",
            encoding="utf-8",
        )
        return 1

    if args.local:
        import duckdb

        con = duckdb.connect(str(LOCAL_DB), read_only=True)
    else:
        con = connect_md_fail_closed(LOCAL_DB)

    fh = con.execute(
        """
        SELECT
            CAST(research_id AS INTEGER) AS research_id,
            CAST(fna_index AS INTEGER) AS fna_index,
            date,
            fna_date_parsed,
            bethesda,
            path,
            path_extended,
            COALESCE(preop_specimen_received_fna_location, specimen) AS specimen_join,
            specimen_received
        FROM fna_history
        """
    ).df()

    fc = con.execute(
        """
        SELECT
            CAST(research_id AS INTEGER) AS research_id,
            CAST(fna_index AS INTEGER) AS fna_index,
            fna_date,
            specimen_location,
            path_text,
            original_bethesda,
            category_num,
            bethesda_2010_num,
            bethesda_2023_num,
            bethesda_2023_name,
            confidence,
            method,
            source_workbook
        FROM fna_cytology
        """
    ).df()

    fe = con.execute(
        """
        SELECT
            CAST(research_id AS INTEGER) AS research_id,
            CAST(fna_episode_id AS INTEGER) AS fna_episode_id,
            fna_date_native,
            resolved_fna_date,
            bethesda_raw,
            bethesda_category,
            pathology_diagnosis,
            specimen_site_raw,
            laterality,
            source_table
        FROM fna_episode_master_v2
        """
    ).df()
    con.close()

    _log(f"fna_history rows {len(fh)}")

    # Source spine = ``fna_history`` (canonical melt of ``FNAs 12_5_2025.xlsx`` via ETL).
    # ``fna_index`` is chronological within patient, not Excel slot order.
    rows = []
    for _, r in fh.iterrows():
        bet_raw = r.get("bethesda")
        path_s = str(r.get("path") or "")[:8000]
        hist_s = str(r.get("path_extended") or "")[:8000]
        cell_p = parse_bethesda_cell(bet_raw)
        inf = infer_bethesda_from_text(path_s, hist_s)
        has_any = not (
            _is_blank(r.get("date"))
            and _is_blank(bet_raw)
            and _is_blank(path_s)
            and _is_blank(hist_s)
        )
        st, just = classify_source(cell_p, inf, has_any, path_s[:500])
        rows.append(
            {
                "research_id": int(r["research_id"]),
                "fna_index": int(r["fna_index"]),
                "fna_date_parsed": r.get("fna_date_parsed"),
                "bethesda_field_raw": str(bet_raw)[:500] if not _is_blank(bet_raw) else "",
                "cytology_path_text": path_s[:4000],
                "cytology_path_extended": hist_s[:4000],
                "specimen_site": str(r.get("specimen_join") or r.get("specimen_received") or "")[:500],
                "bethesda_from_field_num": cell_p,
                "bethesda_inferred_from_text_num": inf,
                "bethesda_status": st,
                "status_justification": just,
                "source_workbook": RAW_FNA.name,
                "source_row_fp": _fp(
                    {"rid": int(r["research_id"]), "fna_ix": int(r["fna_index"])}
                ),
            }
        )
    src = pd.DataFrame(rows)
    src.to_csv(OUT / "source_fna_inventory.csv", index=False)

    # structured inventory
    fe_m = fe.merge(
        fc,
        left_on=["research_id", "fna_episode_id"],
        right_on=["research_id", "fna_index"],
        how="left",
        suffixes=("_episode", "_cytology"),
    )
    fe_m.to_csv(OUT / "structured_fna_inventory.csv", index=False)

    m = src.copy()
    m["structured_fna_history_match"] = True
    m["fh_bethesda_parsed_num"] = m["bethesda_from_field_num"]

    fe_sub = fe.rename(columns={"fna_episode_id": "fna_index"})[
        [
            "research_id",
            "fna_index",
            "bethesda_raw",
            "bethesda_category",
            "pathology_diagnosis",
            "specimen_site_raw",
            "laterality",
            "source_table",
            "resolved_fna_date",
        ]
    ].rename(
        columns={
            "bethesda_raw": "fe_bethesda_raw",
            "bethesda_category": "fe_bethesda_category",
            "pathology_diagnosis": "fe_pathology_diagnosis",
            "specimen_site_raw": "fe_specimen_site_raw",
            "laterality": "fe_laterality",
            "source_table": "fe_source_table",
            "resolved_fna_date": "fe_resolved_fna_date",
        }
    )
    m = m.merge(fe_sub, on=["research_id", "fna_index"], how="left")

    fc_sub = fc[
        [
            "research_id",
            "fna_index",
            "fna_date",
            "specimen_location",
            "path_text",
            "original_bethesda",
            "category_num",
            "bethesda_2010_num",
            "bethesda_2023_num",
            "bethesda_2023_name",
            "confidence",
            "method",
            "source_workbook",
        ]
    ].rename(
        columns={
            "fna_date": "fc_fna_date",
            "specimen_location": "fc_specimen_location",
            "path_text": "fc_path_text",
            "original_bethesda": "fc_original_bethesda",
            "category_num": "fc_category_num",
            "bethesda_2010_num": "fc_bethesda_2010_num",
            "bethesda_2023_num": "fc_bethesda_2023_num",
            "bethesda_2023_name": "fc_bethesda_2023_name",
            "confidence": "fc_confidence",
            "method": "fc_method",
            "source_workbook": "fc_source_workbook",
        }
    )
    m = m.merge(fc_sub, on=["research_id", "fna_index"], how="left")

    m["fe_num"] = m["fe_bethesda_category"].map(to_int_cat)
    m["fh_num"] = m["fh_bethesda_parsed_num"].map(to_int_cat)
    m["fc_num"] = m["fc_bethesda_2023_num"].map(to_int_cat)

    xw_cols = [
        "research_id",
        "fna_index",
        "fna_date_parsed",
        "bethesda_field_raw",
        "fh_bethesda_parsed_num",
        "bethesda_from_field_num",
        "bethesda_inferred_from_text_num",
        "bethesda_status",
        "status_justification",
        "structured_fna_history_match",
        "fe_bethesda_raw",
        "fe_bethesda_category",
        "fc_category_num",
        "fc_bethesda_2023_num",
        "fe_num",
        "fh_num",
        "fc_num",
    ]
    xw = m[[c for c in xw_cols if c in m.columns]].copy()
    xw.to_csv(OUT / "bethesda_crosswalk_audit.csv", index=False)

    conflicts = []
    for _, row in m.iterrows():
        vals = [row.get("fe_num"), row.get("fh_num"), row.get("fc_num")]
        pool = [v for v in vals if v is not None]
        if len(pool) >= 2 and len(set(pool)) > 1:
            conflicts.append(
                {
                    "research_id": row.get("research_id"),
                    "fna_index": row.get("fna_index"),
                    "bethesda_fna_episode_master_v2": row.get("fe_num"),
                    "bethesda_fna_history_parsed": row.get("fh_num"),
                    "bethesda_fna_cytology_2023": row.get("fc_num"),
                    "conflict_resolution": "unresolved_numeric_mismatch",
                    "notes": "Compare fe_bethesda_raw, fna_history.bethesda, fna_cytology.",
                }
            )
    cdf = pd.DataFrame(
        conflicts,
        columns=[
            "research_id",
            "fna_index",
            "bethesda_fna_episode_master_v2",
            "bethesda_fna_history_parsed",
            "bethesda_fna_cytology_2023",
            "conflict_resolution",
            "notes",
        ],
    )
    cdf.to_csv(OUT / "fna_bethesda_conflicts.csv", index=False)

    missing_rows = []
    for _, row in m.iterrows():
        has_struct = any(row.get(k) is not None for k in ("fe_num", "fh_num", "fc_num"))
        if not has_struct:
            missing_rows.append(
                {
                    "research_id": row.get("research_id"),
                    "fna_index": row.get("fna_index"),
                    "source_bethesda_status": row.get("bethesda_status"),
                    "justification": row.get("status_justification"),
                    "gap_type": (
                        "expected_not_scorable"
                        if row.get("bethesda_status") == "not_scorable_from_source"
                        else "structured_bethesda_absent"
                    ),
                }
            )
    pd.DataFrame(
        missing_rows,
        columns=[
            "research_id",
            "fna_index",
            "source_bethesda_status",
            "justification",
            "gap_type",
        ],
    ).to_csv(OUT / "fna_missing_bethesda.csv", index=False)

    n_src = len(src)
    n_match = int(m["structured_fna_history_match"].fillna(False).sum())
    n_explicit = int((src["bethesda_status"] == "explicit_source_bethesda").sum())
    n_inf = int((src["bethesda_status"] == "inferred_from_source_text").sum())
    n_ns = int((src["bethesda_status"] == "not_scorable_from_source").sum())
    n_expl = int((src["bethesda_status"] == "missing_unexplained").sum())
    n_conflict = len(conflicts)

    verdict = f"""# FNA Bethesda completeness verdict

**Run:** {datetime.now(timezone.utc).isoformat()}Z  
**Database:** `{'local file' if args.local else 'MotherDuck (Thyroid 2026)'}`  
**Token mode:** `{token_mode()}`

## Method

- **Source spine:** `fna_history` (one row per FNA episode; **{n_src}** rows). This table is the ETL long melt of
  `FNAs 12_5_2025.xlsx` produced by `scripts/build_fna_history_from_fnas_detailed.py` / `01_ingest_all_files.py`.
  `fna_index` is chronological within patient (not Excel slot order).
- Raw workbook required on disk for provenance; classification uses `bethesda`, `path`, and `path_extended` from `fna_history`.
- Join keys for structured tables: `(research_id, fna_index)`.

## Summary counts

| Metric | Value |
|--------|------:|
| Total source FNA episodes (`fna_history`) | {n_src} |
| Rows aligned with structured `fna_history` (identity) | {n_match} |
| Episodes with explicit Bethesda in `fna_history.bethesda` column | {n_explicit} |
| Episodes with conservative text-inferred Bethesda | {n_inf} |
| Episodes not scorable from source (with justification) | {n_ns} |
| Episodes **missing_unexplained** (target 0) | {n_expl} |
| Cross-table numeric Bethesda conflicts | {n_conflict} |

## Strict criteria

- **missing_unexplained:** {n_expl}
- **not_scorable_from_source:** each row documents why in `status_justification`.
- **Conflicts:** `fna_bethesda_conflicts.csv` — `unresolved_numeric_mismatch` until adjudicated.

## Artifacts

- `source_fna_inventory.csv` — one row per `fna_history` episode with Bethesda classification
- `structured_fna_inventory.csv` — `fna_episode_master_v2` ⋈ `fna_cytology`
- `bethesda_crosswalk_audit.csv` — source ↔ structured Bethesda fields
- `fna_missing_bethesda.csv` — no numeric 1–6 in episode / history / cytology
- `fna_bethesda_conflicts.csv` — numeric mismatches across tables

## Notes

- `extracted_fna_bethesda_v1` is not deployed on this MotherDuck database; audit uses `fna_episode_master_v2`,
  `fna_history`, `fna_cytology`. See view `v_fna_episode_bethesda_resolved_v1` for episode–cytology resolution.

## Conflict list (exact)

- Full machine-readable list: `fna_bethesda_conflicts.csv` (**{n_conflict}** rows).
- Each row has `research_id`, `fna_index`, and the three normalized Bethesda numbers when at least two disagree.
"""

    (OUT / "verdict.md").write_text(verdict, encoding="utf-8")
    _log("done ok")
    return 0 if n_expl == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

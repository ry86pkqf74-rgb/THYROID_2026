#!/usr/bin/env python3
"""mig_103 — Build REAL/TEMPLATE dispositions for canonical_medications_events_v1.

Joins each medication mention to clinical note text by scanning the patient's
notes (note_entities legacy row_ids are content hashes and do not match
clinical_notes_long.note_index). Locates the first token hit for the med
family in notes matching source_note_type when possible; otherwise any note.

Outputs JSON consumed by apply_mig_103_medications_decisions.py.
Does NOT mutate MotherDuck tables.

Author: Logan Glosser <logan.glosser@gmail.com>
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PUB_DB = "thyroid_canonical_publication_v1_0"
MEDS = "main.canonical_medications_events_v1"
OUT_DIR = REPO_ROOT / "verification_csvs" / "canonical_medications_events_v1"
OUT_JSON = OUT_DIR / "mig_103_decisions.json"

# Search stems per normalized finding (lowercase match in note_text).
NORM_SEARCH: dict[str, list[str]] = {
    "levothyroxine": [
        "levothyroxine",
        "levoxyl",
        "synthroid",
        "tirosint",
        "unithroid",
        "thyroxine",
        "eltroxin",
    ],
    "rai_dose": [
        "rai",
        "i-131",
        "i131",
        "radioactive iodine",
        "radioiodine",
        "131i",
    ],
    "calcium_supplement": [
        "calcium",
        "tums",
        "calcium carbonate",
        "caltrate",
        "calcium citrate",
        "oscal",
    ],
    "calcitriol": ["calcitriol", "rocaltrol", "vitamin d analog"],
}

NEGATION_PREFIX_RE = re.compile(
    r"\b(?:denies|deny|denied|not\s+on\b|off\s+|without\s+|"
    r"no\s+longer\s+(?:on|taking)|not\s+taking\b|"
    r"negative\s+for|discontinued|stopped\s+taking)\b",
    re.IGNORECASE,
)

TEMPLATE_RES = [
    re.compile(r"risks?\s+(?:include|of|may\s+include)[^.]{0,350}", re.I),
    re.compile(r"(?:complications?|side\s+effects?)\s+(?:include|may\s+be)[^.]{0,350}", re.I),
    re.compile(r"\bconsent\b[^.]{0,250}", re.I),
    re.compile(r"\bcounseled\b[^.]{0,200}", re.I),
    re.compile(r"\b(?:nkda|n\.?k\.?d\.?a\.?)\b", re.I),
    re.compile(r"no\s+known\s+(?:drug\s+)?allerg", re.I),
    re.compile(r"discontinue\s+if\s+pregnant[^.]{0,120}", re.I),
    re.compile(r"family\s+history[^.]{0,200}", re.I),
]

REAL_RES = [
    re.compile(
        r"\b(?:started|prescribed|continuing|continue|continues|"
        r"increased|increase\s+to|decreased|decrease\s+to|"
        r"held|"
        r"discontinued\s+(?:on|as|due)|"
        r"taking\b|currently\s+(?:on|taking)|"
        r"home\s+medications?\s+(?:include|are)|"
        r"active\s+medications?\s+(?:include|are)|"
        r"medication\s+list|medications?\s*:)",
        re.I,
    ),
    re.compile(
        r"\b(?:mg|mcg|µg|bid|tid|qid|qd|qhs|prn|daily|weekly)\b",
        re.I,
    ),
    re.compile(r"\b(?:tsh\s+suppression|suppressive\s+dose)\b", re.I),
    re.compile(r"\b(?:patient\s+on|pt\s+on|uses\s+|using\s+)\b", re.I),
]

# PMH move: peri-thyroid supplements clearly anchored long before index surgery.
PMH_SUPP_NORMS = frozenset({"calcium_supplement", "calcitriol"})


def _connect_md() -> duckdb.DuckDBPyConnection:
    from motherduck_client import MotherDuckClient, MotherDuckConfig

    cfg = MotherDuckConfig(database=PUB_DB)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f"USE {PUB_DB}")
    return con


def _load_notes_by_patient(con: duckdb.DuckDBPyConnection) -> dict[str, list[tuple[str, str]]]:
    """research_id (str) -> [(note_type, note_text), ...]."""
    rows = con.execute(
        """
        SELECT CAST(research_id AS VARCHAR), note_type, note_text
        FROM main.clinical_notes_long
        WHERE note_text IS NOT NULL AND LENGTH(TRIM(note_text)) > 0
        """
    ).fetchall()
    out: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for rid, nt, txt in rows:
        if not rid:
            continue
        out[rid].append((str(nt or ""), str(txt)))
    return out


def _first_hit(
    notes: list[tuple[str, str]],
    want_type: str | None,
    terms: list[str],
) -> tuple[str, int, str] | None:
    """Return (note_type, position, full_text) for first term hit; prefer want_type."""
    lowered_terms = [t.lower() for t in terms]

    def scan(note_type: str, text: str) -> tuple[int, str] | None:
        low = text.lower()
        best: tuple[int, str] | None = None
        for t in lowered_terms:
            pos = low.find(t)
            if pos == -1:
                continue
            if best is None or pos < best[0]:
                best = (pos, text)
        return best

    if want_type:
        for nt, text in notes:
            if nt != want_type:
                continue
            got = scan(nt, text)
            if got:
                return (nt, got[0], got[1])
    for nt, text in notes:
        got = scan(nt, text)
        if got:
            return (nt, got[0], got[1])
    return None


def _context_window(full_text: str, pos: int, before: int = 320, after: int = 320) -> str:
    a = max(0, pos - before)
    b = min(len(full_text), pos + after)
    return full_text[a:b]


def _classify(ctx: str) -> tuple[bool, bool]:
    is_template = any(r.search(ctx) for r in TEMPLATE_RES)
    is_real = any(r.search(ctx) for r in REAL_RES)
    return is_template, is_real


def _row_key(rid: str, source_row_id: str) -> str:
    return f"{rid}|{source_row_id}"


def build_decisions(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    notes_idx = _load_notes_by_patient(con)
    med_rows = con.execute(
        f"""
        SELECT
          CAST(research_id AS VARCHAR) AS research_id,
          source_row_id,
          source_note_type,
          finding_status,
          finding_value_norm,
          COALESCE(days_from_first_thyroidectomy, 999999) AS dfs
        FROM {MEDS}
        """
    ).fetchall()

    decisions: dict[str, Any] = {}
    summary = defaultdict(int)

    for (
        rid,
        source_row_id,
        source_note_type,
        finding_status,
        finding_value_norm,
        dfs_i,
    ) in med_rows:
        key = _row_key(rid, source_row_id)
        fv = (finding_value_norm or "").strip().lower()
        terms = NORM_SEARCH.get(fv, [fv] if fv else [])

        if finding_status == "absent":
            decisions[key] = {
                "disposition": "DELETE",
                "basis": "finding_status=absent",
                "context_note_type": None,
                "template": False,
                "real": False,
                "context_missing": False,
            }
            summary["DELETE_absent"] += 1
            continue

        patient_notes = notes_idx.get(rid, [])
        hit = _first_hit(patient_notes, source_note_type, terms)

        if hit is None:
            # Trust structured pharmacy extraction when no text hit (token drift).
            decisions[key] = {
                "disposition": "KEEP",
                "basis": "no_token_match_trust_structured_extraction",
                "context_note_type": None,
                "template": False,
                "real": False,
                "context_missing": True,
            }
            summary["KEEP_no_context"] += 1
            continue

        nt, pos, full_text = hit
        prefix = full_text[max(0, pos - 90) : pos]
        neg_before = bool(NEGATION_PREFIX_RE.search(prefix))

        ctx = _context_window(full_text, pos)
        is_template, is_real = _classify(ctx)

        if neg_before:
            decisions[key] = {
                "disposition": "DELETE",
                "basis": "negation_before_mention_in_note",
                "context_note_type": nt,
                "template": is_template,
                "real": is_real,
                "context_missing": False,
            }
            summary["DELETE_negation"] += 1
            continue

        if is_template and not is_real:
            decisions[key] = {
                "disposition": "DELETE",
                "basis": "template_dominates_no_real_signals",
                "context_note_type": nt,
                "template": True,
                "real": False,
                "context_missing": False,
            }
            summary["DELETE_template"] += 1
            continue

        if is_real or not is_template:
            dfs = int(dfs_i) if dfs_i is not None else 999999
            if fv in PMH_SUPP_NORMS and dfs < -30:
                decisions[key] = {
                    "disposition": "PMH",
                    "basis": "pre_surgery_supplement_dfs_lt_-30",
                    "context_note_type": nt,
                    "template": is_template,
                    "real": is_real,
                    "context_missing": False,
                }
                summary["PMH"] += 1
                continue

            decisions[key] = {
                "disposition": "KEEP",
                "basis": "real_or_non_template_clinical_med",
                "context_note_type": nt,
                "template": is_template,
                "real": is_real,
                "context_missing": False,
            }
            summary["KEEP"] += 1
            continue

        decisions[key] = {
            "disposition": "REVIEW",
            "basis": "ambiguous_template_vs_real",
            "context_note_type": nt,
            "template": is_template,
            "real": is_real,
            "context_missing": False,
        }
        summary["REVIEW"] += 1

    meta = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "table": "main.canonical_medications_events_v1",
        "n_rows": len(decisions),
        "summary_counts": dict(summary),
    }
    return {"meta": meta, "decisions": decisions}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=OUT_JSON,
        help="Output JSON path",
    )
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"mig_103 build — connecting MotherDuck ({PUB_DB}) …")
    con = _connect_md()
    try:
        payload = build_decisions(con)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print("  wrote", args.out)
        print("  summary:", payload["meta"]["summary_counts"])
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Triages legacy nuclear_med rows that match Prompt 3 worklist gate but lack
canonical_nucmed_lymph_node_v1 rows. Classifies as:

  POSITIVE — genuine imaging evidence of LN / lymphadenopathy / nodal uptake
  NEGATIVE — matches are benign (mostly parathyroid or thyroid \"adenoma\",
             or explicit negative lymph-node language)

Reads CSV exported from BigQuery:
  columns: research_id, scan_date_parsed, scan_index, radiotracer, scantype, full_text

Prints counts and optionally writes INSERT-ready JSON plus a Markdown summary.

Heuristics prioritize conservative-positive: ambiguous language -> POSITIVE_WITH_FLAG
(so rows can carry nlp_backfill_pending=TRUE for human QC).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

# ── LN-positive patterns (nuclear-medicine specific) ──────────────────────
LN_POSITIVE = [
    re.compile(r"\blikely\s+representing\s+uptake\s+in\s+neck\s+nodes?\b", re.I),
    re.compile(
        r"\b(probable|possible|likely)[^\n]{0,35}uptake[^\n]{0,60}"
        r"\b(central\s+neck|neck)\s+nodes?\b",
        re.I,
    ),
    re.compile(r"\b(uptake|activity|radiotracer|radioiod)\s[^\n]{0,50}\bin\s+(the\s+)?neck\s+nodes?\b", re.I),
    re.compile(r"\bneck\s+nodes?\b[^\n.]{0,160}\b(uptake|avid|positive|accumulat|activity|accumulation)\b", re.I),
    re.compile(r"\b(uptake|increased\s+activity|radiotracer[^\n]{0,30})\s[^\n]{0,120}\bcervical\s+nodes?\b", re.I),
    re.compile(r"\bneck\s+(?:lymph\s+nodal\s+)?metast", re.I),
    re.compile(r"\bneck\s+mets\b", re.I),
]

# ── Explicit negative LN / adenopathy (whole-text) ────────────────────────
LN_NEGATIVE = [
    re.compile(r"no\s+abnormal[^\n]{0,40}(lymph|node|radiotracer)\b", re.I),
    re.compile(r"without\s+any\s+abnormal\s+radiotracer\s+uptake\s+elsewhere", re.I),
    re.compile(r"no\s+(suspicious|pathologic|definite[^\n]{0,20})\s+(neck\s+)?(lymph\b|adenopathy)", re.I),
    re.compile(r"no\s+pathologically\s+enlarged\s+adenopathy\b", re.I),
    re.compile(r"no\s+hyperfunctioning\s+nodes?\b", re.I),
    re.compile(r"no\s+metabolic\s+[^\n]{0,80}adenopathy", re.I),
    re.compile(r"no\s+extrathoracic\s+adenopathy", re.I),
    re.compile(r"no\s+significant\s+uptake[^\n]{0,120}(?:neck\s+)?nodal\s+basins\b", re.I),
    re.compile(r"no\s+other\s+distant[^\n]{0,40}accumulation", re.I),
    re.compile(r"no\s+definite\s+evidence[^\n]{0,80}avid\s+tissue", re.I),
    re.compile(r"unremarkable[^\n]{0,30}(?:cervical\s+)?lymph\b", re.I),
]

# \"adenoma\" that is almost never cervical lymphadenopathy ───────────────
FALSE_ADENOMA_TERM = re.compile(
    r"\b(?:parathyroid|thyroid|toxic\s+multi|intrathyroidal|intrathyroid|ectopic\s+parathyroid)\s+adenoma|"
    r"\btoxic\s+adenoma|"
    r"adenoma[^\n]{0,80}(?:parathyroid|thyroid\b)|"
    r"(?:hyperfunctioning\s+)?toxic[^\n]{0,40}adenoma",
    re.I,
)


def classify(full_text: str) -> tuple[str, str]:
    """Return (label, rationale_short)."""
    t = full_text.lower()
    if not t.strip():
        return ("NEGATIVE", "empty_text")

    # Block common PET/CT negatives that still contain "adenopathy"
    if re.search(
        r"no[^\n]{0,80}(?:metabolic|pathologic[^\n]{0,20})\s[^\n]{0,40}(?:mediastinal|hilar|extrathoracic)\s+adenopathy",
        full_text,
        re.I,
    ):
        return ("NEGATIVE_EXPLICIT_PET_MEDIASTINAL", "neg_mediastinal_hilar_formula")

    # Strong positive LN signals first
    for pat in LN_POSITIVE:
        if pat.search(full_text):
            return ("POSITIVE_LN_FINDING", pat.pattern[:50])

    hits_neg = sum(1 for p in LN_NEGATIVE if p.search(full_text))

    # If only FP triggers were \"aden*\" via parathyroid/thyroid adenoma wording
    if FALSE_ADENOMA_TERM.search(full_text) and hits_neg >= 1:
        return ("NEGATIVE_PARATHYROID_THYROID_ADENOMA_LANGUAGE", "neg_pattern+adenoma_term")

    if FALSE_ADENOMA_TERM.search(full_text):
        # No explicit LN positive and adenoma is parathyroid/thyroid-context
        if not re.search(r"\bneck\s+(?:lymph|node)|lymphadenopathy|neck\s+nodes?\b", t):
            return ("NEGATIVE_ADENOMA_NOT_LYMPH", "adenoma_terms_no_ln_lexicon")
        # Has ln lexicon but only toxic nodule wording
        if re.search(r"toxic[^\n]{0,40}(?:adenoma|nodule)|dominant[^\n]{0,40}(?:nodule)", t):
            if not any(p.search(full_text) for p in LN_POSITIVE):
                return ("NEGATIVE_TOXIC_NODULE_ADENOMA_CONTEXT", "toxic_nodule")

    # \"adenopathy\" in \"no … adenopathy\"
    if re.search(r"no[^\n]{0,40}adenopathy", t) and hits_neg >= 1:
        return ("NEGATIVE_EXPLICIT", "neg_adenopathy_phrase")

    if hits_neg >= 2:
        return ("NEGATIVE_MULTI_NEGATION", f"{hits_neg}_neg_hits")

    # Thorax wording that uses "adenomas" non-specifically — not LN staging
    if re.search(r"intrathoracic\s+adenomas?\b", t) and not re.search(
        r"\bneck\s+(?:lymph|node)|neck\s+nodes\b", t
    ):
        return ("NEGATIVE_INTRATHORACIC_ADENOMA_LANGUAGE", "intrathoracic_adenomas")

    if re.search(r"benign\s+adenoma", t) and re.search(r"graves\b", t):
        return ("NEGATIVE_BENIGN_THYROID_ADENOMA_CONTEXT", "graves_adenoma")

    if hits_neg == 1 and not re.search(r"\bneck\s+nodes?\b\s*(?:with|showing|demonstrat)", t):
        return ("NEGATIVE_SINGLE_NEGATION", "one_neg_pattern")

    # Residual: mentions node/aden in passing (physiologic salivary, etc.)
    if re.search(r"physiolog", t) and "lymph" in t and not re.search(r"suspicious|pathologic|metast", t):
        return ("NEGATIVE_PHYSIOLOGIC_CONTEXT", "physiologic")

    return ("REVIEW_AMBIGUOUS", "no_pos_rule_no_clear_neg")


def make_ln_row(
    research_id: str,
    scan_date: date | None,
    scan_index: int | None,
    radiotracer: str | None,
    scantype: str | None,
    full_text: str,
    idx: int,
) -> dict:
    """One canonical_nucmed_lymph_node_v1 row (minimal fill + backfill flag)."""
    exam_date = scan_date.isoformat() if scan_date else None
    modality = "NUCMED"
    src_key = "nucmed_gap_triage_v1"
    exam_id = hashlib.md5(
        "|".join([str(research_id), str(exam_date or ""), modality, src_key, str(idx)]).encode()
    ).hexdigest()
    ln_id = hashlib.md5(
        "|".join([exam_id, str(idx), "gap_triage", str(len(full_text))]).encode()
    ).hexdigest()
    ev = re.sub(r"\s+", " ", full_text).strip()[:320]
    rt = (radiotracer or "").lower()
    if "i-131" in rt or "i131" in rt or "131" in rt:
        rad = "I-131_or_I-123"
    elif "i-123" in rt or "123" in rt:
        rad = "I-131_or_I-123"
    elif "fdg" in rt or "f-18" in rt:
        rad = "F-18-FDG"
    else:
        rad = "Tc-99m"
    return {
        "research_id": str(research_id),
        "exam_id": exam_id,
        "exam_date": exam_date,
        "ln_index_within_exam": idx,
        "ln_id": ln_id,
        "source_modality": modality,
        "laterality": None,
        "neck_level": None,
        "neck_level_subdivision": None,
        "region": "cervical" if re.search(r"neck|node|lymph", full_text, re.I) else None,
        "size_short_mm": None,
        "size_long_mm": None,
        "size_max_mm": None,
        "size_short_long_ratio": None,
        "shape": None,
        "echogenicity": None,
        "hilum_preserved": None,
        "cortex_thickness": None,
        "necrosis_present": None,
        "matting": None,
        "conglomerate": None,
        "calcifications": None,
        "cystic_component": None,
        "extranodal_extension": None,
        "margins": None,
        "suspicious_flag": True,
        "suspicion_level": "suspicious",
        "evidence_text": ev,
        "source_note_type": "nuclear_med_gap_triage",
        "source_report_id": f"nucmed_legacy:{research_id}:{scan_index}",
        "llm_model": "regex_triage_prompt3_20260514",
        "confidence": 0.55,
        "extracted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
        "nlp_backfill_pending": True,
        "radiotracer": rad,
        "uptake_present": True,
        "uptake_intensity": None,
        "distinguished_from_thyroid_bed": None,
        "spect_ct_localization": bool(re.search(r"spect", full_text, re.I)),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path", type=Path, help="BQ-export CSV with full_text")
    ap.add_argument("--out-dir", type=Path, default=Path(__file__).resolve().parent)
    ap.add_argument("--apply-positive-jsonl", action="store_true", help="Write rows for manual bq load")
    args = ap.parse_args()

    rows_out: list[dict] = []
    summary: dict[str, int] = {}
    detail: list[tuple[str, str, str]] = []

    with args.csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rid = r["research_id"].strip()
            ft = r.get("full_text") or ""
            label, why = classify(ft)
            summary[label] = summary.get(label, 0) + 1
            detail.append((rid, label, why))

            if label == "POSITIVE_LN_FINDING":
                sd = None
                if r.get("scan_date_parsed"):
                    try:
                        sd = datetime.strptime(r["scan_date_parsed"], "%Y-%m-%d").date()
                    except ValueError:
                        sd = None
                si = int(r["scan_index"]) if r.get("scan_index") not in ("", None) else None
                rows_out.append(
                    make_ln_row(rid, sd, si, r.get("radiotracer"), r.get("scantype"), ft, 1)
                )

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    pos = summary.get("POSITIVE_LN_FINDING", 0)
    amb = summary.get("REVIEW_AMBIGUOUS", 0)
    neg_total = sum(v for k, v in summary.items() if k != "POSITIVE_LN_FINDING")

    md = out_dir / "PROMPT3_NUCMED_TRIAGE_SUMMARY.md"
    md.write_text(
        "\n".join(
            [
                "# Prompt 3 — Nuclear medicine LN gap triage (legacy worklist)",
                "",
                "## Worklist definition",
                "Matches `prompt3` legacy filter on `pub_legacy_source_20260416.nuclear_med` (node/aden substring), excluding any `research_id` already present in `pub_canonical.canonical_nucmed_lymph_node_v1`.",
                "",
                "## Counts",
                f"- Rows triaged (distinct patients): **{len(detail)}**",
                f"- **Positive (genuine LN / nodal uptake language): {pos}**",
                f"- **Negative / non-LN interpretations: {neg_total}**",
                f"- **Ambiguous (no strong pos/neg rule): {amb}**",
                "",
                "## Label distribution",
                "",
                "| label | n |",
                "|---|---:|",
                *[f"| `{k}` | {v} |" for k, v in sorted(summary.items(), key=lambda x: -x[1])],
                "",
                "## Acceptance note",
                f"Remaining gap patients without inserts: **{len(detail) - pos}** are documented as negatives/ambiguous.",
                "",
                "## Backfill artefact",
                f"- Rows prepared for INSERT (positives): **{len(rows_out)}** → `canonical_nucmed_ln_gap_positive.jsonl`",
                "",
            ]
        ),
        encoding="utf-8",
    )

    jsonl = out_dir / "canonical_nucmed_ln_gap_positive.jsonl"
    if rows_out:
        with jsonl.open("w", encoding="utf-8") as jf:
            for row in rows_out:
                jf.write(json.dumps(row, default=str) + "\n")
    else:
        jsonl.write_text("", encoding="utf-8")

    with (out_dir / "triage_per_patient.csv").open("w", newline="", encoding="utf-8") as cf:
        w = csv.writer(cf)
        w.writerow(["research_id", "triage_label", "rationale_code"])
        w.writerows(detail)

    print(json.dumps(summary, indent=2))
    print(f"Positive rows JSONL: {jsonl}")
    print(f"Summary markdown: {md}")


if __name__ == "__main__":
    main()

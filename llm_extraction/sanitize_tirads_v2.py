"""
sanitize_tirads_v2.py — post-process the raw ckpt.jsonl from the v2 TIRADS
extraction into a clean, enum-validated, LN-leak-free parquet.

Input:
    runs/tirads_granular/full_v2_output/note_entities_llm_tirads_granular.ckpt.jsonl

    Each line is a JSON object with (among other bookkeeping fields):
        - research_id          (str/int)
        - note_row_id          (str, e.g. "1587_us1")
        - note_type            (str)
        - note_date            (YYYY-MM-DD str)
        - linkage_date         (YYYY-MM-DD str)
        - source_workbook / source_sheet / source_column
        - extracted_at         (ISO UTC)
        - llm_model / llm_base_url
        - result_json          (STRING: JSON payload from the model)

    result_json parses to:
        {
          "nodules":      [ {nodule_id, laterality, pole, ... } ],
          "report_level": { impression_text, overall_recommendation, ... }
        }

Transforms applied:
    (a) Normalize the string "null" -> JSON null (None)         [rule 0a]
    (b) Drop nodules whose location_raw describes cervical lymph nodes or
        post-thyroidectomy fossa findings                        [rule 0c]
        (patterns: "level [234]", "submandibular", "submental",
                   "supraclavicular", "jugular", "fossa" without lobe
                   reference)
    (c) Enum-validate every enumerated string field; invalid → None [rule 0b]
    (d) Emit a flat parquet keyed on
          (research_id, note_row_id, nodule_index_within_exam)
        plus a second, long-format report_level parquet keyed on
          (research_id, note_row_id).

Downstream use: the two parquets drop straight into the TIRADS scoring
table.  String-null, LN-leak, and enum-freetext anomalies observed in the
v2b smoke pass (REC 0, REC 1, REC 2 respectively) are all cleared here
without having to re-prompt the model.

Usage:
    python3 -m llm_extraction.sanitize_tirads_v2 \
        --input  runs/tirads_granular/full_v2_output/note_entities_llm_tirads_granular.ckpt.jsonl \
        --out-nodules runs/tirads_granular/full_v2_output/nodules_clean.parquet \
        --out-reports runs/tirads_granular/full_v2_output/reports_clean.parquet

Exits non-zero on unrecoverable errors (file missing, not JSONL, etc.).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger("sanitize_tirads_v2")

# -----------------------------------------------------------------------------
# Enum catalog — MUST stay in lockstep with tirads_granular_extraction_v2.txt
# -----------------------------------------------------------------------------
ENUMS: dict[str, set[str]] = {
    "laterality": {"left", "right", "isthmus", "bilateral"},
    "pole": {"upper", "mid", "lower"},
    "position": {"anterior", "posterior"},
    "composition": {
        "cystic", "predominantly_cystic", "mixed",
        "predominantly_solid", "solid", "spongiform",
    },
    "echogenicity": {
        "anechoic", "hyperechoic", "isoechoic",
        "hypoechoic", "very_hypoechoic",
    },
    "shape": {"wider_than_tall", "taller_than_wide"},
    "shape_plane": {"transverse", "longitudinal", "unspecified"},
    "margin": {
        "smooth", "ill_defined", "lobulated",
        "irregular", "extrathyroidal_extension",
    },
    "extrathyroidal_extension_on_us": {"none", "suspected", "definite"},
    "tirads_category": {"TR1", "TR2", "TR3", "TR4", "TR5"},
    "halo": {"none", "thin", "thick"},
    "vascularity": {"absent", "peripheral", "internal", "mixed"},
    "chammas_type": {"I", "II", "III", "IV", "V"},
    "elastography": {"soft", "intermediate", "hard"},
    "comparison_statement": {
        "stable", "grown", "shrunk", "new",
        "not_seen_prior", "no_prior",
    },
    "tirads_system_reported": {"ACR", "Kwak", "EU", "ATA"},
    # report_level
    "overall_recommendation": {"fna", "follow_up", "no_further"},
}

# Values allowed inside the echogenic_foci[] list
ECHOGENIC_FOCI_VALUES: set[str] = {
    "none",
    "large_comet_tail",
    "macrocalcifications",
    "peripheral_calcifications",
    "punctate_echogenic_foci",
}

# Fields that are booleans (true/false/null)
BOOL_FIELDS_NODULE: set[str] = {
    "interval_growth_flag",
    "fna_recommended_this_nodule",
    "fna_performed_prior_or_concurrent",
    "not_well_visualized",
    "confidence_hedged",
}
BOOL_FIELDS_REPORT: set[str] = {"suspicious_ln_present"}

# Fields that are numeric
NUMERIC_FIELDS_NODULE: set[str] = {
    "size_mm_ap", "size_mm_tr", "size_mm_cc", "size_cm_max",
    "volume_ml", "tirads_total_points", "prior_size_mm_max",
    "tirads_reported_in_text", "date_confidence", "source_line",
}
NUMERIC_FIELDS_REPORT: set[str] = {
    "follow_up_interval_months", "n_nodules_in_report",
    "date_confidence", "source_line",
}

# Raw-passthrough / free-text fields (allowed to be any string, but we still
# coerce string "null" → None)
FREETEXT_FIELDS_NODULE: set[str] = {
    "nodule_id", "location_raw", "calcifications_raw",
    "date_source_keyword", "entity_date", "evidence_text",
}
FREETEXT_FIELDS_REPORT: set[str] = {
    "report_impression_text", "dominant_nodule_id_by_radiologist",
    "entity_date", "evidence_text",
}

# -----------------------------------------------------------------------------
# LN / fossa leak detector
#
#   A lesion described as being in "level 2", "level 3", "level 4",
#   "submandibular", "submental", "supraclavicular", or "jugular" is a
#   LYMPH NODE, not a thyroid nodule, and must NOT appear in nodules[].
#
#   Post-thyroidectomy "fossa" findings are surgical-bed findings, NOT
#   intrinsic thyroid nodules.  They sneak in when the model tries to
#   shoehorn a neck US into the schema.
# -----------------------------------------------------------------------------
LN_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\blevel\s*[2-6]\b", re.I),
    re.compile(r"\bsubmandibular\b", re.I),
    re.compile(r"\bsubmental\b", re.I),
    re.compile(r"\bsupraclavicular\b", re.I),
    re.compile(r"\bjugular\b", re.I),
    re.compile(r"\bparotid\b", re.I),
]
FOSSA_PATTERN = re.compile(r"\bfossa\b", re.I)
LOBE_PATTERN = re.compile(r"\b(right|left|isthmus|pyramidal)\b", re.I)


def is_ln_or_fossa_leak(nodule: dict[str, Any]) -> tuple[bool, str]:
    """Return (True, reason) if this nodule should be dropped.

    Uses location_raw primarily; falls back to evidence_text if location_raw
    is missing.  A "fossa" finding only counts as a leak if there is NO lobe
    reference (right/left/isthmus/pyramidal) accompanying it.

    NOTE: dimension-based drops are handled in `fix_unit_bug` instead — the
    model has a known tendency to output cm × 100 or cm × 1000 in the mm
    fields, so a "huge" dimension is usually a unit error, not an LN.
    """
    loc = _s(nodule.get("location_raw")) or _s(nodule.get("evidence_text")) or ""
    lat = _s(nodule.get("laterality"))

    # Hard LN keywords
    for pat in LN_PATTERNS:
        if pat.search(loc):
            return True, f"ln_pattern:{pat.pattern}"

    # Fossa without lobe reference AND without thyroid laterality
    if FOSSA_PATTERN.search(loc) and not LOBE_PATTERN.search(loc) and not lat:
        return True, "fossa_no_lobe"

    return False, ""


# -----------------------------------------------------------------------------
# Unit-bug correction
#
#   The model sometimes writes mm dims as cm × 100 or cm × 1000 instead of
#   cm × 10.  Example (real case, rec 1963_us1):
#     evidence:     "3.1 x 1.8 x 1.9 cm"
#     size_cm_max:  3.1   (correct)
#     size_mm_ap:   1900, size_mm_tr: 3100, size_mm_cc: 1800   (× 1000 off)
#
#   Correction strategy: if size_cm_max is plausible (<= 30 cm) and the max
#   of the three size_mm fields is more than 2× the expected mm value
#   (size_cm_max × 10), divide all three by the nearest power of ten that
#   reconciles them.
# -----------------------------------------------------------------------------
_MM_FIELDS = ("size_mm_ap", "size_mm_tr", "size_mm_cc")
# Largest plausible mm value for a thyroid nodule even allowing for pathologic
# goiters (~15 cm = 150 mm).  After unit correction, any dim exceeding this is
# unphysical and the nodule is dropped.
_PLAUSIBLE_MAX_MM = 200


def fix_unit_bug(nodule: dict[str, Any]) -> str | None:
    """Mutates `nodule` in place to repair × 100 / × 1000 mm errors.

    Returns a short tag describing what we did (for stats), or None if no
    correction was needed, or 'absurd' if even after rescaling the dims
    remain unphysical.
    """
    cm_max = nodule.get("size_cm_max")
    have_cm_anchor = isinstance(cm_max, (int, float)) and 0 < cm_max <= 30

    mm_numeric = [nodule.get(f) for f in _MM_FIELDS
                  if isinstance(nodule.get(f), (int, float))]
    if not mm_numeric:
        # no mm values at all — nothing to fix; defer absurd-check to the caller
        return None
    actual_max = max(mm_numeric)

    # --- Case A: we have a cm_max anchor we can trust ---
    if have_cm_anchor:
        expected_mm = cm_max * 10
        if actual_max <= expected_mm * 2:
            return None  # already consistent
        ratio = actual_max / expected_mm
        scale = min((10, 100, 1000), key=lambda s: abs(ratio - s))
    # --- Case B: no cm anchor, fall back on heuristic if dims are huge ---
    elif actual_max > _PLAUSIBLE_MAX_MM:
        # guess the scale that pulls actual_max down into the plausible range
        if actual_max > 50_000:
            scale = 1000
        elif actual_max > 5_000:
            scale = 100
        else:
            scale = 10
    else:
        return None

    for f in _MM_FIELDS:
        v = nodule.get(f)
        if isinstance(v, (int, float)):
            corrected = v / scale
            nodule[f] = round(corrected, 1) if corrected < 10 else round(corrected)

    new_max = max((nodule.get(f) for f in _MM_FIELDS
                   if isinstance(nodule.get(f), (int, float))),
                  default=0)
    if new_max > _PLAUSIBLE_MAX_MM:
        return "absurd"
    return f"rescaled_by_{scale}"


# -----------------------------------------------------------------------------
# Scalar coercion helpers
# -----------------------------------------------------------------------------
def _s(v: Any) -> str | None:
    """Normalize string-null to real None, strip whitespace."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() == "null":
            return None
        return s
    return str(v)


def _num(v: Any) -> float | int | None:
    v = _s(v) if isinstance(v, str) else v
    if v is None:
        return None
    if isinstance(v, bool):  # bool is subclass of int; reject
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f.is_integer() else f


def _bool(v: Any) -> bool | None:
    if isinstance(v, bool):
        return v
    s = _s(v)
    if s is None:
        return None
    s = s.lower()
    if s in {"true", "yes", "1"}:
        return True
    if s in {"false", "no", "0"}:
        return False
    return None


def _enum(v: Any, field_name: str) -> str | None:
    s = _s(v)
    if s is None:
        return None
    allowed = ENUMS[field_name]
    if s in allowed:
        return s
    # try case-insensitive match for enums that are case-sensitive by spec
    # (only chammas_type and tirads_category + tirads_system_reported are mixed case)
    for a in allowed:
        if s.lower() == a.lower():
            return a
    return None


def _echogenic_foci(v: Any) -> list[str]:
    if v is None:
        return []
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for item in v:
        s = _s(item)
        if s is None:
            continue
        if s in ECHOGENIC_FOCI_VALUES:
            out.append(s)
            continue
        # case-insensitive fallback
        for a in ECHOGENIC_FOCI_VALUES:
            if s.lower() == a.lower():
                out.append(a)
                break
    return out


# -----------------------------------------------------------------------------
# Per-record sanitization
# -----------------------------------------------------------------------------
@dataclass
class SanitizeStats:
    rows_in: int = 0
    rows_with_parse_error: int = 0
    nodules_in: int = 0
    nodules_dropped_ln: int = 0
    nodules_dropped_fossa: int = 0
    nodules_dropped_absurd: int = 0
    nodules_out: int = 0
    enum_fixups: dict[str, int] = field(default_factory=dict)
    unit_fixups: dict[str, int] = field(default_factory=dict)
    null_fixups: int = 0  # count of "null"→None coercions


def _clean_nodule(nodule: dict[str, Any], stats: SanitizeStats) -> dict[str, Any]:
    out: dict[str, Any] = {}

    for field_name, _ in ENUMS.items():
        if field_name not in nodule:
            continue
        if field_name == "overall_recommendation":
            continue  # report-level
        raw = nodule.get(field_name)
        cleaned = _enum(raw, field_name)
        if raw is not None and _s(raw) is not None and cleaned is None:
            stats.enum_fixups[field_name] = stats.enum_fixups.get(field_name, 0) + 1
        out[field_name] = cleaned

    for field_name in NUMERIC_FIELDS_NODULE:
        if field_name in nodule:
            out[field_name] = _num(nodule.get(field_name))

    for field_name in BOOL_FIELDS_NODULE:
        if field_name in nodule:
            out[field_name] = _bool(nodule.get(field_name))

    for field_name in FREETEXT_FIELDS_NODULE:
        if field_name in nodule:
            out[field_name] = _s(nodule.get(field_name))

    if "echogenic_foci" in nodule:
        out["echogenic_foci"] = _echogenic_foci(nodule.get("echogenic_foci"))

    return out


def _clean_report(rl: dict[str, Any], stats: SanitizeStats) -> dict[str, Any]:
    if not isinstance(rl, dict):
        return {}
    out: dict[str, Any] = {}

    if "overall_recommendation" in rl:
        raw = rl.get("overall_recommendation")
        cleaned = _enum(raw, "overall_recommendation")
        if raw is not None and _s(raw) is not None and cleaned is None:
            stats.enum_fixups["overall_recommendation"] = (
                stats.enum_fixups.get("overall_recommendation", 0) + 1
            )
        out["overall_recommendation"] = cleaned

    for f in NUMERIC_FIELDS_REPORT:
        if f in rl:
            out[f] = _num(rl.get(f))
    for f in BOOL_FIELDS_REPORT:
        if f in rl:
            out[f] = _bool(rl.get(f))
    for f in FREETEXT_FIELDS_REPORT:
        if f in rl:
            out[f] = _s(rl.get(f))

    return out


# -----------------------------------------------------------------------------
# File-level driver
# -----------------------------------------------------------------------------
def sanitize_ckpt(input_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, SanitizeStats]:
    """Stream the ckpt.jsonl, return (nodules_df, reports_df, stats)."""
    stats = SanitizeStats()
    nodule_rows: list[dict[str, Any]] = []
    report_rows: list[dict[str, Any]] = []

    with input_path.open() as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            stats.rows_in += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning("line %d: JSON decode error: %s", lineno, exc)
                stats.rows_with_parse_error += 1
                continue

            # ckpt uses result_json (string) — parse it
            payload_raw = rec.get("result_json")
            if isinstance(payload_raw, str):
                try:
                    payload = json.loads(payload_raw)
                except json.JSONDecodeError:
                    logger.warning(
                        "line %d (note_row_id=%s): result_json not valid JSON",
                        lineno, rec.get("note_row_id"),
                    )
                    stats.rows_with_parse_error += 1
                    continue
            elif isinstance(payload_raw, dict):
                payload = payload_raw
            else:
                payload = rec.get("result") if isinstance(rec.get("result"), dict) else {}

            base_keys = {
                "research_id": rec.get("research_id"),
                "note_row_id": rec.get("note_row_id"),
                "note_type": rec.get("note_type"),
                "note_date": rec.get("note_date"),
                "linkage_date": rec.get("linkage_date"),
                "source_workbook": rec.get("source_workbook"),
                "source_sheet": rec.get("source_sheet"),
                "source_column": rec.get("source_column"),
                "note_index": rec.get("note_index"),
                "extracted_at": rec.get("extracted_at"),
                "llm_model": rec.get("llm_model"),
            }

            nodules = payload.get("nodules") or []
            if not isinstance(nodules, list):
                nodules = []

            nod_idx_kept = 0
            for raw_nodule in nodules:
                if not isinstance(raw_nodule, dict):
                    continue
                stats.nodules_in += 1

                leak, reason = is_ln_or_fossa_leak(raw_nodule)
                if leak:
                    if reason.startswith("ln_pattern"):
                        stats.nodules_dropped_ln += 1
                    elif reason == "fossa_no_lobe":
                        stats.nodules_dropped_fossa += 1
                    logger.debug(
                        "note_row_id=%s dropping nodule %s: %s",
                        rec.get("note_row_id"),
                        raw_nodule.get("nodule_id"), reason,
                    )
                    continue

                fix_tag = fix_unit_bug(raw_nodule)
                if fix_tag == "absurd":
                    stats.nodules_dropped_absurd += 1
                    logger.debug(
                        "note_row_id=%s dropping nodule %s: absurd dims beyond rescue",
                        rec.get("note_row_id"),
                        raw_nodule.get("nodule_id"),
                    )
                    continue
                if fix_tag:
                    stats.unit_fixups[fix_tag] = stats.unit_fixups.get(fix_tag, 0) + 1

                cleaned = _clean_nodule(raw_nodule, stats)
                out_row = {**base_keys, **cleaned,
                           "nodule_index_within_exam": nod_idx_kept}
                nodule_rows.append(out_row)
                nod_idx_kept += 1
                stats.nodules_out += 1

            report_level = _clean_report(payload.get("report_level") or {}, stats)
            report_rows.append({**base_keys, **report_level,
                                "n_nodules_kept": nod_idx_kept})

    nodules_df = pd.DataFrame(nodule_rows)
    reports_df = pd.DataFrame(report_rows)
    return nodules_df, reports_df, stats


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--input", required=True, type=Path,
                   help="ckpt.jsonl from run_extraction_concurrent.py")
    p.add_argument("--out-nodules", required=True, type=Path,
                   help="output parquet, one row per kept nodule")
    p.add_argument("--out-reports", required=True, type=Path,
                   help="output parquet, one row per source note")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    _setup_logging(args.verbose)

    if not args.input.exists():
        logger.error("input not found: %s", args.input)
        return 2

    nodules_df, reports_df, stats = sanitize_ckpt(args.input)

    args.out_nodules.parent.mkdir(parents=True, exist_ok=True)
    args.out_reports.parent.mkdir(parents=True, exist_ok=True)

    if not nodules_df.empty:
        nodules_df.to_parquet(args.out_nodules, index=False)
    else:
        # still emit an empty parquet to be deterministic
        pd.DataFrame().to_parquet(args.out_nodules, index=False)

    reports_df.to_parquet(args.out_reports, index=False)

    logger.info("---- sanitize_tirads_v2 summary ----")
    logger.info("  input:            %s", args.input)
    logger.info("  rows_in:          %d", stats.rows_in)
    logger.info("  parse_errors:     %d", stats.rows_with_parse_error)
    logger.info("  nodules_in:       %d", stats.nodules_in)
    logger.info("  nodules_dropped_ln:     %d", stats.nodules_dropped_ln)
    logger.info("  nodules_dropped_fossa:  %d", stats.nodules_dropped_fossa)
    logger.info("  nodules_dropped_absurd: %d", stats.nodules_dropped_absurd)
    logger.info("  nodules_out:      %d", stats.nodules_out)
    if stats.enum_fixups:
        logger.info("  enum_fixups_by_field:")
        for k, v in sorted(stats.enum_fixups.items(), key=lambda kv: -kv[1]):
            logger.info("    %-35s %d", k, v)
    if stats.unit_fixups:
        logger.info("  unit_fixups:")
        for k, v in sorted(stats.unit_fixups.items(), key=lambda kv: -kv[1]):
            logger.info("    %-35s %d", k, v)
    logger.info("  wrote nodules:    %s (%d rows)", args.out_nodules, len(nodules_df))
    logger.info("  wrote reports:    %s (%d rows)", args.out_reports, len(reports_df))
    return 0


if __name__ == "__main__":
    sys.exit(main())

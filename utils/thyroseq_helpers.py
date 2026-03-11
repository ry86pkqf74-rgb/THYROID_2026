"""
Parsing and normalization helpers for ThyroSeq Excel workbook integration.

Used by scripts/41_ingest_thyroseq_excel.py.
"""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Identifier normalization
# ---------------------------------------------------------------------------

def normalize_mrn(x: Any) -> str | None:
    """Strip whitespace, trailing '.0', and non-digit chars; return numeric string or None."""
    if pd.isna(x) or x is None:
        return None
    s = str(x).strip().replace(".0", "")
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9]", "", s)
    return s or None


def normalize_dob(x: Any) -> date | None:
    """Parse DOB from Excel serial number, datetime, or string."""
    if pd.isna(x) or x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.date()
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        base = pd.Timestamp("1899-12-30")
        try:
            dt = base + pd.to_timedelta(int(x), unit="D")
            if 1900 <= dt.year <= 2020:
                return dt.date()
        except (ValueError, OverflowError):
            pass
        return None
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    try:
        n = float(s)
        if 1 < n < 100_000:
            dt = pd.Timestamp("1899-12-30") + pd.to_timedelta(int(n), unit="D")
            if 1900 <= dt.year <= 2020:
                return dt.date()
    except (ValueError, OverflowError):
        pass
    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.notna(dt):
        return dt.date()
    return None


def normalize_name(x: Any) -> dict[str, str | None]:
    """Parse 'Last, First Middle' into normalized components.

    Returns dict with keys: name_norm, last_name_norm, first_name_norm.
    """
    if pd.isna(x) or x is None:
        return {"name_norm": None, "last_name_norm": None, "first_name_norm": None}
    s = str(x).upper().strip()
    s = re.sub(r"[^A-Z,\s\-']", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return {"name_norm": None, "last_name_norm": None, "first_name_norm": None}

    parts = [p.strip() for p in s.split(",", 1)]
    last = parts[0] if parts else None
    first = parts[1].split()[0] if len(parts) > 1 and parts[1].strip() else None
    return {"name_norm": s, "last_name_norm": last, "first_name_norm": first}


def compute_row_hash(rec: dict) -> str:
    """Deterministic SHA-256 hash (24-char prefix) from identifying fields."""
    payload = "|".join([
        str(rec.get("Req Patient/Source Name", "")),
        str(rec.get("Pt. MRN", "")),
        str(rec.get("Date of Birth", "")),
        str(rec.get("Pathology", "")),
        str(rec.get("Thyroseq Mutation", "")),
        str(rec.get("Gene Fusions", "")),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


# ---------------------------------------------------------------------------
# Categorical normalization
# ---------------------------------------------------------------------------

_YES_VALS = {"yes", "y", "positive", "postive", "true", "1"}
_NO_VALS = {"no", "n", "negative", "negatiuve", "ngative", "negatiave", "neg",
            "false", "0", "none", "not submitted"}


def _clean(x: Any) -> str | None:
    if pd.isna(x) or x is None:
        return None
    s = str(x).strip().replace("\xa0", " ").strip()
    return s if s else None


def normalize_sex(x: Any) -> str | None:
    s = _clean(x)
    if s is None:
        return None
    low = s.lower()
    if low in ("f", "female"):
        return "Female"
    if low in ("m", "male"):
        return "Male"
    return s


def normalize_race(x: Any) -> str | None:
    s = _clean(x)
    if s is None:
        return None
    low = s.lower().strip()
    race_map = {
        "caucasian": "Caucasian", "white": "Caucasian",
        "african american": "African American", "black": "African American",
        "asian": "Asian", "korean": "Asian", "chinese": "Asian",
        "japanese": "Asian", "vietnamese": "Asian", "indian": "Asian",
        "hispanic": "Hispanic", "latino": "Hispanic", "latina": "Hispanic",
    }
    return race_map.get(low, s.title())


def normalize_tobacco(x: Any) -> bool | None:
    s = _clean(x)
    if s is None:
        return None
    return s.lower() in _YES_VALS


def normalize_margins(x: Any) -> str | None:
    s = _clean(x)
    if s is None:
        return None
    low = s.lower()
    if low in ("negative", "negatiuve", "neg"):
        return "negative"
    if low in ("positive", "postive"):
        return "positive"
    if low in ("close",):
        return "close"
    return low


def normalize_ete(x: Any) -> str | None:
    s = _clean(x)
    if s is None:
        return None
    low = s.lower()
    if low in ("negative", "ngative", "neg", "no", "none", "absent"):
        return "none"
    if low in ("positive", "postive", "yes", "present"):
        return "present"
    if "microscopic" in low or "minimal" in low:
        return "microscopic"
    if "gross" in low or "extensive" in low:
        return "gross"
    return low


def normalize_lymph_nodes(x: Any) -> dict[str, Any]:
    """Parse lymph node text into structured fields."""
    s = _clean(x)
    if s is None:
        return {"ln_status": None, "ln_raw": None}
    low = s.lower()
    if low in ("not submitted", "not sent", "n/a"):
        return {"ln_status": "not_submitted", "ln_raw": s}
    if any(neg in low for neg in ("negative", "negatiave", "neg")):
        return {"ln_status": "negative", "ln_raw": s}
    if any(pos in low for pos in ("positive", "postive")):
        count_m = re.search(r"(\d+)\s*/\s*(\d+)", s)
        if count_m:
            return {"ln_status": "positive", "ln_positive": int(count_m.group(1)),
                    "ln_examined": int(count_m.group(2)), "ln_raw": s}
        return {"ln_status": "positive", "ln_raw": s}
    return {"ln_status": "indeterminate", "ln_raw": s}


def normalize_angioinvasion(x: Any) -> str | None:
    s = _clean(x)
    if s is None:
        return None
    low = s.lower().replace("\xa0", "").strip()
    if low in ("negative", "neg", "no", "absent", "none"):
        return "absent"
    if "extensive" in low:
        return "extensive"
    if "focal" in low or "limited" in low:
        return "focal"
    if low in ("positive", "postive", "present"):
        return "present"
    return low


def normalize_multifocal(x: Any) -> str | None:
    s = _clean(x)
    if s is None:
        return None
    low = s.lower()
    if low in ("no", "n"):
        return "no"
    if low in ("yes", "y") or low.startswith("y "):
        return "yes"
    return low


def normalize_hashimoto_graves(x: Any) -> dict[str, Any]:
    s = _clean(x)
    if s is None:
        return {"autoimmune_raw": None, "hashimoto_flag": None, "graves_flag": None}
    low = s.lower()
    if low in _NO_VALS:
        return {"autoimmune_raw": s, "hashimoto_flag": False, "graves_flag": False}
    hashi = "hashimoto" in low or "hashimotos" in low
    graves = "graves" in low
    return {"autoimmune_raw": s, "hashimoto_flag": hashi or (low in _YES_VALS and not graves),
            "graves_flag": graves}


# ---------------------------------------------------------------------------
# Tg / TgAb / TSH panel parser
# ---------------------------------------------------------------------------

_TG_PATTERN = re.compile(
    r"^"
    r"(?P<tg_op>[<>]?)(?P<tg>[\d.]+)"
    r"\s*/\s*"
    r"(?P<tgab_op>[<>]?)(?P<tgab>[\d.]+)"
    r"\s*/\s*"
    r"(?P<tsh_op>[<>]?)(?P<tsh>[\d.]+)"
    r"(?:\s+(?P<tail>.*))?$",
    re.DOTALL,
)
_PANEL_DATE = re.compile(r"\((\d{1,2}/\d{1,2}/\d{2,4})\)")
_PANEL_DATE_APPROX = re.compile(r"\((\d{1,2}/\d{4})\)")
_STIM_FLAG = re.compile(r"\bstim\b", re.IGNORECASE)


def parse_tg_panel(text: Any) -> dict[str, Any]:
    """Parse composite Tg/TgAb/TSH string with embedded date.

    Supports formats like:
      '0.1/3.5/1.62 (7/29/2025) before completion surgery'
      '<0.1/2.1/.42 (11/6/2025)'
      '0.4/1.2/82.16 stim (1/28/2026)'
      '5/11/6 stim (8/2025, OSH)'
    """
    result: dict[str, Any] = {
        "thyroglobulin_value": None, "thyroglobulin_operator": None,
        "anti_tg_value": None, "anti_tg_operator": None,
        "tsh_value": None, "tsh_operator": None,
        "panel_date": None, "panel_date_precision": None,
        "stimulated_flag": False, "suffix": None,
        "parse_status": "unparsed", "raw_text": None,
    }
    s = _clean(text)
    if s is None:
        result["parse_status"] = "null_input"
        return result
    result["raw_text"] = s

    m = _TG_PATTERN.match(s.strip())
    if not m:
        result["parse_status"] = "parse_failed"
        return result

    def _safe_float(v: str) -> float | None:
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    result["thyroglobulin_value"] = _safe_float(m.group("tg"))
    result["thyroglobulin_operator"] = m.group("tg_op") or None
    result["anti_tg_value"] = _safe_float(m.group("tgab"))
    result["anti_tg_operator"] = m.group("tgab_op") or None
    result["tsh_value"] = _safe_float(m.group("tsh"))
    result["tsh_operator"] = m.group("tsh_op") or None

    tail = m.group("tail") or ""
    result["stimulated_flag"] = bool(_STIM_FLAG.search(s))

    dm = _PANEL_DATE.search(s)
    if dm:
        result["panel_date"] = pd.to_datetime(dm.group(1), errors="coerce",
                                               format=None, dayfirst=False)
        if pd.notna(result["panel_date"]):
            result["panel_date"] = result["panel_date"].strftime("%Y-%m-%d")
            result["panel_date_precision"] = "day"
        else:
            result["panel_date"] = None
    else:
        dm2 = _PANEL_DATE_APPROX.search(s)
        if dm2:
            parts = dm2.group(1).split("/")
            if len(parts) == 2:
                result["panel_date"] = f"{parts[1]}-{int(parts[0]):02d}-01"
                result["panel_date_precision"] = "month"

    suffix_text = _PANEL_DATE.sub("", _PANEL_DATE_APPROX.sub("", tail)).strip()
    suffix_text = _STIM_FLAG.sub("", suffix_text).strip(" ,;()")
    result["suffix"] = suffix_text if suffix_text else None
    result["parse_status"] = "ok"
    return result


# ---------------------------------------------------------------------------
# Surgery parser
# ---------------------------------------------------------------------------

_SURG_DATE = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")
_TT = re.compile(r"\bTT\b|total\s+thyroidectomy", re.IGNORECASE)
_COMPLETION = re.compile(r"\bcompletion\b", re.IGNORECASE)
_HEMI = re.compile(r"\bhemi(?:thyroidectomy)?\b|isthmusectomy|lobectomy", re.IGNORECASE)
_RIGHT = re.compile(r"\b(?:right|R)\b(?:\s+lobe)?", re.IGNORECASE)
_LEFT = re.compile(r"\b(?:left|L)\b(?:\s+lobe)?", re.IGNORECASE)
_OUTSIDE = re.compile(r"\boutside\b|\bOSH\b|\bexternal\b", re.IGNORECASE)


def parse_surgery_text(text: Any) -> dict[str, Any]:
    s = _clean(text)
    result: dict[str, Any] = {
        "surgery_raw": s,
        "total_thyroidectomy_flag": False,
        "completion_thyroidectomy_flag": False,
        "hemithyroidectomy_flag": False,
        "laterality": None,
        "outside_surgery_flag": False,
        "surgery_dates": [],
        "parse_status": "unparsed",
    }
    if s is None:
        result["parse_status"] = "null_input"
        return result

    result["total_thyroidectomy_flag"] = bool(_TT.search(s))
    result["completion_thyroidectomy_flag"] = bool(_COMPLETION.search(s))
    result["hemithyroidectomy_flag"] = bool(_HEMI.search(s))
    result["outside_surgery_flag"] = bool(_OUTSIDE.search(s))

    lat = []
    if _RIGHT.search(s):
        lat.append("right")
    if _LEFT.search(s):
        lat.append("left")
    result["laterality"] = "+".join(lat) if lat else None

    dates = _SURG_DATE.findall(s)
    parsed_dates = []
    for d in dates:
        dt = pd.to_datetime(d, errors="coerce", dayfirst=False)
        if pd.notna(dt) and 2000 <= dt.year <= 2030:
            parsed_dates.append(dt.strftime("%Y-%m-%d"))
    result["surgery_dates"] = parsed_dates
    result["parse_status"] = "ok" if (result["total_thyroidectomy_flag"]
                                       or result["completion_thyroidectomy_flag"]
                                       or result["hemithyroidectomy_flag"]
                                       or parsed_dates) else "partial"
    return result


# ---------------------------------------------------------------------------
# RAI parser
# ---------------------------------------------------------------------------

_RAI_DATE_EXACT = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")
_RAI_DATE_MONTH = re.compile(r"(\d{1,2}/\d{4})")
_RAI_NEGATED = re.compile(r"\b(?:none|no|refused|declined|not\s+given)\b", re.IGNORECASE)
_RAI_PENDING = re.compile(r"\b(?:pending|scheduled|planned|possible|possible\s+pending)\b",
                          re.IGNORECASE)
_RAI_YES = re.compile(r"\byes\b", re.IGNORECASE)


def parse_rai_text(text: Any) -> dict[str, Any]:
    s = _clean(text)
    result: dict[str, Any] = {
        "rai_raw": s,
        "rai_given_flag": None,
        "rai_dates": [],
        "rai_date_precision": None,
        "multiple_rai_flag": False,
        "rai_status": None,
        "outside_rai_flag": False,
        "parse_status": "unparsed",
    }
    if s is None:
        result["parse_status"] = "null_input"
        return result

    if isinstance(text, datetime):
        result["rai_given_flag"] = True
        result["rai_dates"] = [text.strftime("%Y-%m-%d")]
        result["rai_date_precision"] = "day"
        result["rai_status"] = "received"
        result["parse_status"] = "ok"
        return result

    low = s.lower().strip()
    if _RAI_NEGATED.search(low) and not _RAI_YES.search(low):
        result["rai_given_flag"] = False
        if "refused" in low or "declined" in low:
            result["rai_status"] = "refused"
        else:
            result["rai_status"] = "not_given"
        result["parse_status"] = "ok"
        return result

    if _RAI_PENDING.match(low):
        result["rai_given_flag"] = None
        result["rai_status"] = "pending"
        result["parse_status"] = "ok"
        return result

    result["outside_rai_flag"] = bool(_OUTSIDE.search(s))

    exact_dates = _RAI_DATE_EXACT.findall(s)
    parsed_day = []
    for d in exact_dates:
        dt = pd.to_datetime(d, errors="coerce", dayfirst=False)
        if pd.notna(dt) and 2000 <= dt.year <= 2030:
            parsed_day.append(dt.strftime("%Y-%m-%d"))

    # Also collect month-only dates (M/YYYY) that aren't substrings of day-level dates
    exact_positions = {m.start() for m in _RAI_DATE_EXACT.finditer(s)}
    parsed_month = []
    for m in _RAI_DATE_MONTH.finditer(s):
        if m.start() not in exact_positions:
            parts = m.group().split("/")
            if len(parts) == 2:
                try:
                    yr = int(parts[1])
                    if 2000 <= yr <= 2030:
                        parsed_month.append(f"{yr}-{int(parts[0]):02d}-01")
                except ValueError:
                    pass

    all_dates = parsed_month + parsed_day
    if all_dates:
        result["rai_given_flag"] = True
        result["rai_dates"] = all_dates
        result["rai_date_precision"] = "day" if parsed_day else "month"
        result["rai_status"] = "received"
        result["multiple_rai_flag"] = len(all_dates) > 1
        result["parse_status"] = "ok"
        return result

    if _RAI_YES.search(low):
        result["rai_given_flag"] = True
        result["rai_status"] = "received"
        result["parse_status"] = "partial"
        return result

    result["parse_status"] = "parse_failed"
    return result


# ---------------------------------------------------------------------------
# Imaging parser
# ---------------------------------------------------------------------------

_IMG_DATE = re.compile(r"\((\d{1,2}/\d{1,2}/\d{2,4})\)")
_IMG_DATE_APPROX = re.compile(r"\((\d{1,2}/\d{4})\)")
_IMG_DATE_PREFIX = re.compile(r"^(\d{1,2}/\d{1,2}/\d{2,4})\b")
_NEG_IMG = re.compile(r"\bnegative\b|\bno\s+abnormal\b|\bunremarkable\b|\bnormal\b",
                       re.IGNORECASE)
_THYROID_BED = re.compile(r"thyroid\s+bed|thyroid\s+fossa", re.IGNORECASE)
_FOCAL_UPTAKE = re.compile(r"focal\s+uptake|focal\s+iodine", re.IGNORECASE)
_LUNG = re.compile(r"pulmonary|lung", re.IGNORECASE)
_SUSPICIOUS_NODE = re.compile(r"suspicious\s+(?:lymph\s*)?nod|abnormal\s+nod|indeterminate\s+(?:lymph\s*)?nod",
                               re.IGNORECASE)
_RESIDUAL = re.compile(r"residual\s+(?:thyroid\s+)?tissue", re.IGNORECASE)


def parse_imaging_text(text: Any, modality: str) -> dict[str, Any]:
    s = _clean(text)
    result: dict[str, Any] = {
        "modality": modality,
        "imaging_raw": s,
        "imaging_dates": [],
        "imaging_date_precision": None,
        "negative_flag": None,
        "thyroid_bed_uptake": None,
        "focal_uptake": None,
        "pulmonary_findings": None,
        "suspicious_nodal_disease": None,
        "residual_tissue": None,
        "parse_status": "unparsed",
    }
    if s is None or s.lower() in ("none", "n/a", ""):
        result["parse_status"] = "null_input"
        return result

    low = s.lower()
    result["negative_flag"] = bool(_NEG_IMG.search(low))
    result["thyroid_bed_uptake"] = bool(_THYROID_BED.search(low) and _FOCAL_UPTAKE.search(low))
    result["focal_uptake"] = bool(_FOCAL_UPTAKE.search(low))
    result["pulmonary_findings"] = bool(_LUNG.search(low))
    result["suspicious_nodal_disease"] = bool(_SUSPICIOUS_NODE.search(low))
    result["residual_tissue"] = bool(_RESIDUAL.search(low))

    dates = []
    for m in _IMG_DATE.finditer(s):
        dt = pd.to_datetime(m.group(1), errors="coerce", dayfirst=False)
        if pd.notna(dt) and 2000 <= dt.year <= 2030:
            dates.append(dt.strftime("%Y-%m-%d"))
    pm = _IMG_DATE_PREFIX.match(s)
    if pm:
        dt = pd.to_datetime(pm.group(1), errors="coerce", dayfirst=False)
        if pd.notna(dt) and 2000 <= dt.year <= 2030:
            d = dt.strftime("%Y-%m-%d")
            if d not in dates:
                dates.insert(0, d)

    if dates:
        result["imaging_dates"] = dates
        result["imaging_date_precision"] = "day"
    else:
        for m in _IMG_DATE_APPROX.finditer(s):
            parts = m.group(1).split("/")
            if len(parts) == 2:
                try:
                    yr = int(parts[1])
                    if 2000 <= yr <= 2030:
                        dates.append(f"{yr}-{int(parts[0]):02d}-01")
                except ValueError:
                    pass
        if dates:
            result["imaging_dates"] = dates
            result["imaging_date_precision"] = "month"

    result["parse_status"] = "ok"
    return result


# ---------------------------------------------------------------------------
# Mutation / molecular parser
# ---------------------------------------------------------------------------

_BRAF = re.compile(r"\bBRAF\b", re.IGNORECASE)
_NRAS = re.compile(r"\bNRAS\b", re.IGNORECASE)
_HRAS = re.compile(r"\bHRAS\b", re.IGNORECASE)
_KRAS = re.compile(r"\bKRAS\b", re.IGNORECASE)
_TERT = re.compile(r"\bTERT\b", re.IGNORECASE)
_TP53 = re.compile(r"\bTP53\b", re.IGNORECASE)
_PIK3CA = re.compile(r"\bPIK3CA\b", re.IGNORECASE)
_TSHR = re.compile(r"\bTSHR\b", re.IGNORECASE)
_EIF1AX = re.compile(r"\bEIF1AX\b", re.IGNORECASE)
_DICER1 = re.compile(r"\bDICER1\b", re.IGNORECASE)
_AF_PAT = re.compile(r"AF\s*[<>]?\s*([\d.]+)%?", re.IGNORECASE)

_FUSION_RET = re.compile(r"\bRET\b", re.IGNORECASE)
_FUSION_NTRK = re.compile(r"\bNTRK\d?\b", re.IGNORECASE)
_FUSION_ALK = re.compile(r"\bALK\b", re.IGNORECASE)
_FUSION_PPARG = re.compile(r"\bPPARG\b|\bPAX8[/-]PPARG\b", re.IGNORECASE)


def parse_mutation_text(text: Any) -> dict[str, Any]:
    s = _clean(text)
    result: dict[str, Any] = {
        "mutation_raw": s,
        "braf_flag": False, "braf_detail": None,
        "ras_flag": False, "ras_subtype": None,
        "tert_flag": False, "tp53_flag": False,
        "pik3ca_flag": False, "tshr_flag": False,
        "eif1ax_flag": False, "dicer1_flag": False,
        "negative_flag": False,
        "allele_fractions": {},
        "mutation_other": [],
        "parse_status": "unparsed",
    }
    if s is None:
        result["parse_status"] = "null_input"
        return result

    low = s.lower().strip()
    if low.startswith("negative"):
        result["negative_flag"] = True
        result["parse_status"] = "ok"
        return result

    if _BRAF.search(s):
        result["braf_flag"] = True
        result["braf_detail"] = s
    ras_types = []
    if _NRAS.search(s):
        ras_types.append("NRAS")
    if _HRAS.search(s):
        ras_types.append("HRAS")
    if _KRAS.search(s):
        ras_types.append("KRAS")
    if ras_types:
        result["ras_flag"] = True
        result["ras_subtype"] = ",".join(ras_types)
    result["tert_flag"] = bool(_TERT.search(s))
    result["tp53_flag"] = bool(_TP53.search(s))
    result["pik3ca_flag"] = bool(_PIK3CA.search(s))
    result["tshr_flag"] = bool(_TSHR.search(s))
    result["eif1ax_flag"] = bool(_EIF1AX.search(s))
    result["dicer1_flag"] = bool(_DICER1.search(s))

    for af_m in _AF_PAT.finditer(s):
        ctx_start = max(0, af_m.start() - 30)
        ctx = s[ctx_start:af_m.start()].upper()
        gene = None
        for g in ("BRAF", "NRAS", "HRAS", "KRAS", "TERT", "TP53", "PIK3CA", "TSHR"):
            if g in ctx:
                gene = g
                break
        try:
            result["allele_fractions"][gene or "unknown"] = float(af_m.group(1))
        except ValueError:
            pass

    result["parse_status"] = "ok"
    return result


def parse_fusion_text(text: Any) -> dict[str, Any]:
    s = _clean(text)
    result: dict[str, Any] = {
        "fusion_raw": s,
        "fusion_flag": False,
        "ret_flag": False, "ntrk_flag": False,
        "alk_flag": False, "pparg_flag": False,
        "fusion_genes": [],
        "parse_status": "unparsed",
    }
    if s is None:
        result["parse_status"] = "null_input"
        return result

    low = s.lower().strip()
    if low in ("negative", "no data", "no"):
        result["parse_status"] = "ok"
        return result
    if low.startswith("failed"):
        result["parse_status"] = "test_failed"
        result["fusion_raw"] = s
        return result

    result["fusion_flag"] = True
    result["ret_flag"] = bool(_FUSION_RET.search(s))
    result["ntrk_flag"] = bool(_FUSION_NTRK.search(s))
    result["alk_flag"] = bool(_FUSION_ALK.search(s))
    result["pparg_flag"] = bool(_FUSION_PPARG.search(s))

    fusions = re.split(r"[,;]", s)
    result["fusion_genes"] = [f.strip() for f in fusions if f.strip()]
    result["parse_status"] = "ok"
    return result


def parse_cna(text: Any) -> str | None:
    s = _clean(text)
    if s is None:
        return None
    low = s.lower().replace("\xa0", "").strip()
    if low in ("negative", "neg"):
        return "negative"
    if "high" in low:
        return "positive_high"
    if low in ("positive", "postive"):
        return "positive"
    if "non informative" in low:
        return "non_informative"
    return low


def parse_gep(text: Any) -> str | None:
    s = _clean(text)
    if s is None:
        return None
    low = s.lower()
    if low == "negative":
        return "negative"
    if "positive" in low:
        return "positive"
    if low == "failed":
        return "failed"
    return low


# ---------------------------------------------------------------------------
# Days-to-Tg parser
# ---------------------------------------------------------------------------

def parse_days_to_tg(x: Any) -> int | None:
    """Parse 'Days to Tg from Surgery' — may be int, float, or text with embedded number."""
    if pd.isna(x) or x is None:
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        v = int(x)
        return v if 0 <= v <= 10000 else None
    s = str(x).strip()
    m = re.match(r"^(\d+)", s)
    if m:
        v = int(m.group(1))
        return v if 0 <= v <= 10000 else None
    return None

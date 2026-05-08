#!/usr/bin/env python3
"""Script 411 — TI-RADS nodule primitive regex+heuristic extractor v1.

Tier 1 of the Phase A.3 hybrid pipeline. Reads
pub_workspace.tirads_primitive_backfill_input_v1, runs deterministic
regex extraction on every row with non-empty source_text, and writes
results to pub_workspace.tirads_primitive_regex_v1_v1.

Designed as a sibling of exports/ln_multimodal_20260507/extract_ln_multimodal_v3.py.
Reuses the sentence-split, PHI-strip, and evidence-sanitization helpers
but applies nodule-specific feature lexicons rather than LN-specific ones.

Usage:
    GCP_TOKEN=$(gcloud auth print-access-token) \
      .venv/bin/python3 scripts/411_tirads_primitive_regex_v1.py [--dry-run] [--limit N]
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Optional BQ import — only needed for the __main__ runner
# ---------------------------------------------------------------------------
try:
    from google.cloud import bigquery
    from google.oauth2.credentials import Credentials
    import os as _os
    _HAS_BQ = True
except ImportError:
    _HAS_BQ = False

PROJECT = "thyroid-canonical-pub-2026"
LOCATION = "us-central1"
INPUT_TABLE = f"{PROJECT}.pub_workspace.tirads_primitive_backfill_input_v1"
OUTPUT_TABLE = f"{PROJECT}.pub_workspace.tirads_primitive_regex_v1_v1"

# ---------------------------------------------------------------------------
# Shared helpers (sentence tokenisation + evidence sanitisation)
# ---------------------------------------------------------------------------

# Sentence split: period/!/?  followed by whitespace then capital. Avoids decimals.
SENT_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z])|;\s+(?=[A-Z])")

# Strip full date-of-service patterns (PHI)
_DOS_RE = re.compile(r"\b\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b")
# Strip MRN-like patterns: 7–10 consecutive digits (but not sizes like "12 mm")
_MRN_RE = re.compile(r"\b\d{7,10}\b(?!\s*(?:mm|cm|Hz|kg|lb))")


def split_sentences(text: str) -> list[str]:
    """Sentence-level tokenisation, preserving boundary context."""
    if not text:
        return []
    return [s.strip() for s in SENT_SPLIT.split(text) if s.strip()]


def sanitize_evidence(text: str, max_chars: int = 140) -> Optional[str]:
    """Strip PHI, normalise whitespace, truncate to max_chars."""
    if not text:
        return None
    t = _DOS_RE.sub("[date]", text)
    t = _MRN_RE.sub("[id]", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:max_chars] if t else None


# ---------------------------------------------------------------------------
# Negation helpers
# ---------------------------------------------------------------------------

# A negation prefix immediately before the feature mention (within the same
# sentence segment split by comma/semicolon/parenthesis).
_NEG_PREFIX = re.compile(
    r"\b(no|not|without|absent|absence of|deny|denied|denies|"
    r"negative for|ruled out|free of|lack of|lacking)\b",
    re.IGNORECASE,
)


def _negated(sentence: str, feature_match: re.Match) -> bool:
    """Return True if a negation token precedes the match in the same clause.

    Clause boundaries: comma, semicolon, parenthesis, period, !, ?.
    This prevents "No solid component identified. Entirely cystic lesion."
    from triggering negation on "Entirely cystic" because of the "No" in
    the prior clause.
    """
    start = feature_match.start()
    clause_start = max(
        start - 120,
        max(
            (m.end() for m in re.finditer(r"[,;(.!?]", sentence[:start])),
            default=0,
        ),
    )
    prefix = sentence[clause_start:start]
    return bool(_NEG_PREFIX.search(prefix))


# ---------------------------------------------------------------------------
# Composition lexicon
# ---------------------------------------------------------------------------

_COMP_PATTERNS: list[tuple[re.Pattern, str]] = [
    # Must check spongiform before cystic (spongiform has cystic-like features)
    (re.compile(r"\bspongiform\b", re.I), "spongiform"),
    # Purely/completely/almost cystic → cystic
    (re.compile(r"\b(pure(?:ly)?|completely|almost entirely|entirely)\s+cystic\b", re.I), "cystic"),
    # Mixed / complex
    (re.compile(
        r"\b(mixed\s+cystic[- ](?:and[- ])?solid|"
        r"complex\s+cystic|cystic\s+and\s+solid|"
        r"partially\s+cystic|mixed\s+solid[- ](?:and[- ])?cystic)\b",
        re.I,
    ), "mixed_cystic_solid"),
    # predominantly solid
    (re.compile(r"\bpredominantly\s+solid\b", re.I), "predominantly_solid"),
    # predominantly cystic (NOT a TI-RADS 5-tier; maps to mixed or cystic depending on context)
    (re.compile(r"\bpredominantly\s+cystic\b", re.I), "mixed_cystic_solid"),
    # simple cystic (after checking predominantly_solid — this is the fallback cystic)
    (re.compile(r"\bcystic\b", re.I), "cystic"),
    # solid (last resort — only if nothing else matched)
    (re.compile(r"\bsolid\b", re.I), "solid"),
]


def extract_composition(sentences: list[str]) -> Optional[str]:
    for sent in sentences:
        for pat, label in _COMP_PATTERNS:
            m = pat.search(sent)
            if m and not _negated(sent, m):
                return label
    return None


# ---------------------------------------------------------------------------
# Echogenicity lexicon
# ---------------------------------------------------------------------------

_ECHO_PATTERNS: list[tuple[re.Pattern, str]] = [
    # very_hypoechoic must come before plain hypoechoic
    (re.compile(
        r"\b(mark(?:ed)?ly\s+hypoechoic|very\s+hypoechoic|"
        r"profoundly\s+hypoechoic|marked\s+hypoechogenicity|"
        r"markedly\s+hypoechoic)\b",
        re.I,
    ), "very_hypoechoic"),
    (re.compile(r"\banechoic\b", re.I), "anechoic"),
    (re.compile(r"\bhyperechoic\b", re.I), "hyperechoic"),
    (re.compile(r"\bisoechoic\b", re.I), "isoechoic"),
    (re.compile(r"\bhypoechoic\b", re.I), "hypoechoic"),
]


def extract_echogenicity(sentences: list[str]) -> Optional[str]:
    for sent in sentences:
        for pat, label in _ECHO_PATTERNS:
            m = pat.search(sent)
            if m and not _negated(sent, m):
                return label
    return None


# ---------------------------------------------------------------------------
# Shape lexicon
# ---------------------------------------------------------------------------

_TALLER_PAT = re.compile(
    r"\b(taller\s+than\s+(?:wide|tall)|"
    r"AP\s+(?:dimension\s+)?(?:>|greater\s+than)\s+transverse|"
    r"anterior[- ]posterior\s+dimension\s+(?:is\s+)?greater(?:\s+than)?|"
    r"depth\s+(?:>|greater\s+than|exceeds?)\s+width|"
    r"taller[\s-]+than[\s-]+wide)\b",
    re.I,
)
_WIDER_PAT = re.compile(
    r"\b(wider\s+than\s+tall|"
    r"transverse\s+(?:>|greater\s+than)\s+AP|"
    r"width\s+(?:>|greater\s+than|exceeds?)\s+(?:height|depth)|"
    r"wider[\s-]+than[\s-]+tall)\b",
    re.I,
)


def extract_shape(sentences: list[str]) -> Optional[str]:
    for sent in sentences:
        m = _TALLER_PAT.search(sent)
        if m and not _negated(sent, m):
            return "taller_than_wide"
        m = _WIDER_PAT.search(sent)
        if m and not _negated(sent, m):
            return "wider_than_tall"
    return None


# ---------------------------------------------------------------------------
# Margins lexicon
# ---------------------------------------------------------------------------

_MARGIN_PATTERNS: list[tuple[re.Pattern, str]] = [
    # ETE must come first (most specific)
    (re.compile(
        r"\b(extrathyroidal\s+extension|"
        r"strap\s+muscle\s+invasion|"
        r"transcapsular\s+(?:invasion|extension)|"
        r"extra(?:thyroidal|capsular)\s+(?:spread|extension))\b",
        re.I,
    ), "extrathyroidal_extension"),
    # Spiculated is a subset of irregular
    (re.compile(r"\b(spiculated|irregular\s+margins?|irregular\s+border)\b", re.I), "irregular"),
    (re.compile(r"\bmicrolobulated\b", re.I), "microlobulated"),
    (re.compile(r"\blobulated\b", re.I), "lobulated"),
    (re.compile(r"\b(ill[- ]defined|poorly\s+defined|indistinct\s+margin|"
                r"poorly\s+marginated)\b", re.I), "ill_defined"),
    (re.compile(r"\b(smooth(?:\s+margin)?|well[- ](?:circumscribed|defined|marginated)|"
                r"sharp(?:\s+margin)?|well\s+demarcated)\b", re.I), "smooth"),
]


def extract_margins(sentences: list[str]) -> Optional[str]:
    for sent in sentences:
        for pat, label in _MARGIN_PATTERNS:
            m = pat.search(sent)
            if m and not _negated(sent, m):
                return label
    return None


# ---------------------------------------------------------------------------
# Echogenic foci lexicon (multi-value, returns list)
# ---------------------------------------------------------------------------

_EF_PATTERNS: list[tuple[re.Pattern, str]] = [
    # none — full-word patterns, trailing \b safe
    (re.compile(
        r"\b(no\s+(?:echogenic\s+foci|calcifi\w*|microcalcifi\w*)|"
        r"without\s+calcifi\w*|absent\s+(?:echogenic\s+foci|calcifi\w*))",
        re.I,
    ), "none"),
    # large comet-tail (must precede punctate)
    (re.compile(
        r"\b(large\s+comet[- ]tail|comet[- ]tail\s+artifact|ring[- ]down\s+artifact)\b",
        re.I,
    ), "large_comet_tail_artifacts"),
    # peripheral rim — partial stem "calcifi" followed by word chars
    (re.compile(
        r"\b(peripheral\s+(?:rim\s+)?calcifi\w*|eggshell\s+calcifi\w*|"
        r"rim\s+calcifi\w*|peripheral\s+calcifi\w*)",
        re.I,
    ), "peripheral_rim_calcifications"),
    # macrocalcifications (coarse / chunky) — partial stems
    (re.compile(
        r"\b(macrocalcifi\w*|coarse\s+calcifi\w*|chunky\s+calcifi\w*|"
        r"large\s+calcifi\w*|discrete\s+calcifi\w*)",
        re.I,
    ), "macrocalcifications"),
    # punctate echogenic foci / microcalcifications — partial stems
    (re.compile(
        r"\b(punctate\s+echogenic\s+foci|punctate\s+calcifi\w*|"
        r"microcalcifi\w*|echogenic\s+foci|tiny\s+calcifi\w*)",
        re.I,
    ), "punctate_echogenic_foci"),
]

# Negation patterns specific to echogenic foci (broader scope)
_EF_NEG = re.compile(
    r"\b(no|not|without|absent|absence\s+of|no\s+evidence\s+of|"
    r"free\s+of|negative\s+for)\b",
    re.I,
)


def _ef_negated(sentence: str, match: re.Match) -> bool:
    start = match.start()
    # Look up to 60 chars back within same clause
    clause_start = max(
        start - 60,
        max(
            (m.end() for m in re.finditer(r"[,;(]", sentence[:start])),
            default=0,
        ),
    )
    prefix = sentence[clause_start:start]
    return bool(_EF_NEG.search(prefix))


def extract_echogenic_foci(sentences: list[str]) -> list[str]:
    """Returns sorted, deduplicated list of matched echogenic-foci categories."""
    found: set[str] = set()
    none_negated = False

    for sent in sentences:
        for pat, label in _EF_PATTERNS:
            m = pat.search(sent)
            if not m:
                continue
            if label == "none":
                found.add("none")
                none_negated = False
            elif _ef_negated(sent, m):
                # e.g. "no punctate echogenic foci" — record explicit none
                if "none" not in found:
                    found.add("none")
            else:
                found.add(label)

    # Semantic dedup: if we have concrete findings AND "none", remove "none"
    if "none" in found and len(found) > 1:
        found.discard("none")

    # If nothing found, return empty (caller decides default)
    return sorted(found) if found else []


# ---------------------------------------------------------------------------
# Halo extraction (returns dict matching JSON schema)
# ---------------------------------------------------------------------------

_HALO_PRESENT = re.compile(
    r"\b(halo|hypoechoic\s+rim|perilesional\s+halo|surrounding\s+halo)\b", re.I
)
_HALO_ABSENT = re.compile(r"\b(no\s+halo|absent\s+halo|without\s+(?:a\s+)?halo)\b", re.I)
_HALO_COMPLETE = re.compile(r"\bcomplete\s+halo\b", re.I)
_HALO_INCOMPLETE = re.compile(r"\b(incomplete|partial)\s+halo\b", re.I)
_HALO_THIN = re.compile(r"\bthin\s+halo\b", re.I)
_HALO_THICK = re.compile(r"\bthick\s+halo\b", re.I)
_HALO_REGULAR = re.compile(r"\bregular\s+halo\b", re.I)
_HALO_IRREGULAR = re.compile(r"\birregular\s+halo\b", re.I)
_HYPO_RIM = re.compile(r"\bhypoechoic\s+rim\b", re.I)
_DOPPLER_RING = re.compile(
    r"\bring\s+of\s+(?:vascularity|flow|color)|"
    r"peripheral\s+rim\s+of\s+(?:vascularity|flow)|"
    r"ring\s+vascularity|doppler\s+ring",
    re.I,
)


def extract_halo(sentences: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "presence": "unstated",
        "completeness": None,
        "thickness": None,
        "regularity": None,
        "hypoechoic_rim_wording_present": None,
        "doppler_ring_present": "unstated",
    }
    for sent in sentences:
        if _HALO_ABSENT.search(sent):
            result["presence"] = "absent"
        elif _HALO_PRESENT.search(sent) and not _NEG_PREFIX.search(sent[:_HALO_PRESENT.search(sent).start()]):
            result["presence"] = "present"
            if _HALO_COMPLETE.search(sent):
                result["completeness"] = "complete"
            elif _HALO_INCOMPLETE.search(sent):
                result["completeness"] = "incomplete"
            if _HALO_THIN.search(sent):
                result["thickness"] = "thin"
            elif _HALO_THICK.search(sent):
                result["thickness"] = "thick"
            if _HALO_REGULAR.search(sent) and not _HALO_IRREGULAR.search(sent):
                result["regularity"] = "regular"
            elif _HALO_IRREGULAR.search(sent):
                result["regularity"] = "irregular"
        if _HYPO_RIM.search(sent):
            result["hypoechoic_rim_wording_present"] = True
            if result["presence"] == "unstated":
                result["presence"] = "present"
        if _DOPPLER_RING.search(sent):
            result["doppler_ring_present"] = "true"
    return result


# ---------------------------------------------------------------------------
# Vascularity extraction
# ---------------------------------------------------------------------------

_VASC_INFERNO = re.compile(r"\bthyroid\s+inferno\b", re.I)
_VASC_MARKED = re.compile(
    r"\bmarkedly\s+increased\s+(?:vasc\w*|flow)|"
    r"\bmarkedly\s+hypervasc\w*|"
    r"\bhypervascular(?:ity)?\b",
    re.I,
)
_VASC_INCREASED = re.compile(
    r"\bincreased\s+(?:vasc\w*|flow|blood\s+flow|doppler\w*)|"
    r"\belevated\s+(?:vasc\w*|flow)",
    re.I,
)
_VASC_NORMAL = re.compile(
    r"\bnormal\s+(?:vasc\w*|flow|doppler\w*)|"
    r"\bnormal\s+(?:blood\s+)?flow",
    re.I,
)
_VASC_ABSENT = re.compile(
    r"\b(absent|no|without)\s+(?:internal\s+)?(?:vasc\w*|flow|doppler\w*)|"
    r"\bavascu(?:lar|larity)\b",
    re.I,
)
_VASC_PERIPHERAL = re.compile(
    r"\bperipheral\s+(?:vasc\w*|flow|doppler\w*|blood\s+flow)", re.I
)
_VASC_CENTRAL = re.compile(
    r"\bcentral(?:\s+hilar)?\s+(?:vasc\w*|flow|doppler\w*)", re.I
)
_VASC_INTRAN = re.compile(
    r"\b(intranodular|internal|intrinsic)\s+(?:vasc\w*|flow)|"
    r"\bchaotic\s+(?:vasc\w*|flow)|"
    r"\bdisorganized\s+(?:vasc\w*|flow)",
    re.I,
)
_VASC_MIXED = re.compile(r"\bmixed\s+(?:vasc\w*|flow|doppler\w*)", re.I)


def extract_vascularity(sentences: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "intensity": "unstated",
        "distribution": "unstated",
        "doppler_descriptors": "unstated",
    }
    desc_parts: list[str] = []
    for sent in sentences:
        if _VASC_INFERNO.search(sent):
            result["intensity"] = "thyroid_inferno"
        elif _VASC_MARKED.search(sent):
            result["intensity"] = "markedly_increased"
        elif _VASC_INCREASED.search(sent):
            result["intensity"] = "increased"
        elif _VASC_NORMAL.search(sent):
            result["intensity"] = "normal"
        elif _VASC_ABSENT.search(sent):
            result["intensity"] = "none"

        if _VASC_INTRAN.search(sent):
            result["distribution"] = "intranodular"
        elif _VASC_PERIPHERAL.search(sent):
            result["distribution"] = "peripheral"
        elif _VASC_CENTRAL.search(sent):
            result["distribution"] = "central_hilum"
        elif _VASC_MIXED.search(sent):
            result["distribution"] = "mixed"
        elif _VASC_ABSENT.search(sent):
            result["distribution"] = "absent"

        # Collect brief doppler descriptors
        m = re.search(
            r"\b((?:marked(?:ly)?|increased|normal|absent|peripheral|central|intranodular|mixed)\s+"
            r"(?:vasc\w*|flow|doppler\w*))",
            sent,
            re.I,
        )
        if m:
            desc_parts.append(m.group(1).lower())

    if desc_parts:
        result["doppler_descriptors"] = "; ".join(dict.fromkeys(desc_parts))[:80]
    return result


# ---------------------------------------------------------------------------
# ETE on US extraction
# ---------------------------------------------------------------------------

_ETE_STRAP = re.compile(
    r"\bstrap\s+muscle\s+(?:invasion|involvement|infiltration)\b", re.I
)
_ETE_CAPSULE = re.compile(
    r"\b(loss\s+of\s+capsule(?:\s+line)?|capsular\s+disruption|"
    r"breach\s+(?:of\s+)?(?:the\s+)?capsule|disrupted\s+capsule)\b",
    re.I,
)
_ETE_BULGE = re.compile(
    r"\b(contour\s+bulging|outward\s+bulge|bulging\s+(?:of\s+the\s+)?capsule|"
    r"focal\s+bulge)\b",
    re.I,
)
_ETE_ABUT = re.compile(
    r"\b(abuts?\s+(?:the\s+)?capsule|abutment|capsular\s+abutment|"
    r"touches?\s+capsule)\b",
    re.I,
)
_ETE_NONE = re.compile(
    r"\b(no\s+extrathyroidal(?:\s+extension)?|no\s+ete|"
    r"no\s+(?:capsular|strap)|confined\s+to\s+the\s+thyroid|"
    r"no\s+evidence\s+of\s+(?:ete|extrathyroidal))\b",
    re.I,
)
_ABUT_PCT = re.compile(r"(\d+(?:\.\d+)?)\s*%\s*(?:of\s+(?:the\s+)?(?:perimeter|capsule))", re.I)
_ETE_GRADE = re.compile(r"\b(?:ETE\s+)?grade\s+([123])\b", re.I)


def extract_ete_us(sentences: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "presence": "unstated",
        "abutment_percent_perimeter": None,
        "grade": None,
        "transcapsular_vascularity_present": "unstated",
    }
    for sent in sentences:
        if _ETE_NONE.search(sent):
            result["presence"] = "none"
        elif _ETE_STRAP.search(sent) and not _NEG_PREFIX.search(sent[:_ETE_STRAP.search(sent).start()]):
            result["presence"] = "strap_muscle_invasion"
        elif _ETE_CAPSULE.search(sent) and not _NEG_PREFIX.search(sent[:_ETE_CAPSULE.search(sent).start()]):
            result["presence"] = "capsule_loss"
        elif _ETE_BULGE.search(sent) and not _NEG_PREFIX.search(sent[:_ETE_BULGE.search(sent).start()]):
            if result["presence"] in ("unstated", "none"):
                result["presence"] = "bulging"
        elif _ETE_ABUT.search(sent) and not _NEG_PREFIX.search(sent[:_ETE_ABUT.search(sent).start()]):
            if result["presence"] in ("unstated", "none"):
                result["presence"] = "abutment"

        m_pct = _ABUT_PCT.search(sent)
        if m_pct:
            result["abutment_percent_perimeter"] = float(m_pct.group(1))
            pct = float(m_pct.group(1))
            if pct <= 25:
                result["grade"] = 1
            elif pct <= 50:
                result["grade"] = 2
            else:
                result["grade"] = 3

        m_grade = _ETE_GRADE.search(sent)
        if m_grade:
            result["grade"] = int(m_grade.group(1))

        if re.search(r"\btranscapsular\s+(?:vasc|flow)\b", sent, re.I):
            result["transcapsular_vascularity_present"] = "true"

    return result


# ---------------------------------------------------------------------------
# TI-RADS reported system hint
# ---------------------------------------------------------------------------

_TIRADS_SYSTEM_HINTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(Kwak(?:[- ]?TIRADS)?|K[-\s]?TIRADS)\b", re.I), "Kwak"),
    (re.compile(r"\bEU[-\s]?TIRADS\b", re.I), "EU"),
    (re.compile(r"\bK[-\s]?TIRADS\b", re.I), "KTIRADS"),
    # ACR 2017 if the mention is from >= 2017 (exam_date handled in caller)
    (re.compile(r"\b(?:ACR[- ]?)?TI[-\s]?RADS\s*[1-5]?\b", re.I), "ACR2017"),
    (re.compile(r"\bTR\s*[1-5]\b", re.I), "ACR2017"),
]


def extract_tirads_system_hint(
    sentences: list[str], exam_date: Optional[datetime.date] = None
) -> Optional[str]:
    for sent in sentences:
        for pat, label in _TIRADS_SYSTEM_HINTS:
            if pat.search(sent):
                if label == "ACR2017":
                    if exam_date and exam_date < datetime.date(2017, 1, 1):
                        return "unspecified"
                    return "ACR2017"
                return label
    return None


# ---------------------------------------------------------------------------
# Additional binary / categorical fields
# ---------------------------------------------------------------------------

_ENTIRELY_CALC = re.compile(r"\b(entirely\s+calcified|completely\s+calcified|"
                             r"diffusely\s+calcified)\b", re.I)
_HOMO_ECHO = re.compile(r"\bhomogeneous(?:ly)?\s+(?:echotexture|echogenicity|echo)\b", re.I)
_HETERO_ECHO = re.compile(r"\bheterogeneous(?:ly)?\s+(?:echotexture|echogenicity|echo)\b", re.I)
_RIM_INTACT = re.compile(r"\b(intact\s+(?:peripheral\s+)?(?:rim|shell)\s+calcifi|"
                          r"intact\s+eggshell)\b", re.I)
_RIM_DISRUPTED = re.compile(r"\b(disrupted?\s+(?:rim|calcifi|shell)|"
                              r"broken\s+(?:rim|calcifi|shell))\b", re.I)
_INTERVAL_GROWTH = re.compile(
    r"\b(interval\s+growth|enlarging|increased\s+in\s+size|"
    r"grown\s+(?:since|compared)|new\s+(?:nodule|lesion))\b",
    re.I,
)
_STABLE = re.compile(r"\b(stable|unchanged|no\s+(?:significant\s+)?change)\b", re.I)

# Chammas vascularity type (I–V)
_CHAMMAS = re.compile(r"\bChammas\s+(?:type\s+|pattern\s+)?([IVX1-5]+)\b", re.I)
_CHAMMAS_NUM = {"I": "I", "II": "II", "III": "III", "IV": "IV", "V": "V",
                "1": "I", "2": "II", "3": "III", "4": "IV", "5": "V"}


def extract_misc(sentences: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "chammas_type": None,
        "entirely_calcified": None,
        "homogeneous_echotexture": None,
        "rim_calcification_subtype": None,
        "interval_growth": None,
    }
    for sent in sentences:
        if _ENTIRELY_CALC.search(sent):
            result["entirely_calcified"] = True

        if _HOMO_ECHO.search(sent):
            result["homogeneous_echotexture"] = True
        elif _HETERO_ECHO.search(sent):
            result["homogeneous_echotexture"] = False

        if _RIM_DISRUPTED.search(sent):
            result["rim_calcification_subtype"] = "disrupted"
        elif _RIM_INTACT.search(sent):
            result["rim_calcification_subtype"] = "intact"

        if _INTERVAL_GROWTH.search(sent) and not _STABLE.search(sent):
            result["interval_growth"] = True
        elif _STABLE.search(sent) and result["interval_growth"] is None:
            result["interval_growth"] = False

        m = _CHAMMAS.search(sent)
        if m:
            v = m.group(1).upper()
            result["chammas_type"] = _CHAMMAS_NUM.get(v)

    return result


# ---------------------------------------------------------------------------
# Evidence extraction helper
# ---------------------------------------------------------------------------

def _best_evidence(sentences: list[str], features_found: int) -> Optional[str]:
    """Pick the most information-dense sentence as evidence."""
    if not sentences:
        return None
    # Score sentences by feature density
    scored = []
    feature_words = re.compile(
        r"\b(solid|cystic|echogenic|hypoechoic|hyperechoic|isoechoic|anechoic|"
        r"smooth|irregular|calcifi|halo|vascu|flow|doppler|ete|capsule|bulg|abut|"
        r"spong|lobul|taller|wider|margin|composition|echotexture)\b",
        re.I,
    )
    for s in sentences:
        score = len(feature_words.findall(s))
        scored.append((score, s))
    best = max(scored, key=lambda x: x[0])[1]
    return sanitize_evidence(best)


# ---------------------------------------------------------------------------
# Top-level feature extractor
# ---------------------------------------------------------------------------

def extract_nodule_features(
    source_text: str,
    gland_context: dict,
    known_features: dict,
) -> dict[str, Any]:
    """Extract TI-RADS nodule features from free text.

    Parameters
    ----------
    source_text:
        Raw text from the US report (nodule-level or exam-level).
    gland_context:
        Dict of gland-level features (not currently used, reserved for future
        integration with canonical_us_thyroid_gland_v2 join).
    known_features:
        Dict of already-populated canonical values. Fields present here are
        NOT overwritten — the regex output for that field will be None.

    Returns
    -------
    Dict matching the tirads_primitive_regex_v1_v1 column schema.
    """
    sentences = split_sentences(source_text) if source_text else []

    # Parse exam_date hint from source_text header
    _date_re = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
    m = _date_re.search(source_text[:200]) if source_text else None
    exam_date_hint: Optional[datetime.date] = None
    if m:
        try:
            exam_date_hint = datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    def _guard(field: str, value):
        """Return None if canonical already has this field populated."""
        if known_features.get(field) is not None:
            return None
        return value

    composition = _guard("composition", extract_composition(sentences))
    echogenicity = _guard("echogenicity", extract_echogenicity(sentences))
    shape = _guard("shape", extract_shape(sentences))
    margins = _guard("margins", extract_margins(sentences))
    ef = _guard("echogenic_foci", extract_echogenic_foci(sentences))
    halo = extract_halo(sentences)
    vasc = extract_vascularity(sentences)
    ete = extract_ete_us(sentences)
    misc = extract_misc(sentences)
    tirads_hint = extract_tirads_system_hint(sentences, exam_date_hint)

    # Count how many of the 5 core ACR fields are available (regex-extracted OR
    # already in canonical). Known-canonical fields are not written but still
    # indicate the slot is filled, so they should count toward confidence to
    # avoid routing already-complete rows to Flash needlessly.
    def _is_filled(regex_val, known_key: str) -> bool:
        if regex_val is not None and regex_val != []:
            return True
        if known_features.get(known_key) is not None:
            return True
        return False

    core_filled = sum([
        _is_filled(composition, "composition"),
        _is_filled(echogenicity, "echogenicity"),
        _is_filled(shape, "shape"),
        _is_filled(margins, "margins"),
        _is_filled(ef, "echogenic_foci"),
    ])

    if not sentences:
        confidence = 0.0
    elif core_filled >= 4:
        confidence = 0.85
    elif core_filled >= 2:
        confidence = 0.7
    else:
        confidence = 0.5

    evidence = _best_evidence(sentences, core_filled) if sentences else None

    # JSON-encode list fields for BQ STRING column
    ef_json = json.dumps(ef) if ef else None

    return {
        "composition_regex": composition,
        "echogenicity_regex": echogenicity,
        "shape_regex": shape,
        "margins_regex": margins,
        "echogenic_foci_regex_jsonarray": ef_json,
        "halo_jsonb_regex": json.dumps(halo),
        "vascularity_jsonb_regex": json.dumps(vasc),
        "ete_us_jsonb_regex": json.dumps(ete),
        "chammas_type_regex": misc["chammas_type"],
        "entirely_calcified_regex": misc["entirely_calcified"],
        "homogeneous_echotexture_regex": misc["homogeneous_echotexture"],
        "rim_calcification_subtype_regex": misc["rim_calcification_subtype"],
        "interval_growth_regex": misc["interval_growth"],
        "tirads_reported_system_regex": tirads_hint,
        "evidence_short_regex": evidence,
        "confidence_overall_regex": confidence,
    }


# ---------------------------------------------------------------------------
# __main__ — BQ pipeline
# ---------------------------------------------------------------------------

def _bq_client() -> "bigquery.Client":
    import os
    token = os.environ.get("GCP_TOKEN")
    if token:
        creds = Credentials(token=token)
        return bigquery.Client(project=PROJECT, credentials=creds)
    # ADC fallback
    return bigquery.Client(project=PROJECT)


def _fetch_input(bq: "bigquery.Client", limit: Optional[int]) -> list[dict]:
    limit_clause = f"LIMIT {limit}" if limit else ""
    sql = f"""
    SELECT
      nodule_id, research_id, us_exam_id,
      CAST(exam_date AS STRING) AS exam_date,
      source_text,
      -- existing canonical fields (for known_features guard)
      composition AS existing_composition,
      echogenicity AS existing_echogenicity,
      shape AS existing_shape,
      margins AS existing_margins,
      echogenic_foci AS existing_echogenic_foci
    FROM `{INPUT_TABLE}`
    WHERE source_text IS NOT NULL AND TRIM(source_text) != ''
    {limit_clause}
    """
    rows = []
    for row in bq.query(sql).result():
        rows.append(dict(row))
    return rows


def _build_known_features(row: dict) -> dict:
    return {
        "composition": row.get("existing_composition"),
        "echogenicity": row.get("existing_echogenicity"),
        "shape": row.get("existing_shape"),
        "margins": row.get("existing_margins"),
        "echogenic_foci": row.get("existing_echogenic_foci"),
    }


def _parse_exam_date(s: Optional[str]) -> Optional[datetime.date]:
    if not s:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        try:
            return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def main(dry_run: bool = False, limit: Optional[int] = None) -> None:
    if not _HAS_BQ:
        print("ERROR: google-cloud-bigquery not installed. Cannot run __main__.", file=sys.stderr)
        sys.exit(1)

    bq = _bq_client()
    print(f"[411] Fetching input rows from {INPUT_TABLE}…")
    rows = _fetch_input(bq, limit)
    print(f"[411] Got {len(rows)} rows with source_text.")

    extracted: list[dict] = []
    ts = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    for row in rows:
        features = extract_nodule_features(
            source_text=row.get("source_text", "") or "",
            gland_context={},
            known_features=_build_known_features(row),
        )
        out = {
            "nodule_id": row["nodule_id"],
            "research_id": str(row["research_id"]),
            "us_exam_id": row.get("us_exam_id"),
            "exam_date": row.get("exam_date"),
            "extracted_at": ts,
        }
        out.update(features)
        extracted.append(out)

    print(f"[411] Extracted {len(extracted)} rows.")

    if dry_run:
        print("[411] --dry-run: skipping BQ write. Sample output:")
        print(json.dumps(extracted[:3], indent=2, default=str))
        return

    # Write to BQ
    schema = [
        bigquery.SchemaField("nodule_id", "STRING"),
        bigquery.SchemaField("research_id", "STRING"),
        bigquery.SchemaField("us_exam_id", "STRING"),
        bigquery.SchemaField("exam_date", "STRING"),
        bigquery.SchemaField("extracted_at", "STRING"),
        bigquery.SchemaField("composition_regex", "STRING"),
        bigquery.SchemaField("echogenicity_regex", "STRING"),
        bigquery.SchemaField("shape_regex", "STRING"),
        bigquery.SchemaField("margins_regex", "STRING"),
        bigquery.SchemaField("echogenic_foci_regex_jsonarray", "STRING"),
        bigquery.SchemaField("halo_jsonb_regex", "STRING"),
        bigquery.SchemaField("vascularity_jsonb_regex", "STRING"),
        bigquery.SchemaField("ete_us_jsonb_regex", "STRING"),
        bigquery.SchemaField("chammas_type_regex", "STRING"),
        bigquery.SchemaField("entirely_calcified_regex", "BOOL"),
        bigquery.SchemaField("homogeneous_echotexture_regex", "BOOL"),
        bigquery.SchemaField("rim_calcification_subtype_regex", "STRING"),
        bigquery.SchemaField("interval_growth_regex", "BOOL"),
        bigquery.SchemaField("tirads_reported_system_regex", "STRING"),
        bigquery.SchemaField("evidence_short_regex", "STRING"),
        bigquery.SchemaField("confidence_overall_regex", "FLOAT64"),
    ]

    table_ref = bigquery.TableReference.from_string(OUTPUT_TABLE)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    job = bq.load_table_from_json(extracted, table_ref, job_config=job_config)
    job.result()
    print(f"[411] Written {len(extracted)} rows → {OUTPUT_TABLE}")

    # Quick summary stats
    n_conf = sum(1 for r in extracted if (r["confidence_overall_regex"] or 0) >= 0.7)
    n_comp = sum(1 for r in extracted if r["composition_regex"])
    n_echo = sum(1 for r in extracted if r["echogenicity_regex"])
    n_shape = sum(1 for r in extracted if r["shape_regex"])
    n_marg = sum(1 for r in extracted if r["margins_regex"])
    print(f"[411] confidence>=0.7: {n_conf}/{len(extracted)} ({100*n_conf/max(1,len(extracted)):.1f}%)")
    print(f"[411] composition: {n_comp}  echogenicity: {n_echo}  shape: {n_shape}  margins: {n_marg}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TI-RADS regex extractor v1")
    parser.add_argument("--dry-run", action="store_true", help="Parse only; skip BQ write")
    parser.add_argument("--limit", type=int, default=None, help="Process only N rows")
    args = parser.parse_args()
    main(dry_run=args.dry_run, limit=args.limit)

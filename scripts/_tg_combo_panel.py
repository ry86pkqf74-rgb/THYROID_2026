"""Thyroglobulin + TgAb combined-panel helpers (Script 347 + 113 alignment).

Clinical data issue: Epic/test vendor rows labeled
``Thyroglobulin and Thyroglobulin Antibody`` carry two simultaneous results as
duplicate ``test_name`` rows (Tg ng/mL scale vs TgAb IU/mL scale). There is no
explicit component column — analyte MUST NOT be inferred from ``test_name``
alone for those rows.

Interim institution rule (until analyst confirms row order / unit column):
reuse Script 113 pair heuristics + cross-ref fallback, then singleton
value-shape fallback for orphaned combo rows — see ``infer_singleton_combo``.
Documented on ``main.canonical_labs_thyroglobulin_v1`` COMMENT (Script 347).
"""
from __future__ import annotations

import re

# Canonical normalized Epic label for the dual-marker panel (Script 113).
_COMBO_PRIMARY = frozenset(
    {"thyroglobulin and thyroglobulin antibody"}
)

TGAB_ABSOLUTE_SENTINELS = frozenset(
    {"<0.9", "<0.91", "<0.92", "<0.93", "<0.94", "<1.0"}
)
TG_NG_SENTINELS = frozenset({"<0.1", "<0.2", "<0.15"})

_TITER_RE = re.compile(r"^\s*1:\s*\d+\s*$", re.I)
_EDGE_NUM = re.compile(r"[<>≤≥]\s*(\d+\.?\d*)")


def normalized_test_label(name: str | None) -> str:
    if name is None:
        return ""
    return " ".join(str(name).strip().lower().split())


def is_tg_plus_tgab_combo_panel_test_name(name: str | None) -> bool:
    return normalized_test_label(name) in _COMBO_PRIMARY


def heuristic_disambiguate_pair(res_a: str, res_b: str) -> str | None:
    """Mirror Script 113 ``_heuristic_disambiguate`` — stable contract."""
    tgab_sentinel = {"<0.9"}
    tg_sentinel = {"<0.1", "<0.2"}
    tgab_high_sentinel = {"<2", "<2.0", "<20"}

    if res_a in tgab_sentinel and res_b not in tgab_sentinel:
        return "b_is_tg"
    if res_b in tgab_sentinel and res_a not in tgab_sentinel:
        return "a_is_tg"

    if res_a in tg_sentinel and res_b not in tg_sentinel:
        return "a_is_tg"
    if res_b in tg_sentinel and res_a not in tg_sentinel:
        return "b_is_tg"

    if res_a in tgab_high_sentinel and res_b.startswith("<0."):
        return "b_is_tg"
    if res_b in tgab_high_sentinel and res_a.startswith("<0."):
        return "a_is_tg"

    return None


def crossref_disambiguate_pair(
    rid: int,
    res_a: str,
    res_b: str,
    tg_values: dict[int, set[str]],
    tgab_values: dict[int, set[str]],
) -> str | None:
    """Mirror Script 113 ``_crossref_disambiguate``."""
    known_tg = tg_values.get(rid, set())
    known_tgab = tgab_values.get(rid, set())
    if not known_tg and not known_tgab:
        return None

    a_in_tg = res_a in known_tg
    a_in_tgab = res_a in known_tgab
    b_in_tg = res_b in known_tg
    b_in_tgab = res_b in known_tgab

    if a_in_tg and not a_in_tgab and b_in_tgab and not b_in_tg:
        return "a_is_tg"
    if b_in_tg and not b_in_tgab and a_in_tgab and not a_in_tg:
        return "b_is_tg"

    return None


def infer_singleton_combo_analyte(result_raw: str) -> tuple[str, str]:
    """Last-resort single-row combo panel: infer analyte by value morphology.

    Returns (analyte key for normalize_lab_value, assignment_method substring).
    """
    s_raw = str(result_raw or "").strip()
    slug = normalized_test_label(s_raw)

    if _TITER_RE.match(s_raw):
        return "anti_thyroglobulin", "inferred_value_pattern_singleton"

    if slug.startswith("1:") or " titer " in slug:
        return "anti_thyroglobulin", "inferred_value_pattern_singleton"

    if slug in TGAB_ABSOLUTE_SENTINELS:
        return "anti_thyroglobulin", "inferred_value_pattern_singleton"
    if slug in TG_NG_SENTINELS:
        return "thyroglobulin", "inferred_value_pattern_singleton"

    m_edge = _EDGE_NUM.findall(s_raw)
    if m_edge:
        try:
            anchor = float(m_edge[-1])
        except ValueError:
            anchor = -1.0

        stripped = slug.lstrip("<>≤≥")
        nums = re.findall(r"\d+\.?\d*", stripped)
        first = float(nums[0]) if nums else anchor

        if slug.startswith("<") and first <= 2.5:
            if first >= 0.8:
                return "anti_thyroglobulin", "inferred_value_pattern_singleton"
            if first <= 0.3:
                return "thyroglobulin", "inferred_value_pattern_singleton"

        if 10 <= anchor < 80000:
            return "thyroglobulin", "inferred_value_pattern_singleton"
        if anchor >= 80000:
            return "anti_thyroglobulin", "inferred_value_pattern_singleton"

    # Conservative cohort default — most orphan strings are suppressed Tg.
    return ("thyroglobulin",
            "inferred_value_pattern_singleton_default_tg_unclassified")

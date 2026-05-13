#!/usr/bin/env python3
"""
afirma_result_field_parser.py — parse the binary Afirma test call and ROM%
from the `molecular_testing.result` free-text field.

This parser handles **Afirma-only** rows. Do NOT apply it to ThyroSeq rows.
ThyroSeq uses band vocabulary (LOW/INTERMEDIATE/HIGH) not present here.

Result patterns covered:
  Suspicious (N% ROM)  → positive + point ROM
  Suspicious (>N% ROM) → positive + lower-bound ROM
  Suspicious alone     → positive, no ROM
  Suspiciois / Suspicous (OCR typos) → positive
  Benign               → negative
  Negative             → negative
  Positive             → positive (GSC Xpression-Atlas XA-mutation-positive)
  Insufficient / Inadequate / Cancelled / Non-diagnostic → non_diagnostic

band_source is set to 'afirma_result_field' for all rows parsed here.
"""
from __future__ import annotations

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Pattern list — ordered most-specific first
# Each entry: (compiled_regex, overall_result_class, rom_kind)
# rom_kind ∈ {None, 'point', 'lower'}
# ---------------------------------------------------------------------------
_PATTERNS: list[tuple[re.Pattern, str, Optional[str]]] = []


def _add(pat: str, cls: str, rom_kind: Optional[str]) -> None:
    _PATTERNS.append((re.compile(pat, re.IGNORECASE), cls, rom_kind))


# Core suspicious-word stem — covers standard + OCR variants:
# suspicious, suspicous (missing i), sispiciois, suspicuous, Sispiciois, etc.
_SUSP = r"\bs(?:u|i)spici?(?:ous?|ois|uous?|iois?)\b"

# Suspicious with explicit point ROM% — e.g. "Suspicious (15% ROM)"
# Also handles: "approximately 50%" / "~50%" / "10% rom"
# Group 1 = the numeric value
_add(
    r"(?:" + _SUSP + r"|suspicious).{0,80}?"
    r"(?:\(\s*~?\s*(\d{1,3})\s*%\s*(?:rom|risk|malign[a-z]*)?|"
    r"approximately\s+(\d{1,3})\s*%)",
    "positive", "point"
)

# Suspicious with lower-bound ROM% — e.g. "Suspicious (>50% ROM)", "(≥50%)"
_add(
    r"(?:" + _SUSP + r"|suspicious).{0,80}?"
    r"\(\s*(?:[>≥])\s*(\d{1,3})\s*%",
    "positive", "lower"
)

# Suspicious alone (including all OCR variants) — must come AFTER the ROM patterns
_add(r"(?:" + _SUSP + r"|suspicious)", "positive", None)

# Benign (e.g. "Afirma GSC Benign", "Result: Benign", "Benign (Risk of Malignancy ~4%)")
_add(r"\bbenign\b", "negative", None)

# Negative (usually appears at the very start of short result fields)
_add(r"(?:^|\btest\s+result\s*[:=]?\s*)negative\b", "negative", None)
_add(r"^negative\b", "negative", None)

# Positive (rare; usually means XA mutation-positive routed through GSC) 
_add(r"\bpositive\b", "positive", None)

# Non-diagnostic / administrative
_add(r"\b(?:insufficient|inadequate|cancelled?|non[- ]?diagnostic|inadequate\s+specimen|specimen\s+inadequate)\b",
     "non_diagnostic", None)


def parse_afirma_result(result_text: str) -> dict:
    """
    Parse an Afirma `molecular_testing.result` string into:
      overall_result_class  str|None  — 'positive', 'negative', 'non_diagnostic', or None
      rom_percent_point     float|None — numeric ROM% when a point estimate appears in the field
      rom_percent_low       float|None — lower bound for >X% patterns
      band_source           str|None  — 'afirma_result_field' when a pattern matched, else None

    IMPORTANT: this function ONLY extracts from the `result` field, which is a short
    (~10–30 char) structured field. The `thyroseq_afirma` OCR column in
    thyroseq_molecular_enrichment contains full report text and may contain many
    numeric values — do NOT pass that column to this function.

    No PHI is returned. The `result` field is treated as a structured label, not
    free-text clinical notes.
    """
    if not result_text or not result_text.strip():
        return {
            "overall_result_class": None,
            "rom_percent_point": None,
            "rom_percent_low": None,
            "band_source": None,
        }

    s = result_text.strip()

    for pattern, cls, rom_kind in _PATTERNS:
        m = pattern.search(s)
        if m:
            out: dict = {
                "overall_result_class": cls,
                "band_source": "afirma_result_field",
                "rom_percent_point": None,
                "rom_percent_low": None,
            }
            if rom_kind == "point":
                # Try each capture group (some patterns have alternatives)
                for gidx in range(1, (m.lastindex or 0) + 1):
                    try:
                        raw = m.group(gidx)
                        if raw is not None:
                            val = float(raw)
                            if 0 < val <= 100:
                                out["rom_percent_point"] = val
                                break
                    except (ValueError, IndexError):
                        pass
            elif rom_kind == "lower":
                for gidx in range(1, (m.lastindex or 0) + 1):
                    try:
                        raw = m.group(gidx)
                        if raw is not None:
                            val = float(raw)
                            if 0 < val <= 100:
                                out["rom_percent_low"] = val
                                break
                    except (ValueError, IndexError):
                        pass
            return out

    return {
        "overall_result_class": None,
        "rom_percent_point": None,
        "rom_percent_low": None,
        "band_source": None,
    }


# ---------------------------------------------------------------------------
# Simple self-test (run directly: python afirma_result_field_parser.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    _cases = [
        # (input, expected_class, has_rom_point, has_rom_low, description)
        ("Suspicious (15% ROM)", "positive", True, False, "point ROM"),
        ("Suspicious (10% ROM)", "positive", True, False, "point ROM 10"),
        ("Suspicious (>50% ROM)", "positive", False, True, "lower-bound ROM"),
        ("Suspicious (>50%)", "positive", False, True, "lower-bound no ROM suffix"),
        ("Suspicious (~75%)", "positive", True, False, "tilde ROM"),
        ("Suspicious", "positive", False, False, "suspicious alone"),
        ("Suspicious (Afirma GSC)", "positive", False, False, "suspicious with parens no %"),
        ("Suspicous", "positive", False, False, "OCR typo suspicous"),
        ("Sispiciois", "positive", False, False, "OCR typo sispiciois"),
        ("Benign", "negative", False, False, "benign"),
        ("Benign (Risk of Malignancy ~4%)", "negative", False, False, "benign with ROM"),
        ("negative", "negative", False, False, "lowercase negative at start"),
        ("Afirma GSC Benign", "negative", False, False, "afirma gsc benign"),
        ("Positive", "positive", False, False, "positive"),
        ("Insufficient", "non_diagnostic", False, False, "insufficient"),
        ("Inadequate", "non_diagnostic", False, False, "inadequate"),
        ("Cancelled", "non_diagnostic", False, False, "cancelled"),
        ("Non-diagnostic", "non_diagnostic", False, False, "non-diagnostic hyphen"),
        ("", None, False, False, "empty string"),
        ("N/A", None, False, False, "N/A — no match"),
        ("Specimen received", None, False, False, "generic text — no match"),
        ("Afirma GSC Suspicious, which suggests a risk of cancer of approximately 50%.",
         "positive", True, False, "long Afirma sentence with ROM"),
    ]

    passed = failed = 0
    for inp, exp_cls, exp_has_pt, exp_has_lo, desc in _cases:
        r = parse_afirma_result(inp)
        ok = (
            r["overall_result_class"] == exp_cls
            and (r["rom_percent_point"] is not None) == exp_has_pt
            and (r["rom_percent_low"] is not None) == exp_has_lo
        )
        status = "PASS" if ok else "FAIL"
        if not ok:
            failed += 1
            print(f"  {status} [{desc}]")
            print(f"         input={inp!r}")
            print(f"         got  ={r}")
            print(f"         want class={exp_cls} has_pt={exp_has_pt} has_lo={exp_has_lo}")
        else:
            passed += 1
            print(f"  {status} [{desc}]")

    print(f"\n{passed} passed, {failed} failed out of {passed+failed} cases")

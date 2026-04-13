"""
Canonical AJCC 7th edition T-stage and overall-stage derivation for the ETE
(Proposal 2) analyses.

WHY THIS MODULE EXISTS
----------------------
An earlier implementation (see studies/proposal2_ete_staging/audit_report.md:14)
mapped AJCC8 T3b -> AJCC7 T4a. That is incorrect:

  * AJCC 7th T4a requires invasion of subcutaneous soft tissue, larynx,
    trachea, esophagus, or recurrent laryngeal nerve.
  * Invasion limited to strap muscles was classified as T3 in the 7th edition.

The audit workflow (audit_reproduce.derive_ajcc7_corrected) already used the
correct T3b -> T3 mapping. This module promotes that mapping to the single
shared implementation used by every executable ETE code path, so the
`proposal2_ete_analysis.py` and `proposal2_expanded_cohort.py` scripts no
longer disagree with the audit.

The affected numbers (see audit_report.md): 346 patient-level T-stage
revisions and 46 overall-stage reclassifications.

DO NOT inline a copy of this logic back into the analysis scripts. If the
staging rules ever change, they change here and only here.
"""

from __future__ import annotations

from typing import Sequence

import pandas as pd


__all__ = [
    "AJCC7_T3B_MAPS_TO",
    "derive_ajcc7_t_stage",
    "derive_ajcc7_overall_stage",
    "add_ajcc7_columns",
]


#: AJCC 8th T3b -> AJCC 7th T-stage. This is the canonical mapping for the
#: ETE / Proposal 2 analyses. See module docstring for rationale.
AJCC7_T3B_MAPS_TO: str = "T3"


def _derive_one_t_stage(t8: str, ete_group: str, size_cm: float) -> str:
    """Return the AJCC 7th T-stage for one patient.

    Rules (canonical):
      * T8 in {"T4a", "T4b"} -> same code.
      * T8 == "T3b" -> "T3"  (CORRECTED FROM LEGACY T4a)
      * Microscopic ETE (any size) -> "T3"
      * Size > 4 cm -> "T3"
      * Size > 2 cm -> "T2"
      * Size > 1 cm -> "T1b"
      * Size > 0 cm -> "T1a"
      * Otherwise "Unknown"
    """
    if t8 == "T4b":
        return "T4b"
    if t8 == "T4a":
        return "T4a"
    if t8 == "T3b":
        return AJCC7_T3B_MAPS_TO  # CORRECTED: strap-muscle-only invasion was T3 in AJCC 7th.
    if ete_group == "Microscopic ETE":
        return "T3"
    if size_cm > 4:
        return "T3"
    if size_cm > 2:
        return "T2"
    if size_cm > 1:
        return "T1b"
    if size_cm > 0:
        return "T1a"
    return "Unknown"


def derive_ajcc7_t_stage(
    t_stage_ajcc8: Sequence,
    ete_group: Sequence,
    largest_tumor_cm: Sequence,
) -> list[str]:
    """Vectorized wrapper over :func:`_derive_one_t_stage`.

    Inputs may be any Sequence or pandas Series; NaN sizes are treated as 0
    and NaN string fields as empty strings, matching the legacy pipeline
    behavior so that a drop-in replacement does not shift row alignment.
    """
    t8_s = pd.Series(t_stage_ajcc8).fillna("").astype(str)
    eg_s = pd.Series(ete_group).fillna("").astype(str)
    sz_s = pd.to_numeric(pd.Series(largest_tumor_cm), errors="coerce").fillna(0.0)
    n = len(t8_s)
    if not (len(eg_s) == n and len(sz_s) == n):
        raise ValueError("derive_ajcc7_t_stage inputs must have equal length")
    return [_derive_one_t_stage(t8_s.iloc[i], eg_s.iloc[i], float(sz_s.iloc[i])) for i in range(n)]


def derive_ajcc7_overall_stage(
    age_at_surgery: Sequence,
    t_stage_ajcc7: Sequence,
    n_stage_ajcc8: Sequence,
    m_stage_ajcc8: Sequence,
) -> list[str]:
    """Return AJCC 7th overall stage given patient age, AJCC7 T, AJCC8 N, AJCC8 M.

    Canonical rules (matching audit_reproduce.derive_ajcc7_corrected):

      * Age < 45:
          M1 -> Stage II, else Stage I.
      * Age >= 45:
          T4b (any N/M) -> Stage IVB; IVC if M1.
          T4a -> Stage IVA; IVC if M1.
          T3  -> Stage IVA if N1+, else Stage III.
          N1+ with T1/T2 -> Stage III (limitation: N1a vs N1b not
                  reliably available; all N1 with low T is treated as
                  Stage III; N1b low-T should technically be IVA).
          T1a/T1b -> Stage I.
          T2 -> Stage II.
          Otherwise Stage I.
    """
    age_s = pd.to_numeric(pd.Series(age_at_surgery), errors="coerce").fillna(45.0)
    t7_s = pd.Series(t_stage_ajcc7).fillna("").astype(str)
    n_s = pd.Series(n_stage_ajcc8).fillna("NX").astype(str)
    m_s = pd.Series(m_stage_ajcc8).fillna("M0").astype(str)
    n = len(age_s)
    if not (len(t7_s) == n and len(n_s) == n and len(m_s) == n):
        raise ValueError("derive_ajcc7_overall_stage inputs must have equal length")

    out: list[str] = []
    for i in range(n):
        a = float(age_s.iloc[i])
        t7 = t7_s.iloc[i]
        ni = n_s.iloc[i]
        mi = m_s.iloc[i] if isinstance(m_s.iloc[i], str) else "M0"

        if a < 45:
            out.append("II" if mi == "M1" else "I")
            continue

        if t7 in ("T4a", "T4b"):
            if mi == "M1":
                out.append("IVC")
            elif t7 == "T4b":
                out.append("IVB")
            else:
                out.append("IVA")
        elif t7 == "T3":
            out.append("IVA" if ni.startswith("N1") else "III")
        elif ni.startswith("N1"):
            out.append("III")
        elif t7 in ("T1a", "T1b"):
            out.append("I")
        elif t7 == "T2":
            out.append("II")
        else:
            out.append("I")
    return out


def add_ajcc7_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``t_stage_ajcc7`` and ``overall_stage_ajcc7`` columns to a copy of
    ``df`` using the canonical mapping.

    Required input columns (matching the existing ETE pipeline):
      ``t_stage_ajcc8``, ``ete_group``, ``largest_tumor_cm``,
      ``age_at_surgery``, ``n_stage_ajcc8``, ``m_stage_ajcc8``
      (``m_stage_ajcc8`` is optional; missing => treated as "M0").
    """
    df = df.copy()
    if "m_stage_ajcc8" not in df.columns:
        df["m_stage_ajcc8"] = "M0"
    df["t_stage_ajcc7"] = derive_ajcc7_t_stage(
        df["t_stage_ajcc8"], df["ete_group"], df["largest_tumor_cm"]
    )
    df["overall_stage_ajcc7"] = derive_ajcc7_overall_stage(
        df["age_at_surgery"], df["t_stage_ajcc7"], df["n_stage_ajcc8"], df["m_stage_ajcc8"]
    )
    return df

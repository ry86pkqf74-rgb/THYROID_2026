"""Shared provenance helpers for canonical note facts (materialize + tests).

Defaults align with ENTITY_SCHEMA / contract columns on canonical_extracted_fact_long_v1.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import numpy as np
import pandas as pd

MULTI_SURGERY_EP_DIST_THRESH_DAYS = 90
LOW_LLM_DATE_CONF = 0.35
TEMPORAL_CONFLICT_DAYS = 730


def hash_evidence_span(val: object) -> str | None:
    """SHA-256 of trimmed evidence text; None if empty or missing."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def infer_date_source_type(extraction_method: str, date_confidence: object) -> str:
    """Classify how the entity date was sourced (regex vs LLM tiers)."""
    method = str(extraction_method)
    dc = pd.to_numeric(date_confidence, errors="coerce")
    if not method.startswith("llm"):
        return "regex_extractor"
    if pd.isna(dc) or float(dc) <= 0:
        return "unknown"
    dcv = float(dc)
    if dcv >= 0.99:
        return "explicit_lab"
    if dcv >= 0.5:
        return "note_body"
    return "encounter_fallback"


def infer_linkage_confidence(
    research_id: object,
    multi_surgery_rids: set[int],
    has_episode: bool,
    ep_distance_days: object,
) -> float:
    """Episodic surgery linkage score in [0, 1] for one row."""
    rid = pd.to_numeric(research_id, errors="coerce")
    r = int(rid) if pd.notna(rid) else -1
    multi = r in multi_surgery_rids
    if not has_episode:
        return 0.0
    d_raw = ep_distance_days
    d = float(d_raw) if pd.notna(d_raw) else None
    if d is None:
        return round(0.35 if multi else 0.55, 4)
    score = max(0.0, 1.0 - min(d, 180.0) / 180.0)
    if multi and d > MULTI_SURGERY_EP_DIST_THRESH_DAYS:
        score *= 0.65
    if multi and d > 120:
        score *= 0.85
    return round(float(score), 4)


def apply_provenance_contract_columns(
    uni: pd.DataFrame, multi_surgery_rids: set[int]
) -> pd.DataFrame:
    """Add source_file_id, hashes, date_source_type, linkage_confidence, canonical_* aliases."""
    out = uni.copy()
    out["source_file_id"] = (
        out["clin_source_workbook"] if "clin_source_workbook" in out.columns else None
    )
    out["canonical_domain"] = out["fact_domain"]
    out["canonical_fact_type"] = out["entity_type"]

    eg0 = (
        pd.to_numeric(out["evidence_global_start"], errors="coerce")
        if "evidence_global_start" in out.columns
        else None
    )
    eg1 = (
        pd.to_numeric(out["evidence_global_end"], errors="coerce")
        if "evidence_global_end" in out.columns
        else None
    )
    out["source_text_span_start"] = eg0
    out["source_text_span_end"] = eg1

    if "evidence_span" in out.columns:
        out["source_text_hash"] = out["evidence_span"].apply(hash_evidence_span)
    else:
        out["source_text_hash"] = None

    em = out["extraction_method"].astype(str)
    dc = (
        pd.to_numeric(out["date_confidence"], errors="coerce")
        if "date_confidence" in out.columns
        else pd.Series(np.nan, index=out.index, dtype="float64")
    )
    dst: list[str] = []
    for i in range(len(out)):
        dst.append(infer_date_source_type(em.iloc[i], dc.iloc[i]))
    out["date_source_type"] = dst

    epd = (
        pd.to_numeric(out["ep_distance_days"], errors="coerce")
        if "ep_distance_days" in out.columns
        else pd.Series(pd.NA, index=out.index, dtype="Float64")
    )
    rid = pd.to_numeric(out["research_id"], errors="coerce")
    has_ep = (
        out["inferred_surgery_episode_id"].notna()
        if "inferred_surgery_episode_id" in out.columns
        else pd.Series(False, index=out.index)
    )
    lc: list[float] = []
    for i in range(len(out)):
        lc.append(
            infer_linkage_confidence(
                rid.iloc[i],
                multi_surgery_rids,
                bool(has_ep.iloc[i]),
                epd.iloc[i],
            )
        )
    out["linkage_confidence"] = lc
    return out


def quarantine_masks(
    uni: pd.DataFrame,
    multi_surgery_rids: set[int],
    *,
    multi_ep_dist_days: int = MULTI_SURGERY_EP_DIST_THRESH_DAYS,
    low_llm_date_conf: float = LOW_LLM_DATE_CONF,
    temporal_conflict_days: int = TEMPORAL_CONFLICT_DAYS,
) -> tuple[pd.Series, pd.Series]:
    """Return (quarantine_bool_series, reason_series) aligned to uni.index."""
    q = pd.Series(False, index=uni.index)
    reason = pd.Series("", index=uni.index, dtype=object)

    rid = pd.to_numeric(uni["research_id"], errors="coerce")
    multi_mask = rid.apply(
        lambda x: int(x) in multi_surgery_rids if pd.notna(x) else False
    )
    epd = pd.to_numeric(uni["ep_distance_days"], errors="coerce")

    m1 = multi_mask & (epd.isna() | (epd > multi_ep_dist_days))
    q = q | m1
    reason = reason.where(~m1, "multi_surgery_episode_ambiguous")

    em = uni["extraction_method"].astype(str)
    dc = (
        pd.to_numeric(uni["date_confidence"], errors="coerce")
        if "date_confidence" in uni.columns
        else pd.Series(np.nan, index=uni.index, dtype="float64")
    )
    has_ed = (
        uni["entity_date"].notna()
        if "entity_date" in uni.columns
        else pd.Series(False, index=uni.index)
    )
    m2 = em.str.startswith("llm") & has_ed & dc.notna() & (dc < low_llm_date_conf)
    m2 = m2 & ~m1
    q = q | m2
    reason = reason.where(~m2, "low_confidence_llm_date")

    ed = (
        pd.to_datetime(uni["entity_date"], errors="coerce")
        if "entity_date" in uni.columns
        else pd.Series(pd.NaT, index=uni.index)
    )
    sd = (
        pd.to_datetime(uni["inferred_surgery_date"], errors="coerce")
        if "inferred_surgery_date" in uni.columns
        else pd.Series(pd.NaT, index=uni.index)
    )
    delta = (ed - sd).abs().dt.days
    m3 = ed.notna() & sd.notna() & delta.notna() & (delta > temporal_conflict_days)
    m3 = m3 & ~q
    q = q | m3
    reason = reason.where(~m3, "temporal_conflict_entity_vs_surgery")

    return q, reason


def split_quarantine(uni: pd.DataFrame, multi_surgery_rids: set[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    quarantine_date = datetime.now(timezone.utc).date().isoformat()
    q, r = quarantine_masks(uni, multi_surgery_rids)
    quar = uni.loc[q].copy()
    quar["quarantine_reason"] = r.loc[q]
    quar["quarantine_date"] = quarantine_date
    clean = uni.loc[~q].copy()
    return clean, quar

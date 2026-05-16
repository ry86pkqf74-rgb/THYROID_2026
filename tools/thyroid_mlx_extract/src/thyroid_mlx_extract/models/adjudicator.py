"""Two-model adjudication for high-stakes fields.

Pattern:
  1. Run primary extractor.
  2. If primary confidence < threshold OR field is in high-stakes set,
     run adjudicator (different model family).
  3. If results agree → use either, mark verification_status='agreed'.
  4. If results disagree → flag for manual review, mark verification_status='disagreed'.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from .extractor import ExtractionResult, extract


@dataclass
class AdjudicatedResult:
    primary: ExtractionResult
    adjudicator: ExtractionResult | None
    verification_status: str  # 'agreed', 'disagreed', 'primary_only', 'both_failed'
    disagreed_fields: list[str]


HIGH_STAKES_FIELDS = {
    "synoptic": {"ete_grade", "extranodal_extension_present", "capsular_invasion"},
    "death": {"attribution"},
    "complications": {"complication_type"},
    "molecular": {"risk_call", "variants"},
}


def adjudicate(
    task_id: str,
    schema_cls: type[BaseModel],
    prompt_template: str,
    source_text: str,
    primary_model: str,
    adjudicator_model: str | None,
    *,
    confidence_threshold: float = 0.7,
    always_adjudicate: bool = False,
) -> AdjudicatedResult:
    """Run primary, optionally adjudicate, return combined result."""
    primary = extract(primary_model, schema_cls, prompt_template, source_text)

    if not primary.success:
        if adjudicator_model is None:
            return AdjudicatedResult(primary, None, "both_failed", [])
        adj = extract(adjudicator_model, schema_cls, prompt_template, source_text)
        if adj.success:
            return AdjudicatedResult(primary, adj, "primary_only", [])  # adj rescued the parse
        return AdjudicatedResult(primary, adj, "both_failed", [])

    if adjudicator_model is None:
        return AdjudicatedResult(primary, None, "primary_only", [])

    needs_adj = always_adjudicate or _low_confidence(primary.payload, confidence_threshold)
    if not needs_adj:
        return AdjudicatedResult(primary, None, "primary_only", [])

    adj = extract(adjudicator_model, schema_cls, prompt_template, source_text)
    if not adj.success:
        return AdjudicatedResult(primary, adj, "primary_only", [])

    disagreed = _diff_high_stakes(task_id, primary.payload, adj.payload)
    if not disagreed:
        return AdjudicatedResult(primary, adj, "agreed", [])
    return AdjudicatedResult(primary, adj, "disagreed", disagreed)


def _low_confidence(payload: BaseModel, threshold: float) -> bool:
    """Recurse into payload looking for any confidence/overall_confidence < threshold."""
    if payload is None:
        return False
    for field in ("confidence", "overall_confidence", "overall_path_confidence"):
        v = getattr(payload, field, None)
        if isinstance(v, (int, float)) and v < threshold:
            return True
    # Check nested entity lists
    for fname in payload.model_fields:
        v = getattr(payload, fname)
        if isinstance(v, list):
            for item in v:
                if isinstance(item, BaseModel):
                    if _low_confidence(item, threshold):
                        return True
        elif isinstance(v, BaseModel):
            if _low_confidence(v, threshold):
                return True
    return False


def _diff_high_stakes(task_id: str, a: BaseModel, b: BaseModel) -> list[str]:
    """Return field paths where the two payloads disagree on high-stakes values."""
    fields = HIGH_STAKES_FIELDS.get(task_id, set())
    diffs: list[str] = []
    for f in fields:
        va = _get_path(a, f)
        vb = _get_path(b, f)
        if _normalize(va) != _normalize(vb):
            diffs.append(f)
    return diffs


def _get_path(obj: Any, path: str) -> Any:
    """Walk a dotted path or just a top-level attr."""
    cur = obj
    for part in path.split("."):
        if cur is None:
            return None
        cur = getattr(cur, part, None)
    return cur


def _normalize(v: Any) -> Any:
    if isinstance(v, list):
        return frozenset(_normalize(x) for x in v)
    if isinstance(v, BaseModel):
        return v.model_dump_json()
    return v

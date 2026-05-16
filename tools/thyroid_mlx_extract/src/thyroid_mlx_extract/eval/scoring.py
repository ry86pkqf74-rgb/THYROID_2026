"""Scoring: F1 per field, hallucination rate, span coverage.

Gold CSV format (one row per source_pk):
    source_pk, field_path, gold_value, gold_evidence_substring

Prediction JSONL format (one row per source_pk):
    {source_pk, result: {<schema-shaped JSON>}}
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class FieldScore:
    field: str
    tp: int
    fp: int
    fn: int
    @property
    def precision(self) -> float:
        return self.tp / (self.tp + self.fp) if (self.tp + self.fp) else 0.0
    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if (self.tp + self.fn) else 0.0
    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0


@dataclass
class ModelReport:
    model_key: str
    per_field: list[FieldScore]
    macro_f1: float
    micro_f1: float
    hallucination_rate: float    # fraction of predicted values with no source-text evidence
    parse_success_rate: float
    avg_elapsed_seconds: float


def score(gold_csv: Path | str, predictions_jsonl: Path | str, source_jsonl: Path | str) -> ModelReport:
    """Score a single model's predictions against gold."""
    gold = _load_gold(Path(gold_csv))
    preds, parse_ok, parse_total, avg_elapsed, model_key = _load_predictions(Path(predictions_jsonl))
    sources = _load_sources(Path(source_jsonl))

    # Compute per-field TP/FP/FN
    field_stats: dict[str, FieldScore] = defaultdict(lambda: FieldScore("", 0, 0, 0))
    hallucinations = 0
    pred_values = 0

    all_keys = set(gold.keys()) | set(preds.keys())
    for source_pk in all_keys:
        g = gold.get(source_pk, {})
        p = preds.get(source_pk, {})
        src_text = sources.get(source_pk, "")

        for field in set(g) | set(p):
            gv = _normalize(g.get(field))
            pv = _normalize(p.get(field))
            fs = field_stats.setdefault(field, FieldScore(field, 0, 0, 0))
            fs.field = field

            if gv is None and pv is None:
                continue
            elif gv is None and pv is not None:
                fs.fp += 1
                pred_values += 1
                # check hallucination: if predicted value isn't anywhere in source text
                if pv and isinstance(pv, str) and pv not in src_text:
                    hallucinations += 1
            elif gv is not None and pv is None:
                fs.fn += 1
            elif gv == pv:
                fs.tp += 1
                pred_values += 1
            else:
                fs.fp += 1
                fs.fn += 1
                pred_values += 1
                if isinstance(pv, str) and pv and pv not in src_text:
                    hallucinations += 1

    per_field = sorted(field_stats.values(), key=lambda s: s.field)
    macro_f1 = sum(s.f1 for s in per_field) / len(per_field) if per_field else 0.0
    total_tp = sum(s.tp for s in per_field)
    total_fp = sum(s.fp for s in per_field)
    total_fn = sum(s.fn for s in per_field)
    micro_p = total_tp / (total_tp + total_fp) if (total_tp + total_fp) else 0.0
    micro_r = total_tp / (total_tp + total_fn) if (total_tp + total_fn) else 0.0
    micro_f1 = 2 * micro_p * micro_r / (micro_p + micro_r) if (micro_p + micro_r) else 0.0
    halluc_rate = hallucinations / pred_values if pred_values else 0.0
    parse_rate = parse_ok / parse_total if parse_total else 0.0

    return ModelReport(
        model_key=model_key,
        per_field=per_field,
        macro_f1=macro_f1,
        micro_f1=micro_f1,
        hallucination_rate=halluc_rate,
        parse_success_rate=parse_rate,
        avg_elapsed_seconds=avg_elapsed,
    )


def _load_gold(path: Path) -> dict[str, dict[str, object]]:
    df = pd.read_csv(path)
    out: dict[str, dict[str, object]] = defaultdict(dict)
    for _, row in df.iterrows():
        out[str(row["source_pk"])][str(row["field_path"])] = row["gold_value"]
    return out


def _load_predictions(path: Path) -> tuple[dict[str, dict[str, object]], int, int, float, str]:
    preds: dict[str, dict[str, object]] = {}
    parse_ok = 0
    parse_total = 0
    elapsed_sum = 0.0
    model_key = ""
    with path.open() as f:
        for line in f:
            obj = json.loads(line)
            parse_total += 1
            elapsed_sum += obj.get("elapsed_seconds", 0.0)
            model_key = obj.get("model_name", model_key)
            if not obj.get("success", False):
                continue
            parse_ok += 1
            source_pk = obj.get("source_pk")
            preds[source_pk] = _flatten(obj.get("result", {}))
    avg = elapsed_sum / parse_total if parse_total else 0.0
    return preds, parse_ok, parse_total, avg, model_key


def _load_sources(path: Path) -> dict[str, str]:
    sources: dict[str, str] = {}
    with path.open() as f:
        for line in f:
            obj = json.loads(line)
            sources[str(obj["source_pk"])] = obj.get("source_text", "")
    return sources


def _flatten(d: dict, prefix: str = "") -> dict[str, object]:
    """Flatten nested dict into dotted-path keys for field-by-field scoring."""
    out: dict[str, object] = {}
    if not isinstance(d, dict):
        return {prefix.rstrip("."): d}
    for k, v in d.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(_flatten(v, prefix=f"{key}."))
        elif isinstance(v, list):
            # For lists of objects, key by index
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    out.update(_flatten(item, prefix=f"{key}[{i}]."))
                else:
                    out[f"{key}[{i}]"] = item
        else:
            out[key] = v
    return out


def _normalize(v: object) -> object:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str):
        return v.strip().lower() if v else None
    return v


def report_to_markdown(report: ModelReport) -> str:
    lines = [
        f"## {report.model_key}",
        "",
        f"- Macro F1: **{report.macro_f1:.3f}**",
        f"- Micro F1: **{report.micro_f1:.3f}**",
        f"- Parse success rate: {report.parse_success_rate:.1%}",
        f"- Hallucination rate: {report.hallucination_rate:.1%}",
        f"- Avg elapsed: {report.avg_elapsed_seconds:.2f}s",
        "",
        "| Field | TP | FP | FN | P | R | F1 |",
        "|---|---|---|---|---|---|---|",
    ]
    for s in report.per_field:
        lines.append(
            f"| {s.field} | {s.tp} | {s.fp} | {s.fn} | "
            f"{s.precision:.2f} | {s.recall:.2f} | **{s.f1:.2f}** |"
        )
    return "\n".join(lines)

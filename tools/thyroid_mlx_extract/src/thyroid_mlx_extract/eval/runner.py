"""Run candidate models against gold subsets and produce a comparison report."""
from __future__ import annotations

import json
from pathlib import Path

from ..config import TASKS, prompts_dir, results_dir
from ..models.extractor import extract
from ..schemas import load_schema
from . import scoring


def run_eval(
    task_id: str,
    gold_csv: Path | str,
    source_jsonl: Path | str,
    models: list[str],
    *,
    output_dir: Path | str | None = None,
) -> Path:
    """Run multiple candidate models over the source JSONL and score against gold.

    Returns path to the markdown comparison report.
    """
    spec = TASKS[task_id]
    schema_cls = load_schema(spec.schema_module)
    prompt_template = (prompts_dir() / spec.prompt_file).read_text()
    out_dir = Path(output_dir) if output_dir else results_dir() / task_id / "eval"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load source rows once
    sources = [json.loads(line) for line in Path(source_jsonl).open()]

    reports = []
    for model_key in models:
        preds_path = out_dir / f"{model_key}_predictions.jsonl"
        with preds_path.open("w") as f:
            for src in sources:
                result = extract(
                    model_key,
                    schema_cls,
                    prompt_template,
                    src["source_text"],
                    prompt_version="v1",
                )
                row = {
                    "source_pk": src["source_pk"],
                    "model_name": model_key,
                    "success": result.success,
                    "result": result.payload.model_dump() if result.payload else None,
                    "raw_text": result.raw_text,
                    "error": result.error,
                    "elapsed_seconds": result.elapsed_seconds,
                    "prompt_version": result.prompt_version,
                }
                f.write(json.dumps(row, default=str) + "\n")

        report = scoring.score(gold_csv, preds_path, source_jsonl)
        reports.append(report)

    report_md = _comparison_report(task_id, reports)
    md_path = out_dir / "comparison.md"
    md_path.write_text(report_md)
    return md_path


def _comparison_report(task_id: str, reports: list[scoring.ModelReport]) -> str:
    lines = [f"# Eval comparison — {task_id}", "", "## Summary", ""]
    lines.append("| Model | Macro F1 | Micro F1 | Parse % | Halluc % | Avg s |")
    lines.append("|---|---|---|---|---|---|")
    for r in sorted(reports, key=lambda x: x.macro_f1, reverse=True):
        lines.append(
            f"| {r.model_key} | **{r.macro_f1:.3f}** | {r.micro_f1:.3f} | "
            f"{r.parse_success_rate:.0%} | {r.hallucination_rate:.0%} | "
            f"{r.avg_elapsed_seconds:.2f} |"
        )
    lines.append("")
    lines.append("Pick the smallest model that hits Macro F1 ≥ 0.90.")
    lines.append("If none clear 0.90, consider LoRA fine-tuning Llama-3-8B with the gold set.")
    lines.append("")
    for r in reports:
        lines.append(scoring.report_to_markdown(r))
        lines.append("")
    return "\n".join(lines)

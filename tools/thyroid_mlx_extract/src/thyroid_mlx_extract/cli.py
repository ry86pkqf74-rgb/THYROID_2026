"""Click CLI: pull / eval / run / push commands.

Usage:
    thyroid-mlx pull <task> [--limit N] [--out PATH]
    thyroid-mlx eval <task> --gold gold/<task>_gold.csv --models m1,m2,...
    thyroid-mlx run <task> [--model M] [--resume]
    thyroid-mlx push <task> --run-id <id>
    thyroid-mlx list-tasks
    thyroid-mlx list-models
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from .config import MODELS, TASKS, results_dir, runs_dir, prompts_dir
from .utils.provenance import new_run_id

console = Console()


@click.group()
def cli() -> None:
    """thyroid-mlx-extract: on-device clinical text extraction."""


@cli.command("list-tasks")
def list_tasks() -> None:
    """List registered tasks."""
    t = Table(title="Tasks")
    t.add_column("task_id")
    t.add_column("primary model")
    t.add_column("output table")
    t.add_column("notes")
    for spec in TASKS.values():
        t.add_row(spec.task_id, spec.primary_model, spec.output_table, spec.notes[:60])
    console.print(t)


@cli.command("list-models")
def list_models() -> None:
    """List models in the registry."""
    t = Table(title="Models")
    t.add_column("key")
    t.add_column("repo")
    t.add_column("memory (GB)")
    t.add_column("strengths")
    for spec in MODELS.values():
        t.add_row(spec.key, spec.hf_repo, str(spec.memory_gb), ", ".join(spec.strengths))
    console.print(t)


@cli.command()
@click.argument("task_id")
@click.option("--limit", type=int, default=None, help="Row limit")
@click.option("--where", type=str, default=None, help="Additional SQL WHERE clause")
@click.option("--out", "out_path", type=click.Path(), default=None)
def pull(task_id: str, limit: int | None, where: str | None, out_path: str | None) -> None:
    """Pull source rows from BigQuery for a task."""
    from .bq.pull import pull as bq_pull

    p = bq_pull(task_id, limit=limit, where=where, output_path=out_path)
    console.print(f"[green]Wrote {p}[/green]")


@cli.command()
@click.argument("task_id")
@click.option("--gold", "gold_path", required=True, type=click.Path(exists=True))
@click.option("--source", "source_path", default=None, type=click.Path())
@click.option("--models", "models_csv", default=None, help="Comma-separated model keys")
def eval(task_id: str, gold_path: str, source_path: str | None, models_csv: str | None) -> None:
    """Evaluate candidate models against gold for a task."""
    from .eval.runner import run_eval

    if task_id not in TASKS:
        console.print(f"[red]Unknown task '{task_id}'[/red]")
        sys.exit(1)
    spec = TASKS[task_id]

    if source_path is None:
        source_path = runs_dir() / task_id / "source.jsonl"
        if not Path(source_path).exists():
            console.print(
                f"[red]Source file missing. Run `thyroid-mlx pull {task_id}` first or pass --source.[/red]"
            )
            sys.exit(1)

    if models_csv:
        candidates = [m.strip() for m in models_csv.split(",")]
    else:
        candidates = [spec.primary_model, spec.fallback_model]
        if spec.adjudicator_model and spec.adjudicator_model not in candidates:
            candidates.append(spec.adjudicator_model)

    for m in candidates:
        if m not in MODELS:
            console.print(f"[red]Unknown model '{m}'. See `list-models`.[/red]")
            sys.exit(1)

    console.print(f"[cyan]Evaluating {task_id} with models: {candidates}[/cyan]")
    out_md = run_eval(task_id, gold_path, source_path, candidates)
    console.print(f"[green]Report: {out_md}[/green]")


@cli.command()
@click.argument("task_id")
@click.option("--model", "model_key", default=None, help="Model key (defaults to task's primary)")
@click.option("--source", "source_path", default=None, type=click.Path())
@click.option("--limit", type=int, default=None)
@click.option("--adjudicate/--no-adjudicate", default=False)
@click.option("--resume/--no-resume", default=False)
def run(
    task_id: str,
    model_key: str | None,
    source_path: str | None,
    limit: int | None,
    adjudicate: bool,
    resume: bool,
) -> None:
    """Run extraction over source rows for a task."""
    from .models.adjudicator import adjudicate as adj_extract
    from .models.extractor import extract
    from .schemas import load_schema
    from .utils.provenance import sha256, utc_now_iso

    if task_id not in TASKS:
        console.print(f"[red]Unknown task '{task_id}'[/red]")
        sys.exit(1)

    spec = TASKS[task_id]
    model_key = model_key or spec.primary_model
    schema_cls = load_schema(spec.schema_module)
    prompt_template = (prompts_dir() / spec.prompt_file).read_text()

    if source_path is None:
        source_path = runs_dir() / task_id / "source.jsonl"
    source_path = Path(source_path)
    if not source_path.exists():
        console.print(f"[red]Missing source: {source_path}[/red]")
        sys.exit(1)

    run_id = new_run_id(task_id, model_key)
    out_path = runs_dir() / task_id / f"{run_id}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    seen = set()
    if resume and out_path.exists():
        for line in out_path.open():
            obj = json.loads(line)
            seen.add(obj.get("source_pk"))

    n_done = 0
    n_err = 0
    with source_path.open() as fin, out_path.open("a") as fout:
        for line in fin:
            src = json.loads(line)
            if src["source_pk"] in seen:
                continue
            if limit and n_done >= limit:
                break

            if adjudicate and spec.adjudicator_model:
                ar = adj_extract(
                    task_id,
                    schema_cls,
                    prompt_template,
                    src["source_text"],
                    primary_model=model_key,
                    adjudicator_model=spec.adjudicator_model,
                )
                result = ar.primary
                verification_status = ar.verification_status
            else:
                result = extract(model_key, schema_cls, prompt_template, src["source_text"])
                verification_status = "primary_only"

            row = {
                "source_pk": src["source_pk"],
                "research_id": src.get("research_id"),
                "event_date": src.get("event_date"),
                "extraction_run_id": run_id,
                "model_name": model_key,
                "model_version": MODELS[model_key].hf_repo,
                "prompt_version": "v1",
                "success": result.success,
                "result": result.payload.model_dump() if result.payload else None,
                "raw_response_sha256": sha256(result.raw_text),
                "verification_status": verification_status,
                "confidence_score": _extract_confidence(result.payload),
                "elapsed_seconds": result.elapsed_seconds,
                "extraction_timestamp_utc": utc_now_iso(),
                "error": result.error,
            }
            fout.write(json.dumps(row, default=str) + "\n")
            fout.flush()

            n_done += 1
            if not result.success:
                n_err += 1
            if n_done % 25 == 0:
                console.print(f"[dim]processed {n_done}, errors {n_err}[/dim]")

    console.print(
        f"[green]Run {run_id} complete: {n_done} processed, {n_err} parse errors. → {out_path}[/green]"
    )


@cli.command()
@click.argument("task_id")
@click.option("--results", "results_path", required=True, type=click.Path(exists=True))
@click.option("--workspace/--canonical", default=True, help="Push to pub_workspace (default) or pub_canonical")
def push(task_id: str, results_path: str, workspace: bool) -> None:
    """Push extraction results to BigQuery."""
    from .bq.push import push as bq_push

    target = bq_push(task_id, results_path, workspace=workspace)
    console.print(f"[green]Pushed to {target}[/green]")


def _extract_confidence(payload) -> float | None:
    if payload is None:
        return None
    for field in ("overall_confidence", "overall_path_confidence", "confidence"):
        v = getattr(payload, field, None)
        if isinstance(v, (int, float)):
            return float(v)
    return None


if __name__ == "__main__":
    cli()

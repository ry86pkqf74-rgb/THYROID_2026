"""verify subcommand — Phase 3 adjudication of existing extractions."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import click
from rich.console import Console

from .bq.verify_io import SUPPORTED_TABLES, pull_unverified, push_verifications
from .config import runs_dir
from .models.verifier import verify_one
from .utils.provenance import new_run_id, sha256, utc_now_iso

console = Console()


@click.command("verify")
@click.argument("source_table")
@click.option("--verifier", "verifier_model", default="r1-distill-70b",
              help="Model key for second-pass verifier")
@click.option("--limit", type=int, default=None, help="Max rows this run")
@click.option("--entity-types", default=None,
              help="Comma-separated entity_type filter")
@click.option("--resume/--no-resume", default=True)
@click.option("--push/--no-push", default=False,
              help="Also push results to pub_workspace.<table>_verified_v1")
def verify_command(source_table, verifier_model, limit, entity_types, resume, push):
    """Second-pass adjudication of existing note_entities_* rows.

    SUPPORTED TABLES:
      note_entities_complications, note_entities_operative_detail,
      note_entities_problem_list, note_entities_procedures,
      note_entities_staging, note_entities_genetics,
      note_entities_medications
    """
    if source_table not in SUPPORTED_TABLES:
        console.print(f"[red]Unknown table '{source_table}'.[/red]")
        for t in sorted(SUPPORTED_TABLES):
            console.print(f"  {t}")
        sys.exit(1)

    types_list = [t.strip() for t in entity_types.split(",")] if entity_types else None
    run_id = new_run_id(f"verify_{source_table}", verifier_model)
    out_dir = runs_dir() / "verify" / source_table
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{run_id}.jsonl"

    seen = set()
    if resume:
        for prior in out_dir.glob("*.jsonl"):
            for line in prior.open():
                obj = json.loads(line)
                seen.add(obj.get("source_pk"))
    console.print(f"[cyan]Verifying {source_table} with {verifier_model} → {out_path}[/cyan]")
    console.print(f"[dim]Resuming past {len(seen)} previously-processed rows[/dim]")

    n_done = n_err = n_disagree = 0
    with out_path.open("a") as fout:
        for item in pull_unverified(source_table, limit=limit, entity_types=types_list):
            if item.source_pk in seen:
                continue
            if limit and n_done >= limit:
                break

            result = verify_one(item, verifier_model=verifier_model)
            row = {
                "research_id": item.research_id,
                "source_pk": item.source_pk,
                "entity_type": item.entity_type,
                "verification_status": result.verification_status,
                "agrees_with_original": (result.verdict.agrees_with_original
                                          if result.verdict else None),
                "corrected_value": (result.verdict.corrected_value if result.verdict else None),
                "corrected_present_or_negated": (
                    result.verdict.corrected_present_or_negated if result.verdict else None
                ),
                "date_confidence": (result.verdict.date_confidence if result.verdict else None),
                "evidence_present_in_source": (
                    result.verdict.evidence_present_in_source if result.verdict else None
                ),
                "verifier_reasoning": (result.verdict.reasoning if result.verdict else None),
                "verifier_run_id": run_id,
                "verifier_model_name": verifier_model,
                "verifier_prompt_version": "v1",
                "raw_verifier_response_sha256": sha256(result.raw_text),
                "elapsed_seconds": result.elapsed_seconds,
                "extraction_timestamp_utc": utc_now_iso(),
            }
            fout.write(json.dumps(row, default=str) + "\n")
            fout.flush()

            n_done += 1
            if not result.success:
                n_err += 1
            if result.verdict and not result.verdict.agrees_with_original:
                n_disagree += 1
            if n_done % 50 == 0:
                console.print(
                    f"[dim]  processed {n_done}, disagreed {n_disagree}, errors {n_err}[/dim]"
                )

    console.print(
        f"[green]Verified {n_done} rows. Disagreements: {n_disagree}. Errors: {n_err}. → {out_path}[/green]"
    )

    if push:
        console.print(f"[cyan]Pushing to pub_workspace.{source_table}_verified_v1...[/cyan]")
        target = push_verifications(source_table, out_path)
        console.print(f"[green]Pushed to {target}[/green]")

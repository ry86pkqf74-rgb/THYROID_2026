#!/usr/bin/env python3
"""
run_extraction_split.py -- LLM extraction via local Ollama (single server).

Designed for distributed deployment: copy this script + input data to each
ResearchFlow server and run with a subset of --domains.  Each server talks
to its own localhost Ollama (qwen3:14b).

Per-note JSONL checkpointing: every row is flushed immediately so zero
progress is lost on crash/restart.

Input:  processed/remaining/clinical_notes_long.parquet  (11 037 rows)
Output: processed/output/note_entities_llm_<domain>.parquet  (per domain)
        processed/output/note_entities_llm_combined.parquet   (merged)

Usage:
  # Run all 7 domains locally:
  python scripts/run_extraction_split.py

  # Run specific domains only (for distributed deployment):
  python scripts/run_extraction_split.py --domains complications staging

  # Override model / endpoint:
  python scripts/run_extraction_split.py --url http://localhost:11434/v1 --model qwen3:14b

Pip: pandas pyarrow openai tenacity python-dotenv
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from llm_extraction.fleet_domain_prompt import get_fleet_domain_prompt

PROCESSED_REMAINING = ROOT / "processed" / "remaining"
PROCESSED_OUTPUT = ROOT / "processed" / "output"
PROMPTS_DIR = ROOT / "llm_extraction" / "prompts"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)s] %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("split_extraction")

LLM_TIMEOUT_SECONDS = int(os.environ.get("LLM_TIMEOUT_SECONDS", "120"))

DOMAIN_PROMPT = get_fleet_domain_prompt()
ALL_DOMAINS = list(DOMAIN_PROMPT.keys())


def _load_prompt(domain: str) -> str:
    fname = DOMAIN_PROMPT.get(domain)
    if not fname:
        raise KeyError(f"Unknown extraction domain: {domain}")
    path = PROMPTS_DIR / fname
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found for {domain}: {path}")
    prompt_text = path.read_text(encoding="utf-8").strip()
    if not prompt_text:
        raise ValueError(f"Prompt file is empty for {domain}: {path}")
    return prompt_text


def _rewrite_checkpoint(ckpt_path: Path, rows: list[dict]) -> None:
    tmp_path = ckpt_path.with_suffix('.tmp')
    with open(tmp_path, 'w', encoding='utf-8') as handle:
        for row in rows:
            handle.write(json.dumps(row) + '\n')
    tmp_path.replace(ckpt_path)


def _dedupe_checkpoint_rows(ckpt_path: Path, rows: list[dict], domain: str) -> list[dict]:
    if not rows:
        return rows

    seen_note_row_ids: set[str] = set()
    deduped_reversed: list[dict] = []
    duplicates_removed = 0
    for row in reversed(rows):
        note_row_id = str(row.get("note_row_id", ""))
        if note_row_id in seen_note_row_ids:
            duplicates_removed += 1
            continue
        seen_note_row_ids.add(note_row_id)
        deduped_reversed.append(row)

    if duplicates_removed == 0:
        return rows

    deduped_rows = list(reversed(deduped_reversed))
    _rewrite_checkpoint(ckpt_path, deduped_rows)
    log.info(f"  [{domain}] removed {duplicates_removed:,} duplicate checkpoint rows")
    return deduped_rows


def _call_llm(client: Any, model: str, system_prompt: str, note_text: str) -> dict:
    """Call an OpenAI-compatible chat endpoint, return parsed JSON dict.

    Tries json_object format first; falls back to plain text if the server
    rejects it.
    """
    import re as _re

    def _attempt(response_fmt):
        kwargs = dict(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": note_text[:6000]},
            ],
            temperature=0,
            max_tokens=1500,
            timeout=LLM_TIMEOUT_SECONDS,
        )
        if response_fmt:
            kwargs["response_format"] = response_fmt
        return client.chat.completions.create(**kwargs)

    raw = "{}"
    try:
        try:
            response = _attempt({"type": "json_object"})
        except Exception as e:
            if "json_object" in str(e) or "response_format" in str(e) or "400" in str(e):
                response = _attempt(None)
            else:
                raise
        raw = response.choices[0].message.content or "{}"
        # Extract JSON from fenced code blocks if present
        m = _re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, _re.DOTALL)
        if m:
            raw = m.group(1)
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"parse_error": True, "raw": raw[:500]}
    except Exception as exc:
        log.warning(f"LLM call failed: {exc}")
        raise


def extract_domain(
    notes_df: pd.DataFrame,
    domain: str,
    base_url: str,
    api_key: str,
    model: str,
    output_dir: Path,
    output_suffix: str = "",
) -> Path:
    """Run extraction for one domain with per-note JSONL checkpointing.

    Checkpoint file: <output_dir>/note_entities_llm_<domain>.ckpt.jsonl
    - Each processed note is appended immediately (crash-safe, zero progress lost).
    - On restart, already-processed note_row_ids are loaded and skipped.
    - Final parquet is written from the full checkpoint at end of domain run.
    Returns the output parquet path.
    """
    import openai

    suffix = f"_{output_suffix}" if output_suffix else ""
    ckpt_path = output_dir / f"note_entities_llm_{domain}{suffix}.ckpt.jsonl"
    out_path  = output_dir / f"note_entities_llm_{domain}{suffix}.parquet"

    # -- Load existing checkpoint (resume support) -------------------------
    done_ids: set = set()
    existing_rows: list[dict] = []
    if ckpt_path.exists():
        with open(ckpt_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    existing_rows.append(row)
                except json.JSONDecodeError:
                    pass
        existing_rows = _dedupe_checkpoint_rows(ckpt_path, existing_rows, domain)
        for row in existing_rows:
            done_ids.add(str(row.get("note_row_id", "")))
        if done_ids:
            log.info(f"  [{domain}] RESUME -- {len(done_ids):,} notes already done, "
                     f"{len(notes_df) - len(done_ids):,} remaining")

    remaining_df = notes_df[~notes_df["note_row_id"].astype(str).isin(done_ids)]
    n_total = len(notes_df)
    n_remaining = len(remaining_df)

    if n_remaining == 0:
        log.info(f"  [{domain}] already complete ({n_total:,} notes) -- rebuilding parquet")
        _flush_parquet(existing_rows, out_path, domain)
        return out_path

    log.info(f"  [{domain}] starting -- {n_remaining:,}/{n_total:,} notes | model={model} | url={base_url}")
    system_prompt = _load_prompt(domain)
    client = openai.OpenAI(base_url=base_url, api_key=api_key or "EMPTY")

    from tenacity import retry as _retry, stop_after_attempt as _stop, wait_exponential as _wait
    _call_with_retry = _retry(stop=_stop(3), wait=_wait(min=2, max=10))(_call_llm)

    batch_size = 50
    new_rows: list[dict] = []

    with open(ckpt_path, "a", encoding="utf-8") as ckpt_fh:
        for i, (_, note_row) in enumerate(remaining_df.iterrows()):
            n_done_so_far = len(done_ids) + i
            if i % batch_size == 0:
                log.info(f"  [{domain}] {n_done_so_far}/{n_total} notes done "
                         f"({100*n_done_so_far/n_total:.1f}%) ...")

            try:
                result = _call_with_retry(client, model, system_prompt,
                                          str(note_row.get("note_text", "")))
            except Exception as exc:
                result = {"error": str(exc)}

            row = {
                "note_row_id":  str(note_row.get("note_row_id", "")),
                "research_id":  str(note_row.get("research_id", "")),
                "note_type":    str(note_row.get("note_type", "")),
                "note_date":    str(note_row.get("note_date", "") or ""),
                "domain":       domain,
                "llm_model":    model,
                "llm_base_url": base_url,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "result_json":  json.dumps(result),
            }
            # -- Flush each row immediately -- crash-safe ------------------
            ckpt_fh.write(json.dumps(row) + "\n")
            ckpt_fh.flush()
            new_rows.append(row)

    # -- Rebuild parquet from full checkpoint ------------------------------
    all_rows = existing_rows + new_rows
    _flush_parquet(all_rows, out_path, domain)
    log.info(f"  [{domain}] DONE -- {len(all_rows):,} rows -> {out_path.name}  "
             f"(checkpoint: {ckpt_path.name})")
    return out_path


def _flush_parquet(rows: list[dict], out_path: Path, domain: str) -> None:
    """Write rows list to parquet (overwrites). Safe to call multiple times."""
    if not rows:
        log.warning(f"  [{domain}] no rows to write")
        return
    tmp = out_path.with_suffix(".tmp.parquet")
    pd.DataFrame(rows).to_parquet(tmp, index=False)
    tmp.replace(out_path)  # atomic rename


def check_server(base_url: str, label: str) -> bool:
    """Ping /v1/models to confirm server is up."""
    try:
        import urllib.request
        url = f"{base_url.rstrip('/')}/models"
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
        models = [m.get("id", m.get("name", "?")) for m in data.get("data", data.get("models", []))]
        log.info(f"  {label} OK  models: {models}")
        return True
    except Exception as exc:
        log.warning(f"  {label} FAILED ({exc})")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LLM extraction via local Ollama (single or multi-server deployment)")
    parser.add_argument("--input-parquet", type=Path,
                        default=PROCESSED_REMAINING / "clinical_notes_long.parquet")
    parser.add_argument("--output-dir", type=Path, default=PROCESSED_OUTPUT)
    parser.add_argument("--url",   default="http://localhost:11434/v1",
                        help="Ollama OpenAI-compatible endpoint")
    parser.add_argument("--model", default="qwen3:14b",
                        help="Model name served by Ollama")
    parser.add_argument("--api-key", default="ollama",
                        help="API key (ollama doesn't need a real one)")
    parser.add_argument("--domains", nargs="+", default=ALL_DOMAINS,
                        help="Domains to extract (default: all 7)")
    parser.add_argument("--row-start", type=int, default=0,
                        help="Zero-based inclusive start row for note sharding")
    parser.add_argument("--row-end", type=int, default=None,
                        help="Zero-based inclusive end row for note sharding")
    parser.add_argument("--output-suffix", default="",
                        help="Optional suffix appended to checkpoint/parquet filenames")
    parser.add_argument("--dry-run", action="store_true",
                        help="Check connectivity only, do not run extraction")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("  LLM EXTRACTION  (Ollama)")
    log.info("=" * 70)

    # -- Validate input ----------------------------------------------------
    if not args.input_parquet.exists():
        log.error(f"Input parquet not found: {args.input_parquet}")
        sys.exit(1)
    notes_df = pd.read_parquet(args.input_parquet)
    log.info(f"  Input: {args.input_parquet.name}  ({len(notes_df):,} notes)")

    # Ensure note_row_id column exists
    if "note_row_id" not in notes_df.columns:
        notes_df["note_row_id"] = notes_df.index.astype(str)

    total_notes = len(notes_df)
    row_start = max(args.row_start, 0)
    row_end = total_notes - 1 if args.row_end is None else min(args.row_end, total_notes - 1)
    if row_start > row_end:
        log.error(f"Invalid row range: start={row_start} end={row_end}")
        sys.exit(1)
    notes_df = notes_df.iloc[row_start:row_end + 1].copy()
    log.info(f"  Note shard: rows {row_start}-{row_end}  ({len(notes_df):,} notes)")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # -- Server health check -----------------------------------------------
    log.info("  Checking Ollama server ...")
    if not check_server(args.url, f"Ollama ({args.url})"):
        log.error("Ollama not reachable. Is it running?")
        sys.exit(1)

    # Validate domains
    invalid = [d for d in args.domains if d not in DOMAIN_PROMPT]
    if invalid:
        log.error(f"Unknown domains: {invalid}. Valid: {ALL_DOMAINS}")
        sys.exit(1)

    log.info(f"  Domains: {args.domains}")
    log.info(f"  Model:   {args.model}")
    log.info(f"  URL:     {args.url}")
    if args.output_suffix:
        log.info(f"  Output suffix: {args.output_suffix}")

    if args.dry_run:
        log.info("  [dry-run] Connectivity check complete. Exiting.")
        return

    start = time.time()

    # -- Run extraction domain by domain -----------------------------------
    all_output_paths: list[Path] = []
    for domain in args.domains:
        out_path = extract_domain(notes_df, domain, args.url, args.api_key,
                                  args.model, args.output_dir,
                                  output_suffix=args.output_suffix)
        all_output_paths.append(out_path)

    # -- Merge all domain outputs into one combined parquet ----------------
    if all_output_paths:
        parts = [pd.read_parquet(p) for p in all_output_paths if p.exists()]
        if parts:
            combined = pd.concat(parts, ignore_index=True)
            combined_suffix = f"_{args.output_suffix}" if args.output_suffix else ""
            combined_path = args.output_dir / f"note_entities_llm_combined{combined_suffix}.parquet"
            combined.to_parquet(combined_path, index=False)
            log.info(f"  Combined LLM entities: {len(combined):,} rows -> {combined_path.name}")

    elapsed = time.time() - start
    log.info(f"  Total elapsed: {elapsed/60:.1f} min")
    log.info("=" * 70)
    log.info("  EXTRACTION COMPLETE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()

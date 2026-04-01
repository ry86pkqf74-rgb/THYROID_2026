#!/usr/bin/env python3
"""Concurrent Ollama extraction with per-note checkpointing.

This is the server-tracked V2 runtime used on the VastAI qwen3:32b worker.
It preserves JSONL checkpoints per note and writes a parquet only when a
domain finishes. Combined parquet output is intentionally skipped for
single-domain runs so the server does not leave behind misleading stale
combined files.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

PROCESSED_REMAINING = ROOT / "processed" / "remaining"
PROMPTS_DIR = ROOT / "notes_extraction_new" / "prompts"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)s] %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("split_extraction")

CONCURRENCY = int(os.environ.get("EXTRACTION_CONCURRENCY", "3"))

DOMAIN_PROMPT = {
    "complications": "complications_extraction_v1.txt",
    "staging": "staging_extraction_v1.txt",
    "genetics": "genetics_extraction_v1.txt",
    "recurrence": "recurrence_extraction_v1.txt",
    "medications": "medications_extraction_v1.txt",
    "procedures": "procedures_extraction_v1.txt",
    "problem_list": "problem_list_extraction_v1.txt",
    "imaging": "imaging_extraction_v1.txt",
    "pathology": "pathology_extraction_v1.txt",
    "labs": "labs_extraction_v1.txt",
    "physical_exam": "physical_exam_extraction_v1.txt",
    "rad_treatment": "rad_treatment_extraction_v1.txt",
    "past_medical_hx": "past_medical_hx_extraction_v1.txt",
    "past_surgical_hx": "past_surgical_hx_extraction_v1.txt",
    "operative_details": "operative_details_extraction_v1.txt",
    "presenting_symptoms": "presenting_symptoms_extraction_v1.txt",
    "dynamic_risk_response": "dynamic_risk_response_extraction_v1.txt",
    "survival_followup": "survival_followup_extraction_v1.txt",
    "vascular_invasion": "vascular_invasion_extraction_v1.txt",
    "rai_detailed": "rai_detailed_extraction_v1.txt",
    "recurrence_detailed": "recurrence_detailed_extraction_v1.txt",
    "medication_management": "medication_management_extraction_v1.txt",
    "functional_outcomes": "functional_outcomes_extraction_v1.txt",
    "tg_kinetics": "tg_kinetics_extraction_v1.txt",
    "parathyroid_detail": "parathyroid_detail_extraction_v1.txt",
    "airway_invasion": "airway_invasion_extraction_v1.txt",
    "frozen_section_detail": "frozen_section_detail_extraction_v1.txt",
    "us_nodule_dynamics": "us_nodule_dynamics_extraction_v1.txt",
    "cervical_ln_detail": "cervical_ln_detail_extraction_v1.txt",
    "patient_decision_adherence": "patient_decision_adherence_extraction_v1.txt",
    "operative_v2_enrichment": "operative_v2_enrichment_extraction_v1.txt",
    "complications_rln_laryngoscopy": "complications_rln_laryngoscopy_extraction_v1.txt",
    "molecular_thyroseq_afirma": "molecular_thyroseq_afirma_extraction_v1.txt",
    "synoptic_pathology_enrichment": "synoptic_pathology_enrichment_extraction_v1.txt",
    "tirads_granular": "tirads_granular_extraction_v1.txt",
    "parathyroid_per_gland": "parathyroid_per_gland_extraction_v1.txt",
}

ALL_DOMAINS = list(DOMAIN_PROMPT.keys())


def _load_prompt(domain: str) -> str:
    fname = DOMAIN_PROMPT.get(domain)
    if not fname:
        return f"Extract {domain} entities from the following clinical note. Return JSON."
    path = PROMPTS_DIR / fname
    if path.exists():
        return path.read_text(encoding="utf-8")
    log.warning("Prompt file not found: %s -- using fallback.", path)
    return f"Extract {domain} entities from the following clinical note. Return JSON."


def _call_llm(client: Any, model: str, system_prompt: str, note_text: str) -> dict:
    import re as _re

    def _attempt(response_fmt: dict[str, str] | None):
        kwargs: dict[str, Any] = dict(
            model=model,
            messages=[
                {"role": "system", "content": "/no_think\n" + system_prompt},
                {"role": "user", "content": note_text[:6000]},
            ],
            temperature=0,
            max_tokens=1500,
            timeout=120,
        )
        if response_fmt:
            kwargs["response_format"] = response_fmt
        return client.chat.completions.create(**kwargs)

    raw = "{}"
    try:
        try:
            response = _attempt({"type": "json_object"})
        except Exception as exc:
            if "json_object" in str(exc) or "response_format" in str(exc) or "400" in str(exc):
                response = _attempt(None)
            else:
                raise
        raw = response.choices[0].message.content or "{}"
        match = _re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, _re.DOTALL)
        if match:
            raw = match.group(1)
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"parse_error": True, "raw": raw[:500]}
    except Exception as exc:
        log.warning("LLM call failed: %s", exc)
        raise


def _flush_parquet(rows: list[dict], out_path: Path, domain: str) -> None:
    if not rows:
        log.warning("  [%s] no rows to write", domain)
        return
    tmp = out_path.with_suffix(".tmp.parquet")
    pd.DataFrame(rows).to_parquet(tmp, index=False)
    tmp.replace(out_path)


def extract_domain(
    notes_df: pd.DataFrame,
    domain: str,
    base_url: str,
    api_key: str,
    model: str,
    output_dir: Path,
    concurrency: int = 3,
) -> Path:
    import openai
    from tenacity import retry as _retry
    from tenacity import stop_after_attempt as _stop
    from tenacity import wait_exponential as _wait

    ckpt_path = output_dir / f"note_entities_llm_{domain}.ckpt.jsonl"
    out_path = output_dir / f"note_entities_llm_{domain}.parquet"

    done_ids: set[str] = set()
    existing_rows: list[dict] = []
    if ckpt_path.exists():
        with open(ckpt_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                existing_rows.append(row)
                done_ids.add(str(row.get("note_row_id", "")))
        if done_ids:
            log.info(
                "  [%s] RESUME -- %s notes already done, %s remaining",
                domain,
                f"{len(done_ids):,}",
                f"{len(notes_df) - len(done_ids):,}",
            )

    remaining_df = notes_df[~notes_df["note_row_id"].astype(str).isin(done_ids)]
    n_total = len(notes_df)
    n_remaining = len(remaining_df)

    if n_remaining == 0:
        log.info("  [%s] already complete (%s notes) -- rebuilding parquet", domain, f"{n_total:,}")
        _flush_parquet(existing_rows, out_path, domain)
        return out_path

    log.info(
        "  [%s] starting -- %s/%s notes | model=%s | url=%s | concurrency=%s",
        domain,
        f"{n_remaining:,}",
        f"{n_total:,}",
        model,
        base_url,
        concurrency,
    )

    system_prompt = _load_prompt(domain)
    client = openai.OpenAI(base_url=base_url, api_key=api_key or "EMPTY")
    call_with_retry = _retry(stop=_stop(3), wait=_wait(min=2, max=10))(_call_llm)

    ckpt_lock = threading.Lock()
    new_rows: list[dict] = []
    completed_count = 0
    domain_start = time.time()

    def _process_note(note_row: pd.Series) -> dict:
        try:
            result = call_with_retry(client, model, system_prompt, str(note_row.get("note_text", "")))
        except Exception as exc:
            result = {"error": str(exc)}

        return {
            "note_row_id": str(note_row.get("note_row_id", "")),
            "research_id": str(note_row.get("research_id", "")),
            "note_type": str(note_row.get("note_type", "")),
            "note_date": str(note_row.get("note_date", "") or ""),
            "domain": domain,
            "llm_model": model,
            "llm_base_url": base_url,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "result_json": json.dumps(result),
        }

    with open(ckpt_path, "a", encoding="utf-8") as ckpt_handle:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(_process_note, note_row): note_row for _, note_row in remaining_df.iterrows()}
            for future in as_completed(futures):
                row = future.result()
                with ckpt_lock:
                    ckpt_handle.write(json.dumps(row) + "\n")
                    ckpt_handle.flush()
                    new_rows.append(row)
                    completed_count += 1
                    n_done = len(done_ids) + completed_count
                    if completed_count % 50 == 0:
                        elapsed = time.time() - domain_start
                        rate = completed_count / max(1, elapsed)
                        eta_min = (n_remaining - completed_count) / max(rate, 0.01) / 60
                        log.info(
                            "  [%s] %s/%s notes done (%.1f%%) | %.1f notes/sec | ETA %.0fm",
                            domain,
                            n_done,
                            n_total,
                            100 * n_done / n_total,
                            rate,
                            eta_min,
                        )

    all_rows = existing_rows + new_rows
    _flush_parquet(all_rows, out_path, domain)
    elapsed = time.time() - domain_start
    rate = len(new_rows) / max(1, elapsed)
    log.info(
        "  [%s] DONE -- %s rows -> %s (%.1f notes/sec, %.1fm)",
        domain,
        f"{len(all_rows):,}",
        out_path.name,
        rate,
        elapsed / 60,
    )
    return out_path


def check_server(base_url: str, label: str) -> bool:
    try:
        import urllib.request

        url = f"{base_url.rstrip('/')}/models"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read())
        models = [model.get("id", model.get("name", "?")) for model in data.get("data", data.get("models", []))]
        log.info("  %s OK  models: %s", label, models)
        return True
    except Exception as exc:
        log.warning("  %s FAILED (%s)", label, exc)
        return False


def main() -> None:
    global CONCURRENCY

    parser = argparse.ArgumentParser(description="LLM extraction with concurrent Ollama requests")
    parser.add_argument("--input-parquet", type=Path, default=PROCESSED_REMAINING / "clinical_notes_long.parquet")
    parser.add_argument("--output-dir", type=Path, default=PROCESSED_REMAINING)
    parser.add_argument("--url", default="http://localhost:11434/v1")
    parser.add_argument("--model", default="qwen3:32b")
    parser.add_argument("--api-key", default="ollama")
    parser.add_argument("--domains", nargs="+", default=ALL_DOMAINS)
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    CONCURRENCY = args.concurrency

    log.info("=" * 70)
    log.info("  LLM EXTRACTION  (Concurrent Ollama)")
    log.info("  Concurrency: %s threads | Model: %s", CONCURRENCY, args.model)
    log.info("=" * 70)

    if not args.input_parquet.exists():
        log.error("Input parquet not found: %s", args.input_parquet)
        sys.exit(1)

    notes_df = pd.read_parquet(args.input_parquet)
    log.info("  Input: %s  (%s notes)", args.input_parquet.name, f"{len(notes_df):,}")

    if "note_row_id" not in notes_df.columns:
        notes_df["note_row_id"] = notes_df.index.astype(str)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    log.info("  Checking Ollama server ...")
    if not check_server(args.url, f"Ollama ({args.url})"):
        log.error("Ollama not reachable. Is it running?")
        sys.exit(1)

    invalid = [domain for domain in args.domains if domain not in DOMAIN_PROMPT]
    if invalid:
        log.error("Unknown domains: %s. Valid: %s", invalid, ALL_DOMAINS)
        sys.exit(1)

    log.info("  Domains: %s", args.domains)
    log.info("  Model:   %s", args.model)
    log.info("  URL:     %s", args.url)

    if args.dry_run:
        log.info("  [dry-run] Connectivity check complete. Exiting.")
        return

    start = time.time()
    all_output_paths: list[Path] = []

    for domain in args.domains:
        out_path = extract_domain(notes_df, domain, args.url, args.api_key, args.model, args.output_dir, CONCURRENCY)
        all_output_paths.append(out_path)

    if len(all_output_paths) > 1:
        parts = [pd.read_parquet(path) for path in all_output_paths if path.exists()]
        if parts:
            combined = pd.concat(parts, ignore_index=True)
            combined_path = args.output_dir / "note_entities_llm_combined.parquet"
            combined.to_parquet(combined_path, index=False)
            log.info("  Combined LLM entities: %s rows -> %s", f"{len(combined):,}", combined_path.name)
    else:
        log.info("  Single-domain run detected; skipping combined parquet write")

    elapsed = time.time() - start
    log.info("  Total elapsed: %.1f min", elapsed / 60)
    log.info("=" * 70)
    log.info("  EXTRACTION COMPLETE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
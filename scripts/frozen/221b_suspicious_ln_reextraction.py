#!/usr/bin/env python3
"""
Script 221b — Re-extract ``suspicious_ln_present`` for tirads_v2_reports_raw.

Audit (commit 3d01664 follow-up) found a ~36% false-positive rate in the
original Qwen2.5-32B extraction of ``suspicious_ln_present``: 363 / 1,015
TRUE rows had evidence describing benign-appearing lymph nodes with fatty
hila but no positive descriptors, and another 56 rows contained explicit
"no suspicious lymph nodes" negation yet were still flagged TRUE.

This script re-extracts the field with OpenAI ``gpt-5.2`` using a tightened
prompt with explicit FALSE/NULL rules, then overwrites the column in
``thyroid_canonical_publication_v1_0.main.tirads_v2_reports_raw`` and
rebuilds dependent rollups + the canonical_patient_master column.

Methodological note: original extraction used Qwen2.5-32B (open-weights);
re-extraction uses OpenAI gpt-5.2 (closed). Cross-family triangulation
(Qwen → OpenAI) is cleaner for the manuscript methods section than
intra-family correction (Qwen → Anthropic).

Hard rules (Logan, follow-up 2026-04-19):
  * Touch ONLY ``suspicious_ln_present`` and the new
    ``suspicious_ln_rationale`` column.
  * Do NOT modify tirads_v2_nodules_raw / preop_tirads_best /
    tirads_best_score_v12 / any legacy TIRADS columns.
  * Checkpoint every 50 rows so the 8,810-row run is resumable.
  * If (TRUE→FALSE + TRUE→NULL) flips < 200, STOP and report (audit was off).
  * Do NOT push; commit by amending 3d01664.

Subcommands:
  --mode audit           Read-only stats on existing flag distribution.
  --mode smoke           Stratified 50-row pick (20T/15F/15N) + table output.
  --mode extract         Run gpt-5.2 extraction; write checkpoint+results parquet.
  --mode apply           Apply diff to canonical (raw + rollup + CPM column).
  --mode all             extract → apply → re-validate.

Helpful flags:
  --limit N              Smoke-test on first N rows (default: all 8,810).
  --resume               Skip rows already in the checkpoint file.
  --dry-run-apply        Print diff/UPDATE plans without writing canonical.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

# ── constants ────────────────────────────────────────────────────────────────

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
RAW_TABLE = "tirads_v2_reports_raw"
ROLLUP_TABLE = "tirads_v2_report_patient_rollup_v1"
CPM_COL = "tirads_v2_any_suspicious_ln_on_us"
RATIONALE_COL = "suspicious_ln_rationale"
EXPECTED_RAW_ROWS = 8_810
EXPECTED_RAW_RIDS = 4_073
EXPECTED_CPM_ROWS = 10_871

OPENAI_MODEL = "gpt-5.2"  # bumped from claude-sonnet-4-6 (Logan 2026-04-19) — cross-family triangulation Qwen→OpenAI for manuscript methods
MAX_WORKERS = 10
CHECKPOINT_EVERY = 50
MAX_RETRIES = 4
RETRY_BASE_DELAY_S = 2.0
MAX_TOKENS = 250          # enough for {"suspicious_ln":..., "rationale":"..."}
HALT_GATE_TRUE_FLIPS = 200   # gates on (TRUE→FALSE + TRUE→NULL); audit predicted ~419

# Smoke test: stratified pick from raw table.
SMOKE_TRUE = 20
SMOKE_FALSE = 15
SMOKE_NULL = 15
SMOKE_TOTAL = SMOKE_TRUE + SMOKE_FALSE + SMOKE_NULL
SMOKE_CKPT_PATH_NAME = "221b_smoke_ckpt.jsonl"
SMOKE_RESULTS_PARQUET_NAME = "221b_smoke_results.parquet"

SCRIPT_TAG = "scripts/221b_suspicious_ln_reextraction.py"
RUN_TS_ISO = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
RUN_DATE = RUN_TS_ISO[:10]

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CKPT_PATH = OUTPUT_DIR / "221b_ln_ckpt.jsonl"
RESULTS_PARQUET = OUTPUT_DIR / "221b_ln_results.parquet"
DECISIONS_PATH = OUTPUT_DIR / "221b_ln_reextraction.json"
LOG_PATH = OUTPUT_DIR / "221b_ln_reextraction.log"

SYSTEM_PROMPT = """You are a radiology NLP system. Given the evidence text and impression text from one thyroid ultrasound report, return a single JSON object:

  {"suspicious_ln": true | false | null, "rationale": "<1 sentence>"}

RULES:

RULE 1 — TRUE only if ANY of:
  (a) The radiologist explicitly flags a cervical lymph node as SUSPICIOUS, CONCERNING, PATHOLOGIC, METASTATIC, ABNORMAL, or uses "adenopathy" (unqualified, i.e. not "no adenopathy" / "reactive adenopathy").
  (b) The report describes features meeting criteria for suspicious LN: loss of fatty hilum WITH round shape, OR microcalcifications, OR cystic change, OR abnormal vascularity, OR short-axis >10mm with any of the above.
  (c) An LN is described as "indeterminate" but ALSO has a suspicious feature from (b) — clinical override: feature beats hedge.

RULE 2 — FALSE if ANY of:
  (a) All lymph node mentions carry positive benign descriptors: BENIGN, BENIGN-APPEARING, NORMAL MORPHOLOGY, REACTIVE, SHOTTY (with normal hila), PROBABLY BENIGN, FATTY HILUM, OVOID with thin cortex.
  (b) The radiologist explicitly negates suspicion: "no suspicious lymph nodes", "no concerning adenopathy", "no pathologic nodes", "no abnormal lymph nodes", "no evidence of metastatic/recurrent/pathologic adenopathy", "no highly suspicious abnormality" — even without a positive benign descriptor.
  (c) LNs are mentioned with measurements ONLY (no descriptive adjectives, no morphology) AND the report disposition is routine (e.g. "follow up in 1 year", "correlate clinically", "stable") with NO concerning language anywhere — the radiologist's implicit clinical judgment is non-suspicious.
  (d) The only abnormality is in the thyroid fossa or thyroid bed (those are thyroid tissue findings, not LNs).

RULE 3 — NULL ONLY if ANY of:
  (a) No lymph nodes are discussed in the report at all.
  (b) LNs are described with EXPLICIT uncertainty language — one of: "indeterminate", "nonspecific" (when applied to the LN itself, not to a disposition), "cannot exclude", "of uncertain significance", "possibly abnormal", "may be a lymph node" — AND no other clarifying context that would resolve to TRUE (rule 1) or FALSE (rule 2).

RULE 4 — Decision precedence (apply in order):
  1. If Rule 1 fires (any TRUE trigger including the (c) feature override), return TRUE.
  2. Else if Rule 3(a) fires (no LNs discussed at all), return NULL.
  3. Else if Rule 2 fires (any FALSE trigger), return FALSE.
  4. Else if Rule 3(b) fires (explicit uncertainty language), return NULL.
  5. Otherwise return FALSE (LNs were mentioned non-suspiciously without explicit hedge — implicit clinical clearance).

RULE 5 — Return strictly valid JSON, no prose, no markdown fences."""

# ── logging ──────────────────────────────────────────────────────────────────

_log_buf: list[str] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}Z] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def _flush_log() -> None:
    LOG_PATH.write_text("\n".join(_log_buf) + "\n")


# ── connections ──────────────────────────────────────────────────────────────

def md_connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck token (token_mode={token_mode()})."
        )
    log(f"connecting to MotherDuck '{CANONICAL_DB}' (token_mode={token_mode()})")
    return duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")


def get_openai_client():
    try:
        import openai  # noqa: WPS433
    except ImportError as e:
        raise SystemExit(
            "openai package not installed in active interpreter "
            f"({sys.executable}). Run: .venv/bin/python -m pip install openai"
        ) from e
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise SystemExit(
            "OPENAI_API_KEY env var is required for --mode extract / --mode smoke. "
            "Export it (do NOT commit the key)."
        )
    return openai.OpenAI(api_key=key)


# ── source row extraction ────────────────────────────────────────────────────

def fetch_source_rows(con: duckdb.DuckDBPyConnection, limit: int | None = None) -> list[dict]:
    sql = f"""
        SELECT
          research_id,
          note_row_id,
          COALESCE(evidence_text, '')        AS evidence_text,
          COALESCE(report_impression_text,'') AS report_impression_text,
          suspicious_ln_present              AS old_flag
        FROM {RAW_TABLE}
        ORDER BY research_id, note_row_id
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.description]
    return [dict(zip(cols, r)) for r in rows]


def fetch_smoke_rows(con: duckdb.DuckDBPyConnection, seed: int = 7) -> list[dict]:
    """Stratified pick: SMOKE_TRUE TRUE + SMOKE_FALSE FALSE + SMOKE_NULL NULL."""
    parts = [
        ("TRUE",  SMOKE_TRUE,  "suspicious_ln_present = TRUE"),
        ("FALSE", SMOKE_FALSE, "suspicious_ln_present = FALSE"),
        ("NULL",  SMOKE_NULL,  "suspicious_ln_present IS NULL"),
    ]
    out: list[dict] = []
    for label, n_take, where in parts:
        rows = con.execute(
            f"""
            SELECT
              research_id,
              note_row_id,
              COALESCE(evidence_text, '')         AS evidence_text,
              COALESCE(report_impression_text,'') AS report_impression_text,
              suspicious_ln_present               AS old_flag
            FROM {RAW_TABLE}
            WHERE {where}
            ORDER BY hash(research_id || '|' || note_row_id || '|{seed}')
            LIMIT {int(n_take)}
            """
        ).fetchall()
        cols = [d[0] for d in con.description]
        n_got = len(rows)
        log(f"  smoke stratum old={label}: requested {n_take}, got {n_got}")
        out.extend(dict(zip(cols, r)) for r in rows)
    return out


def row_key(row: dict) -> str:
    return f"{row['research_id']}|{row['note_row_id']}"


# ── checkpoint helpers ───────────────────────────────────────────────────────

def load_checkpoint(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                out[obj["row_key"]] = obj
            except Exception:
                pass
    return out


def append_checkpoint(path: Path, results: list[dict]) -> None:
    with path.open("a") as fh:
        for r in results:
            fh.write(json.dumps(r, default=str) + "\n")


# ── OpenAI call ──────────────────────────────────────────────────────────────

def call_llm(client, row: dict) -> dict:
    """Returns {row_key, status, new_flag, new_rationale, raw, error?}.

    Uses ``response_format={"type":"json_object"}`` to enforce strict JSON
    output. The system prompt already instructs the model to return only the
    JSON object — we explicitly mention 'JSON' in the user prompt as required
    by OpenAI's json_object response format API contract.
    """
    user_prompt = (
        "Return JSON describing whether the report contains a suspicious cervical "
        "lymph node, per the rules.\n"
        "\n"
        "Evidence text:\n"
        f"{(row['evidence_text'] or '').strip() or '(none)'}\n"
        "\n"
        "Impression text:\n"
        f"{(row['report_impression_text'] or '').strip() or '(none)'}"
    )
    last_err: str | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                max_completion_tokens=MAX_TOKENS,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            cleaned = raw
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[-1]
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3]
                cleaned = cleaned.strip()
            try:
                obj = json.loads(cleaned)
            except json.JSONDecodeError as e:
                last_err = f"json:{e}"
                if attempt == MAX_RETRIES - 1:
                    return {
                        "row_key": row_key(row),
                        "research_id": row["research_id"],
                        "note_row_id": row["note_row_id"],
                        "old_flag": row["old_flag"],
                        "status": "json_error",
                        "new_flag": None,
                        "new_rationale": None,
                        "raw": raw,
                        "error": last_err,
                    }
                continue

            new_flag_raw = obj.get("suspicious_ln")
            if new_flag_raw is True or new_flag_raw is False or new_flag_raw is None:
                new_flag = new_flag_raw
            elif isinstance(new_flag_raw, str):
                low = new_flag_raw.strip().lower()
                if low in ("true",):
                    new_flag = True
                elif low in ("false",):
                    new_flag = False
                else:
                    new_flag = None
            else:
                new_flag = None
            rationale = obj.get("rationale")
            rationale_str = str(rationale).strip()[:1000] if rationale is not None else None

            return {
                "row_key": row_key(row),
                "research_id": row["research_id"],
                "note_row_id": row["note_row_id"],
                "old_flag": row["old_flag"],
                "status": "ok",
                "new_flag": new_flag,
                "new_rationale": rationale_str,
                "raw": raw,
                "error": None,
            }
        except Exception as e:  # noqa: BLE001
            import openai
            wait = RETRY_BASE_DELAY_S * (2 ** attempt)
            cls = type(e).__name__
            last_err = f"{cls}:{str(e)[:200]}"
            if isinstance(e, openai.RateLimitError):
                time.sleep(wait)
                continue
            if isinstance(e, openai.APIStatusError) and getattr(e, "status_code", 0) >= 500:
                time.sleep(wait)
                continue
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)
                continue
            return {
                "row_key": row_key(row),
                "research_id": row["research_id"],
                "note_row_id": row["note_row_id"],
                "old_flag": row["old_flag"],
                "status": "api_error",
                "new_flag": None,
                "new_rationale": None,
                "raw": None,
                "error": last_err,
            }
    return {
        "row_key": row_key(row),
        "research_id": row["research_id"],
        "note_row_id": row["note_row_id"],
        "old_flag": row["old_flag"],
        "status": "max_retries",
        "new_flag": None,
        "new_rationale": None,
        "raw": None,
        "error": last_err,
    }


# ── modes ────────────────────────────────────────────────────────────────────

def mode_audit(con: duckdb.DuckDBPyConnection) -> dict:
    log("=== MODE: audit (read-only) ===")
    out: dict = {"mode": "audit", "ts": RUN_TS_ISO}
    n_rows, n_rids = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {RAW_TABLE}"
    ).fetchone()
    out["raw"] = {"rows": int(n_rows), "rids": int(n_rids)}
    log(f"  {RAW_TABLE}: {n_rows:,} rows / {n_rids:,} RIDs "
        f"(expected {EXPECTED_RAW_ROWS:,}/{EXPECTED_RAW_RIDS:,})")
    if n_rows != EXPECTED_RAW_ROWS or n_rids != EXPECTED_RAW_RIDS:
        out["raw_size_mismatch"] = True

    by_old = con.execute(
        f"SELECT suspicious_ln_present, COUNT(*) "
        f"FROM {RAW_TABLE} GROUP BY 1 ORDER BY 1 NULLS FIRST"
    ).fetchall()
    out["report_flag_distribution"] = [
        {"flag": ("NULL" if v is None else bool(v)), "n": int(n)} for v, n in by_old
    ]
    log("  suspicious_ln_present (report-level):")
    for v, n in by_old:
        log(f"    {('NULL' if v is None else bool(v))!s:<6}  n={n:,}")

    cpm_true = con.execute(
        f"SELECT COUNT(*) FROM canonical_patient_master "
        f"WHERE {CPM_COL} = TRUE"
    ).fetchone()[0]
    cpm_false = con.execute(
        f"SELECT COUNT(*) FROM canonical_patient_master "
        f"WHERE {CPM_COL} = FALSE"
    ).fetchone()[0]
    cpm_null = con.execute(
        f"SELECT COUNT(*) FROM canonical_patient_master "
        f"WHERE {CPM_COL} IS NULL"
    ).fetchone()[0]
    out["cpm_flag_distribution"] = {
        "true": int(cpm_true), "false": int(cpm_false), "null": int(cpm_null),
    }
    log(f"  CPM {CPM_COL}: TRUE={cpm_true:,} FALSE={cpm_false:,} NULL={cpm_null:,}")

    rationale_present = con.execute(
        "SELECT 1 FROM information_schema.columns "
        f"WHERE table_catalog='{CANONICAL_DB}' AND table_schema='main' "
        f"AND table_name='{RAW_TABLE}' AND column_name='{RATIONALE_COL}'"
    ).fetchone() is not None
    out["rationale_col_present"] = rationale_present
    log(f"  {RAW_TABLE}.{RATIONALE_COL} present: {rationale_present}")
    return out


def mode_extract(args, con: duckdb.DuckDBPyConnection) -> dict:
    log("=== MODE: extract ===")
    rows = fetch_source_rows(con, limit=args.limit)
    log(f"  fetched {len(rows):,} source rows from {RAW_TABLE} "
        f"(limit={args.limit if args.limit else 'none'})")

    completed_keys: set[str] = set()
    if args.resume and CKPT_PATH.exists():
        existing = load_checkpoint(CKPT_PATH)
        completed_keys = set(existing.keys())
        log(f"  resume: {len(completed_keys):,} rows already in checkpoint, will skip")
    elif CKPT_PATH.exists() and not args.resume:
        log(f"  starting fresh — moving previous checkpoint to {CKPT_PATH}.bak.{int(time.time())}")
        CKPT_PATH.rename(CKPT_PATH.with_suffix(f".jsonl.bak.{int(time.time())}"))

    todo = [r for r in rows if row_key(r) not in completed_keys]
    log(f"  to extract: {len(todo):,} rows ({len(rows) - len(todo):,} skipped via resume)")

    if not todo:
        log("  nothing to do.")
        return {"mode": "extract", "n_extracted": 0, "completed_keys": len(completed_keys)}

    client = get_openai_client()

    pending: list[dict] = []
    n_done = 0
    n_ok = 0
    n_err = 0
    started = time.time()

    def _flush() -> None:
        nonlocal pending
        if pending:
            append_checkpoint(CKPT_PATH, pending)
            pending = []

    log(f"  spawning {MAX_WORKERS} workers (model={OPENAI_MODEL}, "
        f"checkpoint every {CHECKPOINT_EVERY} rows)")
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(call_llm, client, r): r for r in todo}
            for fut in as_completed(futures):
                res = fut.result()
                pending.append(res)
                n_done += 1
                if res["status"] == "ok":
                    n_ok += 1
                else:
                    n_err += 1
                if n_done % CHECKPOINT_EVERY == 0:
                    _flush()
                    elapsed = time.time() - started
                    rate = n_done / elapsed if elapsed else 0.0
                    eta = (len(todo) - n_done) / rate if rate else 0.0
                    log(f"    [{n_done:,}/{len(todo):,}] ok={n_ok:,} err={n_err:,} "
                        f"rate={rate:.1f}/s eta={eta/60:.1f}min")
    finally:
        _flush()

    log(f"  extract done: {n_done:,} processed, {n_ok:,} ok, {n_err:,} errors "
        f"in {(time.time()-started)/60:.1f} min")

    materialize_results_parquet()
    return {
        "mode": "extract",
        "n_processed": n_done,
        "n_ok": n_ok,
        "n_err": n_err,
        "checkpoint_path": str(CKPT_PATH),
        "results_parquet": str(RESULTS_PARQUET),
    }


def materialize_results_parquet() -> None:
    if not CKPT_PATH.exists():
        log("  no checkpoint to materialize.")
        return
    loc = duckdb.connect()
    loc.execute(
        f"COPY (SELECT * FROM read_json_auto('{CKPT_PATH}', maximum_object_size=33554432)) "
        f"TO '{RESULTS_PARQUET}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)"
    )
    n = loc.execute(f"SELECT COUNT(*) FROM read_parquet('{RESULTS_PARQUET}')").fetchone()[0]
    log(f"  materialized {n:,} rows → {RESULTS_PARQUET}")


def confusion_matrix(con: duckdb.DuckDBPyConnection) -> dict:
    if not RESULTS_PARQUET.exists():
        raise SystemExit(f"missing {RESULTS_PARQUET} — run --mode extract first")
    rp = str(RESULTS_PARQUET)
    log("=== confusion matrix (old vs new) ===")
    rows = duckdb.sql(f"""
        SELECT
          CASE WHEN old_flag IS NULL THEN 'NULL' WHEN old_flag THEN 'TRUE' ELSE 'FALSE' END AS old,
          CASE WHEN new_flag IS NULL THEN 'NULL' WHEN new_flag THEN 'TRUE' ELSE 'FALSE' END AS new,
          COUNT(*) AS n
        FROM read_parquet('{rp}')
        WHERE status = 'ok'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()
    cells = [{"old": o, "new": n, "n": int(c)} for o, n, c in rows]
    log(f"  {'old':>6s} | {'new':>6s} | n")
    log(f"  {'-'*6} + {'-'*6} + {'-'*7}")
    for c in cells:
        log(f"  {c['old']:>6s} | {c['new']:>6s} | {c['n']:>7,}")

    by_status = duckdb.sql(
        f"SELECT status, COUNT(*) FROM read_parquet('{rp}') GROUP BY 1 ORDER BY 1"
    ).fetchall()
    log("  by status:")
    for s, n in by_status:
        log(f"    {s:14s}  n={int(n):,}")

    true_to_false = sum(c["n"] for c in cells if c["old"] == "TRUE" and c["new"] == "FALSE")
    true_to_null = sum(c["n"] for c in cells if c["old"] == "TRUE" and c["new"] == "NULL")
    false_to_true = sum(c["n"] for c in cells if c["old"] == "FALSE" and c["new"] == "TRUE")
    null_to_true = sum(c["n"] for c in cells if c["old"] == "NULL" and c["new"] == "TRUE")
    log(f"  TRUE→FALSE flips: {true_to_false:,}  (audit predicted ~360)")
    log(f"  TRUE→NULL  flips: {true_to_null:,}   (audit predicted ~50)")
    log(f"  FALSE→TRUE flips: {false_to_true:,}")
    log(f"  NULL →TRUE flips: {null_to_true:,}")

    return {
        "cells": cells,
        "by_status": [{"status": s, "n": int(n)} for s, n in by_status],
        "true_to_false": int(true_to_false),
        "true_to_null": int(true_to_null),
        "false_to_true": int(false_to_true),
        "null_to_true": int(null_to_true),
    }


def mode_smoke(args, con: duckdb.DuckDBPyConnection) -> dict:
    log(f"=== MODE: smoke (stratified {SMOKE_TRUE}T/{SMOKE_FALSE}F/{SMOKE_NULL}N = {SMOKE_TOTAL} rows; model={OPENAI_MODEL}) ===")
    out: dict = {"mode": "smoke", "ts": RUN_TS_ISO, "model": OPENAI_MODEL}

    rows = fetch_smoke_rows(con, seed=args.smoke_seed)
    if not rows:
        raise SystemExit("smoke: no rows fetched.")

    smoke_ckpt = OUTPUT_DIR / SMOKE_CKPT_PATH_NAME
    smoke_results = OUTPUT_DIR / SMOKE_RESULTS_PARQUET_NAME
    if smoke_ckpt.exists():
        bak = smoke_ckpt.with_suffix(f".jsonl.bak.{int(time.time())}")
        log(f"  prior smoke checkpoint moved to {bak}")
        smoke_ckpt.rename(bak)

    client = get_openai_client()

    pending: list[dict] = []
    n_done = 0
    started = time.time()

    log(f"  spawning {MAX_WORKERS} workers ...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(call_llm, client, r): r for r in rows}
        for fut in as_completed(futures):
            res = fut.result()
            pending.append(res)
            n_done += 1
            if n_done % 10 == 0:
                log(f"    smoke progress: {n_done}/{len(rows)}")
    append_checkpoint(smoke_ckpt, pending)
    elapsed = time.time() - started
    log(f"  smoke extract done: {n_done} rows in {elapsed:.1f}s "
        f"({n_done/elapsed:.1f}/s)")

    # Materialize parquet
    loc = duckdb.connect()
    loc.execute(
        f"COPY (SELECT * FROM read_json_auto('{smoke_ckpt}', maximum_object_size=33554432)) "
        f"TO '{smoke_results}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)"
    )

    # Summary stats
    n_ok = sum(1 for r in pending if r["status"] == "ok")
    n_json_err = sum(1 for r in pending if r["status"] == "json_error")
    n_api_err = sum(1 for r in pending if r["status"] in ("api_error", "max_retries"))
    n_null_rationale = sum(
        1 for r in pending if r["status"] == "ok" and not (r.get("new_rationale") or "").strip()
    )
    rats = [r.get("new_rationale") or "" for r in pending if r["status"] == "ok"]
    mean_len = (sum(len(s) for s in rats) / len(rats)) if rats else 0.0

    log("")
    log("──── SMOKE SUMMARY ────")
    log(f"  total rows           : {n_done}")
    log(f"  status=ok            : {n_ok}")
    log(f"  status=json_error    : {n_json_err}")
    log(f"  status=api_error/max : {n_api_err}")
    log(f"  null/empty rationale : {n_null_rationale}")
    log(f"  mean rationale length: {mean_len:.1f} chars")
    out["summary"] = {
        "total": n_done,
        "n_ok": n_ok,
        "n_json_error": n_json_err,
        "n_api_error_or_max": n_api_err,
        "n_null_rationale": n_null_rationale,
        "mean_rationale_len": round(mean_len, 1),
    }

    # 3x3 confusion matrix (TRUE/FALSE/NULL × TRUE/FALSE/NULL)
    def _label(v: Any) -> str:
        if v is None:
            return "NULL"
        if v is True:
            return "TRUE"
        if v is False:
            return "FALSE"
        return str(v)

    grid: dict[tuple[str, str], int] = {}
    for r in pending:
        if r["status"] != "ok":
            continue
        old_l = _label(r["old_flag"])
        new_l = _label(r["new_flag"])
        grid[(old_l, new_l)] = grid.get((old_l, new_l), 0) + 1

    labels = ("TRUE", "FALSE", "NULL")
    log("")
    log(f"  3x3 confusion (rows = old_flag, cols = new_flag); status=ok only")
    header = "         " + "".join(f"{l:>8s}" for l in labels) + "      total"
    log(header)
    log("         " + "-" * (8 * 3 + 11))
    for o in labels:
        cells = [grid.get((o, n), 0) for n in labels]
        row_total = sum(cells)
        log(f"  old={o:5s}" + "".join(f"{c:>8d}" for c in cells) + f"  {row_total:>9d}")
    col_totals = [sum(grid.get((o, n), 0) for o in labels) for n in labels]
    log("  total    " + "".join(f"{c:>8d}" for c in col_totals) + f"  {sum(col_totals):>9d}")
    out["confusion_3x3"] = {f"{o}->{n}": grid.get((o, n), 0) for o in labels for n in labels}

    # Targeted callout: TRUE→ flips among the 20 old=TRUE rows
    old_true_rows = [r for r in pending if r["status"] == "ok" and r["old_flag"] is True]
    n_old_true = len(old_true_rows)
    n_t2f = sum(1 for r in old_true_rows if r["new_flag"] is False)
    n_t2n = sum(1 for r in old_true_rows if r["new_flag"] is None)
    n_tt = sum(1 for r in old_true_rows if r["new_flag"] is True)
    n_t_flips = n_t2f + n_t2n
    log("")
    log(f"  Among {n_old_true} sampled old-TRUE rows:")
    log(f"    TRUE→TRUE  : {n_tt:>3d}   (target ~5)")
    log(f"    TRUE→FALSE : {n_t2f:>3d}   (target band 5-9; explicit benign clearance)")
    log(f"    TRUE→NULL  : {n_t2n:>3d}   (target band 3-7; indeterminate / hedged)")
    log(f"    TRUE flips : {n_t_flips:>3d}   (sum of FALSE+NULL flips)")
    out["old_true_breakdown"] = {
        "n_sampled": n_old_true,
        "true_to_true": n_tt,
        "true_to_false": n_t2f,
        "true_to_null": n_t2n,
        "total_flips": n_t_flips,
    }

    # Single-table format: research_id | evidence (200) | impression (200) | old | new | rationale
    log("")
    log(f"──── ALL {len(pending)} ROWS (single-table) ────")
    header = (f"{'rid':>6s} | {'note':>10s} | {'old':>5s} | {'new':>5s} | "
              f"{'evidence (200ch)':<200s} | {'impression (200ch)':<200s} | rationale")
    log(header)
    log("-" * 100)
    # Sort: old=TRUE first (most diagnostic), then FALSE, then NULL
    def _stratum(r: dict) -> int:
        return {True: 0, False: 1, None: 2}.get(r["old_flag"], 3)
    show = sorted(pending, key=lambda r: (_stratum(r), str(r["research_id"])))
    for r in show:
        src = next((s for s in rows if row_key(s) == r["row_key"]), None)
        ev = ((src["evidence_text"] if src else "") or "")[:200].replace("\n", " ").replace("|", "/")
        im = ((src["report_impression_text"] if src else "") or "")[:200].replace("\n", " ").replace("|", "/")
        old_l = _label(r["old_flag"])
        new_l = _label(r["new_flag"])
        rat = ((r.get("new_rationale") or "")).strip().replace("\n", " ").replace("|", "/")[:300]
        log(f"{str(r['research_id']):>6s} | {str(r['note_row_id']):>10s} | "
            f"{old_l:>5s} | {new_l:>5s} | {ev:<200s} | {im:<200s} | {rat}")

    log("")
    log("──── SMOKE DONE — STOPPING. Awaiting go/no-go for full 8,810-row run. ────")
    log("    Target band reminder: 5-9 TRUE→FALSE + 3-7 TRUE→NULL + ~5 TRUE→TRUE among 20 old-TRUEs.")
    log("    If GPT-5.2 result is meaningfully outside this band, prompt may need re-tuning for the new model.")
    return out


def mode_apply(args, con: duckdb.DuckDBPyConnection) -> dict:
    log("=== MODE: apply ===")
    out: dict = {"mode": "apply", "ts": RUN_TS_ISO}

    cm = confusion_matrix(con)
    out["confusion"] = cm

    total_true_flips = cm["true_to_false"] + cm["true_to_null"]
    if total_true_flips < HALT_GATE_TRUE_FLIPS:
        msg = (f"HALT GATE FIRED: (TRUE→FALSE + TRUE→NULL) = "
               f"{cm['true_to_false']} + {cm['true_to_null']} = {total_true_flips} "
               f"< {HALT_GATE_TRUE_FLIPS}. Audit may have been off; not applying.")
        log("  ⛔ " + msg)
        out["ok"] = False
        out["halt_gate"] = msg
        return out

    if args.dry_run_apply:
        log("  --dry-run-apply set; not writing canonical.")
        out["dry_run"] = True
        out["ok"] = True
        return out

    rp = str(RESULTS_PARQUET)
    log("  staging result rows in canonical (TEMP) ...")
    con.execute("DROP TABLE IF EXISTS _ln_results_stage")
    con.execute(
        f"CREATE TEMP TABLE _ln_results_stage AS "
        f"SELECT research_id, note_row_id, "
        f"  CAST(new_flag AS BOOLEAN) AS new_flag, "
        f"  new_rationale "
        f"FROM read_parquet('{rp}') "
        f"WHERE status = 'ok'"
    )
    n_stage = con.execute("SELECT COUNT(*) FROM _ln_results_stage").fetchone()[0]
    log(f"  staged: {n_stage:,} rows")

    rationale_exists = con.execute(
        "SELECT 1 FROM information_schema.columns "
        f"WHERE table_catalog='{CANONICAL_DB}' AND table_schema='main' "
        f"AND table_name='{RAW_TABLE}' AND column_name='{RATIONALE_COL}'"
    ).fetchone() is not None
    if not rationale_exists:
        log(f"  ALTER TABLE {RAW_TABLE} ADD COLUMN {RATIONALE_COL} VARCHAR")
        con.execute(
            f"ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS {RATIONALE_COL} VARCHAR"
        )
    else:
        log(f"  {RATIONALE_COL} already present.")

    log(f"  UPDATE {RAW_TABLE} SET suspicious_ln_present = stage.new_flag, "
        f"{RATIONALE_COL} = stage.new_rationale ...")
    con.execute(f"""
        UPDATE {RAW_TABLE} AS t
        SET suspicious_ln_present = s.new_flag,
            {RATIONALE_COL}       = s.new_rationale
        FROM _ln_results_stage AS s
        WHERE t.research_id = s.research_id
          AND t.note_row_id = s.note_row_id
    """)

    new_dist = con.execute(
        f"SELECT suspicious_ln_present, COUNT(*) "
        f"FROM {RAW_TABLE} GROUP BY 1 ORDER BY 1 NULLS FIRST"
    ).fetchall()
    out["raw_distribution_after_update"] = [
        {"flag": ("NULL" if v is None else bool(v)), "n": int(n)} for v, n in new_dist
    ]
    log("  post-update report-level distribution:")
    for v, n in new_dist:
        log(f"    {('NULL' if v is None else bool(v))!s:<6}  n={n:,}")

    log(f"  rebuilding {ROLLUP_TABLE} (three-valued precedence TRUE > FALSE > NULL) ...")
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_TABLE} AS
        SELECT
          research_id,
          COUNT(*) AS tirads_v2_n_reports,
          -- Three-valued rollup (Logan 2026-04-19, Script 221c patch):
          -- TRUE  if any report is TRUE  (one suspicious finding across serial imaging is clinically significant)
          -- FALSE if no TRUE reports AND any report is FALSE (explicit clearance on at least one exam)
          -- NULL  if no TRUE and no FALSE reports (only NULLs / silent / genuine uncertainty)
          CASE
            WHEN MAX(CASE WHEN suspicious_ln_present = TRUE  THEN 1 ELSE 0 END) = 1 THEN TRUE
            WHEN MAX(CASE WHEN suspicious_ln_present = FALSE THEN 1 ELSE 0 END) = 1 THEN FALSE
            ELSE NULL
          END                                                                       AS tirads_v2_any_suspicious_ln_on_us,
          MAX(CASE WHEN overall_recommendation = 'fna' THEN 1 ELSE 0 END)::BOOLEAN  AS tirads_v2_any_fna_recommended_report,
          MIN(follow_up_interval_months)                                            AS tirads_v2_shortest_followup_months
        FROM {RAW_TABLE}
        GROUP BY research_id
    """)
    n_rl_rows, n_rl_rids = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {ROLLUP_TABLE}"
    ).fetchone()
    log(f"  rebuilt {ROLLUP_TABLE}: {n_rl_rows:,} rows / {n_rl_rids:,} RIDs")
    out["rollup"] = {"rows": int(n_rl_rows), "rids": int(n_rl_rids)}
    if n_rl_rows != n_rl_rids:
        out["ok"] = False
        raise SystemExit(f"rollup invariant violated: rows={n_rl_rows} rids={n_rl_rids}")

    log(f"  refreshing canonical_patient_master.{CPM_COL} ...")
    # NULL-out previous values then re-populate from refreshed rollup so the
    # column reflects the new distribution exactly (some RIDs may now drop
    # back to NULL if no report had any LN signal — true zero, not stale TRUE).
    con.execute(f"UPDATE canonical_patient_master SET {CPM_COL} = NULL")
    con.execute(f"""
        UPDATE canonical_patient_master AS m
        SET {CPM_COL} = r.{CPM_COL}
        FROM {ROLLUP_TABLE} AS r
        WHERE m.research_id = r.research_id
    """)
    cpm_after = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE {CPM_COL} = TRUE)  AS n_true,
          COUNT(*) FILTER (WHERE {CPM_COL} = FALSE) AS n_false,
          COUNT(*) FILTER (WHERE {CPM_COL} IS NULL) AS n_null
        FROM canonical_patient_master
    """).fetchone()
    out["cpm_flag_distribution_after"] = {
        "true": int(cpm_after[0]),
        "false": int(cpm_after[1]),
        "null": int(cpm_after[2]),
    }
    log(f"  CPM {CPM_COL} after: TRUE={cpm_after[0]:,} FALSE={cpm_after[1]:,} NULL={cpm_after[2]:,}")

    # Provenance: bump cpm_built_at + insert provenance row.
    if con.execute(
        "SELECT 1 FROM information_schema.columns WHERE "
        f"table_catalog='{CANONICAL_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master' AND column_name='cpm_built_at'"
    ).fetchone():
        con.execute(f"""
            UPDATE canonical_patient_master AS m
            SET cpm_built_at = CURRENT_TIMESTAMP
            WHERE m.research_id IN (SELECT research_id FROM {ROLLUP_TABLE})
        """)
    try:
        con.execute(
            "INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "(run_id, started_at, ended_at, phases_applied, "
            " critical_findings_cleared, high_findings_cleared, "
            " med_findings_cleared, held_for_adjudication) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                f"tirads_v2_ln_reextraction_{RUN_DATE.replace('-', '')}",
                RUN_TS_ISO,
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"tirads_v2_ln_reextraction:openai_{OPENAI_MODEL.replace('.', '_')}_tightened_prompt",
                "0",
                str(cm["true_to_false"]),
                str(cm["true_to_null"] + cm["false_to_true"] + cm["null_to_true"]),
                "0",
            ],
        )
        log("  provenance row inserted into cpm_reconciliation_provenance_v1")
    except Exception as e:
        log(f"  provenance insert skipped: {e!r}")

    # COMMENTs.
    try:
        con.execute(
            f"COMMENT ON COLUMN canonical_patient_master.{CPM_COL} IS "
            f"'Patient-level MAX of {RAW_TABLE}.suspicious_ln_present. "
            f"Re-extracted {RUN_DATE} via OpenAI {OPENAI_MODEL} with tightened prompt after "
            f"Script 221b audit found ~36% FP rate in original Qwen extraction "
            f"(benign-appearing LNs flagged TRUE). New prompt enforces explicit benign "
            f"clearance for FALSE and routes indeterminate/hedged cases to NULL. "
            f"Cross-family triangulation Qwen2.5-32B → OpenAI {OPENAI_MODEL}. "
            f"See {RAW_TABLE}.{RATIONALE_COL} for per-report justification.'"
        )
        con.execute(
            f"COMMENT ON COLUMN {RAW_TABLE}.suspicious_ln_present IS "
            f"'RE-EXTRACTED {RUN_DATE} by OpenAI {OPENAI_MODEL} with tightened prompt "
            f"(original Qwen extraction had ~36% FP rate on benign-appearing LNs; "
            f"new prompt routes hedged/indeterminate cases to NULL rather than FALSE). "
            f"See {RATIONALE_COL} column for per-row justification.'"
        )
        try:
            con.execute(
                f"COMMENT ON COLUMN {RAW_TABLE}.{RATIONALE_COL} IS "
                f"'Per-report 1-sentence rationale produced by OpenAI {OPENAI_MODEL} "
                f"({SCRIPT_TAG}, {RUN_DATE}) explaining the suspicious_ln_present value.'"
            )
        except Exception as e:
            log(f"  COMMENT on rationale col skipped: {e!r}")
        log("  COMMENTs applied to raw + CPM columns.")
    except Exception as e:
        log(f"  COMMENT update skipped: {e!r}")
        out["comment_warning"] = repr(e)

    # Phase 6 invariants
    n_cpm, n_dist, n_null = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "COUNT(*) FILTER (WHERE research_id IS NULL) "
        "FROM canonical_patient_master"
    ).fetchone()
    log(f"  invariants: cpm rows={n_cpm:,} distinct={n_dist:,} null={n_null:,}")
    out["invariants"] = {
        "cpm_rows": int(n_cpm),
        "cpm_distinct_rids": int(n_dist),
        "null_rids": int(n_null),
    }
    if n_cpm != EXPECTED_CPM_ROWS or n_dist != EXPECTED_CPM_ROWS or n_null != 0:
        out["ok"] = False
        raise SystemExit(
            f"CPM invariants violated: rows={n_cpm} distinct={n_dist} null={n_null}"
        )
    out["ok"] = True
    return out


# ── orchestrator ─────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Script 221b — re-extract suspicious_ln_present")
    ap.add_argument("--mode", choices=["audit", "smoke", "extract", "apply", "all"], default="audit")
    ap.add_argument("--limit", type=int, default=None,
                    help="optional: only process the first N rows from the raw table (extract mode)")
    ap.add_argument("--smoke-seed", type=int, default=7,
                    help="seed for stratified smoke pick (default 7)")
    ap.add_argument("--resume", action="store_true",
                    help="when --mode extract: skip rows already in checkpoint file")
    ap.add_argument("--dry-run-apply", action="store_true",
                    help="when --mode apply: print plan without writing canonical")
    args = ap.parse_args()

    decisions: dict[str, Any] = {
        "script": SCRIPT_TAG,
        "run_ts": RUN_TS_ISO,
        "mode": args.mode,
        "limit": args.limit,
        "resume": args.resume,
        "dry_run_apply": args.dry_run_apply,
        "phases": {},
    }

    con = md_connect()
    try:
        if args.mode in ("audit", "all"):
            decisions["phases"]["audit"] = mode_audit(con)
        if args.mode == "smoke":
            decisions["phases"]["smoke"] = mode_smoke(args, con)
        if args.mode in ("extract", "all"):
            decisions["phases"]["extract"] = mode_extract(args, con)
        if args.mode in ("apply", "all"):
            decisions["phases"]["apply"] = mode_apply(args, con)
        return 0
    finally:
        DECISIONS_PATH.write_text(json.dumps(decisions, indent=2, default=str))
        _flush_log()
        try:
            con.close()
        except Exception:
            pass
        log(f"decisions → {DECISIONS_PATH}")
        log(f"log       → {LOG_PATH}")


if __name__ == "__main__":
    sys.exit(main())

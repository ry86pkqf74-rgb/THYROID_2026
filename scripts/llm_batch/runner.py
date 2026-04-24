"""scripts/llm_batch/runner.py — Generic LLM batch runner.

Runs one domain at a time from the manifest. Reads:
  - input JSONL (one candidate-note record per line)
  - prompt template (Python str.format with {note_type} / {note_text})

Calls the LLM via OpenAI-compatible /v1/chat/completions (vLLM serves this
natively for gpt-oss-120b; ollama also exposes /v1/chat/completions).

Output JSONL — one record per note: domain, research_id, note_type,
note_index, raw_llm_response, parsed_json, error, elapsed_s, extracted_at,
llm_model.

Resume is keyed on (research_id, note_type, note_index); partial output is
appended.

Usage (on pod):
    python3 runner.py --manifest manifest.json --domain ete_subgrade \
        --model gpt-oss-120b --base-url http://localhost:8000/v1

    # Or loop every domain:
    python3 runner.py --manifest manifest.json --domain ALL \
        --model gpt-oss-120b --base-url http://localhost:8000/v1
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
import threading
import time
import traceback
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed


THINK_STRIP = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
JSON_SALVAGE = re.compile(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", re.DOTALL)


def load_prompt_template(path: pathlib.Path) -> str:
    return path.read_text()


def build_prompt(template: str, note_type: str, note_text: str, max_chars: int = 3000) -> str:
    # Keyword-window trimming — same pattern as 411 script. Generic keyword
    # list passed via template front-matter; fall back to first max_chars.
    # Extract optional [[KEYWORDS]]...[[/KEYWORDS]] block.
    kw_match = re.search(r"\[\[KEYWORDS\]\](.*?)\[\[/KEYWORDS\]\]", template, re.DOTALL)
    keywords = []
    if kw_match:
        keywords = [k.strip() for k in kw_match.group(1).strip().split("\n") if k.strip()]
        template = re.sub(r"\[\[KEYWORDS\]\].*?\[\[/KEYWORDS\]\]", "", template, flags=re.DOTALL).strip()
    if keywords:
        lowered = note_text.lower()
        hits = []
        for kw in keywords:
            kw_lower = kw.lower().strip()
            start = 0
            while True:
                idx = lowered.find(kw_lower, start)
                if idx < 0:
                    break
                hits.append(idx)
                start = idx + len(kw_lower)
        if hits:
            windows = sorted({(max(0, h - 400), min(len(note_text), h + 600)) for h in hits})
            merged: list[tuple[int, int]] = []
            for lo, hi in windows:
                if merged and lo <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
                else:
                    merged.append((lo, hi))
            snippets = [note_text[lo:hi] for lo, hi in merged]
            trimmed = "\n...\n".join(snippets)
            if len(trimmed) > max_chars:
                trimmed = trimmed[:max_chars]
        else:
            trimmed = note_text[:max_chars]
    else:
        trimmed = note_text[:max_chars]
    return template.format(note_type=note_type, note_text=trimmed)


def call_llm_openai(prompt: str, model: str, base_url: str, timeout: int = 240) -> str:
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 1500,
        "stream": False,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=payload,
        headers={"Content-Type": "application/json", "Authorization": "Bearer not-needed"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    data = json.loads(body)
    msg = data["choices"][0]["message"]
    # gpt-oss-120b may return content=None when the model emits only reasoning;
    # fall back to reasoning field, then to empty string.
    return msg.get("content") or msg.get("reasoning") or ""


def parse_response(raw: str) -> dict:
    if not raw:
        return {}
    cleaned = THINK_STRIP.sub("", raw).strip()
    cleaned = re.sub(r"```(?:json)?", "", cleaned).strip("` \n\t")
    try:
        return json.loads(cleaned)
    except Exception:
        pass
    candidates = JSON_SALVAGE.findall(cleaned)
    for cand in reversed(candidates):
        try:
            parsed = json.loads(cand)
            if isinstance(parsed, dict) and parsed:
                return parsed
        except Exception:
            continue
    return {}


def _process_one(note: dict, name: str, template: str, model: str, base_url: str) -> dict:
    t0 = time.time()
    err = 0
    parsed: dict = {}
    raw = ""
    try:
        prompt = build_prompt(template, note["note_type"], note["note_text"])
        raw = call_llm_openai(prompt, model, base_url)
        parsed = parse_response(raw)
    except Exception:
        err = 1
        raw = traceback.format_exc()
    return {
        "domain": name,
        "research_id": note["research_id"],
        "note_type": note["note_type"],
        "note_index": note["note_index"],
        "source_workbook": note.get("source_workbook"),
        "source_sheet": note.get("source_sheet"),
        "source_column": note.get("source_column"),
        "parsed_json": parsed,
        "raw_llm_response": (raw or "")[:6000],
        "error": err,
        "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "llm_model": model,
        "elapsed_s": round(time.time() - t0, 2),
    }


def run_domain(domain_cfg: dict, batch_dir: pathlib.Path, model: str, base_url: str, concurrency: int = 16) -> None:
    name = domain_cfg["name"]
    prompt_path = batch_dir / domain_cfg["prompt"]
    input_path = batch_dir / domain_cfg["input_jsonl"]
    output_path = batch_dir / domain_cfg["output_jsonl"]
    if not input_path.exists():
        print(f"[{name}] SKIP: input JSONL missing at {input_path}", flush=True)
        return
    template = load_prompt_template(prompt_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    done_keys = set()
    if output_path.exists():
        for line in output_path.read_text().splitlines():
            try:
                rec = json.loads(line)
                done_keys.add((rec["research_id"], rec["note_type"], str(rec["note_index"])))
            except Exception:
                pass
        print(f"[{name}] resume: {len(done_keys)} already done", flush=True)

    notes = [json.loads(l) for l in input_path.read_text().splitlines() if l.strip()]
    pending = [n for n in notes if (n["research_id"], n["note_type"], str(n["note_index"])) not in done_keys]
    total = len(pending)
    print(f"[{name}] loaded {len(notes)} candidate notes; {total} pending; concurrency={concurrency}", flush=True)

    t_start = time.time()
    lock = threading.Lock()
    counter = {"i": 0}
    with output_path.open("a") as out, ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = {ex.submit(_process_one, n, name, template, model, base_url): n for n in pending}
        for fut in as_completed(futs):
            rec = fut.result()
            with lock:
                out.write(json.dumps(rec) + "\n")
                out.flush()
                counter["i"] += 1
                idx = counter["i"]
            elapsed = time.time() - t_start
            rate = idx / elapsed if elapsed else 0
            if idx % 10 == 0 or idx == total:
                print(
                    f"[{name}] [{idx}/{total}] rate={rate:.2f}/s "
                    f"elapsed={elapsed:.0f}s ETA={(total-idx)/max(rate,0.01):.0f}s",
                    flush=True,
                )
    print(f"[{name}] done in {round(time.time()-t_start,1)}s -> {output_path}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--domain", required=True, help='one domain name OR "ALL"')
    ap.add_argument("--model", default="gpt-oss-120b")
    ap.add_argument("--base-url", default="http://localhost:8000/v1")
    ap.add_argument("--concurrency", type=int, default=16)
    args = ap.parse_args()

    manifest_path = pathlib.Path(args.manifest)
    manifest = json.loads(manifest_path.read_text())
    batch_dir = manifest_path.parent
    domains = manifest["domains"]
    if args.domain != "ALL":
        domains = [d for d in domains if d["name"] == args.domain]
        if not domains:
            sys.exit(f"domain {args.domain!r} not in manifest")
    for d in domains:
        print(f"\n===== domain: {d['name']} (priority={d.get('priority')}) =====", flush=True)
        run_domain(d, batch_dir, args.model, args.base_url, concurrency=args.concurrency)


if __name__ == "__main__":
    main()

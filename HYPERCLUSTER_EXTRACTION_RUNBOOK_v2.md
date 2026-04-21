# HyperCluster tg_kinetics Extraction Runbook (v2)

**Last verified:** 2026-04-17, Slurm allocation 483914 (EndTime 2026-04-18T05:35:38)
**Stack:** vLLM 0.6.6 + Qwen/Qwen2.5-72B-Instruct-AWQ on 8× L40S, TP=8, AWQ-Marlin
**Dataset:** 11,050 clinical notes / 5,593 unique research_ids → 23 chunks × 250 rids (last = 93)
**Purpose:** Reproducible launch without the iterative debugging path taken on 2026-04-17.

Everything below assumes: fresh node allocation, repo cloned to `/tmp/thyroid_repo`,
Python venv at `/tmp/vllm_env`, HF weights already staged at `/tmp/hf_cache` (39GB, 11
safetensors shards under `models--Qwen--Qwen2.5-72B-Instruct-AWQ/snapshots/…`).

---

## 0. Pre-flight gotchas (the stuff that cost us hours)

These are the exact failure modes hit during the initial launch. Check them FIRST on
every rebuild so we don't re-debug them.

**HF cache location mismatch.** `HF_HUB_OFFLINE=1` plus an unset `HF_HOME` makes vLLM
look in `~/.cache/huggingface/hub/` (NFS), which has a directory shell but NOT the full
39GB of safetensors. The real weights live in `/tmp/hf_cache/hub/`. Fix: export
`HF_HOME=/tmp/hf_cache`, `HF_HUB_CACHE=/tmp/hf_cache/hub`,
`TRANSFORMERS_CACHE=/tmp/hf_cache`, unset `HF_HUB_OFFLINE`, and pass
`--download-dir /tmp/hf_cache` to vLLM.

**Stale `/dev/shm` IPC objects from a crashed vLLM.** If the previous vLLM crashed mid-
init, shared-memory segments from the Python multiprocess workers linger and the new
launch will sometimes hang or refuse. Always clean before relaunch:
`rm -f /dev/shm/psm_* /dev/shm/sem.loky-* /dev/shm/__KMP_REGISTERED_LIB_*`.

**NFS parquet `cp` not `rsync`.** `rsync` to/from `/users/lglosse/...` failed with
"permission denied" on both ends on this allocation (probably an NFS ACL quirk). Use
`cp -v` instead — it just works.

**Extractor env var names.** The extractor reads `VLLM_BASE_URL`, `VLLM_MODEL_ID`, and
`VLLM_API_KEY` (extract_llm.py lines 79–82). Not `OPENAI_*`, not `LLM_*`. Source
`.extract_env` before every invocation.

**Bug-9 / Bug-10 patches must already be in place.** Bug 9 is
`response_format={"type":"json_object"}` which crashes outlines_core 0.1.26. Bug 10 is
the ```json fence stripping before `json.loads()`. Both have to be patched or every
note returns 0 entities. Verification commands below.

**Backup parquet is note-level, not entity-level.** Columns include `result_json`, NOT
`entity_type`. An 11,037-row file is 11,037 LLM calls, not 11,037 entities. Don't
panic-debug on low entity counts for pre-op notes — see §10.

---

## 1. Environment setup (one-time per node allocation)

```bash
# Working tree
cd /tmp/thyroid_repo

# Python venv with vLLM installed
source /tmp/vllm_env/bin/activate
python -c "import vllm; print(vllm.__version__)"   # expect 0.6.6

# HF cache sanity (expect the safetensors shards + config.json)
ls /tmp/hf_cache/hub/models--Qwen--Qwen2.5-72B-Instruct-AWQ/snapshots/*/
# 11 safetensors shards + config.json + tokenizer files, ~39GB total
```

If the HF cache is missing, download it into `/tmp/hf_cache` BEFORE launching vLLM —
the first-run download path inside vLLM has been flaky on this cluster.

---

## 2. The extractor env file (`/tmp/thyroid_repo/.extract_env`)

```bash
cat > /tmp/thyroid_repo/.extract_env <<'EOF'
export VLLM_BASE_URL=http://127.0.0.1:8000/v1
export VLLM_API_KEY=EMPTY
export VLLM_MODEL_ID=qwen2.5-72b
export HF_HOME=/tmp/hf_cache
export SLURM_JOB_ID=483914
EOF
```

Every extraction shell sources this before running `llm_extraction/extract_llm.py`.
The model id `qwen2.5-72b` must match vLLM's `--served-model-name`.

---

## 3. vLLM launch script (`/tmp/launch_vllm.sh`) — the working v2

```bash
#!/bin/bash
export HF_HOME=/tmp/hf_cache
export HF_HUB_CACHE=/tmp/hf_cache/hub
export TRANSFORMERS_CACHE=/tmp/hf_cache
unset HF_HUB_OFFLINE
export VLLM_WORKER_MULTIPROC_METHOD=spawn
export NCCL_DEBUG=WARN

cd /tmp/thyroid_repo
source /tmp/vllm_env/bin/activate

exec python -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen2.5-72B-Instruct-AWQ \
  --download-dir /tmp/hf_cache \
  --quantization awq_marlin \
  --tensor-parallel-size 8 \
  --gpu-memory-utilization 0.90 \
  --max-model-len 8192 \
  --max-num-seqs 64 \
  --enable-chunked-prefill \
  --disable-log-requests \
  --host 127.0.0.1 \
  --port 8000 \
  --served-model-name qwen2.5-72b
```

Launch with:

```bash
chmod +x /tmp/launch_vllm.sh
rm -f /dev/shm/psm_* /dev/shm/sem.loky-* /dev/shm/__KMP_REGISTERED_LIB_*
mkdir -p ~/THYROID_2026/extracted/logs
nohup /tmp/launch_vllm.sh > ~/THYROID_2026/extracted/logs/vllm_server.log 2>&1 &
disown
echo "VLLM_PID=$!" >> ~/THYROID_2026/extracted/logs/vllm_server.log
```

Health check (poll until 200):

```bash
until curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8000/v1/models | grep -q 200; do
  sleep 10
done
curl -s http://127.0.0.1:8000/v1/models | python -m json.tool
```

Expected state once up: 8 GPUs each at ~41888 MiB, `/v1/models` returns
`{"data":[{"id":"qwen2.5-72b",...}]}`.

---

## 4. Bug patch verification

Run these before every extraction launch — they're idempotent and cheap:

```bash
# Bug 9: response_format must NOT be passed (outlines_core 0.1.26 crash)
grep -n 'response_format' /tmp/thyroid_repo/llm_extraction/extract_llm.py
# expect: 0 matches

# Bug 10: ```json fence stripping before json.loads
grep -n 'raw_json.startswith' /tmp/thyroid_repo/llm_extraction/extract_llm.py
# expect: exactly 1 match, in the parse block around line 418-420
```

The exact Bug 10 block (for reference if a git reset wipes it):

```python
try:
    if raw_json.startswith("```"):
        raw_json = raw_json.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    data = json.loads(raw_json)
```

Domain registry sanity:

```bash
grep -nA3 'tg_kinetics' /tmp/thyroid_repo/config/extraction_domain_registry.yaml
# expect: parquet_stem: note_entities_llm_tg_kinetics, tier: v2, note_scope: all
```

---

## 5. NFS data hydration (do NOT use rsync)

```bash
mkdir -p /tmp/thyroid_repo/processed
cp -v /users/lglosse/thyroid_extract/THYROID_2026/processed/clinical_notes_long.parquet \
      /tmp/thyroid_repo/processed/

# Pre-flight: expect 11050 rows, 5593 unique research_ids
python - <<'PY'
import pandas as pd
df = pd.read_parquet("/tmp/thyroid_repo/processed/clinical_notes_long.parquet")
print("rows:", len(df), "unique_rids:", df.research_id.nunique())
assert len(df) == 11050, "row count drift"
assert df.research_id.nunique() == 5593, "rid count drift"
PY
```

---

## 6. Chunk generation (23 chunks × 250 rids, last = 93)

```bash
mkdir -p /tmp/thyroid_repo/chunks
python - <<'PY'
import pandas as pd
rids = sorted(pd.read_parquet(
    "/tmp/thyroid_repo/processed/clinical_notes_long.parquet"
).research_id.unique())
CHUNK = 250
for i in range(0, len(rids), CHUNK):
    n = i // CHUNK + 1
    with open(f"/tmp/thyroid_repo/chunks/chunk_{n:02d}.txt", "w") as f:
        f.write("\n".join(map(str, rids[i:i+CHUNK])) + "\n")
print("wrote", n, "chunks; last chunk size:", len(rids[i:i+CHUNK]))
PY
```

---

## 7. Extraction loop (`/tmp/run_extraction_loop.sh`)

Stored on local `/tmp` so it survives shell death via nohup+disown. Completed-chunk
markers live on NFS at `~/THYROID_2026/extracted/.completed_chunks/chunk_NN.done` so
we can resume on re-launch. Dynamic halt fires 15 minutes before Slurm EndTime.

```bash
#!/bin/bash
set -u
source /tmp/thyroid_repo/.extract_env

LOG_DIR=~/THYROID_2026/extracted/logs
DONE_DIR=~/THYROID_2026/extracted/.completed_chunks
mkdir -p "$LOG_DIR" "$DONE_DIR"

# Slurm end time minus 15-min safety buffer
END_EPOCH=$(date -d "$(scontrol show job $SLURM_JOB_ID | \
            awk -F= '/EndTime/{print $2; exit}' | awk '{print $1}')" +%s)
HALT_EPOCH=$((END_EPOCH - 900))

cd /tmp/thyroid_repo

for i in $(seq -f "%02g" 1 23); do
  [ -f "$DONE_DIR/chunk_$i.done" ] && { echo "SKIP chunk_$i (done)"; continue; }
  NOW=$(date +%s)
  if [ "$NOW" -ge "$HALT_EPOCH" ]; then
    echo "HALT: within 15 min of SLURM EndTime, stopping"; break
  fi

  # vLLM liveness check
  CODE=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8000/v1/models)
  if [ "$CODE" != "200" ]; then
    echo "VLLM DOWN (HTTP $CODE), skipping chunk_$i"; continue
  fi

  echo "[$(date -Iseconds)] start chunk_$i"
  python llm_extraction/extract_llm.py \
    --domain tg_kinetics \
    --rid-file chunks/chunk_$i.txt \
    --output-dir ~/THYROID_2026/extracted \
    > "$LOG_DIR/chunk_$i.log" 2>&1
  rc=$?

  if [ $rc -eq 0 ]; then
    touch "$DONE_DIR/chunk_$i.done"
    echo "[$(date -Iseconds)] done chunk_$i"
  else
    echo "[$(date -Iseconds)] FAIL chunk_$i rc=$rc (continuing)"
  fi
done
```

Launch:

```bash
chmod +x /tmp/run_extraction_loop.sh
nohup /tmp/run_extraction_loop.sh \
  > ~/THYROID_2026/extracted/logs/loop_master.log 2>&1 &
disown
```

---

## 8. Smoke test (5 rids, expect completion in ~50s)

Before kicking off the full loop:

```bash
source /tmp/thyroid_repo/.extract_env
cd /tmp/thyroid_repo
head -5 chunks/chunk_01.txt > /tmp/smoke_rids.txt
python llm_extraction/extract_llm.py \
  --domain tg_kinetics \
  --rid-file /tmp/smoke_rids.txt \
  --output-dir /tmp/smoke_out \
  2>&1 | tee ~/THYROID_2026/extracted/logs/smoke.log
```

**Expected outcome:** completes in ~50s with LOW entity count (often 0). This is
CORRECT behavior — see §10. Success signal is: no traceback, a parquet written to
`/tmp/smoke_out/`, and Qwen responses log as `\`\`\`json\n{"entities": [...]}\n\`\`\``.

---

## 9. Monitoring

```bash
# Loop progress
ls ~/THYROID_2026/extracted/.completed_chunks/ | wc -l
tail -30 ~/THYROID_2026/extracted/logs/loop_master.log

# Per-chunk latest
ls -t ~/THYROID_2026/extracted/logs/chunk_*.log | head -1 | xargs tail -30

# GPU + vLLM health
nvidia-smi --query-gpu=index,memory.used,utilization.gpu --format=csv,noheader
curl -s http://127.0.0.1:8000/v1/models | python -m json.tool

# Slurm time remaining
scontrol show job $SLURM_JOB_ID | grep -E 'EndTime|TimeLeft'
```

Rough throughput: ~14 notes/min → ~13h for the full 23 chunks.

---

## 10. Output schema + "low entity count" caveat

The backup parquet written to
`/tmp/thyroid_repo/scripts/output/parquet_backup/note_entities_llm_tg_kinetics.parquet`
is **note-level**, not entity-level. Columns:

```
note_row_id, domain, llm_model, llm_base_url, extracted_at,
result_json, research_id, note_type, ...
```

Each row = one LLM call. `result_json` is a JSON string whose `entities` key holds
the actual extracted entity list (usually empty for pre-op notes). An 11,037-row
file is 11,037 LLM calls, NOT 11,037 entities. To count entities properly:

```python
import json, pandas as pd
df = pd.read_parquet("…/note_entities_llm_tg_kinetics.parquet")
df["n_entities"] = df.result_json.map(lambda s: len(json.loads(s).get("entities", [])))
print("notes:", len(df), "entities:", df.n_entities.sum())
```

**Why entity rate looks low:** this cohort is dominated by pre-op clinic visits for
thyroid nodule evaluation. Tg surveillance starts POST-OP. Spot checks from the
2026-04-17 run: research_ids 10000/10001/10003 had 0/0/2 Tg keyword mentions, with
the only numerics being TSH values and explicit negations ("No results found for:
TSH, THYROGLB, THYROGLOBAB"). The tg_kinetics prompt is intentionally strict ("DO NOT
extract generic 'Tg was checked' without an actual numeric result"), so the model
correctly emits `{"entities": []}` for these. Verified on smoke-20: 3 entities from
44 notes, 100% of responses fence-formatted, 0 JSON parse errors.

---

## 11. Teardown / resumption

On Slurm timeout the loop halts gracefully via the 15-min buffer. To resume in a new
allocation:

1. Re-do §1 (venv, HF cache) and §2 (.extract_env, updating `SLURM_JOB_ID`).
2. Re-launch vLLM per §3 (including the `/dev/shm` cleanup).
3. Re-run §5 (cp parquet) and §6 (regenerate chunks) only if the working tree was
   wiped. The `.completed_chunks/` markers on NFS persist, so the loop in §7 will
   skip everything already done.

---

## Appendix A: file inventory

| Path | Role |
|---|---|
| `/tmp/launch_vllm.sh` | vLLM launcher |
| `/tmp/run_extraction_loop.sh` | Resumable chunk loop |
| `/tmp/thyroid_repo/.extract_env` | Extractor env vars |
| `/tmp/thyroid_repo/processed/clinical_notes_long.parquet` | Input (9.5MB, 11050 rows) |
| `/tmp/thyroid_repo/chunks/chunk_NN.txt` | 23 rid chunks |
| `/tmp/thyroid_repo/llm_extraction/extract_llm.py` | Extractor (Bug 9+10 patched) |
| `/tmp/thyroid_repo/llm_extraction/prompts/tg_kinetics_extraction_v1.txt` | Domain prompt |
| `/tmp/thyroid_repo/config/extraction_domain_registry.yaml` | Domain registry |
| `/tmp/hf_cache/hub/models--Qwen--Qwen2.5-72B-Instruct-AWQ/...` | 39GB model weights |
| `~/THYROID_2026/extracted/logs/` | vllm_server.log, loop_master.log, chunk_NN.log |
| `~/THYROID_2026/extracted/.completed_chunks/chunk_NN.done` | Resume markers |
| `~/THYROID_2026/extracted/.../note_entities_llm_tg_kinetics.parquet` | Output (note-level) |

## Appendix B: the 5 minutes of paste-and-go

For a fresh allocation on a node where `/tmp/hf_cache`, `/tmp/vllm_env`, and
`/tmp/thyroid_repo` are already staged, the full launch is:

```bash
# 1. env
cat > /tmp/thyroid_repo/.extract_env <<'EOF'
export VLLM_BASE_URL=http://127.0.0.1:8000/v1
export VLLM_API_KEY=EMPTY
export VLLM_MODEL_ID=qwen2.5-72b
export HF_HOME=/tmp/hf_cache
export SLURM_JOB_ID=$(squeue -u $USER -h -o %A | head -1)
EOF

# 2. data
cp -v /users/lglosse/thyroid_extract/THYROID_2026/processed/clinical_notes_long.parquet \
      /tmp/thyroid_repo/processed/

# 3. patches
grep -c 'response_format' /tmp/thyroid_repo/llm_extraction/extract_llm.py  # 0
grep -c 'raw_json.startswith' /tmp/thyroid_repo/llm_extraction/extract_llm.py  # 1

# 4. vLLM
rm -f /dev/shm/psm_* /dev/shm/sem.loky-* /dev/shm/__KMP_REGISTERED_LIB_*
mkdir -p ~/THYROID_2026/extracted/logs
nohup /tmp/launch_vllm.sh > ~/THYROID_2026/extracted/logs/vllm_server.log 2>&1 &
disown
until curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8000/v1/models | grep -q 200; do sleep 10; done

# 5. smoke
source /tmp/thyroid_repo/.extract_env
cd /tmp/thyroid_repo
head -5 chunks/chunk_01.txt > /tmp/smoke_rids.txt
python llm_extraction/extract_llm.py --domain tg_kinetics \
  --rid-file /tmp/smoke_rids.txt --output-dir /tmp/smoke_out

# 6. full loop
nohup /tmp/run_extraction_loop.sh > ~/THYROID_2026/extracted/logs/loop_master.log 2>&1 &
disown
```

That's the whole thing. No iterative debugging required as long as §0 is clean.

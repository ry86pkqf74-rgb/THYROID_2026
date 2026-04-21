# THYROID_2026 Extraction — Emory HyperCluster Handoff

**Created:** 2026-04-17 ~05:00
**Status at save time:** `tg_kinetics` extraction actively running (PID 4085285, launched 04:48)

---

## READ THIS FIRST — the two things that matter

1. **Check if `tg_kinetics` is still running BEFORE touching anything.** If it is, leave it alone; do not relaunch vLLM, do not re-run extraction, do not modify the venv. Monitor only.
2. **Copy parquets to NFS BEFORE the allocation expires at ~09:30.** `/tmp` is wiped when the Slurm job ends. If you lose the output, you lose 2+ hours of GPU time per domain.

```bash
# Status check — run this first, always
ps aux | grep -E "vllm|run_extraction" | grep -v grep
tail -20 /tmp/extract_tg.log
grep -c "Processed" /tmp/extract_tg.log
grep "invalid JSON" /tmp/extract_tg.log | wc -l

# Save outputs — run this as soon as a domain completes
cp /tmp/thyroid_repo/output/*.parquet ~/THYROID_2026/extracted/ && echo "Saved"
```

Healthy signs: `Processed X/11,050` count increasing, zero or near-zero `invalid JSON` warnings, vLLM process still alive.

---

## Current run state (as of save)

| Item | Value |
|---|---|
| vLLM server PID | ~4083xxx (healthy, 200 OK) |
| Extraction PID | 4085285 |
| Launched | 2026-04-17 04:48 |
| Slurm job | 483892 |
| Partition | `l40s-8-gm384-c192-m1536` |
| GPUs | 8× L40S |
| Allocation expires | ~09:30 (6 hrs from ~03:30 launch) |
| Login node | `hyper-01-prod-ondemand-241-199` |
| Compute node | `hyper-01-prod-comp-di-0241-132` |

---

## Infrastructure constants

| Item | Value |
|---|---|
| Cluster | Emory HyperCluster |
| Model | `Qwen/Qwen2.5-72B-Instruct-AWQ` |
| vLLM endpoint | `http://localhost:8000/v1` |
| Repo | `/tmp/thyroid_repo` (cloned from GitHub) |
| venv | `/tmp/vllm_env` |
| HF cache | `/tmp/hf_cache` (symlinked from `~/.cache/huggingface`) |
| vLLM log | `/tmp/vllm.log` |
| Extraction logs | `/tmp/extract_<domain>.log` |
| Output parquets (ephemeral) | `/tmp/thyroid_repo/output/` |
| NFS home (durable) | `~/THYROID_2026/extracted/` |

---

## All bugs fixed tonight (in order they were hit)

### Bug 1 — `srun --gres=gpu:N` rejected by Slurm
**Symptom:** Job submission fails.
**Fix:** Use `--gpus=8` instead of `--gres=gpu:8` or `--gres=gpu:l40s:8`. Emory's site-level Slurm plugin rejects gres syntax.

### Bug 2 — torch cu124 wheel on CUDA 12.2 node
**Fix:**
```bash
pip install "torch==2.5.1+cu121" --index-url https://download.pytorch.org/whl/cu121
```

### Bug 3 — `libcusparse` undefined symbol `__nvJitLinkComplete_12_4`
**Fix:**
```bash
pip install "nvidia-nvjitlink-cu12==12.4.127"
```

### Bug 4 — `transformers==5.x` breaks vLLM 0.6.6 Qwen tokenizer
**Fix:**
```bash
pip install "transformers==4.46.3"
```

### Bug 5 — HF `snapshot_download` hits home quota
**Fix:** Clear `~/.cache/pip` (was 8.3 GB); symlink model into `/tmp/hf_cache`.

### Bug 6 — Extractor not connecting to local vLLM
**Fix:** Set env vars before every extraction run:
```bash
export VLLM_BASE_URL=http://localhost:8000/v1
export VLLM_MODEL_ID=Qwen/Qwen2.5-72B-Instruct-AWQ
```

### Bug 7 — `--domain` / `--out` flags don't exist
**Fix:** Correct flags are `--target` (domain name). Output path is hardcoded — no output flag.

### Bug 8 — `note_scope: path_report` on synoptic domain → 0 extractions
**Fix:** Patch `config/extraction_domain_registry.yaml`:
```bash
sed -i '/^  synoptic_pathology_enrichment:$/,/canonical_target:/ s/note_scope: path_report/note_scope: op_note/' /tmp/thyroid_repo/config/extraction_domain_registry.yaml
```

### Bug 9 — `outlines_core==0.1.26` crashes vLLM on first generation request
**Symptom:** `AttributeError: type object 'TokenizerInfo' has no attribute 'from_huggingface'` in `logits_processor.py`. vLLM loads fine (curl returns model list) but dies on first real inference request. Triggered by the extractor sending `response_format={"type": "json_object"}`, which activates vLLM's outlines-based guided decoding.
**Root cause:** `outlines==0.1.11` (required by vLLM 0.6.6) pins `outlines_core==0.1.26`, which removed `TokenizerInfo.from_huggingface`. No compatible older `outlines-core` is available on this PyPI index.
**Fix:** Remove `response_format` from the extractor API call:
```bash
sed -i '/response_format={"type": "json_object"},/d' /tmp/thyroid_repo/llm_extraction/extract_llm.py
```

### Bug 10 — Model returns JSON wrapped in markdown code fences
**Symptom:** After removing `response_format`, the model responds 200 OK but the extractor logs `invalid JSON ... Expecting value: line 1 column 1 (char 0)`. Output looks like ` ```json\n{...}\n``` ` instead of raw JSON.
**Fix:** Patch `_parse_llm_response()` in `extract_llm.py` to strip code fences before `json.loads()`:
```python
# Inserted inside the try block, before data = json.loads(raw_json)
if raw_json.startswith("```"):
    raw_json = raw_json.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
```

Applied via:
```bash
python3 << 'EOF'
with open('/tmp/thyroid_repo/llm_extraction/extract_llm.py', 'r') as f:
    content = f.read()
old = '            data = json.loads(raw_json)'
new = '            if raw_json.startswith("```"):\n                raw_json = raw_json.split("\\n", 1)[-1].rsplit("```", 1)[0].strip()\n            data = json.loads(raw_json)'
assert old in content, "Pattern not found — check extract_llm.py structure"
content = content.replace(old, new, 1)
with open('/tmp/thyroid_repo/llm_extraction/extract_llm.py', 'w') as f:
    f.write(content)
print("Patched")
EOF

python3 -c "import py_compile; py_compile.compile('/tmp/thyroid_repo/llm_extraction/extract_llm.py', doraise=True)" && echo "Syntax OK"
```

---

## Environment setup (if `/tmp` is lost — new node or expired job)

Run in order on the compute node:

```bash
# 1. Clone repo
git clone https://github.com/ry86pkqf74-rgb/THYROID_2026.git /tmp/thyroid_repo

# 2. Patch synoptic domain note_scope (Bug 8)
sed -i '/^  synoptic_pathology_enrichment:$/,/canonical_target:/ s/note_scope: path_report/note_scope: op_note/' /tmp/thyroid_repo/config/extraction_domain_registry.yaml

# 3. Build venv with pinned versions (Bugs 2, 3, 4)
rm -rf /tmp/vllm_env && python -m venv /tmp/vllm_env && source /tmp/vllm_env/bin/activate \
  && pip install --quiet "torch==2.5.1+cu121" --index-url https://download.pytorch.org/whl/cu121 \
  && pip install --quiet vllm==0.6.6 pandas pyarrow pyyaml \
  && pip install "transformers==4.46.3" --quiet \
  && pip install "nvidia-nvjitlink-cu12==12.4.127" --quiet

# 4. Symlink HF cache (Bug 5)
mkdir -p /tmp/hf_cache/hub && ln -sf ~/.cache/huggingface/hub/models--Qwen--Qwen2.5-72B-Instruct-AWQ /tmp/hf_cache/hub/models--Qwen--Qwen2.5-72B-Instruct-AWQ
```

### Re-apply the two source patches
**These are NOT in the repo — must be reapplied after every clone:**

```bash
# Patch 1: remove response_format (Bug 9)
sed -i '/response_format={"type": "json_object"},/d' /tmp/thyroid_repo/llm_extraction/extract_llm.py

# Patch 2: strip markdown code fences (Bug 10)
python3 << 'EOF'
with open('/tmp/thyroid_repo/llm_extraction/extract_llm.py', 'r') as f:
    content = f.read()
old = '            data = json.loads(raw_json)'
new = '            if raw_json.startswith("```"):\n                raw_json = raw_json.split("\\n", 1)[-1].rsplit("```", 1)[0].strip()\n            data = json.loads(raw_json)'
assert old in content, "Pattern not found — check extract_llm.py structure"
content = content.replace(old, new, 1)
with open('/tmp/thyroid_repo/llm_extraction/extract_llm.py', 'w') as f:
    f.write(content)
print("Patched")
EOF

# Verify syntax
python3 -c "import py_compile; py_compile.compile('/tmp/thyroid_repo/llm_extraction/extract_llm.py', doraise=True)" && echo "Syntax OK"
```

---

## Launching vLLM

```bash
source /tmp/vllm_env/bin/activate
cd /tmp/thyroid_repo && HF_HOME=/tmp/hf_cache nohup python -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen2.5-72B-Instruct-AWQ \
  --quantization awq_marlin \
  --tensor-parallel-size 8 \
  --max-model-len 8192 \
  --gpu-memory-utilization 0.90 \
  --enable-chunked-prefill \
  --disable-log-requests \
  --port 8000 > /tmp/vllm.log 2>&1 & echo "PID: $!"
```

Wait ~90 seconds then verify:
```bash
curl -s http://localhost:8000/v1/models | head -c 200
```
Must return the model name. **Do NOT launch extraction until curl confirms healthy.**

---

## Launching extraction (always set env vars first)

```bash
source /tmp/vllm_env/bin/activate
export VLLM_BASE_URL=http://localhost:8000/v1
export VLLM_MODEL_ID=Qwen/Qwen2.5-72B-Instruct-AWQ
```

**Run domains one at a time — do not run concurrently, vLLM is the bottleneck.**

```bash
# Domain 1 (currently running as of 04:48)
cd /tmp/thyroid_repo && nohup python llm_extraction/run_extraction.py --target tg_kinetics --workers 8 > /tmp/extract_tg.log 2>&1 &

# Domain 2 (run after tg_kinetics finishes)
cd /tmp/thyroid_repo && nohup python llm_extraction/run_extraction.py --target dynamic_risk_response --workers 8 > /tmp/extract_dynamic_risk.log 2>&1 &

# Domain 3
cd /tmp/thyroid_repo && nohup python llm_extraction/run_extraction.py --target us_nodule_dynamics --workers 8 > /tmp/extract_us_nodule.log 2>&1 &

# Domain 4
cd /tmp/thyroid_repo && nohup python llm_extraction/run_extraction.py --target presenting_symptoms --workers 8 > /tmp/extract_symptoms.log 2>&1 &

# Domain 5
cd /tmp/thyroid_repo && nohup python llm_extraction/run_extraction.py --target synoptic_pathology_enrichment --workers 8 > /tmp/extract_synoptic.log 2>&1 &
```

Each domain takes ~2 hours for 11,050 notes.

---

## Monitoring

```bash
# Is extraction actually working?
grep -c "Processed" /tmp/extract_tg.log              # should increase
grep "invalid JSON" /tmp/extract_tg.log | wc -l       # should be 0
grep "Avg generation throughput" /tmp/vllm.log | tail -3  # target >50 tok/s

# Check output files exist after completion
ls -lh /tmp/thyroid_repo/output/
find /tmp/thyroid_repo -name "*.parquet" -newer /tmp/thyroid_repo/config/extraction_domain_registry.yaml
```

**CRITICAL — copy outputs to NFS before job expires:**
```bash
cp /tmp/thyroid_repo/output/*.parquet ~/THYROID_2026/extracted/ && echo "Saved"
```

---

## Allocation management

Job 483892 expires ~09:30. If `tg_kinetics` takes ~2 hrs, you have time for 2 domains max before expiry.

Request a new allocation **before** this one expires:
```bash
# From login node (hyper-01-prod-ondemand-241-199):
srun --partition=l40s-8-gm384-c192-m1536 --gpus=8 --cpus-per-task=192 --mem=384G --time=06:00:00 --pty bash
```

Note: `--gpus=8` is the required syntax. `--gres=gpu:8` and `--gres=gpu:l40s:8` both fail on this cluster.

To attach a second shell to the running job:
```bash
srun --jobid=483892 --overlap --pty bash
```

---

## Remaining work

- [ ] `tg_kinetics` — running now, ~2 hrs, verify with `grep -c "Processed" /tmp/extract_tg.log`
- [ ] `dynamic_risk_response` — queue after `tg_kinetics`
- [ ] `us_nodule_dynamics` — queue after `dynamic_risk_response`
- [ ] `presenting_symptoms` — queue after `us_nodule_dynamics`
- [ ] `synoptic_pathology_enrichment` — queue last (uses `op_note` scope, config already patched)
- [ ] Copy all parquets to `~/THYROID_2026/extracted/` before each allocation expires

# RunPod 2x H200 Batch Plan — llm_batch_20260424

5 domains, **22,706 candidate notes**, one pod session.

## Domain queue

| # | Domain | Notes | Cohort | Notes-per-pt cap | Source(s) |
|---|---|---:|---|---:|---|
| 1 | ete_subgrade | 287 | 167 PTC unspec_remaining | — | clinical + path |
| 2 | t4b_invasion | 944 | path_malignant gross_ete=1 OR T4* | 5 | clinical + path + CT + MRI |
| 3 | vascular_invasion_v2 | 3,861 | all path_malignant | 2 | path only |
| 4 | airway_invasion_v2 | 6,054 | all path_malignant | 2 | clinical + path + CT |
| 5 | parathyroid_detail | 11,560 | all operative | 3 | clinical + path |

## Cost / time budget

- 2x H200 SXM on RunPod ≈ **$5.98/hr** (on-demand; spot is ~$3.80/hr but evictable)
- gpt-oss-120b on vLLM tensor-parallel=2 with batched requests: realistic **80–150 notes/min**
- 22,706 / 100 = ~3.8 hours inference
- +15 min pod bootstrap, +15 min model download (140 GB), +10 min cleanup
- **Total wall time ≈ 4.5 hours. Total cost ≈ $25–35.**

## Pod setup (pick template)

Pod template: `runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04`
GPU: 2x H200 SXM (not PCIe — need NVLink for TP=2)
Storage: 200 GB container disk, 100 GB volume at `/workspace`

## Bootstrap (on pod, paste after SSH)

```bash
# 1. base deps (per runpod_bootstrap memory)
apt-get update && apt-get install -y zstd rsync sshpass htop
pip install --upgrade pip
pip install vllm==0.7.2 tenacity duckdb==1.4.4

# 2. sanity GPUs
nvidia-smi  # expect 2x H200 140GB; if Pwr=ERR! destroy + new pod (per vastai_gpu_err memory)

# 3. pull model (runs in background, ~5-10 min on H200 node bandwidth)
nohup python3 -c "from vllm import LLM; LLM(model='openai/gpt-oss-120b', tensor_parallel_size=2, dtype='bfloat16')" > /workspace/model_pull.log 2>&1 &
```

## Launch vLLM OpenAI-compatible server

```bash
# After model is cached
nohup python3 -m vllm.entrypoints.openai.api_server \
    --model openai/gpt-oss-120b \
    --tensor-parallel-size 2 \
    --dtype bfloat16 \
    --max-model-len 16384 \
    --max-num-seqs 32 \
    --gpu-memory-utilization 0.92 \
    --host 0.0.0.0 --port 8000 \
    > /workspace/vllm.log 2>&1 &

# Wait ~60s, verify
curl -sf http://localhost:8000/v1/models | python3 -m json.tool
```

## Scp batch bundle from Mac → pod

From Mac:
```bash
cd "/Users/ros/THyroid 2026/scripts/llm_batch"
tar czf /tmp/llm_batch_20260424.tar.gz manifest.json runner.py prompts/ output/
# Replace <POD_SSH> with the runpod connection string (e.g. ssh root@<ip> -p <port>)
scp /tmp/llm_batch_20260424.tar.gz <POD_SSH>:/workspace/
```

On pod:
```bash
cd /workspace && tar xzf llm_batch_20260424.tar.gz && ls
```

## Run the batch

```bash
cd /workspace

# Run ALL 5 domains sequentially, smallest-first so we see progress quickly
for d in ete_subgrade t4b_invasion vascular_invasion_v2 airway_invasion_v2 parathyroid_detail; do
  echo "========== $d =========="
  python3 runner.py --manifest manifest.json --domain $d \
    --model openai/gpt-oss-120b --base-url http://localhost:8000/v1 \
    2>&1 | tee -a /workspace/batch_run.log
done
```

Canary stop: after ete_subgrade (should take ~3-5 min), verify first ~10 results
manually before letting the loop continue. If prompt quality looks off, kill the loop,
tune the prompt, relaunch.

```bash
# Quick canary check after ete_subgrade
head -3 /workspace/output/ete_subgrade_results.jsonl | python3 -c "
import json,sys
for l in sys.stdin:
    r=json.loads(l); print(r['research_id'], r['note_type'], '->', r['parsed_json'])"
```

## Pull results back to Mac

On pod:
```bash
cd /workspace && tar czf /tmp/llm_batch_results.tar.gz output/*_results.jsonl batch_run.log
```

From Mac:
```bash
scp <POD_SSH>:/tmp/llm_batch_results.tar.gz /tmp/
cd "/Users/ros/THyroid 2026/scripts/llm_batch"
tar xzf /tmp/llm_batch_results.tar.gz
ls output/*_results.jsonl
```

## Load into MotherDuck

```bash
cd "/Users/ros/THyroid 2026"
for d in ete_subgrade t4b_invasion vascular_invasion_v2 airway_invasion_v2 parathyroid_detail; do
  python3 scripts/llm_batch/loader.py --manifest scripts/llm_batch/manifest.json --domain $d
done
```

## Shutdown

**Immediately after last domain loads** — don't let the pod idle.
- RunPod console → Pods → Stop (preserves volume, no further charge on GPU)
- Or Terminate (destroys volume too) if no re-run needed

## Post-load: domain-specific migrations

Each of the 5 domains needs its own patient-rollup VIEW + analytic integration,
built on top of `main.note_entities_llm_<domain>_v1`:

- ete_subgrade → migration 54 (already drafted, `ete_manuscript_analytic_v3`)
- t4b_invasion → migration 55 (new — `t4b_invasion_patient_v1` + layered AJCC8 T-stage)
- vascular_invasion_v2 → migration 56 (patient rollup, supersedes v1 JSON UNNEST)
- airway_invasion_v2 → migration 57 (T4a refinement on path_malignant events)
- parathyroid_detail → migration 58 (new Tier 2 — canonical_parathyroid_detail_*)

Draft those AFTER load + acceptance probe passes on each domain.

## Safety rails

- **PHI**: JSONL files contain clinical note text. Do not commit them. `.gitignore` the
  `scripts/llm_batch/output/` directory.
- **Pod logs**: `/workspace/batch_run.log` contains patient research_ids + short evidence
  quotes. Don't copy it into git. Scp-back only the per-domain *_results.jsonl files.
- **Token**: do NOT copy `motherduck.local.toml` to the pod. The pod should have zero
  DB access — all DB I/O happens on the Mac before/after.

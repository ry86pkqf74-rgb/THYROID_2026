#!/bin/bash
#SBATCH --job-name=tirads-v11
#SBATCH --account=ai-gpu
#SBATCH --partition=l4-1-gm24-c16-m64
#SBATCH --gpus=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=03:00:00
#SBATCH --output=/users/lglosse/THYROID_2026/runbook/logs/tirads_v11_%j.out
#SBATCH --error=/users/lglosse/THYROID_2026/runbook/logs/tirads_v11_%j.err

# ─────────────────────────────────────────────────────────────────────────────
# TIRADS v1_1 extraction
#   Model     : Qwen/Qwen2.5-7B-Instruct (bf16, single L4)
#   Backend   : vLLM OpenAI-compatible server on localhost:8000
#   Engine    : scripts/vastai/run_extraction_concurrent.py (existing)
#   Domain    : tirads_granular
#   Prompt    : llm_extraction/prompts/tirads_granular_extraction_v1.txt
#   Input     : filtered clinical_notes_long.parquet (see filter_tirads_input.py)
#   Output    : /users/lglosse/THYROID_2026/extractions/tirads_v1_1/
#   Runtime   : ~60-120 min on L4 @ concurrency 6
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

mkdir -p /users/lglosse/THYROID_2026/runbook/logs

echo "================================================================"
echo "  TIRADS v1_1 extraction  — job $SLURM_JOB_ID on $(hostname)"
echo "  started at $(date -Is)"
echo "================================================================"

# Fixed paths ---------------------------------------------------------------
ENV_DIR=/scratch/lglosse/thyroid_vllm_env
REPO_DIR=/scratch/lglosse/thyroid_repo
OUT_DIR=/users/lglosse/THYROID_2026/extractions/tirads_v1_1
INPUT_PARQUET_FULL=${REPO_DIR}/processed/remaining/clinical_notes_long.parquet
INPUT_PARQUET_TIRADS=${REPO_DIR}/processed/remaining/clinical_notes_tirads_input.parquet
HF_CACHE_DIR=/scratch/lglosse/.cache/huggingface
MODEL_ID=Qwen/Qwen2.5-7B-Instruct

mkdir -p "${OUT_DIR}" "${HF_CACHE_DIR}"

# HuggingFace config (use /scratch cache so weights survive across jobs) -----
export HF_HOME="${HF_CACHE_DIR}"
export HUGGINGFACE_HUB_CACHE="${HF_CACHE_DIR}/hub"
export HF_HUB_ENABLE_HF_TRANSFER=1   # fast parallel downloads (installed below if missing)

# Activate conda env ---------------------------------------------------------
echo "[phase 1] Activating conda env at ${ENV_DIR}"
if [ ! -x "${ENV_DIR}/bin/python" ]; then
    echo "FATAL: python not found in ${ENV_DIR}" >&2
    exit 2
fi

export PATH="${ENV_DIR}/bin:${PATH}"

# Fix libnvJitLink symbol shadowing by prepending bundled nvidia libs.
# This is the same fix used for tg_kinetics v5 — without it, torch._C fails
# to import on the compute node because the system libnvJitLink is older
# than what the torch wheel's cusparse needs.
SITE_PKGS="${ENV_DIR}/lib/python3.11/site-packages"
NV_LIB_PATHS=$(ls -d "${SITE_PKGS}"/nvidia/*/lib 2>/dev/null | tr '\n' ':')
export LD_LIBRARY_PATH="${NV_LIB_PATHS}${LD_LIBRARY_PATH:-}"
echo "[phase 1] LD_LIBRARY_PATH prepended with: ${NV_LIB_PATHS}"

# Sanity probe ---------------------------------------------------------------
echo "[phase 1] python + torch + vllm sanity probe"
python - <<'PY'
import sys
print("python:", sys.version.split()[0])
import torch
print("torch :", torch.__version__, "| cuda:", torch.cuda.is_available(), "| device:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "cpu")
import vllm
print("vllm  :", vllm.__version__)
PY

# Make sure hf_transfer is present for fast downloads (no-op if already there)
python -m pip install --quiet --disable-pip-version-check hf_transfer >/dev/null 2>&1 || true

# Stage input ----------------------------------------------------------------
echo "[phase 2] Ensuring full clinical_notes_long.parquet is available"
if [ ! -f "${INPUT_PARQUET_FULL}" ]; then
    echo "FATAL: full parquet missing at ${INPUT_PARQUET_FULL}" >&2
    echo "       scp it from your Mac first (see README.md §1)" >&2
    exit 3
fi
ls -lh "${INPUT_PARQUET_FULL}"

echo "[phase 3] Filtering to TIRADS-relevant notes"
python /users/lglosse/THYROID_2026/runbook/v1_1/filter_tirads_input.py \
    --input "${INPUT_PARQUET_FULL}" \
    --output "${INPUT_PARQUET_TIRADS}"
ls -lh "${INPUT_PARQUET_TIRADS}"

# Pre-download model --------------------------------------------------------
echo "[phase 4] Ensuring ${MODEL_ID} is in HF cache"
if ! python -c "from huggingface_hub import snapshot_download; import os; p=snapshot_download(repo_id='${MODEL_ID}', cache_dir=os.environ['HUGGINGFACE_HUB_CACHE'], max_workers=8); print('cached_at:', p)"; then
    echo "FATAL: model download failed" >&2
    exit 4
fi

# Launch vLLM OpenAI server --------------------------------------------------
echo "[phase 5] Launching vLLM OpenAI server on :8000"
VLLM_LOG="${OUT_DIR}/vllm_server_${SLURM_JOB_ID}.log"

# Qwen2.5-7B fits in bf16 on a 24 GB L4 with room for a generous KV cache.
# --max-model-len 8192 is plenty for clinical notes (TIRADS prompt + body typically <5k tok)
# --gpu-memory-utilization 0.90 gives vLLM ~21 GB of VRAM for weights + KV cache.
python -m vllm.entrypoints.openai.api_server \
    --model "${MODEL_ID}" \
    --served-model-name "${MODEL_ID}" \
    --host 127.0.0.1 \
    --port 8000 \
    --tensor-parallel-size 1 \
    --dtype bfloat16 \
    --max-model-len 8192 \
    --gpu-memory-utilization 0.90 \
    --disable-log-requests \
    > "${VLLM_LOG}" 2>&1 &
VLLM_PID=$!
echo "[phase 5] vLLM PID ${VLLM_PID}  log=${VLLM_LOG}"

# Wait for server to be ready
echo "[phase 6] Waiting for vLLM /v1/models endpoint (up to 10 min)"
for i in $(seq 1 120); do
    if curl -sf http://127.0.0.1:8000/v1/models >/dev/null 2>&1; then
        echo "[phase 6] vLLM ready after ${i}x5s probes"
        break
    fi
    if ! kill -0 ${VLLM_PID} 2>/dev/null; then
        echo "FATAL: vLLM process died during startup. Tail of log:" >&2
        tail -80 "${VLLM_LOG}" >&2
        exit 5
    fi
    sleep 5
done

if ! curl -sf http://127.0.0.1:8000/v1/models >/dev/null 2>&1; then
    echo "FATAL: vLLM did not become ready within 10 minutes. Tail of log:" >&2
    tail -80 "${VLLM_LOG}" >&2
    kill -TERM ${VLLM_PID} || true
    exit 5
fi

curl -s http://127.0.0.1:8000/v1/models | python -m json.tool

# Run extraction -------------------------------------------------------------
echo "[phase 7] Running extraction (domain=tirads_granular)"
cd "${REPO_DIR}"
export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"
export EXTRACTION_CONCURRENCY=6
export LLM_TIMEOUT_SECONDS=120

python scripts/vastai/run_extraction_concurrent.py \
    --input-parquet "${INPUT_PARQUET_TIRADS}" \
    --output-dir "${OUT_DIR}" \
    --url "http://127.0.0.1:8000/v1" \
    --api-key "EMPTY" \
    --model "${MODEL_ID}" \
    --domains tirads_granular \
    --concurrency "${EXTRACTION_CONCURRENCY}"

EXTRACT_EXIT=$?
echo "[phase 7] run_extraction_concurrent exited with ${EXTRACT_EXIT}"

# Shutdown vLLM --------------------------------------------------------------
echo "[phase 8] Shutting down vLLM"
kill -TERM ${VLLM_PID} 2>/dev/null || true
for i in $(seq 1 10); do
    kill -0 ${VLLM_PID} 2>/dev/null || break
    sleep 1
done
kill -KILL ${VLLM_PID} 2>/dev/null || true

# Summary --------------------------------------------------------------------
echo "================================================================"
echo "  SUMMARY"
echo "================================================================"
ls -lh "${OUT_DIR}"
if [ -f "${OUT_DIR}/note_entities_llm_tirads_granular.parquet" ]; then
    python - <<'PY'
import pandas as pd, json, os
out = "/users/lglosse/THYROID_2026/extractions/tirads_v1_1/note_entities_llm_tirads_granular.parquet"
df = pd.read_parquet(out)
print(f"rows written: {len(df):,}")
print(f"columns     : {list(df.columns)}")
# Unpack entities count per row
if "result_json" in df.columns:
    ent_counts = df["result_json"].map(lambda s: len(json.loads(s).get("entities", [])) if isinstance(s, str) else 0)
    print(f"non-empty notes: {(ent_counts > 0).sum():,}  ({100*(ent_counts > 0).mean():.1f}%)")
    print(f"total entities : {ent_counts.sum():,}")
    print(f"mean/note      : {ent_counts.mean():.2f}  |  max/note: {ent_counts.max()}")
PY
fi

echo "[done] finished at $(date -Is). exit=${EXTRACT_EXIT}"
exit ${EXTRACT_EXIT}

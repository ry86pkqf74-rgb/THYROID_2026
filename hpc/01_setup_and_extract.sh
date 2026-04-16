#!/usr/bin/env bash
# ============================================================================
# 01_setup_and_extract.sh — Complete HPC extraction pipeline
# ============================================================================
# Run this ON your Emory HPC L40S session AFTER:
#   1. vLLM server is already running on GPUs 3-6 (port 8000)
#   2. You have scp'd clinical_notes_long.parquet to this machine
#
# This script:
#   A. Clones the THYROID_2026 repo (code + prompts)
#   B. Installs Python deps
#   C. Verifies vLLM is alive
#   D. Runs extraction for the 5 failing domains
#   E. Packages results for download
#
# Usage:
#   bash hpc/01_setup_and_extract.sh
# ============================================================================
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────
VLLM_HOST="${VLLM_HOST:-localhost}"
VLLM_PORT="${VLLM_PORT:-8000}"
VLLM_MODEL="${VLLM_MODEL:-Qwen/Qwen2.5-72B-Instruct-AWQ}"

# 5 failing domains (registry names from extraction_domain_registry.yaml)
DOMAINS=(
  "synoptic_pathology_enrichment"
  "tg_kinetics"
  "dynamic_risk_response"
  "us_nodule_dynamics"
  "presenting_symptoms"
)

# Chunk sizes: larger than defaults since Qwen2.5-72B has 128K context
# Default pipeline uses 6K chars; we use 32K to capture full notes
CHUNK_CHARS=32000
OP_CHUNK_CHARS=48000

# Worker threads per domain (tune for GPU throughput vs memory)
WORKERS=2

WORK_DIR="${HOME}/thyroid_extract"
REPO_DIR="${WORK_DIR}/THYROID_2026"
RESULTS_DIR="${WORK_DIR}/results_$(date +%Y%m%d_%H%M%S)"

echo "============================================================"
echo "  THYROID NLP RE-EXTRACTION PIPELINE — Emory HPC"
echo "  $(date)"
echo "============================================================"
echo ""

# ── Step 1: Setup workspace ───────────────────────────────────────────────
echo "[1/6] Setting up workspace at ${WORK_DIR} ..."
mkdir -p "${WORK_DIR}"
mkdir -p "${RESULTS_DIR}"

# ── Step 2: Clone repo (if not already present) ──────────────────────────
if [ -d "${REPO_DIR}/.git" ]; then
    echo "[2/6] Repo already cloned at ${REPO_DIR}, pulling latest ..."
    cd "${REPO_DIR}"
    git pull --ff-only || echo "  (pull failed, using existing code — check manually)"
else
    echo "[2/6] Cloning THYROID_2026 repo ..."
    cd "${WORK_DIR}"
    git clone https://github.com/ry86pkqf74-rgb/THYROID_2026.git
fi
cd "${REPO_DIR}"

# ── Step 3: Verify clinical notes data ───────────────────────────────────
NOTES_PARQUET="${REPO_DIR}/processed/clinical_notes_long.parquet"
echo ""
echo "[3/6] Checking for clinical notes data ..."

if [ ! -f "${NOTES_PARQUET}" ]; then
    echo ""
    echo "  ╔══════════════════════════════════════════════════════════╗"
    echo "  ║  clinical_notes_long.parquet NOT FOUND                  ║"
    echo "  ║                                                          ║"
    echo "  ║  You need to copy it from your local machine:           ║"
    echo "  ║                                                          ║"
    echo "  ║  Option A (from local terminal):                        ║"
    echo "  ║    scp /path/to/THYROID_2026/processed/                 ║"
    echo "  ║      clinical_notes_long.parquet \\                      ║"
    echo "  ║      <user>@login.hpc.emory.edu:${NOTES_PARQUET}       ║"
    echo "  ║                                                          ║"
    echo "  ║  Option B (DVC pull, if S3 creds are configured):       ║"
    echo "  ║    cd ${REPO_DIR} && dvc pull processed/                ║"
    echo "  ║      clinical_notes_long.parquet                        ║"
    echo "  ║                                                          ║"
    echo "  ║  Then re-run this script.                               ║"
    echo "  ╚══════════════════════════════════════════════════════════╝"
    echo ""
    exit 1
fi

NOTE_COUNT=$(python3 -c "
import pyarrow.parquet as pq
t = pq.read_metadata('${NOTES_PARQUET}')
print(t.num_rows)
" 2>/dev/null || echo "unknown")
echo "  Found: ${NOTES_PARQUET}"
echo "  Notes count: ${NOTE_COUNT}"

# ── Step 4: Install Python dependencies ──────────────────────────────────
echo ""
echo "[4/6] Installing Python dependencies ..."
pip install --user --quiet openai pyarrow pandas pyyaml 2>/dev/null || \
pip install --break-system-packages --quiet openai pyarrow pandas pyyaml 2>/dev/null || \
echo "  (some deps may already be installed)"

# Verify openai is importable
python3 -c "import openai; print(f'  openai SDK version: {openai.__version__}')"
python3 -c "import pandas; print(f'  pandas version: {pandas.__version__}')"
python3 -c "import pyarrow; print(f'  pyarrow version: {pyarrow.__version__}')"

# ── Step 5: Verify vLLM is alive ─────────────────────────────────────────
echo ""
echo "[5/6] Verifying vLLM server at ${VLLM_HOST}:${VLLM_PORT} ..."

VLLM_URL="http://${VLLM_HOST}:${VLLM_PORT}"

# Check /v1/models endpoint
MODELS_RESPONSE=$(curl -s --max-time 10 "${VLLM_URL}/v1/models" 2>/dev/null || echo "FAILED")
if echo "${MODELS_RESPONSE}" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    models = [m['id'] for m in data.get('data', [])]
    print(f'  Available models: {models}')
    if not models:
        sys.exit(1)
except:
    sys.exit(1)
" 2>/dev/null; then
    echo "  vLLM server is alive and serving models."
else
    echo ""
    echo "  ╔══════════════════════════════════════════════════════════╗"
    echo "  ║  vLLM server not responding at ${VLLM_URL}             ║"
    echo "  ║                                                          ║"
    echo "  ║  Make sure you started vLLM first:                      ║"
    echo "  ║    CUDA_VISIBLE_DEVICES=3,4,5,6 python3 -m \\           ║"
    echo "  ║      vllm.entrypoints.openai.api_server \\              ║"
    echo "  ║      --model ${VLLM_MODEL} \\                           ║"
    echo "  ║      --quantization awq --tensor-parallel-size 4 \\     ║"
    echo "  ║      --max-model-len 32768 \\                           ║"
    echo "  ║      --gpu-memory-utilization 0.90 \\                   ║"
    echo "  ║      --host 0.0.0.0 --port ${VLLM_PORT} \\             ║"
    echo "  ║      --dtype float16 --trust-remote-code               ║"
    echo "  ║                                                          ║"
    echo "  ║  Wait for 'Uvicorn running' message, then re-run this. ║"
    echo "  ╚══════════════════════════════════════════════════════════╝"
    echo ""
    exit 1
fi

# Quick smoke test: send a tiny extraction to verify JSON mode works
echo "  Running smoke test (tiny extraction) ..."
SMOKE_OK=$(python3 -c "
import openai, json
client = openai.OpenAI(api_key='dummy', base_url='${VLLM_URL}/v1')
try:
    resp = client.chat.completions.create(
        model='${VLLM_MODEL}',
        messages=[
            {'role': 'system', 'content': 'Extract entities as JSON: {\"entities\": [...]}'},
            {'role': 'user', 'content': 'Patient has papillary thyroid carcinoma with BRAF V600E mutation.'}
        ],
        temperature=0,
        response_format={'type': 'json_object'},
        max_tokens=500,
    )
    data = json.loads(resp.choices[0].message.content)
    n = len(data.get('entities', []))
    print(f'OK ({n} entities extracted)')
except Exception as e:
    print(f'FAIL: {e}')
" 2>&1)
echo "  Smoke test: ${SMOKE_OK}"
if [[ "${SMOKE_OK}" == FAIL* ]]; then
    echo "  WARNING: Smoke test failed. The extraction may not work correctly."
    echo "  Continuing anyway — individual domain errors will be logged."
fi

# ── Step 6: Run extraction for each failing domain ───────────────────────
echo ""
echo "[6/6] Running extraction for 5 failing domains ..."
echo "  Chunk sizes: standard=${CHUNK_CHARS}, operative=${OP_CHUNK_CHARS}"
echo "  Workers: ${WORKERS}"
echo ""

# Set environment variables for vLLM provider
export VLLM_BASE_URL="${VLLM_URL}/v1"
export VLLM_MODEL_ID="${VLLM_MODEL}"
export VLLM_API_KEY="dummy"
export EXTRACT_MAX_CHUNK_CHARS="${CHUNK_CHARS}"
export EXTRACT_OP_CHUNK_CHARS="${OP_CHUNK_CHARS}"

DOMAIN_RESULTS=()
cd "${REPO_DIR}"

for DOMAIN in "${DOMAINS[@]}"; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Extracting domain: ${DOMAIN}"
    echo "  Started: $(date)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    LOG_FILE="${RESULTS_DIR}/${DOMAIN}_extraction.log"

    python3 llm_extraction/run_extraction.py \
        --target "${DOMAIN}" \
        --workers "${WORKERS}" \
        2>&1 | tee "${LOG_FILE}"

    EXIT_CODE=${PIPESTATUS[0]}

    if [ ${EXIT_CODE} -eq 0 ]; then
        echo "  ✓ ${DOMAIN} completed successfully"
        DOMAIN_RESULTS+=("${DOMAIN}:OK")
    else
        echo "  ✗ ${DOMAIN} failed (exit code ${EXIT_CODE})"
        DOMAIN_RESULTS+=("${DOMAIN}:FAILED")
    fi
    echo ""
done

# ── Step 7: Package results ──────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  PACKAGING RESULTS"
echo "============================================================"

# Copy output parquets to results directory
for DOMAIN in "${DOMAINS[@]}"; do
    # Find the parquet stem from the registry domain name
    STEM="note_entities_llm_${DOMAIN}"
    PARQUET_FILE="${REPO_DIR}/processed/${STEM}.parquet"
    if [ -f "${PARQUET_FILE}" ]; then
        cp "${PARQUET_FILE}" "${RESULTS_DIR}/"
        SIZE=$(du -h "${PARQUET_FILE}" | cut -f1)
        ROWS=$(python3 -c "
import pyarrow.parquet as pq
print(pq.read_metadata('${PARQUET_FILE}').num_rows)
" 2>/dev/null || echo "?")
        echo "  ${STEM}.parquet → ${SIZE} (${ROWS} rows)"
    else
        echo "  ${STEM}.parquet → NOT FOUND (extraction may have failed)"
    fi
done

# Create a manifest
python3 -c "
import json, os, glob
from datetime import datetime

results_dir = '${RESULTS_DIR}'
manifest = {
    'extraction_date': datetime.now().isoformat(),
    'vllm_model': '${VLLM_MODEL}',
    'vllm_url': '${VLLM_URL}',
    'chunk_chars': ${CHUNK_CHARS},
    'op_chunk_chars': ${OP_CHUNK_CHARS},
    'domains': {},
}

for f in sorted(glob.glob(os.path.join(results_dir, '*.parquet'))):
    import pyarrow.parquet as pq
    meta = pq.read_metadata(f)
    name = os.path.basename(f).replace('.parquet', '')
    manifest['domains'][name] = {
        'rows': meta.num_rows,
        'size_bytes': os.path.getsize(f),
    }

with open(os.path.join(results_dir, 'manifest.json'), 'w') as fh:
    json.dump(manifest, fh, indent=2)
print(f'  Manifest written to {results_dir}/manifest.json')
"

# Create tarball for easy download
TARBALL="${WORK_DIR}/thyroid_reextract_results.tar.gz"
cd "${WORK_DIR}"
tar czf "${TARBALL}" -C "${RESULTS_DIR}" .
echo "  Tarball: ${TARBALL} ($(du -h ${TARBALL} | cut -f1))"

echo ""
echo "============================================================"
echo "  EXTRACTION COMPLETE"
echo "============================================================"
echo ""
echo "  Results directory: ${RESULTS_DIR}"
echo "  Tarball:           ${TARBALL}"
echo ""
echo "  Domain results:"
for R in "${DOMAIN_RESULTS[@]}"; do
    echo "    ${R}"
done
echo ""
echo "  To download results to your local machine:"
echo "    scp <user>@login.hpc.emory.edu:${TARBALL} ."
echo ""
echo "  Then run 02_ingest_to_motherduck.py locally to upload to MotherDuck."
echo "============================================================"

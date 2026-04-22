#!/usr/bin/env bash
# Script 368 — Vascular invasion v2 extraction on GPT-OSS-120B (RunPod H200 SXM) — Pod B
# Pod: 3xb0skbds6yxz2  |  Cost: ~$3.59/hr  |  Input: 10,268 rows (shard B, rows 10268-20535)
# Runs in parallel with run_vascular_v2.sh (Pod A handles rows 0-10267)
#
# Usage: bash runs/9domain_v5/run_vascular_v2_podB.sh [--dry-run]
set -euo pipefail
cd "$(dirname "$0")/../.."

POD_ID="3xb0skbds6yxz2"
export VLLM_URL="${VLLM_URL:-https://${POD_ID}-8000.proxy.runpod.net/v1}"
export VLLM_MODEL="${VLLM_MODEL:-openai/gpt-oss-120b}"
export VLLM_API_KEY="${VLLM_API_KEY:-thyroid2026}"
export DOMAIN="vascular_invasion"
export EXTRACTION_CONCURRENCY="${CONCURRENCY:-64}"
export LLM_INPUT_CHAR_LIMIT="${LLM_INPUT_CHAR_LIMIT:-6000}"
export LLM_MAX_TOKENS="${LLM_MAX_TOKENS:-1500}"
export LLM_TIMEOUT_SECONDS="${LLM_TIMEOUT_SECONDS:-120}"

INPUT_PARQUET="$(pwd)/processed/remaining/9domain_v5/input_vascular_invasion_v2_B.parquet"
OUTPUT_DIR="$(pwd)/runs/9domain_v5/vascular_invasion/output_B"
mkdir -p "${OUTPUT_DIR}"
mkdir -p "$(pwd)/runs/9domain_v5/logs"

echo "=================================================================="
echo " Script 368 — Vascular Invasion v2 Extraction (Pod B)"
echo " Pod:         ${POD_ID}"
echo " URL:         ${VLLM_URL}"
echo " Model:       ${VLLM_MODEL}"
echo " Input:       ${INPUT_PARQUET}"
echo " Output:      ${OUTPUT_DIR}"
echo " Concurrency: ${EXTRACTION_CONCURRENCY}"
echo " Input chars: ${LLM_INPUT_CHAR_LIMIT}"
echo " Max tokens:  ${LLM_MAX_TOKENS}"
echo "=================================================================="

# Health probe — fail fast if model not serving yet
echo -n "[368B] Probing vLLM /v1/models ... "
probe_http=$(curl -sS -o /tmp/368B_models.json -w '%{http_code}' \
    -H "Authorization: Bearer ${VLLM_API_KEY}" \
    --max-time 20 "${VLLM_URL}/models" || true)
if [[ "${probe_http}" != "200" ]]; then
    echo "FAIL (http=${probe_http}) — model not ready yet. Retry in a few minutes."
    exit 2
fi
echo "OK (http=200)"
python3 -c "
import json
with open('/tmp/368B_models.json') as f:
    d = json.load(f)
models = [m['id'] for m in d.get('data', [])]
print(f'[368B] Serving: {models}')
"

if [[ "${1:-}" == "--dry-run" ]]; then
    echo "[368B] Dry run — exiting before extraction"
    exit 0
fi

START=$(date +%s)
echo "[368B] Starting extraction at $(date -u +%FT%TZ)"

.venv/bin/python scripts/vastai/run_extraction_concurrent.py \
    --input-parquet "${INPUT_PARQUET}" \
    --output-dir "${OUTPUT_DIR}" \
    --url "${VLLM_URL}" \
    --model "${VLLM_MODEL}" \
    --api-key "${VLLM_API_KEY}" \
    --concurrency "${EXTRACTION_CONCURRENCY}" \
    --domains "${DOMAIN}" \
    2>&1 | tee "runs/9domain_v5/logs/vascular_v2_podB_$(date -u +%Y%m%dT%H%M%SZ).log"

END=$(date +%s)
ELAPSED=$(( END - START ))
echo ""
echo "=================================================================="
echo " DONE — elapsed: $(( ELAPSED/60 ))m$(( ELAPSED%60 ))s"
echo " Output: ${OUTPUT_DIR}"
echo " Next: merge output_A + output_B parquets before MD load"
echo "=================================================================="

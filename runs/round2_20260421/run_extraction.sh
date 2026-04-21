#!/usr/bin/env bash
# Driver for the 2026-04-21 qwen2.5-32b extraction round against RunPod pod
# pmza5juk7ru2xl (supersedes the stuck first pod sixogefvgr7qyl, which
# deadlocked on boot because vllm/vllm-openai:latest needs a --model arg
# that runpodctl pod create has no way to pass). Invoke with
# DOMAIN=<domain> ./run_extraction.sh, or run the per-job wrappers.
#
# Env vars honored:
#   DOMAIN             (required — e.g. pathology, cervical_ln_detail,
#                       tirads_granular, esophageal_invasion)
#   INPUT_PARQUET      (required — absolute path to input parquet)
#   OUTPUT_DIR         (default: runs/round2_20260421/<domain>/output)
#   CONCURRENCY        (default: 256)
#   VLLM_URL           (default: RunPod proxy for pod pmza5juk7ru2xl)
#   VLLM_MODEL         (default: qwen2.5-32b)
#   VLLM_API_KEY       (default: thyroid2026)

set -euo pipefail

cd "$(dirname "$0")/../.."
REPO_ROOT="$(pwd)"

: "${DOMAIN:?DOMAIN env var required}"
: "${INPUT_PARQUET:?INPUT_PARQUET env var required}"

OUTPUT_DIR="${OUTPUT_DIR:-runs/round2_20260421/${DOMAIN}/output}"
mkdir -p "${OUTPUT_DIR}"

export VLLM_URL="${VLLM_URL:-https://pmza5juk7ru2xl-8000.proxy.runpod.net/v1}"
export VLLM_MODEL="${VLLM_MODEL:-qwen2.5-32b}"
export VLLM_API_KEY="${VLLM_API_KEY:-thyroid2026}"
export EXTRACTION_CONCURRENCY="${CONCURRENCY:-256}"
# Keep existing budgets (match historical 9domain_v4 run).
export LLM_INPUT_CHAR_LIMIT="${LLM_INPUT_CHAR_LIMIT:-12000}"
# 12000 was the old default; entity-extraction JSON rarely exceeds ~1500
# tokens, and 12000 blew out vLLM's 16384 ctx when combined with ~4k prompts.
export LLM_MAX_TOKENS="${LLM_MAX_TOKENS:-2048}"

echo "[round2] domain=${DOMAIN}"
echo "[round2] input=${INPUT_PARQUET}"
echo "[round2] output_dir=${OUTPUT_DIR}"
echo "[round2] vllm_url=${VLLM_URL}"
echo "[round2] model=${VLLM_MODEL}"
echo "[round2] concurrency=${EXTRACTION_CONCURRENCY}"

# Quick health probe before burning minutes on a stalled pod.
echo -n "[round2] probing vLLM /v1/models ... "
probe_http="$(curl -sS -o /tmp/round2_models.json -w '%{http_code}' \
    -H "Authorization: Bearer ${VLLM_API_KEY}" \
    --max-time 15 "${VLLM_URL}/models" || true)"
if [[ "${probe_http}" != "200" ]]; then
    echo "FAIL (http=${probe_http})"
    echo "[round2] pod not serving yet — aborting before extraction" >&2
    exit 2
fi
echo "OK (http=200)"
grep -o '"id":"[^"]*"' /tmp/round2_models.json | head -3 || true

.venv/bin/python scripts/vastai/run_extraction_concurrent.py \
    --input-parquet "${INPUT_PARQUET}" \
    --output-dir "${OUTPUT_DIR}" \
    --url "${VLLM_URL}" \
    --model "${VLLM_MODEL}" \
    --api-key "${VLLM_API_KEY}" \
    --concurrency "${EXTRACTION_CONCURRENCY}" \
    --domains "${DOMAIN}"

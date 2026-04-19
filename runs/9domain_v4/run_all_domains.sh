#!/bin/bash
set -u
cd /Users/loganglosser/THYROID_2026
source runs/9domain_v4/ssh_config.sh
export EXTRACTION_CONCURRENCY=256
export LLM_MAX_TOKENS=12000
export LLM_INPUT_CHAR_LIMIT=12000
export LLM_TIMEOUT_SECONDS=180

START_ALL=$(date +%s)
for DOMAIN in airway_invasion vascular_invasion parathyroid_detail; do
  echo "=================================================================="
  echo "[$(date -u +%FT%TZ)] BEGIN $DOMAIN (concurrency=256)"
  echo "=================================================================="
  START=$(date +%s)
  .venv/bin/python scripts/vastai/run_extraction_concurrent.py \
    --input-parquet processed/remaining/9domain_v4/input_${DOMAIN}.parquet \
    --output-dir runs/9domain_v4/${DOMAIN}/output \
    --url "$VLLM_URL" \
    --model "$VLLM_MODEL" \
    --api-key vllm \
    --domains "$DOMAIN" \
    --concurrency 256 \
    2>&1 | tee runs/9domain_v4/logs/${DOMAIN}.log
  END=$(date +%s)
  echo "[$(date -u +%FT%TZ)] END $DOMAIN (elapsed: $(( (END-START)/60 ))m$(( (END-START)%60 ))s)"
done
END_ALL=$(date +%s)
echo "=================================================================="
echo "[$(date -u +%FT%TZ)] ALL 4 DOMAINS COMPLETE. Total: $(( (END_ALL-START_ALL)/60 ))m"

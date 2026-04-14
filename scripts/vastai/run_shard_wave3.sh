#!/usr/bin/env bash
# Wave-3 per-host runner. Identical to run_shard.sh except the domain list is
# restricted to cervical_ln_detail (only LN gap remains after wave-1/wave-2).
set -euo pipefail

REPO_DIR=/root/THYROID_2026
SHARD_PARQUET="$REPO_DIR/processed/remaining/clinical_notes_shard.parquet"
MODEL=qwen3:32b
CONCURRENCY=${CONCURRENCY:-8}

if [ ! -f "$SHARD_PARQUET" ]; then
    echo "ERROR: shard parquet not found at $SHARD_PARQUET" >&2
    exit 2
fi

pkill -9 -f run_extraction_concurrent 2>/dev/null || true
pkill -9 -f 'ollama runner' 2>/dev/null || true
pkill -9 -x ollama 2>/dev/null || true
sleep 4

export OLLAMA_NUM_PARALLEL=${OLLAMA_NUM_PARALLEL:-8}
export OLLAMA_MAX_LOADED_MODELS=1
export OLLAMA_KEEP_ALIVE=24h
export OLLAMA_CONTEXT_LENGTH=${OLLAMA_CONTEXT_LENGTH:-8192}
export OLLAMA_FLASH_ATTENTION=1
export OLLAMA_KV_CACHE_TYPE=q8_0

nohup ollama serve > /var/log/ollama.log 2>&1 < /dev/null &
disown
sleep 12

curl -fsS http://localhost:11434/api/generate \
    -d '{"model":"'"$MODEL"'","prompt":"hi","stream":false}' > /var/log/warmup.log 2>&1 || true

echo "==> ollama load verification"
grep -E 'offloading|model weights|kv cache|total memory' /var/log/ollama.log | tail -8 || true

cd "$REPO_DIR"
mkdir -p processed/output
nohup python3 scripts/vastai/run_extraction_concurrent.py \
    --input-parquet "$SHARD_PARQUET" \
    --domains cervical_ln_detail \
    --concurrency "$CONCURRENCY" \
    --model "$MODEL" \
    > /var/log/extraction.log 2>&1 < /dev/null &
disown

echo "==> Wave-3 extraction launched (pid=$!)"
sleep 5
ps -ef | grep run_extraction_concurrent | grep -v grep
echo "==> Tail: ssh <host> 'tail -f /var/log/extraction.log'"

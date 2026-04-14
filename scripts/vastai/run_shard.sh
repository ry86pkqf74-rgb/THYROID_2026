#!/usr/bin/env bash
# Per-host runner for cluster extraction. Assumes:
#   - bootstrap_rerun.sh has been run (ollama installed, repo cloned)
#   - The host's shard parquet has been scp'd to:
#       /root/THYROID_2026/processed/remaining/clinical_notes_shard.parquet
# Output goes to /root/THYROID_2026/processed/output/note_entities_llm_<domain>.ckpt.jsonl
# Aggregator on the Mac side will rsync these back and merge.
set -euo pipefail

REPO_DIR=/root/THYROID_2026
SHARD_PARQUET="$REPO_DIR/processed/remaining/clinical_notes_shard.parquet"
MODEL=qwen3:32b
CONCURRENCY=${CONCURRENCY:-8}

if [ ! -f "$SHARD_PARQUET" ]; then
    echo "ERROR: shard parquet not found at $SHARD_PARQUET" >&2
    exit 2
fi

# Ensure ollama is running with the GPU-friendly env
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

# Pre-warm: ensure model is loaded fully on GPU
curl -fsS http://localhost:11434/api/generate \
    -d '{"model":"'"$MODEL"'","prompt":"hi","stream":false}' > /var/log/warmup.log 2>&1 || true

# Verify GPU residency
echo "==> ollama load verification"
grep -E 'offloading|model weights|kv cache|total memory' /var/log/ollama.log | tail -8 || true

cd "$REPO_DIR"
mkdir -p processed/output
nohup python3 scripts/vastai/run_extraction_concurrent.py \
    --input-parquet "$SHARD_PARQUET" \
    --domains tirads_granular cervical_ln_detail pathology \
    --concurrency "$CONCURRENCY" \
    --model "$MODEL" \
    > /var/log/extraction.log 2>&1 < /dev/null &
disown

echo "==> Extraction launched (pid=$!)"
sleep 5
ps -ef | grep run_extraction_concurrent | grep -v grep
echo "==> Tail extraction log with: ssh <host> 'tail -f /var/log/extraction.log'"

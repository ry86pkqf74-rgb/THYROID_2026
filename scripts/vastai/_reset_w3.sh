#!/usr/bin/env bash
set -x
pkill -9 -f run_extraction_concurrent 2>/dev/null || true
pkill -9 -f "ollama runner" 2>/dev/null || true
pkill -9 -x ollama 2>/dev/null || true
sleep 4

export OLLAMA_NUM_PARALLEL=8
export OLLAMA_MAX_LOADED_MODELS=1
export OLLAMA_KEEP_ALIVE=24h
export OLLAMA_CONTEXT_LENGTH=8192
export OLLAMA_FLASH_ATTENTION=1
export OLLAMA_KV_CACHE_TYPE=q8_0

nohup ollama serve > /var/log/ollama.log 2>&1 < /dev/null &
disown
sleep 12

echo "--- warmup ---"
curl -fsS --max-time 180 http://localhost:11434/api/generate \
  -d '{"model":"qwen3:32b","prompt":"hi","stream":false}' > /var/log/warmup.log 2>&1
echo "warmup_exit=$?"
head -c 200 /var/log/warmup.log
echo
nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader,nounits

cd /root/THYROID_2026
nohup python3 scripts/vastai/run_extraction_concurrent.py \
    --input-parquet processed/remaining/clinical_notes_shard.parquet \
    --domains cervical_ln_detail \
    --concurrency 8 \
    --model qwen3:32b \
    > /var/log/extraction.log 2>&1 < /dev/null &
disown
sleep 5
pgrep -af python3 | grep -v grep | head -3
echo DONE

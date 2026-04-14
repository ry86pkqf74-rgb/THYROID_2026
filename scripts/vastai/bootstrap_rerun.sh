#!/usr/bin/env bash
# One-shot bootstrap for the targeted TIRADS/LN/FNA rerun on a fresh
# Vast.ai pytorch image. Run on the host AFTER the filtered notes parquet
# has been scp'd to /root/clinical_notes_long_rerun.parquet.
set -euo pipefail

REPO_DIR=/root/THYROID_2026
REPO_URL=https://github.com/ry86pkqf74-rgb/THYROID_2026.git
NOTES_PARQUET_SRC=/root/clinical_notes_long_rerun.parquet
MODEL=qwen3:32b
CONCURRENCY=8

echo "==> System: $(uname -a)"
nvidia-smi --query-gpu=name,memory.total --format=csv

# 1. Install ollama (image is pytorch, not ollama)
if ! command -v ollama >/dev/null 2>&1; then
    echo "==> Installing ollama"
    curl -fsSL https://ollama.com/install.sh | sh
fi

# 2. Start ollama daemon in background (idempotent)
if ! pgrep -x ollama >/dev/null; then
    echo "==> Starting ollama serve"
    nohup ollama serve > /var/log/ollama.log 2>&1 &
    sleep 8
fi

# 3. Pull the model (resumable)
echo "==> Pulling $MODEL"
ollama pull "$MODEL"

# 4. Clone the repo
if [ ! -d "$REPO_DIR/.git" ]; then
    echo "==> Cloning repo"
    git clone --depth 1 "$REPO_URL" "$REPO_DIR"
else
    echo "==> Updating existing repo"
    git -C "$REPO_DIR" fetch --depth 1 origin main && git -C "$REPO_DIR" reset --hard origin/main
fi

# 5. Stage filtered notes parquet
mkdir -p "$REPO_DIR/processed/remaining"
if [ -f "$NOTES_PARQUET_SRC" ]; then
    cp "$NOTES_PARQUET_SRC" "$REPO_DIR/processed/remaining/clinical_notes_long_rerun.parquet"
    ls -la "$REPO_DIR/processed/remaining/clinical_notes_long_rerun.parquet"
else
    echo "ERROR: notes parquet not found at $NOTES_PARQUET_SRC" >&2
    exit 2
fi

# 6. Python deps
cd "$REPO_DIR"
pip install --quiet pandas pyarrow openpyxl python-dotenv requests pyyaml

# 7. Smoke test: verify domain prompts load
python3 - <<'PY'
import sys
sys.path.insert(0, ".")
from llm_extraction.fleet_domain_prompt import get_fleet_domain_prompt
d = get_fleet_domain_prompt()
for k in ("tirads_granular", "cervical_ln_detail", "pathology"):
    print(f"  {k}: {d[k]}")
print(f"  total domains registered: {len(d)}")
PY

# 8. Verify ollama health
curl -fsS http://localhost:11434/api/tags | python3 -c "import json,sys;d=json.load(sys.stdin);print('ollama models:', [m['name'] for m in d.get('models', [])])"

echo "==> Bootstrap complete. Launch extractor with:"
echo "    cd $REPO_DIR && nohup python3 scripts/vastai/run_extraction_concurrent.py \\"
echo "        --input-parquet processed/remaining/clinical_notes_long_rerun.parquet \\"
echo "        --domains tirads_granular cervical_ln_detail pathology \\"
echo "        --concurrency $CONCURRENCY --model $MODEL > /var/log/extraction.log 2>&1 &"

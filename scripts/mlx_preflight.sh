#!/usr/bin/env bash
# Preflight check before running any Phase 3/4/5 batch.
# Verifies: Python env, mlx-lm install, BQ auth, model weights cached.
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/tools/thyroid_mlx_extract"

echo "=== thyroid-mlx-extract preflight ==="
echo

# 1. Python + harness install
echo "[1/5] Checking Python + harness..."
python3 -c "import thyroid_mlx_extract; print('  thyroid_mlx_extract', thyroid_mlx_extract.__version__)" \
  || { echo "  FAIL: not installed. Run: cd $REPO_ROOT/tools/thyroid_mlx_extract && make install"; exit 1; }

# 2. mlx-lm
echo "[2/5] Checking mlx-lm..."
python3 -c "import mlx_lm; print('  mlx-lm', mlx_lm.__version__ if hasattr(mlx_lm, '__version__') else 'OK')" \
  || { echo "  FAIL: mlx-lm not installed. pip install mlx-lm"; exit 1; }

# 3. BQ auth
echo "[3/5] Checking BQ auth..."
gcloud auth application-default print-access-token >/dev/null 2>&1 \
  || { echo "  FAIL: not authed. Run: gcloud auth application-default login"; exit 1; }
echo "  OK"

# 4. Project access
echo "[4/5] Checking BQ project access..."
bq query --use_legacy_sql=false --quiet --format=none --max_rows=1 \
  'SELECT 1 FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` LIMIT 1' \
  || { echo "  FAIL: can't read pub_canonical"; exit 1; }
echo "  OK"

# 5. Model weights cached (warn if missing — will download on first use)
echo "[5/5] Checking MLX model cache..."
HF_CACHE="${HF_HOME:-$HOME/.cache/huggingface}"
for model in \
  "models--mlx-community--MedGemma-1.5-27B-IT-4bit" \
  "models--mlx-community--Llama-3.3-70B-Instruct-4bit" \
  "models--mlx-community--DeepSeek-R1-Distill-Llama-70B-4bit" \
  "models--mlx-community--MedGemma-1.5-4B-IT-4bit"; do
    if [ -d "$HF_CACHE/hub/$model" ]; then
      echo "  ✓ $model"
    else
      echo "  ⚠ $model NOT CACHED (will download on first use, ~14-40 GB each)"
    fi
done

echo
echo "Preflight complete. Ready to run phases."

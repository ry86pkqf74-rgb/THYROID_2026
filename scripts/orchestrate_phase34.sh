#!/usr/bin/env bash
# Orchestrator — waits for R1-Distill download, smoke-tests, then launches Phase 3 → Phase 4 sequentially.
# Launched via nohup so it survives terminal close.
set -uo pipefail

REPO=/Users/lgm5maxmac/code/THYROID_2026
LOGS=$REPO/logs
mkdir -p "$LOGS"
PY=/opt/homebrew/bin/python3.11

cd "$REPO/tools/thyroid_mlx_extract"

echo "[$(date)] ORCHESTRATOR START"

# ---------------------------------------------------------------------------
# Step 1: wait for R1-Distill cache to be ready
# ---------------------------------------------------------------------------
echo "[$(date)] Step 1: waiting for R1-Distill-Llama-70B-4bit weights..."
while true; do
    # Try a quick test load — if it succeeds, weights are cached
    if $PY -c "from mlx_lm import load; load('mlx-community/DeepSeek-R1-Distill-Llama-70B-4bit')" >/dev/null 2>&1; then
        echo "[$(date)] Weights cached + loadable. Proceeding."
        break
    fi
    echo "[$(date)] Still waiting... (download/load not yet complete)"
    sleep 60
done

# ---------------------------------------------------------------------------
# Step 2: smoke test with --limit 1
# ---------------------------------------------------------------------------
echo "[$(date)] Step 2: smoke test (1 row verify) ..."
$PY -m thyroid_mlx_extract.cli verify note_entities_complications \
    --verifier r1-distill-70b --limit 1 --no-resume 2>&1 | tee "$LOGS/smoke_phase3.log"

# Check if the smoke output JSONL exists and has a verdict
SMOKE_OUT=$(ls -t runs/verify/note_entities_complications/*.jsonl 2>/dev/null | head -1)
if [ -z "$SMOKE_OUT" ]; then
    echo "[$(date)] SMOKE FAILED — no output JSONL. Aborting."
    exit 1
fi
SMOKE_ROWS=$(wc -l < "$SMOKE_OUT" | tr -d ' ')
echo "[$(date)] Smoke produced $SMOKE_ROWS row(s) in $SMOKE_OUT"
if [ "$SMOKE_ROWS" -lt 1 ]; then
    echo "[$(date)] SMOKE FAILED — empty output. Aborting."
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 3: launch Phase 3 in foreground (we're already in nohup)
# ---------------------------------------------------------------------------
echo "[$(date)] Step 3: launching Phase 3 adjudicate batch ..."
bash "$REPO/scripts/mlx_phase3_adjudicate.sh" 2>&1 | tee "$LOGS/phase3_full.log"
PHASE3_RC=$?
echo "[$(date)] Phase 3 finished with rc=$PHASE3_RC"

if [ "$PHASE3_RC" -ne 0 ]; then
    echo "[$(date)] Phase 3 failed. NOT proceeding to Phase 4."
    exit "$PHASE3_RC"
fi

# ---------------------------------------------------------------------------
# Step 4: launch Phase 4 (sequential per user choice)
# ---------------------------------------------------------------------------
echo "[$(date)] Step 4: launching Phase 4 re-extraction batch ..."
bash "$REPO/scripts/mlx_phase4_reextract.sh" 2>&1 | tee "$LOGS/phase4_full.log"
PHASE4_RC=$?
echo "[$(date)] Phase 4 finished with rc=$PHASE4_RC"

echo "[$(date)] ORCHESTRATOR DONE. Phase 3 rc=$PHASE3_RC, Phase 4 rc=$PHASE4_RC"

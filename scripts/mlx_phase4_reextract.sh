#!/usr/bin/env bash
# Phase 4 — re-extract the four worst-empty note_entities_llm_* tables.
# Expected runtime: ~80 hours total. Each task is independent and resumable.
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/tools/thyroid_mlx_extract"
mkdir -p logs

echo "=== Phase 4: re-extract empty LLM tables ==="
echo

# Order by emptiness rate (most empty first)
# Each task: pull source → eval gold → run corpus → push to workspace
PHASE4_TASKS=(
  "llm_dynamic_risk:medgemma27b"     # 97.7% empty
  "llm_recurrence:medgemma27b"       # 92.3% empty
  "llm_us_dynamics:medgemma4b"       # 87% empty (small/fast model OK)
  "llm_synoptic_enrich:llama33-70b"  # 81.5% empty (hardest semantics)
)

for spec in "${PHASE4_TASKS[@]}"; do
    task="${spec%%:*}"
    model="${spec##*:}"
    echo "----- $task with $model -----"

    # 1. Pull source
    thyroid-mlx pull "$task" 2>&1 | tee "logs/${task}_pull.log"

    # 2. Eval against gold if present
    if [ -f "gold/${task}_gold.csv" ]; then
        thyroid-mlx eval "$task" --gold "gold/${task}_gold.csv" --models "$model" \
            2>&1 | tee "logs/${task}_eval.log"
    else
        echo "  ⚠ No gold/${task}_gold.csv — skipping eval, running anyway."
        echo "  Build a gold set later for retrospective F1 check."
    fi

    # 3. Run over corpus
    thyroid-mlx run "$task" --model "$model" --resume 2>&1 | tee "logs/${task}_run.log"

    # 4. Push to workspace (most recent run file)
    LATEST=$(ls -t runs/${task}/*.jsonl 2>/dev/null | head -1)
    if [ -n "$LATEST" ]; then
        thyroid-mlx push "$task" --results "$LATEST" --workspace \
            2>&1 | tee "logs/${task}_push.log"
    fi
    echo
done

echo "=== Phase 4 complete ==="
echo "Re-extracted tables in pub_workspace with provenance + error-class columns."

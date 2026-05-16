#!/usr/bin/env bash
# Phase 3 — adjudicate all 76,641 existing note_entities_* rows with R1-Distill-70B.
# Expected runtime: 28-42 hours on M5 Max. Use --resume safely if interrupted.
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/tools/thyroid_mlx_extract"

echo "=== Phase 3: adjudicate existing note_entities_* rows ==="
echo "Verifier model: r1-distill-70b"
echo "Expected runtime: ~30-43 hours total"
echo

# Priority order: high-stakes first so even if interrupted, the most-critical data is adjudicated
TABLES=(
  "note_entities_complications"        # 9,359 rows — affects every survival paper
  "note_entities_staging"              # 3,807 — staging is universally needed
  "note_entities_genetics"             # 1,738 — molecular cohort definitions
  "note_entities_operative_detail"     # 20,715 — surgical complication papers
  "note_entities_problem_list"         # 11,579 — PMH context
  "note_entities_procedures"           # 21,942 — largest table, do last
  "note_entities_medications"          # 7,501
)

for tbl in "${TABLES[@]}"; do
    echo "----- $tbl -----"
    thyroid-mlx verify "$tbl" \
        --verifier r1-distill-70b \
        --resume \
        --push \
        2>&1 | tee "logs/verify_${tbl}.log"
    echo
done

echo "=== Phase 3 complete ==="
echo "Results in pub_workspace.<table>_verified_v1 — promote to pub_canonical via signoff."

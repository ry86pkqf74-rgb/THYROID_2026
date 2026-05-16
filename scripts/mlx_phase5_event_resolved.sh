#!/usr/bin/env bash
# Phase 5 — build canonical_<feature>_event_resolved_v1 tables in pub_workspace.
# Mirrors the canonical_ete_event_resolved_v1 pattern. ~5 minutes per feature
# (pure SQL, no LLM).
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/tools/thyroid_mlx_extract"

echo "=== Phase 5: build event-resolved tables ==="
echo

FEATURES=(capsular_invasion perineural_invasion angioinvasion extranodal_extension)

for feat in "${FEATURES[@]}"; do
    echo "----- $feat -----"
    python3 -m thyroid_mlx_extract.sql.build_event_resolved "$feat"
    echo
done

echo "=== Phase 5 complete ==="
echo "Four event-resolved tables built in pub_workspace."
echo "Inspect each, then promote to pub_canonical via the signoff registry."

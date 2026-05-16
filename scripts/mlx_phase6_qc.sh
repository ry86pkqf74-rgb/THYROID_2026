#!/usr/bin/env bash
# Phase 6 — run all 8 deterministic QC assertions against pub_canonical.
# Pure SQL. Runs in seconds-to-minutes. Each writes offending rows to
# pub_eval.qc_assertions_v1 under its own assertion_id.
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ASSERTIONS_DIR="$REPO_ROOT/qc_framework_v1/assertions"

echo "=== Phase 6: deterministic QC assertions ==="
echo

# Ensure table exists
bq --location=us-central1 query --use_legacy_sql=false --quiet --max_rows=0 \
  "CREATE TABLE IF NOT EXISTS \`thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1\` (
    assertion_id STRING,
    research_id STRING,
    event_date DATE,
    detail STRING,
    detected_at TIMESTAMP
  )"

for sql in "$ASSERTIONS_DIR"/qc_*.sql; do
    name=$(basename "$sql" .sql)
    echo "----- $name -----"
    bq --location=us-central1 query --use_legacy_sql=false --quiet --max_rows=0 < "$sql"
    # Count what landed
    count=$(bq --location=us-central1 query --use_legacy_sql=false --quiet --format=csv --max_rows=1 \
      "SELECT COUNT(*) FROM \`thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1\`
       WHERE assertion_id = '$name'" | tail -1)
    echo "  -> $count offending rows in pub_eval.qc_assertions_v1"
done

echo
echo "=== Phase 6 complete ==="
echo "Browse violations:"
echo "  bq query 'SELECT assertion_id, COUNT(*) FROM \`thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1\` GROUP BY 1'"

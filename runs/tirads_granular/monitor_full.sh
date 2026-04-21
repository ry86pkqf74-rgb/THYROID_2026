#!/bin/bash
CKPT=runs/tirads_granular/full_output/note_entities_llm_tirads_granular.ckpt.jsonl
LOG=runs/tirads_granular/full_run.log
TOTAL=8810
if ! pgrep -f run_extraction_concurrent >/dev/null; then
  state="DONE"
else
  state="RUNNING"
fi
n=$(wc -l < "$CKPT" 2>/dev/null || echo 0)
pct=$(awk -v n="$n" -v t="$TOTAL" 'BEGIN{printf "%.1f", 100*n/t}')
perr=$(grep -c '"parse_error": true' "$CKPT" 2>/dev/null || echo 0)
# Throughput: lines in last 60s based on file mtime scan impractical; use driver start time
start_line=$(grep -m1 'starting --' "$LOG" | head -1)
start_ts=$(echo "$start_line" | awk '{print $1}')
now_ts=$(date +%H:%M:%S)
# Rough elapsed in seconds
elapsed=$(python3 -c "
from datetime import datetime
s = datetime.strptime('$start_ts', '%H:%M:%S')
n = datetime.strptime('$now_ts', '%H:%M:%S')
d = (n - s).total_seconds()
if d < 0: d += 86400
print(int(d))
")
rate=$(awk -v n="$n" -v e="$elapsed" 'BEGIN{if(e>0) printf "%.2f", n/e; else print "0"}')
remaining=$((TOTAL - n))
eta_min=$(awk -v r="$remaining" -v rt="$rate" 'BEGIN{if(rt>0) printf "%.1f", (r/rt)/60; else print "?"}')
echo "[$(date +%H:%M:%S)] state=$state progress=$n/$TOTAL (${pct}%)  parse_errors=$perr  elapsed=${elapsed}s  rate=${rate}/s  eta=${eta_min}min"

#!/usr/bin/env bash
# Deploy half-B of wave-1+2 shards to 6 new boost hosts, one shard each.
# Runs bootstrap + run_shard_wave12_boost on each host in parallel.
set -u

BASE="/Users/ros/THyroid 2026/THYROID_2026"
BOOTSTRAP="$BASE/scripts/vastai/bootstrap_rerun.sh"
RUN_SH="$BASE/scripts/vastai/run_shard.sh"
SHARD_DIR="$BASE/processed/remaining/shards_half"

# shard_index host port
HOSTS=(
  "00 ssh8.vast.ai 35640"
  "01 ssh7.vast.ai 35642"
  "02 ssh1.vast.ai 35642"
  "03 ssh8.vast.ai 35644"
  "04 ssh7.vast.ai 35644"
  "05 ssh4.vast.ai 35652"
)

for spec in "${HOSTS[@]}"; do
  set -- $spec
  IDX=$1; H=$2; P=$3
  (
    echo "[${IDX}] ${H}:${P} — scp bootstrap/run_shard/halfB shard"
    scp -o StrictHostKeyChecking=no -P $P "$BOOTSTRAP" root@$H:/root/bootstrap_rerun.sh > /dev/null 2>&1
    scp -o StrictHostKeyChecking=no -P $P "$RUN_SH" root@$H:/root/run_shard.sh > /dev/null 2>&1
    scp -o StrictHostKeyChecking=no -P $P "$SHARD_DIR/shard_${IDX}_halfB.parquet" root@$H:/root/clinical_notes_shard.parquet > /dev/null 2>&1
    echo "[${IDX}] scp done; running bootstrap"
    ssh -o StrictHostKeyChecking=no -p $P root@$H 'bash /root/bootstrap_rerun.sh > /var/log/bootstrap.log 2>&1 && \
        cp /root/run_shard.sh /root/THYROID_2026/scripts/vastai/run_shard.sh && \
        chmod +x /root/THYROID_2026/scripts/vastai/run_shard.sh && \
        nohup bash /root/THYROID_2026/scripts/vastai/run_shard.sh > /var/log/run_shard.log 2>&1 & disown' \
        > /tmp/boost_${IDX}.log 2>&1
    echo "[${IDX}] launched"
  ) &
done
wait
echo "All 6 boost hosts deployed."

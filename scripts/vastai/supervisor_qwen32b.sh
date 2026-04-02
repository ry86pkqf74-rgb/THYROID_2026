#!/bin/bash
set -euo pipefail

cd /opt/thyroid_extraction
umask 002

MODEL="${MODEL:-qwen3:32b}"
CONCURRENCY="${EXTRACTION_CONCURRENCY:-3}"
TOTAL_NOTES="${TOTAL_NOTES:-11037}"
INVALIDATE_DOMAINS="${INVALIDATE_DOMAINS:-}"
LOCK_FILE="/var/run/thyroid_qwen32b_supervisor.lock"
LOG="/var/log/supervisor_qwen32b.log"

# Prioritize domains that already have meaningful checkpoint progress or are
# the next highest-value V2 follow-ons. This avoids serializing the whole queue
# through dozens of low-signal domains.
DOMAINS="${DOMAINS:-dynamic_risk_response presenting_symptoms past_medical_hx rad_treatment survival_followup patient_decision_adherence functional_outcomes past_surgical_hx operative_details airway_invasion complications_rln_laryngoscopy synoptic_pathology_enrichment vascular_invasion molecular_thyroseq_afirma}"

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG"
}

cleanup_lock() {
    rm -f "$LOCK_FILE"
}

acquire_lock() {
    if [[ -f "$LOCK_FILE" ]]; then
        local old_pid
        old_pid="$(cat "$LOCK_FILE" 2>/dev/null || true)"
        if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
            log "ABORT another supervisor is already running (pid=$old_pid)"
            exit 1
        fi
        log "Removing stale supervisor lock: $LOCK_FILE"
        rm -f "$LOCK_FILE"
    fi
    echo $$ > "$LOCK_FILE"
    trap cleanup_lock EXIT
}

archive_stale_artifacts() {
    local archive_dir
    archive_dir="processed/output/archive_$(date '+%Y%m%d_%H%M%S')"
    mkdir -p "$archive_dir"

    if [[ -f processed/output/note_entities_llm_combined.parquet ]]; then
        mv processed/output/note_entities_llm_combined.parquet "$archive_dir/"
        log "Archived stale single-domain combined parquet"
    fi

    if [[ -f processed/output/note_entities_llm_staging.parquet.contaminated ]]; then
        mv processed/output/note_entities_llm_staging.parquet.contaminated "$archive_dir/"
        log "Archived contaminated staging parquet marker"
    fi

    for domain in complications functional_outcomes operative_details past_surgical_hx patient_decision_adherence; do
        local ckpt
        ckpt="processed/output/note_entities_llm_${domain}.ckpt.jsonl"
        if [[ -f "$ckpt" ]] && [[ "$(wc -l < "$ckpt")" -eq 0 ]]; then
            mv "$ckpt" "$archive_dir/"
            log "Archived zero-row checkpoint: $domain"
        fi
    done

    find /var/log -maxdepth 1 -type f -name 'worker_*.log' -size -2k -print0 2>/dev/null |
        while IFS= read -r -d '' file; do
            mv "$file" "$archive_dir/$(basename "$file")"
            log "Archived tiny stale worker log: $(basename "$file")"
        done
}

archive_domain_artifacts() {
    local domain="$1"
    local reason="$2"
    local archive_dir
    archive_dir="processed/output/archive_$(date '+%Y%m%d_%H%M%S')_${domain}"
    mkdir -p "$archive_dir"

    for artifact in \
        "processed/output/note_entities_llm_${domain}.ckpt.jsonl" \
        "processed/output/note_entities_llm_${domain}.parquet" \
        "/var/log/worker_${domain}.log"
    do
        if [[ -f "$artifact" ]]; then
            mv "$artifact" "$archive_dir/"
            log "Archived $domain artifact due to $reason: $(basename "$artifact")"
        fi
    done
}

invalidate_requested_domains() {
    if [[ -z "$INVALIDATE_DOMAINS" ]]; then
        return 0
    fi

    log "Invalidating domains before run: $INVALIDATE_DOMAINS"
    for domain in $INVALIDATE_DOMAINS; do
        archive_domain_artifacts "$domain" "requested_invalidation"
    done
}

parquet_note_count() {
    local parquet_path="$1"
    python3 - "$parquet_path" <<'PY'
import sys

import pandas as pd

path = sys.argv[1]
try:
    frame = pd.read_parquet(path, columns=["note_row_id"])
    print(int(frame["note_row_id"].astype(str).nunique()))
except Exception:
    print(0)
PY
}

domain_is_complete() {
    local domain="$1"
    local ckpt="processed/output/note_entities_llm_${domain}.ckpt.jsonl"
    local parquet="processed/output/note_entities_llm_${domain}.parquet"

    if [[ -f "$ckpt" ]] && [[ "$(wc -l < "$ckpt")" -ge "$TOTAL_NOTES" ]]; then
        return 0
    fi

    if [[ -f "$parquet" ]] && [[ "$(parquet_note_count "$parquet")" -ge "$TOTAL_NOTES" ]]; then
        return 0
    fi

    return 1
}

filter_completed_domains() {
    local filtered=()
    local removed=()
    local domain

    for domain in $DOMAINS; do
        if domain_is_complete "$domain"; then
            removed+=("$domain")
        else
            filtered+=("$domain")
        fi
    done

    if (( ${#removed[@]} > 0 )); then
        log "Filtered completed domains from queue: ${removed[*]}"
    fi

    printf '%s' "${filtered[*]}"
}

run_domain() {
    local domain="$1"
    local ckpt="processed/output/note_entities_llm_${domain}.ckpt.jsonl"
    local before_count=0
    local after_count=0

    if domain_is_complete "$domain"; then
        log "SKIP $domain -- complete artifact already present"
        return 0
    fi

    if [[ -f "$ckpt" ]]; then
        before_count="$(wc -l < "$ckpt")"
        if [[ "$before_count" -ge "$TOTAL_NOTES" ]]; then
            log "SKIP $domain -- complete ($before_count/$TOTAL_NOTES)"
            return 0
        fi
        log "RESUME $domain ($before_count/$TOTAL_NOTES)"
    else
        log "START $domain (fresh)"
    fi

    mkdir -p processed/remaining processed/output llm_extraction/prompts
    ln -sf /opt/thyroid_extraction/clinical_notes_long.parquet processed/remaining/clinical_notes_long.parquet

    python3 scripts/run_extraction_concurrent.py \
        --url http://localhost:11434/v1 \
        --model "$MODEL" \
        --domains "$domain" \
        --concurrency "$CONCURRENCY" \
        --output-dir /opt/thyroid_extraction/processed/output \
        --input-parquet /opt/thyroid_extraction/processed/remaining/clinical_notes_long.parquet \
        2>&1 | tee -a "/var/log/worker_${domain}.log"

    if [[ -f "$ckpt" ]]; then
        after_count="$(wc -l < "$ckpt")"
    fi

    if [[ "$after_count" -le "$before_count" ]]; then
        log "NO_PROGRESS $domain ($before_count -> $after_count)"
        return 1
    fi

    log "FINISHED $domain ($after_count/$TOTAL_NOTES)"
    return 0
}

acquire_lock
archive_stale_artifacts
invalidate_requested_domains
DOMAINS="$(filter_completed_domains)"

log "=== QWEN32B SUPERVISOR (model=$MODEL, concurrency=$CONCURRENCY) ==="
log "Queue: $DOMAINS"

if [[ -z "$DOMAINS" ]]; then
    log "No remaining domains after completion filter"
    exit 0
fi

for domain in $DOMAINS; do
    run_domain "$domain"
done

log "=== SUPERVISOR QUEUE COMPLETE ==="
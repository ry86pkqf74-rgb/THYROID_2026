# THYROID 2026 — Vast.ai Extraction Fleet Handoff
## State as of April 2, 2026 ~21:45 UTC

This document is the single source of truth to resume fleet monitoring and parquet sync
from any machine. All servers are running and do not need to be restarted unless something
has failed.

---

## CRITICAL RULES

- **NEVER print or expose clinical note text** — use `research_id` only, never PHI
- **All extraction runs on remote servers** — never run locally
- **SSH key auth only** — no passwords

---

## Active Fleet (3 servers, all running)

### Primary H200 NVL
| Field | Value |
|-------|-------|
| Instance ID | `33534710` |
| SSH | `ssh -o StrictHostKeyChecking=no -p 43384 root@107.206.71.138` |
| GPU | H200 NVL, 144GB VRAM |
| Ollama | 0.9.0 |
| Output dir | `/opt/thyroid_extraction/output/` |
| Rate | ~42 notes/min |
| Cost | ~$2.68/hr |

**DOMAINS env (in order):**
```
synoptic_pathology_enrichment presenting_symptoms tg_kinetics
```

### Fast1 H200 NVL
| Field | Value |
|-------|-------|
| Instance ID | `34022537` |
| SSH | `ssh -o StrictHostKeyChecking=no -p 22536 root@ssh3.vast.ai` |
| GPU | H200 NVL, 144GB VRAM |
| Ollama | 0.9.0 |
| Output dir | `/opt/thyroid_extraction/processed/output/` |
| Rate | ~52 notes/min (fastest) |
| Cost | ~$2.68/hr |

**DOMAINS env (in order):**
```
complications_rln_laryngoscopy operative_details patient_decision_adherence frozen_section_detail us_nodule_dynamics cervical_ln_detail molecular_thyroseq_afirma
```

### NewFast2 H200 NVL  ← wind-down server, destroy after parathyroid_detail completes
| Field | Value |
|-------|-------|
| Instance ID | `34034310` |
| SSH | `ssh -o StrictHostKeyChecking=no -p 34310 root@ssh9.vast.ai` |
| GPU | H200 NVL, 144GB VRAM |
| Ollama | 0.9.0 |
| Output dir | `/opt/thyroid_extraction/processed/output/` |
| Rate | ~40 notes/min |
| Cost | ~$2.72/hr |

**DOMAINS env (in order):**
```
vascular_invasion past_surgical_hx parathyroid_detail
```
> After `parathyroid_detail` completes, destroy this instance with `vastai destroy instance 34034310`

---

## Domain Assignment & Status (as of ~21:45 UTC Apr 2)

### Validated & pushed to GitHub (26/36)
| Domain | Entities | Commit |
|--------|----------|--------|
| airway_invasion | 3,116 | prior |
| combined | 8,428 | prior |
| complications | 6 | prior |
| complications_rln_laryngoscopy | 21 | Apr 2 session |
| dynamic_risk_response | 53 | prior |
| functional_outcomes | 3,322 | prior |
| genetics | 855 | prior |
| imaging | 8,428 | prior |
| labs | 2,462 | prior |
| medication_management | 1,948 | prior |
| medications | 3,577 | prior |
| operative_v2_enrichment | 5,475 | prior |
| parathyroid_per_gland | 824 | prior |
| past_medical_hx | 755 | prior |
| pathology | 10,894 | prior |
| physical_exam | 2,025 | prior |
| problem_list | 11,480 | prior |
| procedures | 12,669 | prior |
| rad_treatment | 580 | prior |
| rai_detailed | 3,747 | prior |
| recurrence | 303 | prior |
| recurrence_detailed | 25 | prior |
| staging | 1,117 | prior |
| survival_followup | 9,809 | prior |
| tirads_granular | 181 | prior |
| vascular_invasion | 4,241 | Apr 2 session |

### In-progress on servers (10/36)
| Domain | Server | Progress at ~21:45 UTC | Rate | ETA |
|--------|--------|----------------------|------|-----|
| synoptic_pathology_enrichment | Primary | ~7,260/11,037 | 42/min | ~23:05 UTC |
| presenting_symptoms | Primary | 587/11,037 (queued) | 42/min | ~Apr 3 03:20 UTC |
| tg_kinetics | Primary | 0/11,037 (queued) | 42/min | ~Apr 3 07:40 UTC |
| operative_details | Fast1 | ~4,790/11,037 | 52/min | ~Apr 3 00:00 UTC |
| patient_decision_adherence | Fast1 | 425/11,037 (queued) | 52/min | ~Apr 3 03:30 UTC |
| frozen_section_detail | Fast1 | 0/11,037 (queued) | 52/min | ~Apr 3 07:00 UTC |
| us_nodule_dynamics | Fast1 | 0/11,037 (queued) | 52/min | ~Apr 3 10:30 UTC |
| cervical_ln_detail | Fast1 | 0/11,037 (queued) | 52/min | ~Apr 3 14:00 UTC |
| molecular_thyroseq_afirma | Fast1 | 0/11,037 (queued) | 52/min | ~Apr 3 17:30 UTC |
| past_surgical_hx | NewFast2 | ~5,460/11,037 | 40/min | ~Apr 3 00:20 UTC |
| parathyroid_detail | NewFast2 | 2,270/11,037 (queued) | 40/min | ~Apr 3 07:40 UTC |

**All 36 domains complete: ~April 3, 2026 17:30 UTC (~1:30 PM ET)**

---

## Monitoring — Quick status check (run from Mac)

```bash
# All 3 servers in parallel
echo "=== $(date -u '+%H:%M UTC') ===" && \
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 43384 root@107.206.71.138 \
  'for f in /opt/thyroid_extraction/output/note_entities_llm_*.ckpt.jsonl; do bn=$(basename $f .ckpt.jsonl | sed "s/note_entities_llm_//"); count=$(wc -l < $f); if [ "$count" -eq 11037 ]; then echo "DONE $bn"; else echo "PARTIAL $bn: $count/11037"; fi; done | sort; ls /opt/thyroid_extraction/output/*.parquet 2>/dev/null | xargs -I{} basename {}' &
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 22536 root@ssh3.vast.ai \
  'for f in /opt/thyroid_extraction/processed/output/note_entities_llm_*.ckpt.jsonl; do bn=$(basename $f .ckpt.jsonl | sed "s/note_entities_llm_//"); count=$(wc -l < $f); if [ "$count" -eq 11037 ]; then echo "DONE $bn"; else echo "PARTIAL $bn: $count/11037"; fi; done | sort; ls /opt/thyroid_extraction/processed/output/*.parquet 2>/dev/null | xargs -I{} basename {}' &
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 34310 root@ssh9.vast.ai \
  'for f in /opt/thyroid_extraction/processed/output/note_entities_llm_*.ckpt.jsonl; do bn=$(basename $f .ckpt.jsonl | sed "s/note_entities_llm_//"); count=$(wc -l < $f); if [ "$count" -eq 11037 ]; then echo "DONE $bn"; else echo "PARTIAL $bn: $count/11037"; fi; done | sort; ls /opt/thyroid_extraction/processed/output/*.parquet 2>/dev/null | xargs -I{} basename {}' &
wait
```

---

## Syncing a completed parquet

When a domain shows `DONE` and a `.parquet` file appears on the server:

### Step 1 — Download
```bash
# Primary server
scp -o StrictHostKeyChecking=no -P 43384 \
  root@107.206.71.138:/opt/thyroid_extraction/output/note_entities_llm_DOMAIN.parquet \
  "/Users/ros/THyroid 2026/THYROID_2026/processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet"

# Fast1
scp -o StrictHostKeyChecking=no -P 22536 \
  root@ssh3.vast.ai:/opt/thyroid_extraction/processed/output/note_entities_llm_DOMAIN.parquet \
  "/Users/ros/THyroid 2026/THYROID_2026/processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet"

# NewFast2
scp -o StrictHostKeyChecking=no -P 34310 \
  root@ssh9.vast.ai:/opt/thyroid_extraction/processed/output/note_entities_llm_DOMAIN.parquet \
  "/Users/ros/THyroid 2026/THYROID_2026/processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet"
```

### Step 2 — Validate (must pass all checks before committing)
```python
import pyarrow.parquet as pq, json

source = pq.read_table("processed/remaining/clinical_notes_long.parquet")
source_rids = {nrid: str(rid) for nrid, rid in
               zip(source["note_row_id"].to_pylist(), source["research_id"].to_pylist())}
source_ids = set(source["note_row_id"].to_pylist())

domain = "DOMAIN_NAME"
tbl = pq.read_table(f"processed/output/v2_parquets/note_entities_llm_{domain}.parquet")
nrids = tbl["note_row_id"].to_pylist()
rids = [str(r) for r in tbl["research_id"].to_pylist()]

unmatched    = sum(1 for n in nrids if n not in source_ids)
rid_mismatch = sum(1 for n, r in zip(nrids, rids) if n in source_rids and source_rids[n] != r)
invalid_json = 0
entity_count = 0
for rj in tbl["result_json"].to_pylist():
    try:
        d = json.loads(rj) if isinstance(rj, str) else rj
        entity_count += len(d.get("entities", []))
    except:
        invalid_json += 1

print(f"{domain}: rows={len(tbl)} unmatched={unmatched} rid_mismatch={rid_mismatch} invalid_json={invalid_json} entities={entity_count}")
# Expected: rows=11037, unmatched=0, rid_mismatch=0, invalid_json=0
```

### Step 3 — Commit & push
```bash
cd "/Users/ros/THyroid 2026/THYROID_2026"
git add processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet
git commit -m "Add validated DOMAIN extraction artifact

Synced from SERVER after checkpoint reached 11,037/11,037.
Validation: 0 unmatched, 0 rid_mismatch, 0 invalid_json.
ENTITY_COUNT entities extracted across 11,037 notes."
git push origin main
```

---

## Restart procedures (if a server goes down)

### Any server — check if dead
```bash
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p PORT root@HOST \
  'nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader; ps aux | grep -E "supervisor_qwen|run_extraction" | grep -v grep'
```

### Primary (107.206.71.138:43384) — Ollama 0.9.0
```bash
ssh -p 43384 root@107.206.71.138 '
pkill -f supervisor_qwen32b; pkill -f run_extraction; pkill ollama; sleep 3
OLLAMA_NUM_PARALLEL=4 OLLAMA_FLASH_ATTENTION=1 nohup ollama serve > /var/log/ollama.log 2>&1 &
sleep 8
curl -sf http://localhost:11434/api/generate -d "{\"model\":\"qwen3:32b\",\"prompt\":\"/no_think hi\",\"stream\":false}" > /dev/null
rm -f /var/run/thyroid_qwen32b_supervisor.lock
cd /opt/thyroid_extraction
DOMAINS="synoptic_pathology_enrichment presenting_symptoms tg_kinetics" \
  MODEL="qwen3:32b" EXTRACTION_CONCURRENCY="6" \
  nohup bash supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &
echo "Restarted"'
```

### Fast1 (ssh3.vast.ai:22536) — Ollama 0.9.0
```bash
ssh -p 22536 root@ssh3.vast.ai '
pkill -f supervisor_qwen32b; pkill -f run_extraction; pkill ollama; sleep 3
OLLAMA_NUM_PARALLEL=4 OLLAMA_FLASH_ATTENTION=1 nohup ollama serve > /var/log/ollama.log 2>&1 &
sleep 8
curl -sf http://localhost:11434/api/generate -d "{\"model\":\"qwen3:32b\",\"prompt\":\"/no_think hi\",\"stream\":false}" > /dev/null
rm -f /var/run/thyroid_qwen32b_supervisor.lock
cd /opt/thyroid_extraction
DOMAINS="complications_rln_laryngoscopy operative_details patient_decision_adherence frozen_section_detail us_nodule_dynamics cervical_ln_detail molecular_thyroseq_afirma" \
  MODEL="qwen3:32b" EXTRACTION_CONCURRENCY="6" \
  nohup bash supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &
echo "Restarted"'
```

### NewFast2 (ssh9.vast.ai:34310) — Ollama 0.9.0
```bash
ssh -p 34310 root@ssh9.vast.ai '
pkill -f supervisor_qwen32b; pkill -f run_extraction; pkill ollama; sleep 3
OLLAMA_NUM_PARALLEL=4 OLLAMA_FLASH_ATTENTION=1 nohup ollama serve > /var/log/ollama.log 2>&1 &
sleep 8
curl -sf http://localhost:11434/api/generate -d "{\"model\":\"qwen3:32b\",\"prompt\":\"/no_think hi\",\"stream\":false}" > /dev/null
rm -f /var/run/thyroid_qwen32b_supervisor.lock
cd /opt/thyroid_extraction
DOMAINS="vascular_invasion past_surgical_hx parathyroid_detail" \
  MODEL="qwen3:32b" EXTRACTION_CONCURRENCY="6" \
  nohup bash supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &
echo "Restarted"'
```

> **Destroy NewFast2 after `parathyroid_detail` is validated and committed:**
> ```bash
> vastai destroy instance 34034310
> ```

---

## Known issues & fixes applied this session

### Supervisor script bug — log stdout contamination (FIXED Apr 2)
The `filter_completed_domains()` function in `supervisor_qwen32b.sh` called `log()` inside
a command substitution `$(...)`, mixing the timestamp log line into the captured DOMAINS
string. This caused `run_extraction_concurrent.py` to receive `[2026-04-02` as a domain name
and error out immediately.

**Fix applied** (line 152 of supervisor_qwen32b.sh on Fast1 and NewFast2):
```bash
# Before (broken):
log "Filtered completed domains from queue: ${removed[*]}"
# After (fixed):
log "Filtered completed domains from queue: ${removed[*]}" >&2
```

### Ollama version matters
- **0.9.0** = correct for all H200 NVL servers, ~40-52 notes/min
- **0.19.0** = slower on NVL (~9 notes/min) due to larger default KV cache allocation
- Old Fast2 (France, non-NVL) used 0.19.0 with `OLLAMA_CONTEXT_LENGTH=4096` — that server was **destroyed** this session

### research_id type divergence (expected, non-fatal)
Source parquet stores `research_id` as `int64`; extraction parquets store it as `string`.
Values are identical when string-coerced. Validation script accounts for this. Do not fail
on this during validation.

---

## Vast.ai CLI

```bash
# Search for H200 NVL offers
vastai search offers 'gpu_name="H200 NVL" num_gpus=1 reliability>0.95 disk_space>=80' -o 'dph' --raw

# Check instance status
vastai show instance INSTANCE_ID --raw

# Destroy instance
vastai destroy instance INSTANCE_ID
```

---

## Repo
```
Local:   /Users/ros/THyroid 2026/THYROID_2026
Remote:  https://github.com/ry86pkqf74-rgb/THYROID_2026.git
Branch:  main
Parquets: processed/output/v2_parquets/
Source:   processed/remaining/clinical_notes_long.parquet  (11,037 rows)
```

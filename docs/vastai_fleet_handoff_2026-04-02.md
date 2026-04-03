# THYROID 2026 — Vast.ai Extraction Fleet Handoff
## State as of April 3, 2026 ~04:35 UTC (post-crash recovery, 5 servers)

This document is the single source of truth to resume fleet monitoring and parquet sync
from any machine. All servers are running and do not need to be restarted unless something
has failed.

---

## CRITICAL RULES

- **NEVER print or expose clinical note text** — use `research_id` only, never PHI
- **All extraction runs on remote servers** — never run locally
- **SSH key auth only** — no passwords

---

## Active Fleet (5 servers, all running)

### Primary H200 NVL
| Field | Value |
|-------|-------|
| Instance ID | `33534710` |
| SSH | `ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 43384 root@107.206.71.138` |
| GPU | H200 NVL, 144GB VRAM |
| Ollama | 0.9.0 |
| Output dir | `/opt/thyroid_extraction/processed/output/` |
| Rate | ~42 notes/min |
| Cost | ~$3.04/hr |

**DOMAINS env (in order):**
```
past_medical_hx presenting_symptoms rad_treatment
```
> `synoptic_pathology_enrichment` completed (11,037/11,037) — parquet synced locally.

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
patient_decision_adherence cervical_ln_detail molecular_thyroseq_afirma
```
> `operative_details` completed (11,037/11,037) — parquet synced locally.
> `complications_rln_laryngoscopy` completed (11,037/11,037) — parquet synced locally.

### Fast2 H200 NVL
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
parathyroid_detail tg_kinetics
```
> `past_surgical_hx` completed (11,037/11,037) — parquet synced locally.
> `vascular_invasion` completed (11,037/11,037) — parquet synced locally.

### Server5 H200 NVL (launched Apr 3 ~03:00 UTC, post-crash replacement)
| Field | Value |
|-------|-------|
| Instance ID | `34050323` |
| SSH | `ssh -o StrictHostKeyChecking=no -p 10322 root@ssh5.vast.ai` |
| GPU | H200 NVL, 144GB VRAM |
| Ollama | **0.20.0** (0.9.0 did not detect GPU on driver 560; 0.20.0 with lspci/lshw installed works) |
| Output dir | `/opt/thyroid_extraction/processed/output/` |
| Rate | ~24 notes/min |
| Cost | ~$2.48/hr |

**DOMAINS env (in order):**
```
frozen_section_detail
```
> `us_nodule_dynamics` moved to Server6 (faster). After `frozen_section_detail` completes, destroy with `vastai destroy instance 34050323`

### Server6 H200 NVL (launched Apr 3 ~04:25 UTC, accelerator for us_nodule_dynamics)
| Field | Value |
|-------|-------|
| Instance ID | `34050871` |
| SSH | `ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 10870 root@ssh1.vast.ai` |
| GPU | H200 NVL, 144GB VRAM (Bulgaria, driver 565.57.01) |
| Ollama | **0.20.0** (0.9.0 binary loads model to CPU on this driver) |
| Output dir | `/opt/thyroid_extraction/processed/output/` |
| Rate | ~24 notes/min |
| Cost | ~$2.53/hr |

**DOMAINS env (in order):**
```
us_nodule_dynamics
```
> After domain completes, destroy with `vastai destroy instance 34050871`

### NewFast3 (DEAD — resources unavailable)
| Field | Value |
|-------|-------|
| Instance ID | `34042551` |
| Status | `exited` — crashed during fleet outage, cannot restart (resources unavailable) |
| Domains | `frozen_section_detail us_nodule_dynamics` — reassigned to Server5 |

---

## Dead/Destroyed Instances
- `34042551` (NewFast3) — exited, resources unavailable, domains reassigned to Server5

---

## Domain Assignment & Progress (as of ~04:35 UTC Apr 3)

### Completed and synced locally (5 fresh + 24 prior = 29/36 total)
| Domain | Entities | Source | Synced |
|--------|----------|--------|--------|
| synoptic_pathology_enrichment | TBD | Primary | Apr 3 |
| operative_details | TBD | Fast1 | Apr 3 |
| complications_rln_laryngoscopy | TBD | Fast1 | Apr 3 |
| past_surgical_hx | TBD | Fast2 | Apr 3 |
| vascular_invasion | TBD | Fast2 | Apr 3 |
| *(plus 24 prior domains — see previous version)* | | | prior |

### In-progress on servers (9 remaining across 5 servers, zero overlap)
| Domain | Server | Progress at 04:35 UTC | Remaining | Rate | ETA (UTC) |
|--------|--------|----------------------|-----------|------|-----------|
| past_medical_hx | Primary | 10,276/11,037 | 761 | 42/min | ~04:55 |
| presenting_symptoms | Primary | 62/11,037 | 10,975 | 42/min | ~09:15 |
| rad_treatment | Primary | 10/11,037 | 11,027 | 42/min | ~13:35 |
| patient_decision_adherence | Fast1 | 4,781/11,037 | 6,256 | 52/min | ~06:35 |
| cervical_ln_detail | Fast1 | 0/11,037 | 11,037 | 52/min | ~10:05 |
| molecular_thyroseq_afirma | Fast1 | 0/11,037 | 11,037 | 52/min | ~13:35 |
| parathyroid_detail | Fast2 | 10,255/11,037 | 782 | 40/min | ~04:55 |
| tg_kinetics | Fast2 | 0/11,037 | 11,037 | 40/min | ~09:35 |
| frozen_section_detail | Server5 | 210/11,037 | 10,827 | 24/min | ~12:05 |
| us_nodule_dynamics | **Server6** | 4/11,037 | 11,033 | 24/min | ~12:15 |

**Bottleneck servers:**
- **Primary** and **Fast1** finish at ~13:35 UTC (~9:35 AM ET) — last domains `rad_treatment` and `molecular_thyroseq_afirma`
- **Server5** finishes at ~12:05 UTC — `frozen_section_detail` only
- **Server6** finishes at ~12:15 UTC — `us_nodule_dynamics` only

**All 36 domains complete: ~April 3, 2026 13:35 UTC (~9:35 AM ET)**
> 6 hours faster than previous estimate (~19:40 UTC) thanks to parallelizing `us_nodule_dynamics` onto Server6

---

## Monitoring — Quick status check (run from Mac)

```bash
echo "=== $(date -u '+%H:%M UTC') ===" && \
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 43384 root@107.206.71.138 \
  'echo "PRIMARY:"; for f in /opt/thyroid_extraction/processed/output/note_entities_llm_*.ckpt.jsonl; do bn=$(basename $f .ckpt.jsonl | sed "s/note_entities_llm_//"); count=$(wc -l < $f); if [ "$count" -eq 11037 ]; then echo "  DONE $bn"; else echo "  PARTIAL $bn: $count/11037"; fi; done | sort' &
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 22536 root@ssh3.vast.ai \
  'echo "FAST1:"; for f in /opt/thyroid_extraction/processed/output/note_entities_llm_*.ckpt.jsonl; do bn=$(basename $f .ckpt.jsonl | sed "s/note_entities_llm_//"); count=$(wc -l < $f); if [ "$count" -eq 11037 ]; then echo "  DONE $bn"; else echo "  PARTIAL $bn: $count/11037"; fi; done | sort' &
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 34310 root@ssh9.vast.ai \
  'echo "FAST2:"; for f in /opt/thyroid_extraction/processed/output/note_entities_llm_*.ckpt.jsonl; do bn=$(basename $f .ckpt.jsonl | sed "s/note_entities_llm_//"); count=$(wc -l < $f); if [ "$count" -eq 11037 ]; then echo "  DONE $bn"; else echo "  PARTIAL $bn: $count/11037"; fi; done | sort' &
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 10322 root@ssh5.vast.ai \
  'echo "SERVER5:"; for f in /opt/thyroid_extraction/processed/output/note_entities_llm_*.ckpt.jsonl; do bn=$(basename $f .ckpt.jsonl | sed "s/note_entities_llm_//"); count=$(wc -l < $f); if [ "$count" -eq 11037 ]; then echo "  DONE $bn"; else echo "  PARTIAL $bn: $count/11037"; fi; done | sort' &
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -p 10870 root@ssh1.vast.ai \
  'echo "SERVER6:"; for f in /opt/thyroid_extraction/processed/output/note_entities_llm_*.ckpt.jsonl; do bn=$(basename $f .ckpt.jsonl | sed "s/note_entities_llm_//"); count=$(wc -l < $f); if [ "$count" -eq 11037 ]; then echo "  DONE $bn"; else echo "  PARTIAL $bn: $count/11037"; fi; done | sort' &
wait
```

---

## Syncing a completed parquet

When a domain shows `DONE` and a `.parquet` file appears on the server:

### Step 1 — Download
```bash
# Primary server
scp -o StrictHostKeyChecking=no -P 43384 \
  root@107.206.71.138:/opt/thyroid_extraction/processed/output/note_entities_llm_DOMAIN.parquet \
  processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet

# Fast1
scp -o StrictHostKeyChecking=no -P 22536 \
  root@ssh3.vast.ai:/opt/thyroid_extraction/processed/output/note_entities_llm_DOMAIN.parquet \
  processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet

# Fast2
scp -o StrictHostKeyChecking=no -P 34310 \
  root@ssh9.vast.ai:/opt/thyroid_extraction/processed/output/note_entities_llm_DOMAIN.parquet \
  processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet

# Server5
scp -o StrictHostKeyChecking=no -P 10322 \
  root@ssh5.vast.ai:/opt/thyroid_extraction/processed/output/note_entities_llm_DOMAIN.parquet \
  processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet
```

### Step 2 — Validate (must pass all checks before committing)
```python
import pyarrow.parquet as pq, json

source = pq.read_table("processed/clinical_notes_long.parquet")
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
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 43384 root@107.206.71.138 '
pkill -f supervisor_qwen32b; pkill -f run_extraction; pkill ollama; sleep 3
OLLAMA_NUM_PARALLEL=4 OLLAMA_FLASH_ATTENTION=1 nohup ollama serve > /var/log/ollama.log 2>&1 &
sleep 8
curl -sf http://localhost:11434/api/generate -d "{\"model\":\"qwen3:32b\",\"prompt\":\"/no_think hi\",\"stream\":false}" > /dev/null
rm -f /var/run/thyroid_qwen32b_supervisor.lock
cd /opt/thyroid_extraction
DOMAINS="past_medical_hx presenting_symptoms rad_treatment" \
  MODEL="qwen3:32b" EXTRACTION_CONCURRENCY="6" \
  nohup bash supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &
echo "Restarted"'
```
> **NOTE:** SSH proxy port 14710 does NOT work after crash recovery. Use direct port 43384.

### Fast1 (ssh3.vast.ai:22536) — Ollama 0.9.0
```bash
ssh -p 22536 root@ssh3.vast.ai '
pkill -f supervisor_qwen32b; pkill -f run_extraction; pkill ollama; sleep 3
OLLAMA_NUM_PARALLEL=4 OLLAMA_FLASH_ATTENTION=1 nohup ollama serve > /var/log/ollama.log 2>&1 &
sleep 8
curl -sf http://localhost:11434/api/generate -d "{\"model\":\"qwen3:32b\",\"prompt\":\"/no_think hi\",\"stream\":false}" > /dev/null
rm -f /var/run/thyroid_qwen32b_supervisor.lock
cd /opt/thyroid_extraction
DOMAINS="patient_decision_adherence cervical_ln_detail molecular_thyroseq_afirma" \
  MODEL="qwen3:32b" EXTRACTION_CONCURRENCY="6" \
  nohup bash supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &
echo "Restarted"'
```

### Fast2 (ssh9.vast.ai:34310) — Ollama 0.9.0
```bash
ssh -p 34310 root@ssh9.vast.ai '
pkill -f supervisor_qwen32b; pkill -f run_extraction; pkill ollama; sleep 3
OLLAMA_NUM_PARALLEL=4 OLLAMA_FLASH_ATTENTION=1 nohup ollama serve > /var/log/ollama.log 2>&1 &
sleep 8
curl -sf http://localhost:11434/api/generate -d "{\"model\":\"qwen3:32b\",\"prompt\":\"/no_think hi\",\"stream\":false}" > /dev/null
rm -f /var/run/thyroid_qwen32b_supervisor.lock
cd /opt/thyroid_extraction
DOMAINS="parathyroid_detail tg_kinetics" \
  MODEL="qwen3:32b" EXTRACTION_CONCURRENCY="6" \
  nohup bash supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &
echo "Restarted"'
```

### Server5 (ssh5.vast.ai:10322) — Ollama 0.20.0
```bash
ssh -p 10322 root@ssh5.vast.ai '
pkill -f supervisor_qwen32b; pkill -f run_extraction; pkill ollama; sleep 3
OLLAMA_NUM_PARALLEL=6 OLLAMA_FLASH_ATTENTION=1 OLLAMA_CONTEXT_LENGTH=4096 OLLAMA_KEEP_ALIVE=24h \
  nohup ollama serve > /var/log/ollama.log 2>&1 &
sleep 8
curl -sf http://localhost:11434/api/generate -d "{\"model\":\"qwen3:32b\",\"prompt\":\"/no_think hi\",\"stream\":false}" > /dev/null
rm -f /var/run/thyroid_qwen32b_supervisor.lock
cd /opt/thyroid_extraction
DOMAINS="frozen_section_detail us_nodule_dynamics" \
  MODEL="qwen3:32b" EXTRACTION_CONCURRENCY="6" \
  nohup bash supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &
echo "Restarted"'
```

> **Destroy Server5 after `frozen_section_detail` is validated and committed:**
> ```bash
> vastai destroy instance 34050323
> ```

### Server6 (ssh1.vast.ai:10870) — Ollama 0.20.0
```bash
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 10870 root@ssh1.vast.ai '
pkill -f supervisor_qwen32b; pkill -f run_extraction; pkill ollama; sleep 3
OLLAMA_NUM_PARALLEL=6 OLLAMA_FLASH_ATTENTION=1 OLLAMA_CONTEXT_LENGTH=4096 OLLAMA_KEEP_ALIVE=24h \
  nohup ollama serve > /var/log/ollama.log 2>&1 &
sleep 8
curl -sf http://localhost:11434/api/generate -d "{\"model\":\"qwen3:32b\",\"prompt\":\"/no_think hi\",\"stream\":false}" > /dev/null
rm -f /var/run/thyroid_qwen32b_supervisor.lock
cd /opt/thyroid_extraction
DOMAINS="us_nodule_dynamics" \
  MODEL="qwen3:32b" EXTRACTION_CONCURRENCY="6" \
  nohup bash supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &
echo "Restarted"'
```

> **Destroy Server6 after `us_nodule_dynamics` is validated and committed:**
> ```bash
> vastai destroy instance 34050871
> ```

---

## Known issues & fixes applied this session

### Fleet-wide outage (Apr 3 ~02:30 UTC)
All 4 servers crashed simultaneously ("Required resources are currently unavailable"). Primary and NewFast3 lost their instances; Fast1 and Fast2 eventually came back with checkpoints intact. NewFast3 remains dead. Server5 was rented as a replacement. Primary came back but on a different base image (cuda-13.0.2-auto) and only responds on direct port 43384 (not proxy port 14710).

### Supervisor script bug — log stdout contamination (FIXED Apr 2)
The `filter_completed_domains()` function in `supervisor_qwen32b.sh` called `log()` inside
a command substitution `$(...)`, mixing the timestamp log line into the captured DOMAINS
string. Fix: redirect log output to stderr with `>&2`.

### Ollama version matters
- **0.9.0** = correct for H200 NVL servers with compatible drivers, ~40-52 notes/min
- **0.20.0** = required when 0.9.0 fails to detect GPU (driver 560.35.03); ~24 notes/min on NVL; requires `apt install pciutils lshw` BEFORE install for GPU detection
- **0.19.0** = slower on NVL (~9 notes/min) due to larger default KV cache allocation

### NewFast3 GPU detection issue (FIXED Apr 3)
Ollama 0.9.0 binary from Fast1 copied to NewFast3 but loaded model entirely on CPU. Root cause: driver 560.35.03 not recognized by 0.9.0 CUDA detection. Fix: install `pciutils lshw` packages, then use Ollama 0.20.0.

### research_id type divergence (expected, non-fatal)
Source parquet stores `research_id` as `int64`; extraction parquets store it as `string`.
Values are identical when string-coerced. Validation script accounts for this.

### Duplicate process hazard (FIXED Apr 2-3)
`pkill -f supervisor_qwen32b` in SSH can match the SSH remote command string itself. Use explicit PID-based `kill` commands instead.

### Primary SSH port change (FIXED Apr 3)
After fleet crash, Primary proxy port 14710 returns "Connection closed". Direct port 43384 works. Updated all SSH commands accordingly.

---

## Vast.ai CLI

```bash
# Set API key
.venv/bin/vastai set api-key YOUR_KEY

# Search for H200 NVL offers
.venv/bin/vastai search offers 'gpu_name="H200 NVL" num_gpus=1 reliability>0.90 disk_space>=80' -o 'dph' --raw

# Rent an instance
.venv/bin/vastai create instance OFFER_ID --image pytorch/pytorch:2.5.1-cuda12.4-cudnn9-devel --disk 100 --ssh --raw

# Check instance status
.venv/bin/vastai show instance INSTANCE_ID --raw

# Destroy instance
.venv/bin/vastai destroy instance INSTANCE_ID
```

---

## Repo
```
Local:   /Users/loganglosser/THYROID_2026
Remote:  https://github.com/ry86pkqf74-rgb/THYROID_2026.git
Branch:  main
Parquets: processed/output/v2_parquets/
Source:   processed/clinical_notes_long.parquet  (11,037 rows)
```

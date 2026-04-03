# THYROID 2026 — Vast.ai Extraction Fleet Handoff
## State as of April 3, 2026 ~14:05 UTC — **FLEET COMPLETE, 0 active instances**

This document records the Apr 2–3, 2026 Qwen32b note-entity extraction fleet: operational
notes, destroyed instance IDs, and procedures for a **future** rerun. **There are no
running vast.ai boxes for this wave** — all artifacts are in GitHub under
`processed/output/v2_parquets/`.

---

## CRITICAL RULES

- **NEVER print or expose clinical note text** — use `research_id` only, never PHI
- **All extraction runs on remote servers** — never run locally (for this fleet pattern)
- **SSH key auth only** — no passwords

---

## Fleet status — **COMPLETE**

| Item | Status |
|------|--------|
| **Active instances** | **0** |
| **Domains** | **36 / 36** complete (11,037 notes each where applicable) |
| **Canonical artifacts** | `processed/output/v2_parquets/note_entities_llm_*.parquet` on `main` |
| **Validation** | Row/linkage/JSON checks; MotherDuck (`Thyroid 2026` DB) used for deep SQL validation on late domains |
| **Last parquet pushed** | `molecular_thyroseq_afirma` — finished on Primary after Fast1 checkpoint handoff (~14:00 UTC) |
| **Primary destroyed** | Instance `33534710` — after audit confirmed no newer data than GitHub |

**Late-run highlights (Apr 3):** checkpoint migrations (Server5→Primary for `frozen_section_detail`; Fast1→Primary for `molecular_thyroseq_afirma`); Server6/5/Fast2/Fast1 destroyed when idle or superseded; `us_nodule_dynamics` completed on Fast2 then synced.

---

## Destroyed / exited instances (audit trail)

| Instance ID | Role | Notes |
|---------------|------|--------|
| `34042551` | NewFast3 | Exited during outage; work reassigned |
| `34050871` | Server6 | `us_nodule_dynamics` → Fast2 |
| `34050323` | Server5 | `frozen_section_detail` checkpoint → Primary |
| `34034310` | Fast2 | Idle after `us_nodule_dynamics`; destroyed |
| `34022537` | Fast1 | Checkpoint → Primary for `molecular_thyroseq_afirma`; destroyed |
| `33534710` | Primary | Last box; final domain + prior Primary domains; destroyed after empty-server check |

---

## Historical instance reference (SSH no longer valid — destroyed)

Use this table only when reading old logs or planning a **new** rental (new IPs/ports).

| Name | Instance ID | Last known SSH / host |
|------|-------------|------------------------|
| Primary | `33534710` | `ssh … -p 43384 root@107.206.71.138` |
| Fast1 | `34022537` | `ssh … -p 22536 root@ssh3.vast.ai` |
| Fast2 | `34034310` | `ssh … -p 34310 root@ssh9.vast.ai` |
| Server5 | `34050323` | `ssh … -p 10322 root@ssh5.vast.ai` |
| Server6 | `34050871` | `ssh … -p 10870 root@ssh1.vast.ai` |

---

## Domain outcome (Apr 3 wave)

All domains listed in the supervisor queues for this fleet completed; parquets validated
and pushed incrementally. **No remaining server-side-only outputs** after final Primary
audit (archive folders on disk were older/smaller than repo — repo is source of truth).

---

## Monitoring — **N/A** (fleet complete)

For a **future** fleet, reuse the per-host loop pattern: `wc -l` on
`/opt/thyroid_extraction/processed/output/note_entities_llm_*.ckpt.jsonl` and compare to
11,037; confirm `.parquet` exists before `scp`.

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

### Step 2b — MotherDuck deep validation (optional, faster SQL)

Connect with `duckdb.connect("md:?motherduck_token=...")`, `USE "Thyroid 2026"`, then:

- `CREATE TEMP VIEW source AS SELECT note_row_id, CAST(research_id AS VARCHAR) … FROM read_parquet('processed/clinical_notes_long.parquet')`
- `CREATE TEMP VIEW tgt AS SELECT * FROM read_parquet('processed/output/v2_parquets/note_entities_llm_DOMAIN.parquet')`
- Join `tgt` → `source` on `note_row_id`; assert `COUNT(*)=11037`, `COUNT(DISTINCT note_row_id)=11037`, zero unmatched / `research_id` mismatch / duplicate keys; `json_array_length` on `result_json` for entity totals and type histograms.

Token: `.streamlit/secrets.toml` → `MOTHERDUCK_TOKEN` (do not commit the token).

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

## Restart procedures (historical — **all instances above destroyed Apr 3, 2026**)

Reference for a **future** fleet only. Replace host/port/instance IDs after a new `vastai create`.

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

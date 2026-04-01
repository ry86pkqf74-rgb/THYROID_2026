# VastAI New Chat Handoff — 2026-04-01

Use the prompt below to start a fresh chat that begins with a direct fleet audit, validates active extraction, checks for completed domains that can be copied back and pushed, and ends with a full progress report.

## Current operational state

This is the latest known good state after recovering the interrupted scale-out, repairing queue rollover on H200 and worker D, cleaning stale wrapper shells on workers B/C, and destroying the retired A40 instance.

- Corpus size per domain: `11,037` notes
- Model: `qwen3:32b`
- Remote base dir on each worker: `/opt/thyroid_extraction`
- Runtime entrypoint on workers: `/opt/thyroid_extraction/scripts/run_extraction_concurrent.py`
- Supervisor wrapper on workers: `/opt/thyroid_extraction/supervisor_qwen32b.sh`
- Supervisor source in repo: `scripts/vastai/supervisor_qwen32b.sh`
- Runtime source in repo: `scripts/vastai/run_extraction_concurrent.py`
- Ollama endpoint on each worker: `http://localhost:11434/v1`

## Active workers

### 1. Primary H200
- Instance ID: `33534710`
- Direct SSH: `ssh -p 43384 -o StrictHostKeyChecking=no root@107.206.71.138`
- Vast proxy: `ssh -p 14710 -o StrictHostKeyChecking=no root@ssh1.vast.ai`
- Intended queue: `tirads_granular parathyroid_per_gland operative_v2_enrichment`
- Last verified active domain: `tirads_granular`
- Last verified checkpoint count: `5,655`
- Important note: this host is now running under `supervisor_qwen32b.sh` with a valid lock file and should auto-roll to the remaining queue after `tirads_granular` completes.

### 2. Fast worker A
- Instance ID: `33935193`
- SSH: `ssh -p 15192 -o StrictHostKeyChecking=no root@ssh8.vast.ai`
- Intended queue: `vascular_invasion dynamic_risk_response presenting_symptoms past_medical_hx rad_treatment`
- Last verified active domain: `vascular_invasion`
- Last verified checkpoint count: `286`

### 3. Fast worker B
- Instance ID: `33935507`
- SSH: `ssh -p 15506 -o StrictHostKeyChecking=no root@ssh6.vast.ai`
- Intended queue: `rai_detailed recurrence_detailed medication_management`
- Last verified active domain: `rai_detailed`
- Last verified checkpoint count: `167`
- Important note: a stale launcher shell was cleaned up; the real supervisor and extractor remained healthy.

### 4. Fast worker C
- Instance ID: `33937332`
- Label: `thyroid2026-v2-fast-c3`
- SSH: `ssh -p 17332 -o StrictHostKeyChecking=no root@ssh4.vast.ai`
- Intended queue: `survival_followup tg_kinetics parathyroid_detail`
- Last verified active domain: `survival_followup`
- Last verified checkpoint count after clean relaunch: `193`
- Important note: this worker was explicitly relaunched after prompt sync so `survival_followup` should now be using the domain-specific prompt, not the earlier fallback prompt.
- Important note: a stale launcher shell was cleaned up; the real supervisor and extractor remained healthy.

### 5. Fast worker D
- Instance ID: `33937333`
- Label: `thyroid2026-v2-fast-d3`
- SSH: `ssh -p 17332 -o StrictHostKeyChecking=no root@ssh6.vast.ai`
- Intended queue: `airway_invasion frozen_section_detail us_nodule_dynamics cervical_ln_detail complications_rln_laryngoscopy molecular_thyroseq_afirma synoptic_pathology_enrichment`
- Last verified active domain: `airway_invasion`
- Last verified checkpoint count: `162`
- Important note: this host is now running under `supervisor_qwen32b.sh` with a valid lock file and should auto-roll through the remaining queue after `airway_invasion` completes.

## Retired worker

### A40
- Instance ID: `33933782`
- SSH: `ssh -p 13782 -o StrictHostKeyChecking=no root@ssh5.vast.ai`
- Status: destroyed
- Reason: replaced by worker C after survival_followup checkpoint handoff, then destroyed after confirming all `survival_followup` checkpoint rows on A40 were already present on worker C
- Important note: this host is gone and should not be referenced as a recovery option unless a new instance is created from scratch

## Remote file paths to inspect

- Supervisor log: `/var/log/supervisor_qwen32b.log`
- Per-domain log: `/var/log/worker_<domain>.log`
- Checkpoint: `/opt/thyroid_extraction/output/note_entities_llm_<domain>.ckpt.jsonl`
- Final parquet: `/opt/thyroid_extraction/output/note_entities_llm_<domain>.parquet`
- Input parquet: `/opt/thyroid_extraction/processed/remaining/clinical_notes_long.parquet`
- Prompt directory: `/opt/thyroid_extraction/notes_extraction_new/prompts`

## Exact operator prompt for a new chat

```text
You are taking over a live VastAI multi-worker extraction run for THYROID_2026.

Start by directly checking every active worker to prove extraction is still executing and generating the expected outputs. Do not rely on earlier chat summaries without re-checking the live machines.

Repository context:
- Repo root: /Users/ros/THyroid 2026/THYROID_2026
- Runtime script in repo: scripts/vastai/run_extraction_concurrent.py
- Supervisor wrapper in repo: scripts/vastai/supervisor_qwen32b.sh
- Remote runtime path: /opt/thyroid_extraction/scripts/run_extraction_concurrent.py
- Remote supervisor path: /opt/thyroid_extraction/supervisor_qwen32b.sh
- Corpus size per domain: 11037 notes
- Model: qwen3:32b

Active fleet to audit:

1. Primary H200
- ssh -p 43384 -o StrictHostKeyChecking=no root@107.206.71.138
- fallback proxy: ssh -p 14710 -o StrictHostKeyChecking=no root@ssh1.vast.ai
- intended queue: tirads_granular parathyroid_per_gland operative_v2_enrichment
- last known active domain: tirads_granular

2. Fast worker A
- ssh -p 15192 -o StrictHostKeyChecking=no root@ssh8.vast.ai
- intended queue: vascular_invasion dynamic_risk_response presenting_symptoms past_medical_hx rad_treatment
- last known active domain: vascular_invasion

3. Fast worker B
- ssh -p 15506 -o StrictHostKeyChecking=no root@ssh6.vast.ai
- intended queue: rai_detailed recurrence_detailed medication_management
- last known active domain: rai_detailed

4. Fast worker C
- ssh -p 17332 -o StrictHostKeyChecking=no root@ssh4.vast.ai
- intended queue: survival_followup tg_kinetics parathyroid_detail
- last known active domain: survival_followup
- note: this replaced the A40 after checkpoint handoff

5. Fast worker D
- ssh -p 17332 -o StrictHostKeyChecking=no root@ssh6.vast.ai
- intended queue: airway_invasion frozen_section_detail us_nodule_dynamics cervical_ln_detail complications_rln_laryngoscopy molecular_thyroseq_afirma synoptic_pathology_enrichment
- last known active domain: airway_invasion

Retired worker:
- A40: ssh -p 13782 -o StrictHostKeyChecking=no root@ssh5.vast.ai
- It should remain down unless recovery is needed.

For each active worker, do all of the following:
1. Confirm only the expected supervisor and extractor are running:
   - pgrep -af "supervisor_qwen32b|run_extraction_concurrent|ollama serve"
2. Inspect the latest supervisor and worker logs:
   - tail -n 40 /var/log/supervisor_qwen32b.log
   - tail -n 40 /var/log/worker_<current-domain>.log if present
3. Confirm live forward progress:
   - wc -l /opt/thyroid_extraction/output/note_entities_llm_<current-domain>.ckpt.jsonl
   - look for recent HTTP 200 lines in logs
4. Confirm the queue and prompt wiring make sense:
   - verify the intended prompt files exist under /opt/thyroid_extraction/notes_extraction_new/prompts
   - verify the input parquet exists at /opt/thyroid_extraction/processed/remaining/clinical_notes_long.parquet
5. Check for overlap or accidental duplicate processing across workers.

Then check whether any domains are complete and can be copied back to the repo and pushed to GitHub.

Completion rule:
- treat a domain as complete if its checkpoint has 11037 rows and/or the domain parquet exists and is consistent with the checkpoint.

For any completed domain:
1. Copy the parquet back into the repo under output/v2_parquets/
2. Validate the parquet is readable and appears to contain the expected provenance fields
3. Check git status carefully
4. Stage only the completed-domain artifact(s) and any directly related documentation updates
5. Commit with a focused message
6. Push to the active working branch unless the operator explicitly wants `main`

Also confirm the destroyed A40 remains absent:
- `~/.local/bin/vastai show instances | cat`
- verify instance `33933782` is absent and SSH to `ssh5.vast.ai:13782` fails

End with a comprehensive progress report covering all domains, not just the active ones.

The final report must include:
1. A per-server status table with ssh target, running domain, checkpoint count, and health notes
2. A per-domain status table with one of: complete, in progress, queued/not started, or unknown
3. Identification of any completed domains already copied back and pushed
4. Any overlap, stale supervisors, missing prompts, missing parquet inputs, or provenance regressions found
5. A realistic estimate of which domains are likely to finish next and any risk to overall completion

Be concrete. Use live command evidence, not assumptions.
```

## Useful command patterns for the next chat

### Quick per-worker health check

```bash
ssh -p <PORT> -o StrictHostKeyChecking=no root@<HOST> '
  echo HOST=$(hostname);
  pgrep -af "supervisor_qwen32b|run_extraction_concurrent|ollama serve" || true;
  tail -n 20 /var/log/supervisor_qwen32b.log 2>/dev/null | cat
'
```

### Check a current domain checkpoint

```bash
ssh -p <PORT> -o StrictHostKeyChecking=no root@<HOST> '
  wc -l /opt/thyroid_extraction/output/note_entities_llm_<domain>.ckpt.jsonl 2>/dev/null || true;
  tail -n 20 /var/log/worker_<domain>.log 2>/dev/null | cat
'
```

### Check whether a final parquet exists

```bash
ssh -p <PORT> -o StrictHostKeyChecking=no root@<HOST> '
  ls -lh /opt/thyroid_extraction/output/note_entities_llm_<domain>.parquet 2>/dev/null || true
'
```

### Copy a completed parquet back into the repo

```bash
cd /Users/ros/THyroid\ 2026/THYROID_2026
scp -P <PORT> -o StrictHostKeyChecking=no \
  root@<HOST>:/opt/thyroid_extraction/output/note_entities_llm_<domain>.parquet \
  output/v2_parquets/
```

### Validate a copied parquet locally

```bash
cd /Users/ros/THyroid\ 2026/THYROID_2026
.venv/bin/python - <<'PY'
import pandas as pd
path = 'output/v2_parquets/note_entities_llm_<domain>.parquet'
df = pd.read_parquet(path)
print(path, len(df), sorted(set(df.columns) & {
    'research_id', 'note_row_id', 'note_type', 'note_date', 'linkage_date',
    'source_workbook', 'source_sheet', 'source_column', 'preprocessed_at_utc'
}))
print(df.head(3).to_dict(orient='records'))
PY
```

### Focused git push flow for completed domains

```bash
cd /Users/ros/THyroid\ 2026/THYROID_2026
git status --short
git add output/v2_parquets/note_entities_llm_<domain>.parquet docs/<optional-doc-update>.md
git commit -m "Archive completed <domain> extraction artifact"
git push origin <active-branch>
```

## Notes

- The older file `docs/vastai_extraction_fleet_2026-04-01.md` has now been updated to reflect the post-remediation five-worker fleet plus A40 destruction.
- Worker C originally started once with a fallback prompt because prompt files had not yet been copied. That was corrected and the worker was relaunched.
- Worker C received the `survival_followup` checkpoint transferred from the A40 before the A40 was retired.
- H200 and worker D originally ran direct extractors without a supervising queue. That has now been corrected; both are running under `supervisor_qwen32b.sh` with lock files and should roll to the next domain automatically.

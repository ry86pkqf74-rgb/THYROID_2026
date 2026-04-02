# VastAI New Chat Handoff — 2026-04-01

Use the prompt below to start a fresh chat that begins with a direct fleet audit, validates active extraction, checks for completed domains that can be copied back and pushed, verifies provenance and traceability, and ends with a full progress report.

## Current operational state

This is the latest known good state after recovering the interrupted scale-out, repairing queue rollover, launching the final additional H200 lane (`thyroid2026-v2-h200-h2`), trimming H200 F down to `survival_followup` only, and destroying the retired A40 instance.

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
- Intended queue: `recurrence_detailed medication_management dynamic_risk_response presenting_symptoms past_medical_hx rad_treatment`
- Last verified active domain: `recurrence_detailed`
- Last verified checkpoint count: `4,836`

### 2. H200 F
- Instance ID: `33939816`
- SSH: `ssh -p 19816 -o StrictHostKeyChecking=no root@ssh9.vast.ai`
- Intended queue: `survival_followup`
- Last verified active domain: `survival_followup`
- Last verified checkpoint count: `2,852`
- Important note: this host was intentionally trimmed to a single-domain run after the final H200 was added, so it should not advance into `airway_invasion` or the rest of the former tail queue.

### 3. H200 G
- Instance ID: `33964874`
- SSH: `ssh -p 14874 -o StrictHostKeyChecking=no root@ssh5.vast.ai`
- Intended queue: `functional_outcomes patient_decision_adherence past_surgical_hx operative_details complications`
- Last verified active domain: `functional_outcomes`
- Last verified checkpoint count: `913`

### 4. H200 H2
- Instance ID: `33968613`
- Label: `thyroid2026-v2-h200-h2`
- SSH: `ssh -p 18612 -o StrictHostKeyChecking=no root@ssh9.vast.ai`
- Intended queue: `airway_invasion tg_kinetics parathyroid_detail frozen_section_detail us_nodule_dynamics cervical_ln_detail complications_rln_laryngoscopy molecular_thyroseq_afirma synoptic_pathology_enrichment`
- Last verified active domain: `airway_invasion`
- Last verified checkpoint count: `156`
- Important note: this is the final added H200 lane. It was bootstrapped with the repo-tracked runtime and is currently the only host that should advance into the former H200 F tail queue.

### 5. Fast worker A
- Instance ID: `33935193`
- SSH: `ssh -p 15192 -o StrictHostKeyChecking=no root@ssh8.vast.ai`
- Intended queue: `vascular_invasion`
- Last verified active domain: `vascular_invasion`
- Last verified checkpoint count: `3,976`

### 6. Fast worker B
- Instance ID: `33935507`
- SSH: `ssh -p 15506 -o StrictHostKeyChecking=no root@ssh6.vast.ai`
- Intended queue: `rai_detailed`
- Last verified active domain: `rai_detailed`
- Last verified checkpoint count: `5,038`
- Important note: a stale launcher shell was cleaned up earlier; the active extractor remains healthy.

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
- Checkpoint: `/opt/thyroid_extraction/processed/output/note_entities_llm_<domain>.ckpt.jsonl`
- Final parquet: `/opt/thyroid_extraction/processed/output/note_entities_llm_<domain>.parquet`
- Input parquet: `/opt/thyroid_extraction/processed/remaining/clinical_notes_long.parquet`
- Root input parquet on H200 H2 bootstrap path: `/opt/thyroid_extraction/clinical_notes_long.parquet`
- Prompt directory: `/opt/thyroid_extraction/llm_extraction/prompts`

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
- intended queue: recurrence_detailed medication_management dynamic_risk_response presenting_symptoms past_medical_hx rad_treatment
- last known active domain: recurrence_detailed

2. H200 F
- ssh -p 19816 -o StrictHostKeyChecking=no root@ssh9.vast.ai
- intended queue: survival_followup only
- last known active domain: survival_followup

3. H200 G
- ssh -p 14874 -o StrictHostKeyChecking=no root@ssh5.vast.ai
- intended queue: functional_outcomes patient_decision_adherence past_surgical_hx operative_details complications
- last known active domain: functional_outcomes

4. H200 H2
- ssh -p 18612 -o StrictHostKeyChecking=no root@ssh9.vast.ai
- intended queue: airway_invasion tg_kinetics parathyroid_detail frozen_section_detail us_nodule_dynamics cervical_ln_detail complications_rln_laryngoscopy molecular_thyroseq_afirma synoptic_pathology_enrichment
- last known active domain: airway_invasion

5. Fast worker A
- ssh -p 15192 -o StrictHostKeyChecking=no root@ssh8.vast.ai
- intended queue: vascular_invasion
- last known active domain: vascular_invasion

6. Fast worker B
- ssh -p 15506 -o StrictHostKeyChecking=no root@ssh6.vast.ai
- intended queue: rai_detailed
- last known active domain: rai_detailed

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
   - wc -l /opt/thyroid_extraction/processed/output/note_entities_llm_<current-domain>.ckpt.jsonl
   - look for recent HTTP 200 lines in logs
  - if the worker is meant to be a single-domain host, confirm its queue/env does not still include former tail domains
4. Confirm the queue and prompt wiring make sense:
   - verify the intended prompt files exist under /opt/thyroid_extraction/llm_extraction/prompts
   - verify the input parquet exists at /opt/thyroid_extraction/processed/remaining/clinical_notes_long.parquet
5. Check for overlap or accidental duplicate processing across workers.

Then validate provenance and traceability for both copied artifacts and downstream schema expectations.

6. For every completed domain parquet already copied back into the repo, confirm row-level provenance:
  - require columns: research_id, note_row_id, note_type, note_date, linkage_date, source_workbook, source_sheet, source_column, preprocessed_at_utc, result_json
  - join the parquet back to processed/remaining/clinical_notes_long.parquet on note_row_id and confirm research_id, note_date, source_workbook, source_sheet, and source_column still match the original note row
7. Validate that requested datapoints are not being lost inside free-form JSON only:
  - inspect the domain prompt file
  - list the expected datapoints from the prompt
  - compare those datapoints to the downstream explicit schema / data dictionary / analytic tables
  - flag any requested datapoint that is only trapped in result_json and does not map to a dedicated downstream column keyed by research_id or note_row_id

Then check whether any domains are complete and can be copied back to the repo and pushed to GitHub.

Completion rule:
- treat a domain as complete if its checkpoint has 11037 rows and/or the domain parquet exists and is consistent with the checkpoint.

For any completed domain:
1. Copy the parquet back into the repo under processed/output/v2_parquets/
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
4. Any overlap, stale supervisors, missing prompts, missing parquet inputs, provenance regressions, or missing downstream column mappings found
5. A table of provenance checks showing whether copied artifacts still match the original note row by note_row_id and research_id
6. A prompt-to-schema audit showing which requested datapoints already have dedicated downstream columns and which do not
7. A realistic estimate of which domains are likely to finish next and any risk to overall completion

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
  wc -l /opt/thyroid_extraction/processed/output/note_entities_llm_<domain>.ckpt.jsonl 2>/dev/null || true;
  tail -n 20 /var/log/worker_<domain>.log 2>/dev/null | cat
'
```

### Check whether a final parquet exists

```bash
ssh -p <PORT> -o StrictHostKeyChecking=no root@<HOST> '
  ls -lh /opt/thyroid_extraction/processed/output/note_entities_llm_<domain>.parquet 2>/dev/null || true
'
```

### Copy a completed parquet back into the repo

```bash
cd /Users/ros/THyroid\ 2026/THYROID_2026
scp -P <PORT> -o StrictHostKeyChecking=no \
  root@<HOST>:/opt/thyroid_extraction/processed/output/note_entities_llm_<domain>.parquet \
  processed/output/v2_parquets/
```

### Validate a copied parquet locally

```bash
cd /Users/ros/THyroid\ 2026/THYROID_2026
.venv/bin/python - <<'PY'
import pandas as pd
path = 'processed/output/v2_parquets/note_entities_llm_<domain>.parquet'
df = pd.read_parquet(path)
print(path, len(df), sorted(set(df.columns) & {
    'research_id', 'note_row_id', 'note_type', 'note_date', 'linkage_date',
    'source_workbook', 'source_sheet', 'source_column', 'preprocessed_at_utc'
}))
print(df.head(3).to_dict(orient='records'))
PY
```

### Validate copied parquet rows against original note provenance

```bash
cd /Users/ros/THyroid\ 2026/THYROID_2026
.venv/bin/python - <<'PY'
import pandas as pd

domain = '<domain>'
artifact = pd.read_parquet(f'processed/output/v2_parquets/note_entities_llm_{domain}.parquet')
source = pd.read_parquet(
  'processed/remaining/clinical_notes_long.parquet',
  columns=['note_row_id', 'research_id', 'note_date', 'source_workbook', 'source_sheet', 'source_column']
)
merged = artifact.merge(source, on='note_row_id', how='left', suffixes=('_artifact', '_source'))
for column in ['research_id', 'note_date', 'source_workbook', 'source_sheet', 'source_column']:
  mismatches = (
    merged[f'{column}_artifact'].fillna('').astype(str)
    != merged[f'{column}_source'].fillna('').astype(str)
  ).sum()
  print(column, int(mismatches))
print('rows', len(artifact), 'unique note_row_id', artifact['note_row_id'].astype(str).nunique())
PY
```

### Prompt-to-schema traceability check

```bash
cd /Users/ros/THyroid\ 2026/THYROID_2026
.venv/bin/python - <<'PY'
from pathlib import Path

domain = '<domain>'
prompt_path = Path('llm_extraction/prompts') / f'{domain}_extraction_v1.txt'
print(prompt_path.read_text()[:4000])
print('\nNext step: compare the prompt fields above against the explicit downstream columns for this domain and flag any field that is only present inside result_json.')
PY
```

### Focused git push flow for completed domains

```bash
cd /Users/ros/THyroid\ 2026/THYROID_2026
git status --short
git add processed/output/v2_parquets/note_entities_llm_<domain>.parquet docs/<optional-doc-update>.md
git commit -m "Archive completed <domain> extraction artifact"
git push origin <active-branch>
```

## Notes

- The older file `docs/vastai_extraction_fleet_2026-04-01.md` has now been updated to reflect the current six-lane fleet plus the final added H200.
- H200 H2 is the only host that should advance into `airway_invasion`, `tg_kinetics`, `parathyroid_detail`, `frozen_section_detail`, `us_nodule_dynamics`, `cervical_ln_detail`, `complications_rln_laryngoscopy`, `molecular_thyroseq_afirma`, and `synoptic_pathology_enrichment`.
- H200 F should remain on `survival_followup` only after the H200 H2 split.
- The next chat should treat provenance validation and prompt-to-schema traceability as first-class audit tasks, not optional spot checks.

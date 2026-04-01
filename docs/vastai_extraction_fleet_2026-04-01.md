# VastAI Extraction Fleet Status 2026-04-01

Snapshot taken on 2026-04-01 during the active V2 thyroid note extraction run.

## Server access

All workers expose Ollama locally at `http://localhost:11434/v1`.

### Primary H200
- SSH: `ssh -p 43384 -o StrictHostKeyChecking=no root@107.206.71.138`
- Vast proxy: `ssh -p 14710 -o StrictHostKeyChecking=no root@ssh1.vast.ai`
- Active queue: `physical_exam tirads_granular parathyroid_per_gland operative_v2_enrichment dynamic_risk_response presenting_symptoms past_medical_hx rad_treatment`
- Active domain at snapshot: `tirads_granular`

### Fast worker A
- SSH: `ssh -p 15192 -o StrictHostKeyChecking=no root@ssh8.vast.ai`
- Active queue: `vascular_invasion`
- Active domain at snapshot: `vascular_invasion`

### Fast worker B
- SSH: `ssh -p 15506 -o StrictHostKeyChecking=no root@ssh6.vast.ai`
- Active queue: `rai_detailed recurrence_detailed medication_management tg_kinetics parathyroid_detail airway_invasion frozen_section_detail us_nodule_dynamics cervical_ln_detail complications_rln_laryngoscopy molecular_thyroseq_afirma synoptic_pathology_enrichment`
- Active domain at snapshot: `rai_detailed`

### A40 worker
- SSH: `ssh -p 13782 -o StrictHostKeyChecking=no root@ssh5.vast.ai`
- Active queue: `survival_followup`
- Active domain at snapshot: `survival_followup`
- Operational note: this worker was timing out repeatedly at concurrency 3. It was restarted on 2026-04-01 05:13 UTC-equivalent local session time with `EXTRACTION_CONCURRENCY=1`, after which it resumed forward progress.

## Progress snapshot

Input corpus size for each domain: 11,037 notes.

| Server | Domain | Rows complete | Status | Notes |
| --- | --- | ---: | --- | --- |
| Primary H200 | `tirads_granular` | 3,807 | Active | Provenance present and positive entity payloads verified |
| Fast worker A | `vascular_invasion` | 197 | Active | Provenance present |
| Fast worker B | `rai_detailed` | 66 | Active | Provenance present |
| A40 worker | `survival_followup` | 71 | Active after restart | Concurrency reduced from 3 to 1 to clear timeout loop |

## Completed domains archived locally

- `physical_exam` completed on the primary H200 and was copied into the repo at `output/v2_parquets/note_entities_llm_physical_exam.parquet`.

## Overlap check

No domain overlap was present at the time of audit.

- Primary H200 current domain: `tirads_granular`
- Fast worker A current domain: `vascular_invasion`
- Fast worker B current domain: `rai_detailed`
- A40 current domain: `survival_followup`

Queued domains are also non-overlapping across servers.

## Output validation notes

- All sampled rows included provenance fields such as `research_id`, `source_workbook`, and `linkage_date` when running on the current patched runtime.
- Positive extraction payloads were confirmed on the primary H200 for `tirads_granular` and for the completed `physical_exam` domain.
- First sampled rows on several domains were valid empty negatives (`{"entities": []}`), which is expected for notes without domain-specific findings.

## Stale process check

- Primary H200: one supervisor, one extraction worker, one Ollama server.
- Fast worker A: one supervisor, one extraction worker, one Ollama server.
- Fast worker B: one supervisor, one extraction worker, one Ollama server.
- A40 worker: one supervisor, one extraction worker, one Ollama server after restart.

No concurrent duplicate extractors were running for the same domain at the time of this snapshot.
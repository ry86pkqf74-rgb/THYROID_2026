# VastAI Extraction Fleet Status 2026-04-01

Post-remediation snapshot taken on 2026-04-01 after direct live audit, queue rollover repair, A40 retirement/destruction, and same-day H200 queue rebalancing.

## Server access

All workers expose Ollama locally at `http://localhost:11434/v1`.

### Primary H200
- SSH: `ssh -p 43384 -o StrictHostKeyChecking=no root@107.206.71.138`
- Vast proxy: `ssh -p 14710 -o StrictHostKeyChecking=no root@ssh1.vast.ai`
- Active queue after recovery: `staging recurrence_detailed medication_management dynamic_risk_response presenting_symptoms past_medical_hx rad_treatment`
- Active domain after recovery: `staging`
- Operational note: this host had completed its earlier three-domain queue and briefly went idle. It was recovered through the direct SSH endpoint, resumed from the existing `staging` checkpoint, and then rebalanced to absorb the A/B follow-on backlog so it will not go idle after `staging` completes.

### High-throughput H200 G
- Vast instance ID: `33964874`
- SSH: `ssh -p 14874 -o StrictHostKeyChecking=no root@ssh5.vast.ai`
- Active queue: `functional_outcomes patient_decision_adherence past_surgical_hx operative_details complications`
- Active domain at bring-up: `functional_outcomes`
- Runtime profile: `OLLAMA_NUM_PARALLEL=6`, `EXTRACTION_CONCURRENCY=6`, `MODEL=qwen3:32b`
- Operational note: this host was added on 2026-04-01 as a higher-throughput H200 lane using the repo-tracked VastAI runtime. Bootstrap issues were fixed in sequence (missing Python packages, misplaced parquet input symlink), after which the node came up cleanly under one supervisor plus one worker with live HTTP 200 inference traffic, nonzero checkpoint growth, and ~82 GB VRAM in use.

### Fast worker A
- SSH: `ssh -p 15192 -o StrictHostKeyChecking=no root@ssh8.vast.ai`
- Active queue after rebalance: `vascular_invasion`
- Active domain at snapshot: `vascular_invasion`

### Fast worker B
- SSH: `ssh -p 15506 -o StrictHostKeyChecking=no root@ssh6.vast.ai`
- Active queue after rebalance: `rai_detailed`
- Active domain at snapshot: `rai_detailed`
- Operational note: a stale launcher shell was cleaned up. The real supervisor and extractor remained healthy.

### Fast worker C
- SSH: `ssh -p 17332 -o StrictHostKeyChecking=no root@ssh4.vast.ai`
- Active queue: `survival_followup tg_kinetics parathyroid_detail`
- Active domain at snapshot: `survival_followup`
- Operational note: this worker replaced the A40 after the `survival_followup` checkpoint handoff. A stale launcher shell was cleaned up. The real supervisor and extractor remained healthy.

### Fast worker D
- SSH: `ssh -p 17332 -o StrictHostKeyChecking=no root@ssh6.vast.ai`
- Active queue: `airway_invasion frozen_section_detail us_nodule_dynamics cervical_ln_detail complications_rln_laryngoscopy molecular_thyroseq_afirma synoptic_pathology_enrichment`
- Active domain at snapshot: `airway_invasion`
- Operational note: this host had been launched directly under the extractor and would not have rolled to the next queue item. It was relaunched under `supervisor_qwen32b.sh`, lock file recreated, and automatic rollover is now restored.

### Retired A40
- Former instance ID: `33933782`
- Former SSH: `ssh -p 13782 -o StrictHostKeyChecking=no root@ssh5.vast.ai`
- Final disposition: destroyed after verifying its local `survival_followup` checkpoint rows were fully contained on worker C and that its remaining tarball only wrapped the same checkpoint.

## Progress snapshot

Input corpus size for each domain: 11,037 notes.

| Server | Domain | Rows complete | Status | Notes |
| --- | --- | ---: | --- | --- |
| Primary H200 | `tirads_granular` | 5,655 | Active | Running under supervisor with lock file; fresh HTTP 200 traffic after relaunch |
| Fast worker A | `vascular_invasion` | 286 | Active | Running under supervisor with non-overlapping 5-domain queue |
| Fast worker B | `rai_detailed` | 167 | Active | Running under supervisor after stale wrapper cleanup |
| Fast worker C | `survival_followup` | 193 | Active | Running under supervisor after A40 checkpoint handoff |
| Fast worker D | `airway_invasion` | 162 | Active | Running under supervisor with lock file; resumed from checkpoint after relaunch |

## Completed domains archived locally

- `physical_exam` completed on the primary H200 and was copied into the repo at `output/v2_parquets/note_entities_llm_physical_exam.parquet`.
- `operative_v2_enrichment` completed on the primary H200, validated locally with 11,037 rows plus provenance/date fields, and was copied into `output/v2_parquets/note_entities_llm_operative_v2_enrichment.parquet`.
- `parathyroid_per_gland` completed on the primary H200, validated locally with 11,037 rows plus provenance/date fields, and was copied into `output/v2_parquets/note_entities_llm_parathyroid_per_gland.parquet`.
- `tirads_granular` completed on the primary H200, validated locally with 11,037 rows plus provenance/date fields, and was copied into `output/v2_parquets/note_entities_llm_tirads_granular.parquet`.

## Current queue distribution after H200 rebalance

- Primary H200 carries the bulk post-`staging` queue: `recurrence_detailed`, `medication_management`, `dynamic_risk_response`, `presenting_symptoms`, `past_medical_hx`, `rad_treatment`.
- H200 F carries the other bulk queue: `survival_followup`, `airway_invasion`, `tg_kinetics`, `parathyroid_detail`, `frozen_section_detail`, `us_nodule_dynamics`, `cervical_ln_detail`, `complications_rln_laryngoscopy`, `molecular_thyroseq_afirma`, `synoptic_pathology_enrichment`.
- H200 G carries an additional non-overlapping backlog queue: `functional_outcomes`, `patient_decision_adherence`, `past_surgical_hx`, `operative_details`, `complications`.
- Worker A is intentionally reduced to `vascular_invasion` only.
- Worker B is intentionally reduced to `rai_detailed` only.

## Overlap check

No domain overlap was present at the time of audit.

- Primary H200 current domain: `tirads_granular`
- Fast worker A current domain: `vascular_invasion`
- Fast worker B current domain: `rai_detailed`
- Fast worker C current domain: `survival_followup`
- Fast worker D current domain: `airway_invasion`

Queued domains are also non-overlapping across servers.

## Output validation notes

- All sampled rows included provenance fields such as `research_id`, `source_workbook`, and `linkage_date` when running on the current patched runtime.
- Positive extraction payloads were confirmed on the primary H200 for `tirads_granular` and for the completed `physical_exam` domain.
- First sampled rows on several domains were valid empty negatives (`{"entities": []}`), which is expected for notes without domain-specific findings.

## Stale process check

- Primary H200: one supervisor, one extraction worker, one Ollama server.
- Fast worker A: one supervisor, one extraction worker, one Ollama server.
- Fast worker B: one supervisor, one extraction worker, one Ollama server.
- Fast worker C: one supervisor, one extraction worker, one Ollama server.
- Fast worker D: one supervisor, one extraction worker, one Ollama server.
- A40: no remaining extraction or Ollama processes; instance destroyed.

No concurrent duplicate extractors were running for the same domain at the time of this snapshot.
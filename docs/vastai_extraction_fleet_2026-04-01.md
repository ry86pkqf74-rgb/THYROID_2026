# VastAI Extraction Fleet Status 2026-04-01

Post-remediation snapshot taken on 2026-04-01 after direct live audit, queue rollover repair, A40 retirement/destruction, same-day H200 rebalancing, and launch of the final additional H200 lane (`thyroid2026-v2-h200-h2`).

## Server access

All workers expose Ollama locally at `http://localhost:11434/v1`.

### Primary H200
- Vast instance ID: `33534710`
- SSH: `ssh -p 43384 -o StrictHostKeyChecking=no root@107.206.71.138`
- Vast proxy: `ssh -p 14710 -o StrictHostKeyChecking=no root@ssh1.vast.ai`
- Active queue after rebalance: `recurrence_detailed medication_management dynamic_risk_response presenting_symptoms past_medical_hx rad_treatment`
- Active domain at latest check: `recurrence_detailed`
- Latest verified checkpoint count: `4,836`
- Operational note: this host absorbed the main follow-on backlog after earlier queue recovery and remains the fastest active lane.

### H200 F
- Vast instance ID: `33939816`
- SSH: `ssh -p 19816 -o StrictHostKeyChecking=no root@ssh9.vast.ai`
- Active queue after H200 H2 split: `survival_followup`
- Active domain at latest check: `survival_followup`
- Latest verified checkpoint count: `2,852`
- Operational note: this host originally carried a long tail queue. After `thyroid2026-v2-h200-h2` was launched, H200 F was intentionally trimmed down to `survival_followup` only so the new H200 could absorb the remaining tail domains without future overlap.

### High-throughput H200 G
- Vast instance ID: `33964874`
- SSH: `ssh -p 14874 -o StrictHostKeyChecking=no root@ssh5.vast.ai`
- Active queue: `functional_outcomes patient_decision_adherence past_surgical_hx operative_details complications`
- Active domain at latest check: `functional_outcomes`
- Latest verified checkpoint count: `913`
- Runtime profile: `OLLAMA_NUM_PARALLEL=6`, `EXTRACTION_CONCURRENCY=6`, `MODEL=qwen3:32b`
- Operational note: this host was added on 2026-04-01 as a higher-throughput H200 lane using the repo-tracked VastAI runtime. Bootstrap issues were fixed in sequence (missing Python packages, misplaced parquet input symlink), after which the node came up cleanly under one supervisor plus one worker with live HTTP 200 inference traffic, nonzero checkpoint growth, and ~82 GB VRAM in use.

### H200 H2
- Vast instance ID: `33968613`
- Label: `thyroid2026-v2-h200-h2`
- SSH: `ssh -p 18612 -o StrictHostKeyChecking=no root@ssh9.vast.ai`
- Active queue: `airway_invasion tg_kinetics parathyroid_detail frozen_section_detail us_nodule_dynamics cervical_ln_detail complications_rln_laryngoscopy molecular_thyroseq_afirma synoptic_pathology_enrichment`
- Active domain at latest check: `airway_invasion`
- Latest verified checkpoint count: `156`
- Runtime profile: `OLLAMA_NUM_PARALLEL=6`, `EXTRACTION_CONCURRENCY=6`, `MODEL=qwen3:32b`
- Operational note: this was the final additional H200 launched to shorten the slow H200 F critical path. It required explicit bootstrap fixes for Ollama persistence, the root input parquet path (`/opt/thyroid_extraction/clinical_notes_long.parquet`), and a missing `tenacity` package. It is now stable with live HTTP 200 inference traffic, active checkpoint growth, and ~82 GB VRAM in use.

### Fast worker A
- SSH: `ssh -p 15192 -o StrictHostKeyChecking=no root@ssh8.vast.ai`
- Active queue after rebalance: `vascular_invasion`
- Active domain at latest check: `vascular_invasion`
- Latest verified checkpoint count: `3,976`

### Fast worker B
- SSH: `ssh -p 15506 -o StrictHostKeyChecking=no root@ssh6.vast.ai`
- Active queue after rebalance: `rai_detailed`
- Active domain at latest check: `rai_detailed`
- Latest verified checkpoint count: `5,038`
- Operational note: a stale launcher shell was cleaned up. The real supervisor and extractor remained healthy.

### Retired A40
- Former instance ID: `33933782`
- Former SSH: `ssh -p 13782 -o StrictHostKeyChecking=no root@ssh5.vast.ai`
- Final disposition: destroyed after verifying its local `survival_followup` checkpoint rows were fully contained on worker C and that its remaining tarball only wrapped the same checkpoint.

## Progress snapshot

Input corpus size for each domain: 11,037 notes.

| Server | Domain | Rows complete | Status | Notes |
| --- | --- | ---: | --- | --- |
| Primary H200 | `recurrence_detailed` | 4,836 | Active | Fresh HTTP 200 traffic observed; still the fastest lane |
| H200 F | `survival_followup` | 2,852 | Active | Trimmed to a single-domain queue after H200 H2 launch |
| H200 G | `functional_outcomes` | 913 | Active | H200 lane remains saturated with non-overlapping backlog |
| H200 H2 | `airway_invasion` | 156 | Active | New final H200 lane; live HTTP 200 traffic and growing checkpoint |
| Fast worker A | `vascular_invasion` | 3,976 | Active | Single-domain worker; live HTTP 200 traffic observed |
| Fast worker B | `rai_detailed` | 5,038 | Active | Single-domain worker; live HTTP 200 traffic observed |

## Completed domains archived locally

- `physical_exam` completed on the primary H200 and was copied into the repo at `processed/output/v2_parquets/note_entities_llm_physical_exam.parquet`.
- `operative_v2_enrichment` completed on the primary H200, validated locally with 11,037 rows plus provenance/date fields, and was copied into `processed/output/v2_parquets/note_entities_llm_operative_v2_enrichment.parquet`.
- `parathyroid_per_gland` completed on the primary H200, validated locally with 11,037 rows plus provenance/date fields, and was copied into `processed/output/v2_parquets/note_entities_llm_parathyroid_per_gland.parquet`.
- `tirads_granular` completed on the primary H200, validated locally with 11,037 rows plus provenance/date fields, and was copied into `processed/output/v2_parquets/note_entities_llm_tirads_granular.parquet`.

## Current queue distribution after final H200 launch

- Primary H200 carries the main remaining bulk queue: `recurrence_detailed`, `medication_management`, `dynamic_risk_response`, `presenting_symptoms`, `past_medical_hx`, `rad_treatment`.
- H200 F is intentionally reduced to `survival_followup` only.
- H200 G carries `functional_outcomes`, `patient_decision_adherence`, `past_surgical_hx`, `operative_details`, `complications`.
- H200 H2 carries the former H200 F tail queue: `airway_invasion`, `tg_kinetics`, `parathyroid_detail`, `frozen_section_detail`, `us_nodule_dynamics`, `cervical_ln_detail`, `complications_rln_laryngoscopy`, `molecular_thyroseq_afirma`, `synoptic_pathology_enrichment`.
- Worker A is intentionally reduced to `vascular_invasion` only.
- Worker B is intentionally reduced to `rai_detailed` only.

## Overlap check

No current domain overlap was present at the time of the latest audit.

- Primary H200 current domain: `recurrence_detailed`
- H200 F current domain: `survival_followup`
- H200 G current domain: `functional_outcomes`
- H200 H2 current domain: `airway_invasion`
- Fast worker A current domain: `vascular_invasion`
- Fast worker B current domain: `rai_detailed`

Queued domains are also intentionally non-overlapping after the H200 F tail queue was moved onto H200 H2.

## Output validation notes

- All sampled rows included provenance fields such as `research_id`, `source_workbook`, and `linkage_date` when running on the current patched runtime.
- The core row-level traceability columns to require on copied parquet artifacts are: `research_id`, `note_row_id`, `note_type`, `note_date`, `linkage_date`, `source_workbook`, `source_sheet`, `source_column`, `preprocessed_at_utc`, and `result_json`.
- Each completed artifact should be cross-checked back to `processed/remaining/clinical_notes_long.parquet` using `note_row_id` to confirm the extraction row still points to the original source note, source workbook/sheet/column, and note date.
- The next audit should explicitly compare the prompt-defined fields for each domain with the downstream schema expectations and flag any requested datapoint that exists only in free-form JSON but has no dedicated downstream column mapping.
- Positive extraction payloads were confirmed on the primary H200 for `tirads_granular` and for the completed `physical_exam` domain.
- First sampled rows on several domains were valid empty negatives (`{"entities": []}`), which is expected for notes without domain-specific findings.

## Stale process check

- Primary H200: one supervisor, one extraction worker, one Ollama server.
- H200 F: active `survival_followup` extractor plus Ollama; queue intentionally reduced to a single domain after H200 H2 launch.
- H200 G: one supervisor, one extraction worker, one Ollama server.
- H200 H2: one supervisor, one extraction worker, one Ollama server.
- Fast worker A: one supervisor, one extraction worker, one Ollama server.
- Fast worker B: one supervisor, one extraction worker, one Ollama server.
- A40: no remaining extraction or Ollama processes; instance destroyed.

No concurrent duplicate extractors were running for the same domain at the time of this snapshot.
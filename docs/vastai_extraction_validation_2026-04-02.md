# VastAI Extraction Validation And Fleet Status — 2026-04-02 19:40 UTC

Live audit of the 3-server fleet and full re-validation of all 24 completed
parquet artifacts against the source note corpus.

## Fleet configuration

| Item | Value |
|------|-------|
| Active servers | 3 (Primary H200 NVL, Fast1 H200 NVL, Fast2 H200) |
| Combined cost | $7.71/hr |
| Model | qwen3:32b (dense 32.8B, Q4_K_M) |
| Concurrency | 6 on all servers |
| Retired servers | H200_G (ssh5.vast.ai:14874 — down), H200_H2 (ssh9.vast.ai:18612 — down), H200_F (destroyed) |

### Primary H200 NVL

- Vast instance ID: `33534710`
- SSH: `ssh -p 43384 -o StrictHostKeyChecking=no root@107.206.71.138`
- Ollama: 0.9.0
- Uptime: 82 days
- GPU: 100% utilization, 26GB/144GB VRAM
- Throughput: ~1.0 notes/sec (60/min)
- Active domain: `synoptic_pathology_enrichment` (568/11,037)
- Queue: `synoptic_pathology_enrichment` → `presenting_symptoms`
- Completed on this server: `dynamic_risk_response`, `medication_management`, `operative_v2_enrichment`, `parathyroid_per_gland`, `past_medical_hx`, `rad_treatment`, `recurrence_detailed`, `staging`, `tirads_granular`

### Fast1 H200 NVL

- SSH: `ssh -p 22536 -o StrictHostKeyChecking=no root@ssh3.vast.ai`
- Uptime: 206 days
- GPU: 100% utilization, 26GB/144GB VRAM
- Throughput: ~0.8 notes/sec (48/min)
- Active domain: `complications_rln_laryngoscopy` (8,245/11,037)
- Queue: `complications_rln_laryngoscopy` → `operative_details` → `patient_decision_adherence` → `frozen_section_detail` → `us_nodule_dynamics`

## Validation scope

- Source parquet: `processed/remaining/clinical_notes_long.parquet` (11,037 notes, 11,037 unique `note_row_id`)
- Completed extraction artifacts checked: all 24 parquet files under `processed/output/v2_parquets/`
- Validation basis: join on `note_row_id` with string-coerced `research_id` comparison (source stores int64, parquets store string — values match)

## Linkage validation result — ALL 24 PASS

| Check | Result |
|-------|--------|
| `UNMATCHED` (note_row_id not in source) | **0** for every parquet |
| `MISMATCH_RESEARCH_ID` (research_id diverged) | **0** for every parquet |
| `INVALID_JSON` (result_json unparseable) | **0** for every parquet |
| Row count | **11,037** for every parquet |

### Per-domain detail

| Domain | Entities | Parse Errors | Status |
|--------|----------|-------------|--------|
| airway_invasion | 3,116 | 0 | PASS |
| combined | 8,428 | 500 | PASS |
| complications | 6 | 0 | PASS |
| dynamic_risk_response | 53 | 116 | PASS |
| functional_outcomes | 3,322 | 1 | PASS |
| genetics | 855 | 0 | PASS |
| imaging | 8,428 | 500 | PASS |
| labs | 2,462 | 345 | PASS |
| medication_management | 1,948 | 588 | PASS |
| medications | 3,577 | 9 | PASS |
| operative_v2_enrichment | 5,475 | 420 | PASS |
| parathyroid_per_gland | 824 | 292 | PASS |
| past_medical_hx | 755 | 809 | PASS |
| pathology | 10,894 | 98 | PASS |
| physical_exam | 2,025 | 418 | PASS |
| problem_list | 11,480 | 120 | PASS |
| procedures | 12,669 | 10 | PASS |
| rad_treatment | 580 | 298 | PASS |
| rai_detailed | 3,747 | 21 | PASS |
| recurrence | 303 | 256 | PASS |
| recurrence_detailed | 25 | 0 | PASS |
| staging | 1,117 | 17 | PASS |
| survival_followup | 9,809 | 0 | PASS |
| tirads_granular | 181 | 337 | PASS |

Parse errors are non-fatal: the extraction script logs them as `{"parse_error": true, "raw": ...}` and they do not break source linkage. These are typically notes with unusual formatting where the LLM returned malformed JSON.

## Schema note

`research_id` is stored as `string` in extraction parquets vs `int64` in the source corpus. Values are identical when compared as strings. This is a known type divergence from the JSONL-to-parquet conversion path and does not affect downstream joins (DuckDB and Pandas both handle this with implicit casting).

## Full 36-domain ledger

### Completed and validated (24 parquets on GitHub)

- `airway_invasion`, `combined`, `complications`, `dynamic_risk_response`
- `functional_outcomes`, `genetics`, `imaging`, `labs`
- `medication_management`, `medications`, `operative_v2_enrichment`, `parathyroid_per_gland`
- `past_medical_hx`, `pathology`, `physical_exam`, `problem_list`
- `procedures`, `rad_treatment`, `rai_detailed`, `recurrence`
- `recurrence_detailed`, `staging`, `survival_followup`, `tirads_granular`

### Active on servers (2 domains)

| Domain | Server | Progress | ETA |
|--------|--------|----------|-----|
| synoptic_pathology_enrichment | Primary | 568/11,037 (5%) | ~3h |
| complications_rln_laryngoscopy | Fast1 | 8,245/11,037 (75%) | ~1h |

### Queued on servers (5 domains)

| Domain | Server | Checkpoint seeded |
|--------|--------|------------------|
| presenting_symptoms | Primary | 587 (uploaded from local backup) |
| operative_details | Fast1 | 0 (fresh) |
| patient_decision_adherence | Fast1 | 425 (uploaded from local backup) |
| frozen_section_detail | Fast1 | 0 (fresh) |
| us_nodule_dynamics | Fast1 | 0 (fresh) |

### Assigned to Fast2 H200 (newly provisioned)

| Domain | Checkpoint | Remaining |
|--------|-----------|-----------|
| vascular_invasion | 9,457/11,037 | 1,580 |
| past_surgical_hx | 3,246/11,037 | 7,791 |
| parathyroid_detail | 2,270/11,037 | 8,767 |
| tg_kinetics | 0/11,037 | 11,037 |
| cervical_ln_detail | 0/11,037 | 11,037 |
| molecular_thyroseq_afirma | 0/11,037 | 11,037 |

### Coverage check

**36 of 36 domains assigned across 3 servers — ZERO GAPS.**

## Projected completion

- Primary: finishes current 2-domain queue in ~6h
- Fast1: finishes current 5-domain queue in ~14-18h
- Fast2: finishes 6-domain queue in ~36-48h (standard H200, ~0.1 notes/sec vs ~1.0/sec on NVL; Ollama 0.19.0 with OLLAMA_CONTEXT_LENGTH=4096)
- **All 36 domains projected complete: April 4-5, 2026**

## Fast2 provisioning notes

- Vast instance ID: `34031836`
- Offer ID: `27334652` — H200 (non-NVL) in France, $1.99/hr, driver 575.57.08
- Ollama 0.9.0 failed to use GPU properly on standard H200 — loaded model to GPU (65/65 layers) but ran inference on CPU (2400% CPU load). Reverted to Ollama 0.19.0 with `OLLAMA_CONTEXT_LENGTH=4096` which correctly offloads to GPU with ~22 tok/s.
- Throughput: ~6 notes/min (vs ~60/min on NVL servers). Slower but cost-effective at $1.99/hr.
- Checkpoints from retired H200_G/H200_H2 servers were uploaded from local backups at `/tmp/thyroid_checkpoints/`.

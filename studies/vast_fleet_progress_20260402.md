# Vast Fleet Progress — 2026-04-02

## Live status (19:00 UTC)

Fleet expanded to 3 servers: 2 H200 NVL (Ollama 0.9.0) + 1 standard H200
(Ollama 0.19.0 with OLLAMA_CONTEXT_LENGTH=4096). Combined cost $7.71/hr.
Previous servers H200_G, H200_H2, and H200_F have been retired. All their
checkpoint progress was backed up locally before retirement and redistributed
to the active fleet.

Ollama version note: 0.9.0 runs 6-8x faster on H200 NVL hardware (nvidia
driver >= 575). On standard H200, 0.9.0 incorrectly runs inference on CPU
despite loading model to GPU — use 0.19.0 with `OLLAMA_CONTEXT_LENGTH=4096`
as workaround.

## Per-server matrix

| Server | SSH target | Ollama | Active domain | Progress | Throughput | Queue depth |
| --- | --- | --- | --- | ---: | --- | --- |
| Primary H200 NVL | `ssh -p 43384 root@107.206.71.138` | 0.9.0 | `synoptic_pathology_enrichment` | 568 / 11,037 | 1.0 notes/sec | 2 |
| Fast1 H200 NVL | `ssh -p 22536 root@ssh3.vast.ai` | 0.9.0 | `complications_rln_laryngoscopy` | 8,245 / 11,037 | 0.8 notes/sec | 5 |
| Fast2 H200 | `ssh -p 31836 root@ssh7.vast.ai` | 0.19.0 | `vascular_invasion` | 9,457 / 11,037 | 0.1 notes/sec | 6 |

## Domain progress snapshot (36 domains)

### Completed and validated — 24 parquets on GitHub

| Domain | Entities | Validated |
| --- | ---: | --- |
| airway_invasion | 3,116 | PASS |
| combined | 8,428 | PASS |
| complications | 6 | PASS |
| dynamic_risk_response | 53 | PASS |
| functional_outcomes | 3,322 | PASS |
| genetics | 855 | PASS |
| imaging | 8,428 | PASS |
| labs | 2,462 | PASS |
| medication_management | 1,948 | PASS |
| medications | 3,577 | PASS |
| operative_v2_enrichment | 5,475 | PASS |
| parathyroid_per_gland | 824 | PASS |
| past_medical_hx | 755 | PASS |
| pathology | 10,894 | PASS |
| physical_exam | 2,025 | PASS |
| problem_list | 11,480 | PASS |
| procedures | 12,669 | PASS |
| rad_treatment | 580 | PASS |
| rai_detailed | 3,747 | PASS |
| recurrence | 303 | PASS |
| recurrence_detailed | 25 | PASS |
| staging | 1,117 | PASS |
| survival_followup | 9,809 | PASS |
| tirads_granular | 181 | PASS |

### Active extraction — 2 domains

| Domain | Server | Progress | ETA |
| --- | --- | ---: | --- |
| synoptic_pathology_enrichment | Primary | 568 / 11,037 | ~3h |
| complications_rln_laryngoscopy | Fast1 | 8,245 / 11,037 | ~1h |

### Queued on servers — 5 domains

| Domain | Server | Seeded checkpoint |
| --- | --- | ---: |
| presenting_symptoms | Primary | 587 |
| operative_details | Fast1 | 0 |
| patient_decision_adherence | Fast1 | 425 |
| frozen_section_detail | Fast1 | 0 |
| us_nodule_dynamics | Fast1 | 0 |

### Assigned to Fast2 H200 — 6 domains

| Domain | Checkpoint | Remaining |
| --- | ---: | ---: |
| vascular_invasion | 9,457 | 1,580 |
| past_surgical_hx | 3,246 | 7,791 |
| parathyroid_detail | 2,270 | 8,767 |
| tg_kinetics | 0 | 11,037 |
| cervical_ln_detail | 0 | 11,037 |
| molecular_thyroseq_afirma | 0 | 11,037 |

Checkpoints uploaded from local backups at `/tmp/thyroid_checkpoints/`.

## Coverage audit

**36 of 36 extraction domains assigned — zero gaps.**

## Estimated completion

- Primary NVL: current 2-domain queue ~6h
- Fast1 NVL: current 5-domain queue ~14-18h
- Fast2: current 6-domain queue ~36-48h (standard H200, lower throughput)
- **All 36 domains projected complete: April 4-5, 2026**

## Retired servers

| Server | SSH | Status | Disposition |
| --- | --- | --- | --- |
| H200_G | ssh5.vast.ai:14874 | Connection refused | Checkpoints backed up and redistributed to Fast2 |
| H200_H2 | ssh9.vast.ai:18612 | Connection refused | Checkpoints backed up and redistributed to Fast2 |
| H200_F | (destroyed) | Destroyed | survival_followup artifacts + patient_decision_adherence checkpoint saved |
| Fast worker A | ssh8.vast.ai:15192 | Retired | vascular_invasion checkpoint saved and uploaded to Fast2 |
| Fast worker B | ssh6.vast.ai:15506 | Retired | presenting_symptoms checkpoint saved |

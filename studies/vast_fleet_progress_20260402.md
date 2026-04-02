# Vast Fleet Progress — 2026-04-02

## Live status (19:00 UTC)

Fleet consolidated to 2 H200 NVL servers running Ollama 0.9.0 (6-8x faster
than 0.19.0; requires nvidia driver >= 575). Combined cost $5.72/hr.
Previous servers H200_G, H200_H2, and H200_F have been retired (connection
refused or destroyed). All their checkpoint progress was backed up locally
before retirement.

Root cause of prior throughput issues: Ollama 0.19.0 KV cache bloat. Mitigated
by `OLLAMA_CONTEXT_LENGTH=4096` on 0.19.0, but Ollama 0.9.0 on H200 NVL
hardware eliminates the issue entirely.

## Per-server matrix

| Server | SSH target | Ollama | Active domain | Progress | Throughput | Queue depth |
| --- | --- | --- | --- | ---: | --- | --- |
| Primary H200 | `ssh -p 43384 root@107.206.71.138` | 0.9.0 | `synoptic_pathology_enrichment` | 568 / 11,037 | 1.0 notes/sec | 2 (+ 6 awaiting upload) |
| Fast1 H200 NVL | `ssh -p 22536 root@ssh3.vast.ai` | 0.9.0 | `complications_rln_laryngoscopy` | 8,245 / 11,037 | 0.8 notes/sec | 5 |

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

### Awaiting upload to Primary — 6 domains

| Domain | Local checkpoint | Remaining |
| --- | ---: | ---: |
| vascular_invasion | 9,424 | 1,613 |
| past_surgical_hx | 3,246 | 7,791 |
| parathyroid_detail | 2,270 | 8,767 |
| tg_kinetics | 681 | 10,356 |
| cervical_ln_detail | 0 | 11,037 |
| molecular_thyroseq_afirma | 0 | 11,037 |

Local checkpoints at `/tmp/thyroid_checkpoints/` on Mac. Upload to Primary
after its current 2-domain queue clears (~6h).

## Coverage audit

**36 of 36 extraction domains assigned — zero gaps.**

## Estimated completion

- Primary: current queue ~6h, then 6 upload domains ~18-24h
- Fast1: current 5-domain queue ~14-18h
- **All 36 domains projected complete: April 3-4, 2026**

## Retired servers

| Server | SSH | Status | Disposition |
| --- | --- | --- | --- |
| H200_G | ssh5.vast.ai:14874 | Connection refused | Checkpoints backed up locally |
| H200_H2 | ssh9.vast.ai:18612 | Connection refused | Checkpoints backed up locally |
| H200_F | (destroyed) | Destroyed | survival_followup artifacts + patient_decision_adherence checkpoint saved |
| Fast worker A | ssh8.vast.ai:15192 | Retired | vascular_invasion checkpoint saved |
| Fast worker B | ssh6.vast.ai:15506 | Retired | presenting_symptoms checkpoint saved |

# VastAI Extraction Validation And Fleet Status — 2026-04-02 (updated 21:45 UTC)

Live audit of the 3-server fleet and full re-validation of all completed
parquet artifacts against the source note corpus.

> **See `vastai_fleet_handoff_2026-04-02.md` for the current handoff document
> including SSH commands, restart procedures, and per-domain sync workflow.**

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

## Full 36-domain ledger (updated 21:45 UTC Apr 2)

### Completed and validated (26 parquets on GitHub)

- `airway_invasion`, `combined`, `complications`, `complications_rln_laryngoscopy`, `dynamic_risk_response`
- `functional_outcomes`, `genetics`, `imaging`, `labs`
- `medication_management`, `medications`, `operative_v2_enrichment`, `parathyroid_per_gland`
- `past_medical_hx`, `pathology`, `physical_exam`, `problem_list`
- `procedures`, `rad_treatment`, `rai_detailed`, `recurrence`
- `recurrence_detailed`, `staging`, `survival_followup`, `tirads_granular`
- `vascular_invasion`

Added this session: `complications_rln_laryngoscopy` (Fast1), `vascular_invasion` (NewFast2 instance 34034310)

### Active on servers (10 domains)

| Domain | Server | Progress ~21:45 UTC | Rate | ETA |
|--------|--------|---------------------|------|-----|
| synoptic_pathology_enrichment | Primary | ~7,260/11,037 | 42/min | ~23:05 UTC |
| presenting_symptoms | Primary | 587/11,037 (queued) | 42/min | ~Apr 3 03:20 UTC |
| tg_kinetics | Primary | 0/11,037 (queued) | 42/min | ~Apr 3 07:40 UTC |
| operative_details | Fast1 | ~4,790/11,037 | 52/min | ~Apr 3 00:00 UTC |
| patient_decision_adherence | Fast1 | 425/11,037 (queued) | 52/min | ~Apr 3 03:30 UTC |
| frozen_section_detail | Fast1 | 0/11,037 (queued) | 52/min | ~Apr 3 07:00 UTC |
| us_nodule_dynamics | Fast1 | 0/11,037 (queued) | 52/min | ~Apr 3 10:30 UTC |
| cervical_ln_detail | Fast1 | 0/11,037 (queued) | 52/min | ~Apr 3 14:00 UTC |
| molecular_thyroseq_afirma | Fast1 | 0/11,037 (queued) | 52/min | ~Apr 3 17:30 UTC |
| past_surgical_hx | NewFast2 | ~5,460/11,037 | 40/min | ~Apr 3 00:20 UTC |
| parathyroid_detail | NewFast2 | 2,270/11,037 (queued) | 40/min | ~Apr 3 07:40 UTC |

### Coverage check

**36 of 36 domains assigned — ZERO GAPS.**

## Projected completion (updated)

- Primary: synoptic → presenting_symptoms → tg_kinetics, done ~Apr 3 07:40 UTC
- Fast1: operative_details → … → molecular_thyroseq_afirma, **done ~Apr 3 17:30 UTC** ← bottleneck
- NewFast2: past_surgical_hx → parathyroid_detail, done ~Apr 3 07:40 UTC → **destroy after**
- **All 36 domains complete: ~April 3, 2026 17:30 UTC (~1:30 PM ET)**

## Fleet changes this session (Apr 2, 2026)

- **Old Fast2 (34031836) destroyed** — H200 (non-NVL), France, Ollama 0.19.0, only 10 notes/min
- **NewFast2 (34034310) provisioned** — H200 NVL, Czechia, Ollama 0.9.0, 40-51 notes/min
- **NewFast2 queue trimmed** — `tg_kinetics`, `cervical_ln_detail`, `molecular_thyroseq_afirma` moved to Primary and Fast1 respectively
- **Supervisor bug fixed** — `filter_completed_domains()` log() redirect to stderr on Fast1 and NewFast2

# Vast Fleet Progress — 2026-04-02

## Live status

All 6 active Vast workers were re-audited live after the primary H200 failure was traced to a malformed `entity_date` payload in `scripts/vastai/run_extraction_concurrent.py`.

The runtime was patched in-repo and synced to the primary worker. The supervisor wrapper on the primary host did not stay alive, so the active lane was restored by launching the runtime directly. The primary lane resumed `dynamic_risk_response`, advanced from 1,932 to 2,013 checkpoint rows during verification, and worker logs returned to live `HTTP 200` traffic.

## Per-server matrix

| Server | SSH target | Active domain checked | Checkpoint rows | Live evidence | Health note |
| --- | --- | --- | ---: | --- | --- |
| Primary H200 | `ssh -p 43384 root@107.206.71.138` | `dynamic_risk_response` | 2,667 / 11,037 | direct runtime process on host | Recovered after date-normalization hotfix sync; supervisor bypassed |
| H200 F | `ssh -p 19816 root@ssh9.vast.ai` | `survival_followup` | 9,621 / 11,037 | supervisor + runtime process present | Healthy single-domain lane |
| H200 G | `ssh -p 14874 root@ssh5.vast.ai` | `functional_outcomes` | 10,504 / 11,037 | supervisor + runtime process present | Healthy; near completion |
| H200 H2 | `ssh -p 18612 root@ssh9.vast.ai` | `airway_invasion` | 9,849 / 11,037 | supervisor + runtime process present | Healthy; near completion |
| Fast worker A | `ssh -p 15192 root@ssh8.vast.ai` | `vascular_invasion` | 8,169 / 11,037 | supervisor + runtime process present | Healthy single-domain lane |
| Fast worker B | `ssh -p 15506 root@ssh6.vast.ai` | `presenting_symptoms` | 255 / 11,037 | supervisor + runtime process present | Healthy; intentionally reassigned from completed `rai_detailed` |

## Domain progress snapshot

| Domain | Status | Evidence |
| --- | --- | --- |
| `operative_v2_enrichment` | complete | local parquet present in `output/v2_parquets/` |
| `parathyroid_per_gland` | complete | local parquet present in `output/v2_parquets/` |
| `tirads_granular` | complete | local parquet present in `output/v2_parquets/` |
| `physical_exam` | complete | local parquet present in `output/v2_parquets/` |
| `rai_detailed` | complete at row-count level, not promoted in this session | prior Fast B checkpoint completed; current lane moved off domain |
| `dynamic_risk_response` | in progress | primary H200 at 2,667 / 11,037 |
| `survival_followup` | in progress | H200 F at 9,621 / 11,037 |
| `functional_outcomes` | in progress | H200 G at 10,504 / 11,037 |
| `airway_invasion` | in progress | H200 H2 at 9,849 / 11,037 |
| `vascular_invasion` | in progress | Fast worker A at 8,169 / 11,037 |
| `presenting_symptoms` | in progress | Fast worker B at 255 / 11,037 |
| `past_medical_hx` | pre-started, not active | primary host checkpoint exists at 24 rows |
| `rad_treatment` | pre-started, not active | primary host checkpoint exists at 10 rows |
| `patient_decision_adherence` | queued and unstarted | no checkpoint file on H200 G |
| `past_surgical_hx` | queued and unstarted | no checkpoint file on H200 G |
| `operative_details` | queued and unstarted | no checkpoint file on H200 G |
| `complications` | queued and unstarted | no checkpoint file on H200 G |
| `tg_kinetics` | queued and unstarted | no checkpoint file on H200 H2 |
| `parathyroid_detail` | queued and unstarted | no checkpoint file on H200 H2 |
| `frozen_section_detail` | queued and unstarted | no checkpoint file on H200 H2 |
| `us_nodule_dynamics` | queued and unstarted | no checkpoint file on H200 H2 |
| `cervical_ln_detail` | queued and unstarted | no checkpoint file on H200 H2 |
| `complications_rln_laryngoscopy` | queued and unstarted | no checkpoint file on H200 H2 |
| `molecular_thyroseq_afirma` | queued and unstarted | no checkpoint file on H200 H2 |
| `synoptic_pathology_enrichment` | queued and unstarted | no checkpoint file on H200 H2 |

## Notes

- Primary root cause fixed: `_normalize_iso_date(...)` in `scripts/vastai/run_extraction_concurrent.py` now safely handles mapping/list payloads instead of passing them straight into `pandas.to_datetime(...)`.
- The fleet is currently non-overlapping by active domain.
- Fast worker B is now the active `presenting_symptoms` lane; `rai_detailed` should stay off the active queue unless a semantic cleanup rerun is explicitly required.
- The primary host currently runs the active domain directly instead of under `supervisor_qwen32b.sh`.
- The primary host's two downstream intended domains are not pristine: `past_medical_hx` already has 24 checkpoint rows and `rad_treatment` already has 10.
- All queued follow-on domains behind H200 G and H200 H2 remain unstarted at this snapshot.

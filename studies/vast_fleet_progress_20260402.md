# Vast Fleet Progress — 2026-04-02

## Live status

All 6 active Vast workers were re-audited live after the primary H200 failure was traced to a malformed `entity_date` payload in `scripts/vastai/run_extraction_concurrent.py`.

The runtime was patched in-repo and synced to the primary worker. The primary lane resumed `dynamic_risk_response` and worker logs returned to live `HTTP 200` traffic.

## Per-server matrix

| Server | SSH target | Active domain checked | Checkpoint rows | Live evidence | Health note |
| --- | --- | --- | ---: | --- | --- |
| Primary H200 | `ssh -p 43384 root@107.206.71.138` | `dynamic_risk_response` | 1,932 | `HTTP 200` lines in `worker_dynamic_risk_response.log` at 07:58 | Recovered after date-normalization hotfix sync |
| H200 F | `ssh -p 19816 root@ssh9.vast.ai` | `survival_followup` | 9,411 | `HTTP 200` lines at 08:19–08:20 | Healthy single-domain lane |
| H200 G | `ssh -p 14874 root@ssh5.vast.ai` | `functional_outcomes` | 10,260 | `HTTP 200` lines at 08:19–08:20 | Healthy; near completion |
| H200 H2 | `ssh -p 18612 root@ssh9.vast.ai` | `airway_invasion` | 9,637 | `HTTP 200` lines at 08:19–08:20 | Healthy; near completion |
| Fast worker A | `ssh -p 15192 root@ssh8.vast.ai` | `vascular_invasion` | 8,078 | `HTTP 200` lines at 08:19–08:20 | Healthy single-domain lane |
| Fast worker B | `ssh -p 15506 root@ssh6.vast.ai` | `presenting_symptoms` | 152 | `HTTP 200` lines and progress line `150/11037` at 08:20 | Healthy; intentionally reassigned from completed `rai_detailed` |

## Domain progress snapshot

| Domain | Status | Evidence |
| --- | --- | --- |
| `operative_v2_enrichment` | complete | local parquet present in `output/v2_parquets/` |
| `parathyroid_per_gland` | complete | local parquet present in `output/v2_parquets/` |
| `tirads_granular` | complete | local parquet present in `output/v2_parquets/` |
| `physical_exam` | complete | local parquet present in `output/v2_parquets/` |
| `rai_detailed` | complete at row-count level, not promoted in this session | prior Fast B checkpoint completed; current lane moved off domain |
| `dynamic_risk_response` | in progress | primary H200 at 1,932 rows with live traffic |
| `survival_followup` | in progress | H200 F at 9,411 rows with live traffic |
| `functional_outcomes` | in progress | H200 G at 10,260 rows with live traffic |
| `airway_invasion` | in progress | H200 H2 at 9,637 rows with live traffic |
| `vascular_invasion` | in progress | Fast worker A at 8,078 rows with live traffic |
| `presenting_symptoms` | in progress | Fast worker B at 152 rows with live traffic |
| `past_medical_hx` | queued behind primary active domain | queue tail on primary lane |
| `rad_treatment` | queued behind primary active domain | queue tail on primary lane |
| `patient_decision_adherence` | queued behind H200 G active domain | same lane as `functional_outcomes` |
| `past_surgical_hx` | queued behind H200 G active domain | same lane as `functional_outcomes` |
| `operative_details` | queued behind H200 G active domain | same lane as `functional_outcomes` |
| `complications` | queued behind H200 G active domain | same lane as `functional_outcomes` |
| `tg_kinetics` | queued behind H200 H2 active domain | same lane as `airway_invasion` |
| `parathyroid_detail` | queued behind H200 H2 active domain | same lane as `airway_invasion` |
| `frozen_section_detail` | queued behind H200 H2 active domain | same lane as `airway_invasion` |
| `us_nodule_dynamics` | queued behind H200 H2 active domain | same lane as `airway_invasion` |
| `cervical_ln_detail` | queued behind H200 H2 active domain | same lane as `airway_invasion` |
| `complications_rln_laryngoscopy` | queued behind H200 H2 active domain | same lane as `airway_invasion` |
| `molecular_thyroseq_afirma` | queued behind H200 H2 active domain | same lane as `airway_invasion` |
| `synoptic_pathology_enrichment` | queued behind H200 H2 active domain | same lane as `airway_invasion` |

## Notes

- Primary root cause fixed: `_normalize_iso_date(...)` in `scripts/vastai/run_extraction_concurrent.py` now safely handles mapping/list payloads instead of passing them straight into `pandas.to_datetime(...)`.
- The fleet is currently non-overlapping by active domain.
- Fast worker B is now the active `presenting_symptoms` lane; `rai_detailed` should stay off the active queue unless a semantic cleanup rerun is explicitly required.

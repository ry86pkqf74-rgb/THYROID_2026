# mig_243 Snake Case Aliases VIEW v1 Closeout — 2026-05-01

Migration: `qc_framework_v1/migrations/243_snake_case_aliases_VIEW_v1_20260501.sql`

Target view: `semantic_publication.vw_snake_case_aliases_VIEW_v1`

Batch id: `mig_243_snake_case_aliases`

Verified by: `cline_gpt_5_5_mig_243`

## Alias Map

| Base object | Nonstandard column | Snake case alias | mig_243 disposition |
|---|---|---|---|
| `main.canonical_airway_invasion_patient_rollup_v1` | `any_pT4a_direct` | `any_pt4a_direct` | Included as live patient-grain alias |
| `main.canonical_airway_invasion_patient_rollup_v1` | `any_pT4a_final` | `any_pt4a_final` | Included as live patient-grain alias |
| `main.canonical_airway_invasion_patient_rollup_v1` | `n_pT4a_events` | `n_pt4a_events` | Included as live patient-grain alias |
| `main.canonical_ete_subgrade_patient_rollup_v1` | `any_pT3b` | `any_pt3b` | Included as live patient-grain alias |
| `main.canonical_ete_subgrade_patient_rollup_v1` | `any_pT4a` | `any_pt4a` | Included as live patient-grain alias |
| `main.canonical_ete_subgrade_patient_rollup_v1` | `any_pT4b` | `any_pt4b` | Included as live patient-grain alias |
| `main.canonical_ete_subgrade_patient_rollup_v1` | `any_pT4b_from_t4b_invasion` | `any_pt4b_from_t4b_invasion` | Included as live patient-grain alias |
| `main.canonical_ete_subgrade_patient_rollup_v1` | `pT4b_ete_vs_t4b_invasion_discordant` | `pt4b_ete_vs_t4b_invasion_discordant` | Included as live patient-grain alias |
| `main.canonical_invasion_patient_rollup_v1` | `any_pT4a_final_anywhere` | `any_pt4a_final_anywhere` | Included as typed `NULL` compatibility alias; live source column was previously dropped and registry marks it `deprecated_dropped_from_live` by mig_209 |
| `main.canonical_invasion_patient_rollup_v1` | `any_pT4b_final_anywhere` | `any_pt4b_final_anywhere` | Included as typed `NULL` compatibility alias; live source column was previously dropped and registry marks it `deprecated_dropped_from_live` by mig_209 |
| `main.canonical_parathyroid_events_v1` | `intact_pth_value_ngL` | `intact_pth_value_ng_l` | Deferred: event-grain column, not flattened into patient-grain alias view |
| `main.canonical_parathyroid_patient_rollup_v1` | `max_intact_pth_value_ngL` | `max_intact_pth_value_ng_l` | Included as live patient-grain alias |
| `main.canonical_parathyroid_patient_rollup_v1` | `min_intact_pth_value_ngL` | `min_intact_pth_value_ng_l` | Included as live patient-grain alias |
| `main.canonical_patient_master` | `ajcc8_t_stage_with_microete_t3b_DEPRECATED` | `ajcc8_t_stage_with_microete_t3b_deprecated` | Included as live patient-grain alias |
| `main.canonical_t4b_invasion_patient_rollup_v1` | `any_pT4b_direct` | `any_pt4b_direct` | Included as live patient-grain alias |
| `main.canonical_t4b_invasion_patient_rollup_v1` | `any_pT4b_final` | `any_pt4b_final` | Included as live patient-grain alias |
| `main.canonical_t4b_invasion_patient_rollup_v1` | `n_pT4b_events` | `n_pt4b_events` | Included as live patient-grain alias |

## Notes

- The view is patient-grain via `main.canonical_patient_master`, with `research_id` cast to `VARCHAR` per mig_239 semantic-publication convention.
- `canonical_parathyroid_events_v1.intact_pth_value_ngL` was deferred because it is event-grain; flattening it into a one-row-per-patient view would change the source grain.
- The two `canonical_invasion_patient_rollup_v1.any_pT4*_final_anywhere` columns were listed in the v17 prompt but are absent from the live table on 2026-05-01; registry rows show `deprecated_dropped_from_live` from mig_209. mig_243 therefore does not read archived stale copies.
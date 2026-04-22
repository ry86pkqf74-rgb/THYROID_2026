# Script 363 invasion breakdown — 2026-04-22
BUILD_TS: `20260422_032954`  cohort_size (canonical_patient_master): 10,871

Per-(source_modality × source_kind × invasion_type × finding_status) counts. **Compare 'present' percentages against Logan's clinical realism table** (~6.2% vascular, ~7.2% lymphatic, ~5.4% ETE, ~0.9% perineural) before signing off on the cascade strip.

| modality | source_kind | invasion_type | finding_status | n_mentions | n_patients | % cohort |
|---|---|---|---|---:|---:|---:|
| `ct` | `llm` | `airway` | `absent` | 10 | 9 | 0.08% |
| `ct` | `llm` | `airway` | `indeterminate` | 74 | 48 | 0.44% |
| `ct` | `llm` | `airway` | `suspected` | 9 | 7 | 0.06% |
| `ct` | `llm` | `gross_ete` | `absent` | 122 | 111 | 1.02% |
| `ct` | `llm` | `gross_ete` | `indeterminate` | 135 | 100 | 0.92% |
| `ct` | `llm` | `gross_ete` | `present` | 107 | 90 | 0.83% |
| `ct` | `llm` | `gross_ete` | `suspected` | 18 | 17 | 0.16% |
| `ct` | `llm` | `microscopic_ete` | `indeterminate` | 1 | 1 | 0.01% |
| `ct` | `llm` | `microscopic_ete` | `suspected` | 1 | 1 | 0.01% |
| `mri` | `llm` | `airway` | `absent` | 2 | 2 | 0.02% |
| `mri` | `llm` | `airway` | `indeterminate` | 4 | 4 | 0.04% |
| `mri` | `llm` | `gross_ete` | `absent` | 3 | 3 | 0.03% |
| `mri` | `llm` | `gross_ete` | `indeterminate` | 9 | 8 | 0.07% |
| `mri` | `llm` | `gross_ete` | `present` | 6 | 6 | 0.06% |
| `mri` | `llm` | `gross_ete` | `suspected` | 1 | 1 | 0.01% |
| `op_note` | `llm` | `airway` | `absent` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `airway` | `indeterminate` | 24 | 18 | 0.17% |
| `op_note` | `llm` | `capsular` | `absent` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `capsular` | `present` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `gross_ete` | `absent` | 33 | 30 | 0.28% |
| `op_note` | `llm` | `gross_ete` | `indeterminate` | 40 | 26 | 0.24% |
| `op_note` | `llm` | `gross_ete` | `present` | 19 | 15 | 0.14% |
| `op_note` | `llm` | `gross_ete` | `suspected` | 1 | 1 | 0.01% |
| `op_note` | `llm` | `perineural` | `indeterminate` | 1 | 1 | 0.01% |
| `op_note` | `llm` | `perineural` | `present` | 2 | 1 | 0.01% |
| `op_note` | `llm` | `soft_tissue` | `absent` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `soft_tissue` | `indeterminate` | 25 | 14 | 0.13% |
| `op_note` | `llm` | `vascular_microscopic` | `absent` | 1 | 1 | 0.01% |
| `op_note` | `llm` | `vascular_microscopic` | `present` | 10 | 6 | 0.06% |
| `op_note` | `structured` | `esophageal` | `absent` | 11,704 | 10,814 | 99.48% |
| `op_note` | `structured` | `esophageal` | `present` | 69 | 69 | 0.63% |
| `op_note` | `structured` | `gross_ete` | `present` | 28 | 28 | 0.26% |
| `op_note` | `structured` | `soft_tissue` | `present` | 29 | 29 | 0.27% |
| `op_note` | `structured` | `tracheal` | `present` | 14 | 14 | 0.13% |
| `synoptic_path` | `llm` | `airway` | `absent` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `airway` | `indeterminate` | 3 | 3 | 0.03% |
| `synoptic_path` | `llm` | `airway` | `present` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `capsular` | `absent` | 1,109 | 1,022 | 9.40% |
| `synoptic_path` | `llm` | `capsular` | `indeterminate` | 240 | 233 | 2.14% |
| `synoptic_path` | `llm` | `capsular` | `present` | 982 | 719 | 6.61% |
| `synoptic_path` | `llm` | `capsular` | `suspected` | 19 | 18 | 0.17% |
| `synoptic_path` | `llm` | `gross_ete` | `absent` | 4 | 4 | 0.04% |
| `synoptic_path` | `llm` | `gross_ete` | `indeterminate` | 8 | 7 | 0.06% |
| `synoptic_path` | `llm` | `gross_ete` | `present` | 7 | 7 | 0.06% |
| `synoptic_path` | `llm` | `gross_ete` | `suspected` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `microscopic_ete` | `indeterminate` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `perineural` | `absent` | 1,654 | 1,408 | 12.95% |
| `synoptic_path` | `llm` | `perineural` | `indeterminate` | 9 | 9 | 0.08% |
| `synoptic_path` | `llm` | `perineural` | `present` | 200 | 115 | 1.06% |
| `synoptic_path` | `llm` | `soft_tissue` | `absent` | 3,530 | 2,657 | 24.44% |
| `synoptic_path` | `llm` | `soft_tissue` | `indeterminate` | 663 | 489 | 4.50% |
| `synoptic_path` | `llm` | `soft_tissue` | `present` | 561 | 473 | 4.35% |
| `synoptic_path` | `llm` | `soft_tissue` | `suspected` | 11 | 9 | 0.08% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `absent` | 5,182 | 3,470 | 31.92% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `indeterminate` | 62 | 56 | 0.52% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `present` | 1,877 | 1,095 | 10.07% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `suspected` | 33 | 27 | 0.25% |
| `synoptic_path` | `structured` | `capsular` | `absent` | 705 | 487 | 4.48% |
| `synoptic_path` | `structured` | `capsular` | `indeterminate` | 54 | 35 | 0.32% |
| `synoptic_path` | `structured` | `capsular` | `present` | 1,147 | 793 | 7.29% |
| `synoptic_path` | `structured` | `capsular` | `suspected` | 4 | 4 | 0.04% |
| `synoptic_path` | `structured` | `gross_ete` | `absent` | 5,275 | 3,528 | 32.45% |
| `synoptic_path` | `structured` | `gross_ete` | `indeterminate` | 64 | 43 | 0.40% |
| `synoptic_path` | `structured` | `gross_ete` | `present` | 2,022 | 1,077 | 9.91% |
| `synoptic_path` | `structured` | `lymphatic_microscopic` | `absent` | 4,002 | 2,701 | 24.85% |
| `synoptic_path` | `structured` | `lymphatic_microscopic` | `indeterminate` | 115 | 66 | 0.61% |
| `synoptic_path` | `structured` | `lymphatic_microscopic` | `present` | 1,233 | 780 | 7.18% |
| `synoptic_path` | `structured` | `lymphatic_microscopic` | `suspected` | 2 | 2 | 0.02% |
| `synoptic_path` | `structured` | `microscopic_ete` | `present` | 454 | 279 | 2.57% |
| `synoptic_path` | `structured` | `perineural` | `absent` | 2,057 | 1,394 | 12.82% |
| `synoptic_path` | `structured` | `perineural` | `indeterminate` | 3 | 3 | 0.03% |
| `synoptic_path` | `structured` | `perineural` | `present` | 158 | 102 | 0.94% |
| `synoptic_path` | `structured` | `vascular_microscopic` | `absent` | 4,719 | 3,125 | 28.75% |
| `synoptic_path` | `structured` | `vascular_microscopic` | `indeterminate` | 90 | 61 | 0.56% |
| `synoptic_path` | `structured` | `vascular_microscopic` | `present` | 996 | 681 | 6.26% |
| `synoptic_path` | `structured` | `vascular_microscopic` | `suspected` | 1 | 1 | 0.01% |

## Cross-modal `present anywhere` per invasion_type
(consumed by canonical_invasion_patient_rollup_v1.any_<type>_anywhere)

| invasion_type | n_patients_present_anywhere | % cohort |
|---|---:|---:|
| `airway` | 1 | 0.01% |
| `capsular` | 941 | 8.66% |
| `esophageal` | 69 | 0.63% |
| `gross_ete` | 1,146 | 10.54% |
| `lymphatic_microscopic` | 780 | 7.18% |
| `microscopic_ete` | 279 | 2.57% |
| `perineural` | 122 | 1.12% |
| `soft_tissue` | 493 | 4.54% |
| `tracheal` | 14 | 0.13% |
| `vascular_microscopic` | 1,109 | 10.20% |

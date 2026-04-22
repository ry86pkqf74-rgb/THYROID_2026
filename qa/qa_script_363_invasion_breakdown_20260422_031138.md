# Script 363 invasion breakdown — 2026-04-22
BUILD_TS: `20260422_031138`  cohort_size (canonical_patient_master): 10,871

Per-(source_modality × source_kind × invasion_type × finding_status) counts. **Compare 'present' percentages against Logan's clinical realism table** (~6.2% vascular, ~7.2% lymphatic, ~5.4% ETE, ~0.9% perineural) before signing off on the cascade strip.

| modality | source_kind | invasion_type | finding_status | n_mentions | n_patients | % cohort |
|---|---|---|---|---:|---:|---:|
| `ct` | `llm` | `airway` | `absent` | 11 | 10 | 0.09% |
| `ct` | `llm` | `airway` | `present` | 82 | 51 | 0.47% |
| `ct` | `llm` | `gross_ete` | `absent` | 124 | 113 | 1.04% |
| `ct` | `llm` | `gross_ete` | `present` | 258 | 182 | 1.67% |
| `ct` | `llm` | `microscopic_ete` | `present` | 2 | 2 | 0.02% |
| `mri` | `llm` | `airway` | `absent` | 3 | 3 | 0.03% |
| `mri` | `llm` | `airway` | `present` | 3 | 3 | 0.03% |
| `mri` | `llm` | `gross_ete` | `absent` | 3 | 3 | 0.03% |
| `mri` | `llm` | `gross_ete` | `present` | 16 | 14 | 0.13% |
| `op_note` | `llm` | `airway` | `absent` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `airway` | `present` | 24 | 18 | 0.17% |
| `op_note` | `llm` | `capsular` | `absent` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `capsular` | `present` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `gross_ete` | `absent` | 33 | 30 | 0.28% |
| `op_note` | `llm` | `gross_ete` | `present` | 60 | 42 | 0.39% |
| `op_note` | `llm` | `perineural` | `present` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `soft_tissue` | `absent` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `soft_tissue` | `present` | 25 | 14 | 0.13% |
| `op_note` | `llm` | `vascular_microscopic` | `absent` | 1 | 1 | 0.01% |
| `op_note` | `llm` | `vascular_microscopic` | `present` | 10 | 6 | 0.06% |
| `op_note` | `structured` | `esophageal` | `absent` | 11,704 | 10,814 | 99.48% |
| `op_note` | `structured` | `esophageal` | `present` | 69 | 69 | 0.63% |
| `op_note` | `structured` | `gross_ete` | `present` | 28 | 28 | 0.26% |
| `op_note` | `structured` | `soft_tissue` | `present` | 29 | 29 | 0.27% |
| `op_note` | `structured` | `tracheal` | `present` | 14 | 14 | 0.13% |
| `synoptic_path` | `llm` | `airway` | `absent` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `airway` | `present` | 4 | 4 | 0.04% |
| `synoptic_path` | `llm` | `capsular` | `absent` | 1,239 | 1,124 | 10.34% |
| `synoptic_path` | `llm` | `capsular` | `indeterminate` | 13 | 12 | 0.11% |
| `synoptic_path` | `llm` | `capsular` | `present` | 1,098 | 812 | 7.47% |
| `synoptic_path` | `llm` | `gross_ete` | `absent` | 5 | 4 | 0.04% |
| `synoptic_path` | `llm` | `gross_ete` | `present` | 15 | 12 | 0.11% |
| `synoptic_path` | `llm` | `microscopic_ete` | `present` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `perineural` | `absent` | 1,656 | 1,410 | 12.97% |
| `synoptic_path` | `llm` | `perineural` | `indeterminate` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `perineural` | `present` | 206 | 119 | 1.09% |
| `synoptic_path` | `llm` | `soft_tissue` | `absent` | 3,564 | 2,675 | 24.61% |
| `synoptic_path` | `llm` | `soft_tissue` | `indeterminate` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `soft_tissue` | `present` | 1,200 | 675 | 6.21% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `absent` | 5,195 | 3,476 | 31.97% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `indeterminate` | 23 | 22 | 0.20% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `present` | 1,934 | 1,124 | 10.34% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `suspected` | 2 | 2 | 0.02% |
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
| `airway` | 72 | 0.66% |
| `capsular` | 1,030 | 9.47% |
| `esophageal` | 69 | 0.63% |
| `gross_ete` | 1,209 | 11.12% |
| `lymphatic_microscopic` | 780 | 7.18% |
| `microscopic_ete` | 282 | 2.59% |
| `perineural` | 127 | 1.17% |
| `soft_tissue` | 693 | 6.37% |
| `tracheal` | 14 | 0.13% |
| `vascular_microscopic` | 1,138 | 10.47% |

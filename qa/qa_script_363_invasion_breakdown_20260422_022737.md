# Script 363 invasion breakdown — 2026-04-22
BUILD_TS: `20260422_022737`  cohort_size (canonical_patient_master): 10,871

Per-(source_modality × source_kind × invasion_type × finding_status) counts. **Compare 'present' percentages against Logan's clinical realism table** (~6.2% vascular, ~7.2% lymphatic, ~5.4% ETE, ~0.9% perineural) before signing off on the cascade strip.

| modality | source_kind | invasion_type | finding_status | n_mentions | n_patients | % cohort |
|---|---|---|---|---:|---:|---:|
| `ct` | `llm` | `airway` | `absent` | 11 | 10 | 0.09% |
| `ct` | `llm` | `airway` | `present` | 82 | 51 | 0.47% |
| `ct` | `llm` | `esophageal` | `absent` | 42 | 40 | 0.37% |
| `ct` | `llm` | `esophageal` | `present` | 161 | 122 | 1.12% |
| `ct` | `llm` | `gross_ete` | `absent` | 124 | 113 | 1.04% |
| `ct` | `llm` | `gross_ete` | `present` | 260 | 182 | 1.67% |
| `ct` | `llm` | `local` | `absent` | 136 | 120 | 1.10% |
| `ct` | `llm` | `local` | `present` | 3,926 | 1,131 | 10.40% |
| `mri` | `llm` | `airway` | `absent` | 3 | 3 | 0.03% |
| `mri` | `llm` | `airway` | `present` | 3 | 3 | 0.03% |
| `mri` | `llm` | `esophageal` | `absent` | 4 | 3 | 0.03% |
| `mri` | `llm` | `esophageal` | `present` | 6 | 6 | 0.06% |
| `mri` | `llm` | `gross_ete` | `absent` | 3 | 3 | 0.03% |
| `mri` | `llm` | `gross_ete` | `present` | 16 | 14 | 0.13% |
| `mri` | `llm` | `local` | `absent` | 13 | 12 | 0.11% |
| `mri` | `llm` | `local` | `present` | 84 | 46 | 0.42% |
| `narrative_path` | `structured` | `gross_ete` | `absent` | 7,846 | 3,624 | 33.34% |
| `narrative_path` | `structured` | `gross_ete` | `indeterminate` | 85 | 43 | 0.40% |
| `narrative_path` | `structured` | `gross_ete` | `present` | 1,672 | 1,129 | 10.39% |
| `narrative_path` | `structured` | `local` | `absent` | 2,087 | 1,705 | 15.68% |
| `narrative_path` | `structured` | `local` | `indeterminate` | 41 | 38 | 0.35% |
| `narrative_path` | `structured` | `local` | `present` | 937 | 881 | 8.10% |
| `narrative_path` | `structured` | `local` | `suspected` | 4 | 4 | 0.04% |
| `narrative_path` | `structured` | `microscopic_ete` | `present` | 560 | 279 | 2.57% |
| `narrative_path` | `structured` | `vascular_microscopic` | `absent` | 9,983 | 3,282 | 30.19% |
| `narrative_path` | `structured` | `vascular_microscopic` | `indeterminate` | 191 | 104 | 0.96% |
| `narrative_path` | `structured` | `vascular_microscopic` | `present` | 2,234 | 1,169 | 10.75% |
| `narrative_path` | `structured` | `vascular_microscopic` | `suspected` | 4 | 3 | 0.03% |
| `op_note` | `llm` | `airway` | `absent` | 3 | 2 | 0.02% |
| `op_note` | `llm` | `airway` | `present` | 24 | 18 | 0.17% |
| `op_note` | `llm` | `esophageal` | `absent` | 3 | 3 | 0.03% |
| `op_note` | `llm` | `esophageal` | `present` | 25 | 18 | 0.17% |
| `op_note` | `llm` | `gross_ete` | `absent` | 33 | 30 | 0.28% |
| `op_note` | `llm` | `gross_ete` | `present` | 60 | 42 | 0.39% |
| `op_note` | `llm` | `local` | `absent` | 40 | 29 | 0.27% |
| `op_note` | `llm` | `local` | `present` | 572 | 377 | 3.47% |
| `op_note` | `llm` | `vascular_microscopic` | `absent` | 1 | 1 | 0.01% |
| `op_note` | `llm` | `vascular_microscopic` | `present` | 10 | 6 | 0.06% |
| `op_note` | `structured` | `esophageal` | `absent` | 11,704 | 10,814 | 99.48% |
| `op_note` | `structured` | `esophageal` | `present` | 69 | 69 | 0.63% |
| `op_note` | `structured` | `gross_ete` | `present` | 28 | 28 | 0.26% |
| `op_note` | `structured` | `local` | `present` | 29 | 29 | 0.27% |
| `op_note` | `structured` | `tracheal` | `present` | 14 | 14 | 0.13% |
| `synoptic_path` | `llm` | `airway` | `absent` | 1 | 1 | 0.01% |
| `synoptic_path` | `llm` | `airway` | `present` | 4 | 4 | 0.04% |
| `synoptic_path` | `llm` | `esophageal` | `present` | 4 | 4 | 0.04% |
| `synoptic_path` | `llm` | `gross_ete` | `absent` | 5 | 4 | 0.04% |
| `synoptic_path` | `llm` | `gross_ete` | `present` | 16 | 13 | 0.12% |
| `synoptic_path` | `llm` | `local` | `absent` | 6,460 | 3,274 | 30.12% |
| `synoptic_path` | `llm` | `local` | `indeterminate` | 15 | 14 | 0.13% |
| `synoptic_path` | `llm` | `local` | `present` | 2,505 | 1,362 | 12.53% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `absent` | 5,195 | 3,476 | 31.97% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `indeterminate` | 23 | 22 | 0.20% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `present` | 1,934 | 1,124 | 10.34% |
| `synoptic_path` | `llm` | `vascular_microscopic` | `suspected` | 2 | 2 | 0.02% |
| `synoptic_path` | `structured` | `gross_ete` | `absent` | 5,275 | 3,528 | 32.45% |
| `synoptic_path` | `structured` | `gross_ete` | `indeterminate` | 64 | 43 | 0.40% |
| `synoptic_path` | `structured` | `gross_ete` | `present` | 2,022 | 1,077 | 9.91% |
| `synoptic_path` | `structured` | `local` | `absent` | 2,762 | 1,704 | 15.67% |
| `synoptic_path` | `structured` | `local` | `indeterminate` | 57 | 38 | 0.35% |
| `synoptic_path` | `structured` | `local` | `present` | 1,305 | 881 | 8.10% |
| `synoptic_path` | `structured` | `local` | `suspected` | 4 | 4 | 0.04% |
| `synoptic_path` | `structured` | `microscopic_ete` | `present` | 454 | 279 | 2.57% |
| `synoptic_path` | `structured` | `vascular_microscopic` | `absent` | 8,721 | 3,280 | 30.17% |
| `synoptic_path` | `structured` | `vascular_microscopic` | `indeterminate` | 205 | 104 | 0.96% |
| `synoptic_path` | `structured` | `vascular_microscopic` | `present` | 2,229 | 1,168 | 10.74% |
| `synoptic_path` | `structured` | `vascular_microscopic` | `suspected` | 3 | 3 | 0.03% |

## Cross-modal `present anywhere` per invasion_type
(consumed by canonical_invasion_patient_rollup_v1.any_<type>_anywhere)

| invasion_type | n_patients_present_anywhere | % cohort |
|---|---:|---:|
| `airway` | 72 | 0.66% |
| `esophageal` | 185 | 1.70% |
| `gross_ete` | 1,257 | 11.56% |
| `local` | 2,688 | 24.73% |
| `microscopic_ete` | 279 | 2.57% |
| `tracheal` | 14 | 0.13% |
| `vascular_microscopic` | 1,260 | 11.59% |

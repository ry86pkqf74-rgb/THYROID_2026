# Schema / grain inventory

Generated UTC: 2026-04-07T13:26:44.983166+00:00

| table | present | n_rows | n_cols | notes |
|-------|---------|--------|--------|-------|
| us_nodules_tirads | True | 10862 | 180 | see source_profile.csv for full column list |
| serial_imaging_us | False | None | None |  |
| ultrasound_reports | True | 6793 | 1115 | see source_profile.csv for full column list |
| imaging_nodule_master_v1 | True | 19891 | 125 | see source_profile.csv for full column list |
| fna_history | True | 8119 | 50 | see source_profile.csv for full column list |
| fna_cytology | True | 8063 | 135 | see source_profile.csv for full column list |
| molecular_testing | False | None | None |  |
| path_synoptics | True | 11688 | 542 | see source_profile.csv for full column list |
| tumor_pathology | True | 4290 | 1245 | see source_profile.csv for full column list |
| benign_pathology | False | None | None |  |
| operative_details | False | None | None |  |
| thyroid_weights | False | None | None |  |
| tumor_episode_master_v2 | True | 11691 | 185 | see source_profile.csv for full column list |
| molecular_test_episode_v2 | True | 10126 | 210 | see source_profile.csv for full column list |
| operative_episode_detail_v2 | True | 9371 | 195 | see source_profile.csv for full column list |
| fna_episode_master_v2 | True | 8119 | 85 | see source_profile.csv for full column list |
| imaging_fna_linkage_mm_v1 | True | 2804 | 110 | see source_profile.csv for full column list |
| imaging_fna_linkage_v3 | True | 9911 | 40 | see source_profile.csv for full column list |
| fna_molecular_linkage_v3 | True | 838 | 30 | see source_profile.csv for full column list |
| preop_surgery_linkage_v3 | True | 3517 | 32 | see source_profile.csv for full column list |
| surgery_pathology_linkage_v3 | True | 9409 | 32 | see source_profile.csv for full column list |

## Grain expectations

- `imaging_nodule_master_v1`: one row per nodule per ultrasound exam (long).
- `fna_episode_master_v2`: one row per FNA episode (ordered per patient).
- v3 linkages: ranked candidates with `score_rank`; `n_candidates` flags ambiguity.

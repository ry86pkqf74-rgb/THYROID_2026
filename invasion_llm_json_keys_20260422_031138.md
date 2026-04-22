# LLM result_json key probe — Script 363 (2026-04-22)
BUILD_TS: `20260422_031138`

Samples ~30 substantive `result_json` rows per (table × note_type) combo and enumerates the distinct `entity_type` values + `entity_value` shapes. Unmapped `entity_type`s are listed as carry-forward (mapped to NULL invasion_type, then dropped from CTEs).

## `note_entities_llm_airway_invasion`

### note_type=`OPNOTE`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `tracheal_deviation` | 17 | **UNMAPPED→dropped** |
| `substernal_extension` | 16 | **UNMAPPED→dropped** |
| `mass_effect` | 11 | **UNMAPPED→dropped** |
| `ete_on_imaging` | 8 | `gross_ete` |
| `vascular_encasement` | 6 | **UNMAPPED→dropped** |
| `esophageal_compression` | 3 | **UNMAPPED→dropped** |
| `tracheal_narrowing` | 2 | **UNMAPPED→dropped** |
| `laryngeal_invasion` | 2 | `airway` |

### note_type=`ct_imaging`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `tracheal_deviation` | 16 | **UNMAPPED→dropped** |
| `mass_effect` | 13 | **UNMAPPED→dropped** |
| `tracheal_narrowing` | 11 | **UNMAPPED→dropped** |
| `substernal_extension` | 8 | **UNMAPPED→dropped** |
| `ete_on_imaging` | 3 | `gross_ete` |
| `esophageal_compression` | 2 | **UNMAPPED→dropped** |
| `vascular_encasement` | 2 | **UNMAPPED→dropped** |
| `laryngeal_invasion` | 1 | `airway` |
| `airway_compromise_grade` | 1 | **UNMAPPED→dropped** |
| `vocal_cord_imaging` | 1 | **UNMAPPED→dropped** |

### note_type=`mri_imaging`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `vocal_cord_imaging` | 11 | **UNMAPPED→dropped** |
| `mass_effect` | 11 | **UNMAPPED→dropped** |
| `tracheal_narrowing` | 8 | **UNMAPPED→dropped** |
| `tracheal_deviation` | 7 | **UNMAPPED→dropped** |
| `vascular_encasement` | 4 | **UNMAPPED→dropped** |
| `substernal_extension` | 3 | **UNMAPPED→dropped** |
| `ete_on_imaging` | 3 | `gross_ete` |
| `airway_compromise_grade` | 3 | **UNMAPPED→dropped** |
| `laryngeal_invasion` | 1 | `airway` |
| `esophageal_compression` | 1 | **UNMAPPED→dropped** |

### note_type=`path_synoptics`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `vascular_encasement` | 23 | **UNMAPPED→dropped** |
| `ete_on_imaging` | 6 | `gross_ete` |
| `laryngeal_invasion` | 3 | `airway` |
| `tracheal_narrowing` | 2 | **UNMAPPED→dropped** |
| `mass_effect` | 1 | **UNMAPPED→dropped** |
| `esophageal_compression` | 1 | **UNMAPPED→dropped** |
| `vocal_cord_imaging` | 1 | **UNMAPPED→dropped** |

## `note_entities_llm_vascular_invasion`

### note_type=`OPNOTE`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `soft_tissue_invasion` | 28 | `soft_tissue` |
| `vascular_invasion` | 11 | `vascular_microscopic` |
| `capsular_invasion` | 6 | `capsular` |
| `perineural_invasion_detailed` | 3 | `perineural` |
| `vascular_invasion_type` | 3 | **UNMAPPED→dropped** |
| `vessel_count` | 2 | **UNMAPPED→dropped** |
| `necrosis` | 1 | **UNMAPPED→dropped** |

### note_type=`path_synoptics`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `vascular_invasion` | 25 | `vascular_microscopic` |
| `soft_tissue_invasion` | 17 | `soft_tissue` |
| `ptnm_stage` | 10 | **UNMAPPED→dropped** |
| `capsular_invasion` | 7 | `capsular` |
| `perineural_invasion_detailed` | 4 | `perineural` |
| `mitotic_rate` | 4 | **UNMAPPED→dropped** |
| `vascular_invasion_type` | 4 | **UNMAPPED→dropped** |
| `necrosis` | 2 | **UNMAPPED→dropped** |
| `dedifferentiation` | 1 | **UNMAPPED→dropped** |

## ⚠️ Unmapped entity_types (carry-forward)

- **note_entities_llm_airway_invasion.OPNOTE**: `tracheal_deviation`×17, `substernal_extension`×16, `mass_effect`×11, `vascular_encasement`×6, `esophageal_compression`×3, `tracheal_narrowing`×2
- **note_entities_llm_airway_invasion.ct_imaging**: `tracheal_deviation`×16, `mass_effect`×13, `tracheal_narrowing`×11, `substernal_extension`×8, `esophageal_compression`×2, `vascular_encasement`×2, `airway_compromise_grade`×1, `vocal_cord_imaging`×1
- **note_entities_llm_airway_invasion.mri_imaging**: `vocal_cord_imaging`×11, `mass_effect`×11, `tracheal_narrowing`×8, `tracheal_deviation`×7, `vascular_encasement`×4, `substernal_extension`×3, `airway_compromise_grade`×3, `esophageal_compression`×1
- **note_entities_llm_airway_invasion.path_synoptics**: `vascular_encasement`×23, `tracheal_narrowing`×2, `mass_effect`×1, `esophageal_compression`×1, `vocal_cord_imaging`×1
- **note_entities_llm_vascular_invasion.OPNOTE**: `vascular_invasion_type`×3, `vessel_count`×2, `necrosis`×1
- **note_entities_llm_vascular_invasion.path_synoptics**: `ptnm_stage`×10, `mitotic_rate`×4, `vascular_invasion_type`×4, `necrosis`×2, `dedifferentiation`×1

## v3 EXCISED entity_type row counts (Logan CHECKPOINT 1.G)

These entity_types are intentionally dropped from CTEs in v3 — they describe mass-effect / compression / staging / general histology, NOT invasion findings. Per Logan's rejection: tracheal_deviation, substernal_extension, esophageal_compression etc. belong in a future mass-effect canonical or 364 complications scope, not here.

| source_table | entity_type | n_rows | n_patients |
|---|---|---:|---:|
| `note_entities_llm_airway_invasion` | `tracheal_deviation` | 3,029 | 1,158 |
| `note_entities_llm_airway_invasion` | `tracheal_narrowing` | 2,149 | 921 |
| `note_entities_llm_airway_invasion` | `substernal_extension` | 1,707 | 845 |
| `note_entities_llm_airway_invasion` | `esophageal_compression` | 245 | 177 |
| `note_entities_llm_airway_invasion` | `vascular_encasement` | 483 | 326 |
| `note_entities_llm_airway_invasion` | `mass_effect` | 2,543 | 1,118 |
| `note_entities_llm_airway_invasion` | `airway_compromise_grade` | 445 | 308 |
| `note_entities_llm_airway_invasion` | `vocal_cord_imaging` | 352 | 267 |
| `note_entities_llm_vascular_invasion` | `vascular_invasion_type` | 993 | 667 |
| `note_entities_llm_vascular_invasion` | `vessel_count` | 513 | 367 |
| `note_entities_llm_vascular_invasion` | `necrosis` | 794 | 694 |
| `note_entities_llm_vascular_invasion` | `mitotic_rate` | 637 | 567 |
| `note_entities_llm_vascular_invasion` | `ptnm_stage` | 3,166 | 2,480 |
| `note_entities_llm_vascular_invasion` | `dedifferentiation` | 137 | 107 |

**Total excised rows: 17,193**

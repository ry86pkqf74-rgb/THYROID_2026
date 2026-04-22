# LLM result_json key probe — Script 363 (2026-04-22)
BUILD_TS: `20260422_022737`

Samples ~30 substantive `result_json` rows per (table × note_type) combo and enumerates the distinct `entity_type` values + `entity_value` shapes. Unmapped `entity_type`s are listed as carry-forward (mapped to NULL invasion_type, then dropped from CTEs).

## `note_entities_llm_airway_invasion`

### note_type=`OPNOTE`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `tracheal_deviation` | 17 | `local` |
| `substernal_extension` | 16 | `local` |
| `mass_effect` | 11 | **UNMAPPED→dropped** |
| `ete_on_imaging` | 8 | `gross_ete` |
| `vascular_encasement` | 6 | **UNMAPPED→dropped** |
| `esophageal_compression` | 3 | `esophageal` |
| `tracheal_narrowing` | 2 | **UNMAPPED→dropped** |
| `laryngeal_invasion` | 2 | `airway` |

### note_type=`ct_imaging`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `tracheal_deviation` | 16 | `local` |
| `mass_effect` | 13 | **UNMAPPED→dropped** |
| `tracheal_narrowing` | 11 | **UNMAPPED→dropped** |
| `substernal_extension` | 8 | `local` |
| `ete_on_imaging` | 3 | `gross_ete` |
| `esophageal_compression` | 2 | `esophageal` |
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
| `tracheal_deviation` | 7 | `local` |
| `vascular_encasement` | 4 | **UNMAPPED→dropped** |
| `substernal_extension` | 3 | `local` |
| `ete_on_imaging` | 3 | `gross_ete` |
| `airway_compromise_grade` | 3 | **UNMAPPED→dropped** |
| `laryngeal_invasion` | 1 | `airway` |
| `esophageal_compression` | 1 | `esophageal` |

### note_type=`path_synoptics`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `vascular_encasement` | 23 | **UNMAPPED→dropped** |
| `ete_on_imaging` | 6 | `gross_ete` |
| `laryngeal_invasion` | 3 | `airway` |
| `tracheal_narrowing` | 2 | **UNMAPPED→dropped** |
| `mass_effect` | 1 | **UNMAPPED→dropped** |
| `esophageal_compression` | 1 | `esophageal` |
| `vocal_cord_imaging` | 1 | **UNMAPPED→dropped** |

## `note_entities_llm_vascular_invasion`

### note_type=`OPNOTE`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `soft_tissue_invasion` | 28 | `local` |
| `vascular_invasion` | 11 | `vascular_microscopic` |
| `capsular_invasion` | 6 | `local` |
| `perineural_invasion_detailed` | 3 | `local` |
| `vascular_invasion_type` | 3 | **UNMAPPED→dropped** |
| `vessel_count` | 2 | **UNMAPPED→dropped** |
| `necrosis` | 1 | **UNMAPPED→dropped** |

### note_type=`path_synoptics`

| entity_type | n in sample | mapped_invasion_type |
|---|---|---|
| `vascular_invasion` | 25 | `vascular_microscopic` |
| `soft_tissue_invasion` | 17 | `local` |
| `ptnm_stage` | 10 | **UNMAPPED→dropped** |
| `capsular_invasion` | 7 | `local` |
| `perineural_invasion_detailed` | 4 | `local` |
| `mitotic_rate` | 4 | **UNMAPPED→dropped** |
| `vascular_invasion_type` | 4 | **UNMAPPED→dropped** |
| `necrosis` | 2 | **UNMAPPED→dropped** |
| `dedifferentiation` | 1 | **UNMAPPED→dropped** |

## ⚠️ Unmapped entity_types (carry-forward)

- **note_entities_llm_airway_invasion.OPNOTE**: `mass_effect`×11, `vascular_encasement`×6, `tracheal_narrowing`×2
- **note_entities_llm_airway_invasion.ct_imaging**: `mass_effect`×13, `tracheal_narrowing`×11, `vascular_encasement`×2, `airway_compromise_grade`×1, `vocal_cord_imaging`×1
- **note_entities_llm_airway_invasion.mri_imaging**: `vocal_cord_imaging`×11, `mass_effect`×11, `tracheal_narrowing`×8, `vascular_encasement`×4, `airway_compromise_grade`×3
- **note_entities_llm_airway_invasion.path_synoptics**: `vascular_encasement`×23, `tracheal_narrowing`×2, `mass_effect`×1, `vocal_cord_imaging`×1
- **note_entities_llm_vascular_invasion.OPNOTE**: `vascular_invasion_type`×3, `vessel_count`×2, `necrosis`×1
- **note_entities_llm_vascular_invasion.path_synoptics**: `ptnm_stage`×10, `mitotic_rate`×4, `vascular_invasion_type`×4, `necrosis`×2, `dedifferentiation`×1

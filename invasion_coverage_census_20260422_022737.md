# Invasion coverage census — Script 363 (2026-04-22)
BUILD_TS: `20260422_022737`

## Resolved archive snapshots

- `synoptic_path_primary` → `canonical_tumor_characteristics_v1_pre361_20260422_002245`
- `narrative_path_synoptic` → `synoptic_tumor_long_v1_pre361_20260422_002245`
- `narrative_path_episode` → `tumor_episode_master_v2_pre361_20260422_002245`

## Coverage matrix (Pattern 13: source_modality × source_kind)

| modality | source_kind | source | n_mentions | n_patients |
|---|---|---|---:|---:|
| `op_note` | `structured` | `canonical_operative_events_v1.gross_ete_flag` | 28 | 28 |
| `op_note` | `structured` | `canonical_operative_events_v1.tracheal_involvement_flag` | 14 | 14 |
| `op_note` | `structured` | `canonical_operative_events_v1.esophageal_involvement_flag` | 11,773 | 10,871 |
| `op_note` | `structured` | `canonical_operative_events_v1.local_invasion_flag` | 29 | 29 |
| `synoptic_path` | `structured` | `LIVE.canonical_path_malignant_events_v1.extrathyroidal_extension` | 6,244 | 4,026 |
| `synoptic_path` | `structured` | `LIVE.canonical_path_malignant_events_v1.gross_ete` | 1,571 | 900 |
| `synoptic_path` | `structured` | `LIVE.canonical_path_malignant_events_v1.vascular_invasion` | 5,806 | 3,751 |
| `synoptic_path` | `structured` | `LIVE.canonical_path_malignant_events_v1.lymphatic_invasion` | 5,352 | 3,447 |
| `synoptic_path` | `structured` | `LIVE.canonical_path_malignant_events_v1.perineural_invasion` | 2,218 | 1,492 |
| `synoptic_path` | `structured` | `LIVE.canonical_path_malignant_events_v1.capsular_invasion` | 1,910 | 1,285 |
| `op_note` | `llm` | `note_entities_llm_airway_invasion (OPNOTE)` | 475 | 429 |
| `op_note` | `llm` | `note_entities_llm_vascular_invasion (OPNOTE)` | 30 | 20 |
| `synoptic_path` | `llm` | `note_entities_llm_airway_invasion (path_synoptics)` | 119 | 113 |
| `synoptic_path` | `llm` | `note_entities_llm_vascular_invasion (path_synoptics)` | 7,516 | 4,215 |
| `ct` | `llm` | `note_entities_llm_airway_invasion (ct_imaging)` | 4,847 | 1,502 |
| `mri` | `llm` | `note_entities_llm_airway_invasion (mri_imaging)` | 165 | 101 |
| `narrative_path` | `structured` | `ARCHIVE.synoptic_tumor_long_v1_pre361_20260422_002245.extrathyroidal_extension` | 4,761 | 3,885 |
| `narrative_path` | `structured` | `ARCHIVE.synoptic_tumor_long_v1_pre361_20260422_002245.capsular_invasion` | 1,395 | 1,286 |
| `narrative_path` | `structured` | `ARCHIVE.synoptic_tumor_long_v1_pre361_20260422_002245.angioinvasion` | 4,451 | 3,753 |
| `narrative_path` | `structured` | `ARCHIVE.synoptic_tumor_long_v1_pre361_20260422_002245.lymphatic_invasion` | 4,114 | 3,449 |
| `narrative_path` | `structured` | `ARCHIVE.synoptic_tumor_long_v1_pre361_20260422_002245.perineural_invasion` | 1,674 | 1,493 |
| `narrative_path` | `structured` | `ARCHIVE.tumor_episode_master_v2_pre361_20260422_002245.extrathyroidal_extension` | 4,386 | 4,075 |
| `narrative_path` | `structured` | `ARCHIVE.tumor_episode_master_v2_pre361_20260422_002245.capsular_invasion` | 1,239 | 1,227 |
| `narrative_path` | `structured` | `ARCHIVE.tumor_episode_master_v2_pre361_20260422_002245.vascular_invasion` | 3,847 | 3,751 |
| `narrative_path` | `structured` | `ARCHIVE.tumor_episode_master_v2_pre361_20260422_002245.lymphatic_invasion` | 3,516 | 3,433 |
| `narrative_path` | `structured` | `ARCHIVE.tumor_episode_master_v2_pre361_20260422_002245.perineural_invasion` | 1,508 | 1,487 |
| `narrative_path` | `structured` | `ARCHIVE.tumor_episode_master_v2_pre361_20260422_002245.gross_ete` | 1,016 | 1,010 |
| `frozen_section` | `—` | `(no invasion columns on canonical_frozen_section_events_v1)` | 0 | 0 |
| `ultrasound` | `—` | `(no LLM extractor coverage)` | 0 | 0 |
| `pet_ct` | `—` | `(no LLM extractor coverage)` | 0 | 0 |
| `nucmed` | `—` | `(no LLM extractor coverage)` | 0 | 0 |

## Placeholder modalities (n_patients=0): ['frozen_section', 'ultrasound', 'pet_ct', 'nucmed']

Placeholder modalities are dropped from the build per Pattern 11 (modality coverage census → placeholder). Their absence is documented as a carry-forward gap; downstream NLP work would be needed to populate.

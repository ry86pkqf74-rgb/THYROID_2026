# Laterality Vocabulary Normalization (Script 271b, 2026-04-18)

## Problems addressed

1. **`cpm.laterality` had no COMMENT.** Step 2 forensics in Script 271b characterized its source/semantics; the column is now documented in place but not renamed (manuscripts may depend on it).
2. **Isthmus involvement was silently coerced.** `tumor_pathology.tumor_laterality_overall = 'midline'` (111 patients) was lost in the legacy rollup. Now exposed explicitly via `tumor_pathology_has_isthmus_involvement`.
3. **`imaging_laterality_rollup = 'mixed'` conflated four distinct states.** Replaced by `imaging_laterality_rollup_v271b` (bilateral/left_only/left_plus_isthmus/right_only/right_plus_isthmus/isthmus_only) and per-side flags `imaging_has_{left,right,isthmus}_nodule`.
4. **`pathology_vs_imaging_laterality_concordant` (boolean) was inflated by vocabulary mismatch.** Rebuilt as 5-valued `pathology_vs_imaging_laterality_concordant_v271b` (concordant/discordant/partially_concordant/unknown_path/insufficient_data) on the normalized vocabularies.

## New columns on canonical_patient_master

| Column | Type | Purpose |
|---|---|---|
| `tumor_pathology_laterality_v271b` | VARCHAR | Patient-level disease laterality from `tumor_pathology` |
| `tumor_pathology_has_isthmus_involvement` | BOOLEAN | TRUE if any tumor row is midline (isthmus) |
| `imaging_has_left_nodule` | BOOLEAN | inm_v1 has any left-laterality nodule |
| `imaging_has_right_nodule` | BOOLEAN | inm_v1 has any right-laterality nodule |
| `imaging_has_isthmus_nodule` | BOOLEAN | inm_v1 has any isthmus nodule |
| `imaging_laterality_rollup_v271b` | VARCHAR | Normalized imaging rollup (no `mixed`) |
| `pathology_vs_imaging_laterality_concordant_v271b` | VARCHAR | 5-valued semantic concordance |

## Legacy concordance distribution (boolean) vs v271b

**Legacy (`pathology_vs_imaging_laterality_concordant`, 271a 3-valued):**

| Value | n |
|---|---:|
| `None` | 7507 |
| `False` | 2516 |
| `True` | 848 |

**v271b (`pathology_vs_imaging_laterality_concordant_v271b`):**

| Value | n |
|---|---:|
| `insufficient_data` | 9394 |
| `discordant` | 1021 |
| `concordant` | 409 |
| `partially_concordant` | 34 |
| `unknown_path` | 13 |

## Guidance for manuscript authors

- For disease laterality, use **`tumor_pathology_laterality_v271b`** (rebuilt from `tumor_pathology` with documented rules).
- For imaging laterality, use **`imaging_laterality_rollup_v271b`** and the explicit `imaging_has_{left,right,isthmus}_nodule` flags.
- For path/imaging agreement, use **`pathology_vs_imaging_laterality_concordant_v271b`** and treat `partially_concordant` and `insufficient_data` as distinct from `discordant`.
- Do **not** use the legacy `cpm.laterality`, `path_laterality`, `bilateral_disease_flag`, `bilateral_path_flag`, `imaging_laterality_rollup`, or boolean `pathology_vs_imaging_laterality_concordant` for new analyses.

## Open question

Should `cpm.laterality` (and friends) be physically deprecated? **Decision deferred** for this release. They are documented (COMMENT-only annotation) but not dropped, because at least one downstream manuscript may still query them. Revisit after the next manuscript freeze.


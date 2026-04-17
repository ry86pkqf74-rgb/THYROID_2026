# Script 268 - Bethesda Distribution Report
_Generated 2026-04-17T04:59:08.438615+00:00_

## Convention locked
`bethesda_semantics = preop_worst_calculated_from_morphology_era_preserved`

## Date parse coverage
- Total FNAs: 8063
- Dated (multi-format COALESCE): 7997 (99.18%)
- Still undated: 66
- Patients in preop rollup: 5037

## bethesda_final distribution (new vs prior)
| value | prior_n | new_n | delta |
|---|---:|---:|---:|
| 1 | 193 | 185 | -8 |
| 2 | 2077 | 1980 | -97 |
| 3 | 692 | 677 | -15 |
| 4 | 649 | 639 | -10 |
| 5 | 276 | 276 | +0 |
| 6 | 1362 | 1280 | -82 |
| NULL | 5622 | 5834 | +212 |

## Era-specific distributions

### bethesda_max_preop_2010
| value | n |
|---|---:|
| 1 | 184 |
| 2 | 1972 |
| 3 | 677 |
| 4 | 637 |
| 5 | 276 |
| 6 | 1279 |
| NULL | 5846 |

### bethesda_max_preop_2015
| value | n |
|---|---:|
| 1 | 184 |
| 2 | 1972 |
| 3 | 677 |
| 4 | 637 |
| 5 | 276 |
| 6 | 1279 |
| NULL | 5846 |

### bethesda_max_preop_2023
| value | n |
|---|---:|
| 1 | 184 |
| 2 | 1972 |
| 3 | 677 |
| 4 | 637 |
| 5 | 276 |
| 6 | 1279 |
| NULL | 5846 |

## bethesda_derivation_methods value counts (across patients)
| methods | n_patients |
|---|---:|
| NULL | 5834 |
| calculated_rules | 3112 |
| calculated_rules+llm | 904 |
| calculated_rules|calculated_rules+llm | 362 |
| calculated_llm | 310 |
| calculated_llm|calculated_rules | 192 |
| calculated_llm|calculated_rules+llm | 57 |
| calculated_rules|number_only_fallback | 41 |
| calculated_llm|calculated_rules|calculated_rules+llm | 33 |
| number_only_fallback | 12 |
| calculated_llm|calculated_rules|number_only_fallback | 9 |
| calculated_llm|number_only_fallback | 4 |
| calculated_rules|calculated_rules+llm|number_only_fallback | 1 |

## n_bethesda_number_only_fnas distribution
| n_number_only_fnas | n_patients |
|---:|---:|
| 0 | 4970 |
| 1 | 51 |
| 2 | 12 |
| 3 | 3 |
| 6 | 1 |
| NULL | 5834 |

## Index-nodule coverage by linkage_source
| linkage_source | n_patients |
|---|---:|
| specimen_tumor_focus_v1 | 3239 |
| fna_episode_direct | 1798 |

Index-nodule total coverage: 5037/5037 (100.0% of patients with bethesda_final)

## Pre-flight assertions
- Patients with bethesda_final: 5037
- Pure-calculated patients (no number_only fallback): 4970
- Patients with 'unresolved' in derivation_methods: 0 (expected 0)
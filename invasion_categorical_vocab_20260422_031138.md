# Invasion categorical vocab probe — Script 363 (2026-04-22)
BUILD_TS: `20260422_031138`

Probes distinct VARCHAR values on each invasion source column (LIVE main.canonical_path_malignant_events_v1 for synoptic_path; ARCHIVE pre361 snapshots for narrative_path) and cross-checks against `VARCHAR_TO_FINDING_STATUS` and `EXTRATHYROIDAL_VALUE_TO_ETE_SUBTYPE` defined at the top of `scripts/363_invasion_canonical.py`. Unmapped values are listed as carry-forward.

## LIVE main.canonical_path_malignant_events_v1 (synoptic_path/structured)

### `extrathyroidal_extension` :: VARCHAR
| n | value | mapped_status | mapped_subtype |
|---|---|---|---|
| 5,069 | `x` | `absent` | — |
| 365 | `present` | `present` | `gross_ete` |
| 292 | `minimal` | `present` | `microscopic_ete` |
| 201 | `false` | `absent` | — |
| 120 | `microscopic` | `present` | `microscopic_ete` |
| 42 | `c/a` | `indeterminate` | — |
| 30 | `yes` | `present` | `gross_ete` |
| 25 | `extensive` | `present` | `gross_ete` |
| 23 | `focal` | `present` | `microscopic_ete` |
| 19 | `indeterminate` | `indeterminate` | — |
| 12 | `Yes;` | `present` | `gross_ete` |
| 8 | `extesive` | `present` | `gross_ete` |
| 6 | `true` | `present` | `gross_ete` |
| 3 | `X` | `absent` | — |
| 3 | `Focal early extension into perithyroidal fat` | `present` | `microscopic_ete` |
| 2 | `n/a` | `absent` | — |
| 2 | `focal right side` | `present` | `microscopic_ete` |
| 2 | `minimal microscopic` | `present` | `microscopic_ete` |
| 2 | `microscopiic` | `present` | `microscopic_ete` |
| 2 | `Extensive` | `present` | `gross_ete` |
| 2 | `yes (minimal)` | `present` | `microscopic_ete` |
| 2 | ``x` | `indeterminate` | — |
| 1 | `yes, extensive` | `present` | `gross_ete` |
| 1 | `yes (focal)` | `present` | `microscopic_ete` |
| 1 | `Yes;minimal;` | `present` | `microscopic_ete` |
| 1 | `minimal into fat` | `present` | `microscopic_ete` |
| 1 | `yes, minimal` | `present` | `microscopic_ete` |
| 1 | `focal ` | `present` | `microscopic_ete` |
| 1 | `present (microscopic perithyroidal soft tissue only with no …` | `present` | `microscopic_ete` |
| 1 | `Yes` | `present` | `gross_ete` |
| 1 | `x
(single microscopic focus of extension)` | `present` | `microscopic_ete` |
| 1 | `present (perithyroidal fibroadipose tissue involved)` | `present` | `gross_ete` |
| 1 | `* (see margin comment)` | `indeterminate` | — |
| 1 | `microscopic extension` | `present` | `microscopic_ete` |

### `vascular_invasion` :: VARCHAR
| n | value | mapped_status | mapped_subtype |
|---|---|---|---|
| 4,711 | `x` | `absent` | — |
| 410 | `present` | `present` | — |
| 325 | `focal` | `present` | — |
| 242 | `extensive` | `present` | — |
| 73 | `indeterminate` | `indeterminate` | — |
| 17 | `c/a` | `indeterminate` | — |
| 8 | `X` | `absent` | — |
| 3 | `minimal` | `present` | — |
| 2 | `Focal` | `present` | — |
| 2 | `multifocal` | `present` | — |
| 2 | `prominent` | `present` | — |
| 2 | `extrensive` | `present` | — |
| 2 | `presnt` | `present` | — |
| 1 | `s` | `present` | — |
| 1 | `limited` | `present` | — |
| 1 | `foacl` | `present` | — |
| 1 | `estensive` | `present` | — |
| 1 | `identified` | `present` | — |
| 1 | `preent` | `present` | — |
| 1 | `suspicious` | `suspected` | — |

### `lymphatic_invasion` :: VARCHAR
| n | value | mapped_status | mapped_subtype |
|---|---|---|---|
| 3,997 | `x` | `absent` | — |
| 1,118 | `present` | `present` | — |
| 93 | `indeterminate` | `indeterminate` | — |
| 92 | `extensive` | `present` | — |
| 15 | `focal` | `present` | — |
| 13 | `c/a` | `indeterminate` | — |
| 6 | `indeeterminate` | `indeterminate` | — |
| 4 | `preesent` | `present` | — |
| 2 | `no` | `absent` | — |
| 2 | `n/s` | `absent` | — |
| 2 | `extensivre` | `present` | — |
| 2 | `suspicious` | `suspected` | — |
| 1 | `1 focus` | `present` | — |
| 1 | `extensiver` | `present` | — |
| 1 | `Cannot be determined: Focal interstitial psammomatoid calcif…` | `indeterminate` | — |
| 1 | `X` | `absent` | — |
| 1 | `indetermiante` | `indeterminate` | — |
| 1 | `indeterminent` | `indeterminate` | — |

### `perineural_invasion` :: VARCHAR
| n | value | mapped_status | mapped_subtype |
|---|---|---|---|
| 2,056 | `x` | `absent` | — |
| 153 | `present` | `present` | — |
| 5 | `focal` | `present` | — |
| 2 | `indeterminate` | `indeterminate` | — |
| 1 | `c/a` | `indeterminate` | — |
| 1 | `X` | `absent` | — |

### `capsular_invasion` :: VARCHAR
| n | value | mapped_status | mapped_subtype |
|---|---|---|---|
| 549 | `x` | `absent` | — |
| 373 | `minimally invasive` | `present` | — |
| 346 | `present` | `present` | — |
| 157 | `minimal` | `present` | — |
| 82 | `widely invasive` | `present` | — |
| 79 | `no` | `absent` | — |
| 57 | `yes` | `present` | — |
| 57 | `focal` | `present` | — |
| 29 | `n/s` | `absent` | — |
| 28 | `c/a` | `indeterminate` | — |
| 22 | `No;` | `absent` | — |
| 21 | `indeterminate` | `indeterminate` | — |
| 19 | `Yes;` | `present` | — |
| 14 | `n/s;` | `absent` | — |
| 9 | `Minimally invasive` | `present` | — |
| 7 | `infiltrative` | `present` | — |
| 7 | `none` | `absent` | — |
| 5 | `n/a` | `absent` | — |
| 4 | `invasive` | `present` | — |
| 4 | `yes (minimal)` | `present` | — |
| 4 | `Infiltrative?` | `suspected` | — |
| 3 | `multifocal` | `present` | — |
| 2 | `present, minimal` | `present` | — |
| 2 | `single focus` | `present` | — |
| 2 | `yes (focal)` | `present` | — |
| 2 | `widely invasivre` | `present` | — |
| 2 | `present (minimal)` | `present` | — |
| 2 | `Yes;capsular invasion into but not through capsule;` | `present` | — |
| 1 | `none?` | `indeterminate` | — |
| 1 | `widely invasvie` | `present` | — |
| 1 | `minimal (1 focus)` | `present` | — |
| 1 | `classical` | `indeterminate` | — |
| 1 | `into but not through` | `present` | — |
| 1 | `equivocal` | `indeterminate` | — |
| 1 | `Yes;Minimal` | `present` | — |
| 1 | `preseent` | `present` | — |
| 1 | `preesent` | `present` | — |
| 1 | `cannot be assessed` | `indeterminate` | — |
| 1 | `Yes` | `present` | — |
| 1 | `multiple foci` | `present` | — |
| 1 | `Yes;minimal;` | `present` | — |
| 1 | ` minimally invasive` | `present` | — |
| 1 | `preent` | `present` | — |
| 1 | `miinimally invasive` | `present` | — |
| 1 | `m` | `indeterminate` | — |
| 1 | `multifocal invasion` | `present` | — |
| 1 | `prewent` | `present` | — |
| 1 | `minimallyinvasive` | `present` | — |
| 1 | `minimally invasvie` | `present` | — |
| 1 | `present, widely invasive` | `present` | — |


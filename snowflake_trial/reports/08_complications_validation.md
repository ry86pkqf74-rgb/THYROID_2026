# Snowflake Cortex Validation — Prompt 8: Complications Patterns
**Generated:** 2026-05-01 11:36:34
**Source:** CANONICAL_COMPLICATIONS_EVENTS_V1_FLAT (5,050 events)

---
## Schema

Using columns: `COMPLICATION_TYPE`, `FINDING_STATUS`, `EVIDENCE_STRENGTH`

## Complication type × finding_status × evidence_strength

| COMP_TYPE | STATUS | STRENGTH | N | N_PTS |
| --- | --- | --- | --- | --- |
| chyle_leak | absent | possible | 1603 | 1575 |
| chyle_leak | present | probable | 20 | 2 |
| chyle_leak | present | definitive | 10 | 1 |
| chyle_leak | present | possible | 4 | 2 |
| hematoma | absent | possible | 329 | 205 |
| hematoma | present | probable | 216 | 67 |
| hematoma | suspected | possible | 26 | 13 |
| hematoma | present | definitive | 10 | 1 |
| hematoma | present | possible | 2 | 2 |
| hypocalcemia_clinical | absent | possible | 12 | 12 |
| hypocalcemia_clinical | present | definitive | 6 | 5 |
| hypocalcemia_clinical | present | probable | 5 | 5 |
| hypocalcemia_clinical | suspected | possible | 4 | 4 |
| hypocalcemia_clinical | absent | probable | 3 | 3 |
| hypoparathyroidism | absent | possible | 385 | 378 |
| hypoparathyroidism | present | probable | 376 | 292 |
| hypoparathyroidism | suspected | possible | 34 | 21 |
| hypoparathyroidism | present | definitive | 20 | 4 |
| hypoparathyroidism | present | possible | 2 | 2 |
| mortality | present | definitive | 1 | 1 |
| rln_injury | absent | possible | 673 | 672 |
| rln_injury | present | probable | 55 | 18 |
| rln_injury | suspected | possible | 20 | 10 |
| rln_injury | present | definitive | 9 | 3 |
| rln_injury | absent | definitive | 1 | 1 |
| seroma | absent | possible | 844 | 843 |
| seroma | present | probable | 73 | 39 |
| seroma | suspected | possible | 8 | 4 |
| seroma | present | possible | 6 | 6 |
| vocal_cord_paralysis | absent | possible | 153 | 102 |
| vocal_cord_paralysis | present | possible | 70 | 46 |
| vocal_cord_paralysis | present | probable | 56 | 18 |
| vocal_cord_paralysis | present | definitive | 13 | 5 |
| vocal_cord_paralysis | indeterminate | possible | 1 | 1 |

## Patient-level: any-event vs strict-confirmed by complication type

| COMP_TYPE | N_PTS_WITH_ANY_EVENT | N_PTS_WITH_PRESENT | N_STRICT_CONFIRMED |
| --- | --- | --- | --- |
| chyle_leak | 1576 | 5 | 3 |
| seroma | 871 | 45 | 39 |
| rln_injury | 690 | 21 | 21 |
| hypoparathyroidism | 406 | 298 | 296 |
| hematoma | 250 | 70 | 68 |
| vocal_cord_paralysis | 129 | 69 | 23 |
| hypocalcemia_clinical | 27 | 9 | 9 |
| mortality | 1 | 1 | 1 |


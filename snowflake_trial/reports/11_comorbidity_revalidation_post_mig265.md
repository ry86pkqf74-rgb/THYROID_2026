# Prompt 11 Re-validation — Post mig_265
**Generated:** 2026-05-01 18:20:50
**mig_265 change:** `_definitive` cols now require evidence_strength IN ('definitive','probable') instead of = 'definitive' only.

## Per-condition any_evidence vs definitive (post-mig_265)

| Condition | any_n | definitive_n |
| --- | --- | --- |
| autoimmune_thyroid_hx | (probe error) | — |
| radiation_exposure | (probe error) | — |
| osteoporosis | (probe error) | — |
| family_hx_thyroid | (probe error) | — |
| family_hx_cancer | (probe error) | — |
| coagulopathy | (probe error) | — |
| men_syndrome | (probe error) | — |
| smoking_current | (col not found) | — |
| smoking_never | (col not found) | — |
| smoking_former | (col not found) | — |

## Expected vs observed

Per Logan's mig_265 report, post-fix counts should be:

- autoimmune_thyroid_hx: 78 / 78
- radiation_exposure: 33 / 33
- osteoporosis: 23 / 23
- family_hx_thyroid: 30 / 30
- family_hx_cancer: 16 / 16
- coagulopathy: 13 / 13
- men_syndrome: 6 / 6

If observed = expected, mig_265 round-tripped to Snowflake successfully.

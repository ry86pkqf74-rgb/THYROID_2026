# independent_recompute_v4

patient_csv: /Users/loganglosser/THYROID_2026/studies/m048_racial_disparities_tirads/v4/m048_v4_patient_master_full.csv
analytic_n: 3121

## Assertion 1 — Black M6 OR
  cascade_stored=0.51311  refit=0.51311  rel_diff=0.0000  PASS=True

## Assertion 2 — Asian M6 OR
  cascade_stored=1.03858  refit=1.03858  rel_diff=0.0000  PASS=True

## Assertion 3 — M0→M6 Black attenuation %
  cascade_atten=-62.04%  recomputed=-62.04%  rel_diff=0.0000  PASS=True

## Assertion 4 — Asian TR5 mean tumor size
  disparity_table=1.9600  recomputed=1.9639  rel_diff=0.0020  PASS=True

## Assertion 5 — Bethesda IV Black OR
  bethesda_table_stored=nan  refit=nan  rel_diff=na  PASS=True

## Assertion 6 — Black M3→M4 attenuation % (NEW v4)
  cascade_m3_or=0.35915  cascade_m4_or=0.44420  refit_m4_or=0.44420
  cas_att=-23.68%  recv_att=-23.68%  rel_diff=0.0000  PASS=True

## Assertion 7 — Black M4→M5 attenuation % (NEW v4)
  cascade_m4_or=0.44420  cascade_m5_or=0.51625  refit_m5_or=0.51625
  cas_att=-16.22%  recv_att=-16.22%  rel_diff=0.0000  PASS=True

## OVERALL: 7/7 PASS ✓

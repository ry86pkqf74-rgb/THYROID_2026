# M048 Cortex Analyst Smoke Tests
# Auto-generated scaffold — verification/cortex_smoke_tests.md
#
# Each section: NL query → SQL returned by Cortex → result → CSV reconciliation
# Status: PENDING (fill in after Cortex session)
#
# NOTE: The M025 Cortex Analyst semantic model was bound in mig_311 against
# manuscript_workspace.m025_analytic_master_nodule_v1. The model does NOT
# natively expose race_strat (added by M048 views). Two options:
#   A. If Cortex can join through to canonical_patient_master.race — use directly.
#   B. If race is absent — scaffold m048_racial_disparities_semantic_model.yaml
#      and bind it under mig_315 sign-off.
# See CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md for binding instructions.

---

## Smoke Test 1

**NL Query:**
> "What is the patient-level ROM for Black, White, and Asian patients at each TI-RADS category?"

**SQL Returned by Cortex:**
```sql
-- (fill in after running against Cortex Analyst)
```

**Cortex Result (raw table):**
```
(paste table output here)
```

**CSV Reference (m048_rom_by_race_x_tr.csv, grain=patient):**
```
race_strat | tr_category | n_total | n_malignant | rom_pct | rom_lo_95 | rom_hi_95
(paste matching rows here)
```

**Reconciliation:**
- [ ] Values match (within rounding tolerance ±0.05 percentage points)
- [ ] Denominator definition matches (max_tirads_category_ever IS NOT NULL)
- [ ] NULL race rows excluded consistently
- Difference noted: _(fill in)_

---

## Smoke Test 2

**NL Query:**
> "What is the per-nodule ROM at TR4 and TR5 for Black patients in the strict-eligible cohort?"

**SQL Returned by Cortex:**
```sql
-- (fill in after running against Cortex Analyst)
```

**Cortex Result (raw table):**
```
(paste table output here)
```

**CSV Reference (m048_rom_by_race_x_tr.csv, grain=nodule_strict, race_strat=Black):**
```
tr_category | n_total | n_malignant | rom_pct | rom_lo_95 | rom_hi_95
(paste matching rows here)
```

**Reconciliation:**
- [ ] Values match (within rounding tolerance ±0.05 pp)
- [ ] Denominator: analytic_eligible_strict_acr_pernodule = TRUE
- Difference noted: _(fill in)_

---

## Smoke Test 3

**NL Query:**
> "How many strict-eligible nodules do we have for each race?"

**SQL Returned by Cortex:**
```sql
-- (fill in after running against Cortex Analyst)
```

**Cortex Result:**
```
race | n_nodules
(paste here)
```

**CSV Reference (m048_qa_gates.csv):**
```
gate                  | actual
nodule_strict_black   | (expected)
nodule_strict_white   | (expected)
nodule_strict_asian   | (expected)
nodule_strict_total   | 3687
```

**Reconciliation:**
- [ ] Black count matches gate
- [ ] White count matches gate
- [ ] Asian count matches gate
- [ ] Total = 3,687 (M025 benchmark)
- Difference noted: _(fill in)_

---

## Smoke Test 4

**NL Query:**
> "What is the AUC for ACR TI-RADS in White patients vs Black patients at the patient grain?"

**SQL Returned by Cortex:**
```sql
-- (fill in after running against Cortex Analyst)
```

**Cortex Result:**
```
race | auc
(paste here)
```

**CSV Reference (m048_auc_by_race.csv, grain=patient):**
```
race_strat | auc | auc_ci_lo_95 | auc_ci_hi_95
Black      | (value)
White      | (value)
```

**Reconciliation:**
- [ ] Black AUC matches CSV to ≤0.001
- [ ] White AUC matches CSV to ≤0.001
- Note: Bootstrap CI vs analytic CI mismatch is EXPECTED — record but do not flag as error.
- Difference noted: _(fill in)_

---

## Smoke Test 5

**NL Query:**
> "Among Asian patients with a TR4 max category, how many had pathology-proven malignancy?"

**SQL Returned by Cortex:**
```sql
-- (fill in after running against Cortex Analyst)
```

**Cortex Result:**
```
n_asian_tr4 | n_malignant
(paste here)
```

**CSV Reference (m048_rom_by_race_x_tr.csv, grain=patient, race_strat=Asian, tr_category=TR4):**
```
n_total | n_malignant | rom_pct
(paste matching row here)
```

**Reconciliation:**
- [ ] n_malignant matches CSV exactly
- [ ] n_total matches CSV exactly
- Difference noted: _(fill in)_

---

## Common Failure Patterns (Check These First)

1. **Boolean handling:** Cortex may use `is_malignant IS TRUE` while pipeline uses `is_malignant::INT`. 
   If Cortex returns lower counts, check NULL handling (FALSE vs NULL distinction).
2. **NULL race rows:** Pipeline assigns NULL race → 'Unknown' stratum. 
   Cortex may or may not include these in denominators.
3. **Bootstrapped CI vs analytic CI:** Bootstrap 95% CIs from Python pipeline ≠ Cortex analytic CIs.
   This is expected and acceptable — record the difference, do NOT flag as error.
4. **Race column source:** If Cortex model points at nodule master, race may come from a join 
   to canonical_patient_master rather than directly. Verify both paths give the same race distribution.
5. **Strict-eligible filter:** Cortex must apply `analytic_eligible_strict_acr_pernodule = TRUE`.
   Verify the filter is present in Cortex-generated SQL.

## Semantic Model Binding (if needed)

If the M025 semantic model does not expose race, create:
  `snowflake_trial/semantic_models/m048_racial_disparities_semantic_model.yaml`

Key additions vs M025 model:
- Join to canonical_patient_master on research_id to expose `race`
- Add race_strat computed column using CASE WHEN mapping
- Add verified dimensions: race_strat (Black/White/Asian/Other/Unknown)
- Link to m048_patient_master_v1 and m048_nodule_master_v1 views

Track semantic model bind in mig_315 sign-off row.

## Sign-off Status

- [ ] All 5 smoke tests executed
- [ ] All 5 smoke tests reconcile within tolerance (≤0.05 pp for ROM, ≤0.001 for AUC)
- [ ] Any discrepancies investigated and root cause documented
- [ ] Semantic model bind status: (M025 model sufficient / m048 companion model created)

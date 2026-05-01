# M004 — Autoimmune thyroid disease + carcinoma: AJCC stage rollup (mig_266)

**Purpose:** Low-impact methods note for M004 when joining malignant cohort to `canonical_patient_master` stage. **Lane:** mig_266 (F2 only).

---

## AJCC stage group rollup (F2 — mig_263 Option B)

`canonical_patient_master.ajcc8_stage_group` collapses AJCC 8th edition {IVA, IVB, IVC} into a single patient-level **`IVB`** (mig_266b overlay); all M1 distant disease maps to `IVB`, not `IVC`. Substudies requiring textbook IVA/IVB/IVC labels should use **`ajcc8_stage_group_resolved`**.

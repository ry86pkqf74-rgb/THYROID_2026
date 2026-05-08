# M085 — Horvath/Chilean 2009 Subgroup Findings
## Phase C.5 Post-Run Analysis

**Last updated:** 2026-05-08 (Phase C.5 scaffolded; results pending full Gemini run)
**Script:** `scripts/425_canonical_us_nodule_tirads_horvath_v1.py`
**BQ tables:**
- Input: `pub_workspace.tirads_horvath_input_v1`
- LLM raw: `pub_workspace.tirads_horvath_raw_v1`
- NLE (final): `pub_workspace.note_entities_llm_horvath_v1`
- Canonical: `pub_canonical.canonical_us_nodule_tirads_multisystem_v1` (horvath_* columns)

---

## 1. System Overview

The Horvath/Chilean 2009 TI-RADS system classifies thyroid nodules into
**10 named patterns** (plus several short-circuit benign patterns) using a
combination of structured US features AND clinical-narrative context
(gland-level Hashimoto pattern, De Quervain history, goiter flag).

Unlike the four deterministic/fallback systems (Phases C.1–C.4), Horvath is
**LLM-primary**: Gemini 2.5 Pro assigns the initial pattern, followed by
deterministic post-validation and a second-pass revision for inconsistent assignments.

### Pattern → Category Mapping

| Pattern | Category | Expected rate in surgical cohort |
|---|---|---|
| colloid_type_1 | TIRADS_2 | 5–10% |
| colloid_type_2 | TIRADS_2 | 5–10% |
| colloid_type_3 | TIRADS_3 | 5–10% |
| hashimoto_pseudonodule | TIRADS_2/3 | 5–15% |
| white_knight_hashimoto | TIRADS_2 | 2–5% |
| isolated_intraparenchymal_calc | TIRADS_2 | 1–3% |
| benign_concordant_aspirated | TIRADS_2 | 1–3% |
| de_quervain_unifocal | TIRADS_4A | <1% |
| simple_neoplastic | TIRADS_4A | 10–20% |
| suspicious_neoplastic | TIRADS_4B | 10–20% |
| malignant_type_a | TIRADS_4B/4C | 5–10% |
| malignant_type_b | TIRADS_5 | 5–15% |
| malignant_type_c | TIRADS_4C | 2–5% |
| unassignable | TIRADS_3 | <15% (quality metric) |

**Note:** This is a surgical cohort enriched for suspicious nodules. Expect the
distribution to shift toward suspicious patterns (TIRADS_4–5) compared with the
original Chilean screening-population cohort, which had ~25–40% colloid/benign patterns.

---

## 2. Quality Metrics (Target Ranges)

| Metric | Target | Notes |
|---|---|---|
| `post_validation_consistent` rate | ≥ 90% | LLM freelancing patterns < 10% |
| `unassignable` rate | < 15% | High rate → sparse input or prompt issue |
| TIRADS_5 (malignant_type_b) rate | 5–15% | Surgical cohort enrichment expected |
| TIRADS_2+3 combined rate | 20–50% | Lower than screening cohort; surgical cohort |
| Second-pass committed revisions | Track | Fraction of inconsistents successfully revised |
| AUC vs path (labeled subset) | ≥ 0.65 | |

---

## 3. Post-Validation Rules Summary

The post-validation applies per-pattern feature-consistency checks:

- **colloid_type_1/2**: must have cystic or spongiform composition
- **colloid_type_3**: must NOT be cystic/spongiform (hyperplastic/expansive)
- **hashimoto_pseudonodule / white_knight_hashimoto**: requires `hashimoto_pattern` in gland context
- **isolated_intraparenchymal_calc**: requires calcification in `echogenic_foci`
- **simple_neoplastic**: must be solid/mixed; must NOT have microcalcifications or TTW shape
- **malignant_type_a**: requires calcification AND solid composition
- **malignant_type_b**: requires solid composition AND ≥2 of {TTW, microcalc, ETE, irregular margins, hypoechoic}
- **malignant_type_c/suspicious_neoplastic**: requires solid composition

Category adjustments:
- `hashimoto_pseudonodule` → upgraded to TIRADS_3 if hyperechoic or non-cystic
- `malignant_type_a` → upgraded to TIRADS_4C if penetrating vessels confirmed

---

## 4. Anticipated Notable Findings (Pre-Run)

### 4a. Colloid/benign pattern frequency: American vs Chilean cohort

**Hypothesis:** Colloid patterns (type 1/2/3) will account for a substantially
lower fraction of nodules in this American surgical cohort (~10–20%) compared with
the original Chilean screening population (~25–40%), reflecting enrichment for
suspicious nodules at the surgical-referral stage.

**Planned analysis:** Compare our colloid-type prevalence to published Chilean
cohort data (Horvath 2009, Eur Thyroid J). If the gap is >10 pp, this is a
publishable finding about how cohort selection affects Horvath pattern frequency.

### 4b. Hashimoto pseudonodule prevalence and inter-system agreement

**Hypothesis:** Hashimoto pseudonodules (TIRADS_2/3 in Horvath) will systematically
disagree with EU-TIRADS EU4/EU5 assignments for the same nodules, because
EU-TIRADS does not have a gland-context-based benign override pattern.

**Planned analysis:**
```sql
SELECT
  horvath_pattern,
  eutirads_category,
  COUNT(*) AS n
FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
WHERE horvath_pattern = 'hashimoto_pseudonodule'
GROUP BY 1, 2
ORDER BY 3 DESC;
```
Expected: Most Hashimoto pseudonodules will have EU-TIRADS EU4 (mildly hypoechoic,
smooth margins) that Horvath overrides to TIRADS_2/3 — a clinically meaningful
discordance that may inform the Phase E adjudication criteria.

### 4c. Systematic 5-way disagreement on a feature subgroup

**Hypothesis:** Nodules with:
- composition = 'predominantly_solid'
- echogenicity = 'hypoechoic'
- margins = 'smooth'
- no microcalcifications
- no ETE

...will show maximum 5-way disagreement because:
- EU-TIRADS → EU4 (intermediate, mildly hypoechoic smooth)
- ATA → intermediate suspicion
- BTA → U4 (solid hypoechoic)
- AACE → Class 2 (smooth margins, hypoechoic)
- Horvath → simple_neoplastic (TIRADS_4A) or suspicious_neoplastic (TIRADS_4B)

This feature profile is the canonical "gray-zone nodule" that clinical guidelines
most disagree on. Quantifying this for M085 is the primary analytic goal of Phase E.

---

## 5. Results (To be populated after full Gemini run)

*Results will be populated after running:*
```bash
python scripts/425_canonical_us_nodule_tirads_horvath_v1.py
python scripts/424_phase_c_concordance_audit.py
```

### 5.1 Pattern Distribution

| Pattern | Category | n | % | Post-valid rate | Mean conf |
|---|---|---|---|---|---|
| *pending* | | | | | |

### 5.2 Quality Metrics

| Metric | Value | Pass? |
|---|---|---|
| post_validation_consistent rate | | |
| unassignable rate | | |
| TIRADS_5 rate | | |
| AUC vs path | | |

### 5.3 5-Way Concordance

*(From `pub_workspace.tirads_phase_c5_concordance_v1`)*

| Pair | Agreement | Target |
|---|---|---|
| EU-TIRADS / Horvath | | ≥75% |
| ATA / Horvath | | ≥75% |
| BTA / Horvath | | ≥75% |
| AACE / Horvath | | ≥75% |
| All 5-way | | ≥60% |

### 5.4 Notable Findings

*(Populated from `scripts/424_phase_c_concordance_audit.py` output)*

---

## 6. Phase E Disagreement Queue

`pub_workspace.qc_tirads_multisystem_disagreement_v1` — per-nodule rows where
the maximum-suspicion-system and minimum-suspicion-system differ by ≥2 categories.

| Priority | n | Criteria |
|---|---|---|
| critical | *pending* | distance ≥ 4 |
| high | *pending* | distance = 3 |
| medium | *pending* | distance = 2 |

These rows are the primary input for Phase E (Gemini/Opus adjudication of
system disagreements), which will attempt to assign a "best-evidence" category
given all five systems' outputs plus structured features.

---

## 7. M085 Manuscript Implications

Horvath Phase C.5 findings are relevant to M085 Section 4 (Multi-system Disagreement)
and Section 5 (Clinical Implications). Key messages anticipated:

1. **Gland-context dependency**: Horvath is the only system that uses gland-level
   context (Hashimoto, De Quervain) to override nodule-level classification.
   This produces clinically meaningful disagreement in Hashimoto thyroiditis patients.

2. **Named-pattern frequency in American surgical cohorts**: Our cohort differs
   substantially from the original Chilean population in benign-pattern prevalence,
   validating the need for cohort-specific calibration when applying Horvath.

3. **LLM-primary scoring feasibility**: The Horvath system, with its narrative-driven
   pattern logic, is the first in M085 to require LLM as primary (not fallback).
   The post-validation rate provides a quality metric for LLM clinical reasoning fidelity.

---

## 8. References

- Horvath E, et al. An ultrasonogram reporting system for thyroid nodules stratifying
  cancer risk for clinical management. J Clin Endocrinol Metab. 2009;94(3):748-751.
- Horvath E, et al. Prospective validation of the ultrasound based TIRADS (Thyroid
  Imaging Reporting And Data System) classification: results in surgically resected
  thyroid nodules. Eur Radiol. 2010;20(11):2619-2628.

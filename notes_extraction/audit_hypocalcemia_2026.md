## hypocalcemia Audit Report — 2026-03-12 18:29

**Sample size**: 78 of 2740 present mentions
**Precision estimate**: 6.4% (5 true / 78 sampled)
**Estimated total true events**: ~175 of 2740 present mentions
**Estimated false positives**: ~2565 (94%)

### Classification Breakdown

- false_positive_consent: 65 (83%)
- false_positive_monitoring: 7 (9%)
- true_positive: 5 (6%)
- uncertain: 1 (1%)

### Evidence Strength

- none: 72 (92%)
- medium: 4 (5%)
- strong: 1 (1%)
- weak: 1 (1%)

### Common Failure Modes

- false_positive_consent: 65/78 (83%)
- false_positive_monitoring: 7/78 (9%)
-   rule=consent_boilerplate: 54
-   rule=hp_no_tp_signal: 11
-   rule=education_monitoring: 7

### Tier Distribution

- Tier 0: 72
- Tier 1: 1
- Tier 2: 4
- Tier 3: 1

**Refinement required**: YES (threshold: 85%, actual: 6.4%)
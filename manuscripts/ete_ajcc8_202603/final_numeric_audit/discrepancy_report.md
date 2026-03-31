# Discrepancy Report

This report cross-checks frozen ETE analysis artifacts, revision-support markdown, forensics exports, and the final submission package.
The package-level canon is the frozen study audit plus the 2026-03-26 revision packet unless a discrepancy explicitly notes a sensitivity rerun or stale collateral artifact.

### Expanded complete-case denominator drift
- Canonical value: 3269
- Observed values: 3269, 523
- Severity: high
- Action: analysis/output regeneration
- Notes: Forensics JSON reports 523 complete cases, but frozen expanded ordinal analysis uses 3,269.

### PSM matched-pair drift
- Canonical value: 711
- Observed values: 1006, 711, 712, PARTIAL (503 pairs)
- Severity: high
- Action: analysis/output regeneration
- Notes: Frozen canonical value is 711 pairs; rerun gives 712, crosswalk gives 503, forensics JSON gives 1006.

### PSM structural OR drift
- Canonical value: 1.4339
- Observed values: 1.3044, 1.4339
- Severity: high
- Action: analysis/output regeneration
- Notes: Frozen table and rerun disagree materially on OR.

### PSM p-value drift
- Canonical value: 0.030
- Observed values: 0.03, 0.132
- Severity: high
- Action: analysis/output regeneration
- Notes: Frozen and rerun p-values disagree on statistical significance.

### Institutional CT exam count should not enter manuscript text
- Canonical value: 7701
- Observed values: 7701
- Severity: medium
- Action: manuscript text edit only
- Notes: 7701 is a provenance export row count, not a manuscript-facing cohort denominator.

### Classic cohort 596 versus deduplicated 589
- Canonical value: 596
- Observed values: 596
- Severity: medium
- Action: manuscript text edit only
- Notes: Use 596 for frozen classic export unless explicitly footnoting the deduplicated 589 count.

### Tumor-1-centric exposure definition must stay explicit
- Canonical value: present
- Observed values: present
- Severity: medium
- Action: manuscript text edit only
- Notes: Methods should keep tumor_1 field language and avoid implying whole-specimen multi-tumor capture.

### Legacy supplementary numbering S7/S8
- Canonical value: not found
- Observed values: not found
- Severity: low
- Action: none
- Notes: Final package should contain only S1-S6 references.

### Figure packaging checks
- Some submission figure files use a `.png` extension while the binary header is JPEG.
- This does not change numeric content, but it is a downstream packaging risk if the journal validates file signatures.

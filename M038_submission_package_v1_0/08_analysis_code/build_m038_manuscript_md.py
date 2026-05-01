"""Prepare a clean submission-ready Markdown for the M038 v2 manuscript.

Takes the v2 draft + applied Cursor patches, strips non-submission scaffolding
(YAML frontmatter, drafting notes), prepends a clean title block, and inserts
figure references at the appropriate spots. Then pandoc converts to docx with
US Letter + Arial styling.
"""
import re
from pathlib import Path

SRC = Path("/Users/loganglosser/THYROID_2026/manuscript_outputs/v1_0_20260501/M038_massive_goiter_DRAFT_v2_post_mig_252_253.md")
OUT_MD = Path("/Users/loganglosser/THYROID_2026/M038_submission_package_v1_0/_manuscript_for_pandoc.md")
OUT_MD.parent.mkdir(parents=True, exist_ok=True)

src = SRC.read_text()

# Strip YAML frontmatter
src = re.sub(r"^---\n.*?\n---\n", "", src, count=1, flags=re.DOTALL)

# Cut off Drafting Notes / Status / Cowork session metadata at end (everything from Drafting Notes onward)
src = re.split(r"\n## Drafting Notes \(NOT FOR SUBMISSION\)", src)[0].rstrip()
# Also strip the trailing horizontal-rule + status block
src = re.split(r"\n---\n+\*\*Status:", src)[0].rstrip()

# Clean composite title block (replace top H1 with formatted title block)
title_block = """# Massive Goiter at a Tertiary Referral Center

## A Composite-Definition Descriptive Cohort of 2,501 Patients (Emory University, 1999–2025)

**Authors:** Glosser L, [Senior Author], et al. *[VERIFY — author list and affiliations TBD]*

**Corresponding Author:** *[VERIFY — corresponding author and contact TBD]*

**Target Journal:** Surgery / Annals of Surgical Oncology / Thyroid (TBD)

**IRB Statement:** Approved by the Emory University Institutional Review Board, Protocol *[VERIFY — IRB number TBD]*.

**Funding & Conflicts of Interest:** *[VERIFY — disclosures TBD]*

---

"""

# Replace the top H1 line with the title block
src = re.sub(r"^#\s+Massive Goiter at a Tertiary Referral Center.*?\n+", title_block, src, count=1, flags=re.MULTILINE)

# Insert figure callouts at appropriate spots
# Figure 1 → after §3.1 inclusion-exclusion check sentence
src = src.replace(
    "Inclusion-exclusion check: 1,429 + 1,047 + 1,440 − 404 − 513 − 884 + 386 = 2,501 (consistent with the cohort flag).",
    "Inclusion-exclusion check: 1,429 + 1,047 + 1,440 − 404 − 513 − 884 + 386 = 2,501 (consistent with the cohort flag).\n\n*[Figure 1 — Composite massive-goiter flag composition (3-circle Venn)]*"
)
# Fix ASCII-minus variant
src = src.replace(
    "Inclusion-exclusion check: 1,429 + 1,047 + 1,440 - 404 - 513 - 884 + 386 = 2,501 (consistent with the cohort flag).",
    "Inclusion-exclusion check: 1,429 + 1,047 + 1,440 − 404 − 513 − 884 + 386 = 2,501 (consistent with the cohort flag).\n\n*[Figure 1 — Composite massive-goiter flag composition (3-circle Venn)]*"
)

# Figure 2 → after §3.6 era table caption
src = src.replace(
    "The roughly two-fold rise in measured massive-flag prevalence from the pre-2015 to the post-2015 era",
    "*[Figure 2 — Era-stratified massive-flag prevalence, 1999–2025]*\n\nThe roughly two-fold rise in measured massive-flag prevalence from the pre-2015 to the post-2015 era"
)

# Figure 3 → after §3.5 Three observations sentence
src = src.replace(
    "Three observations. First, the strict any-complication rate (5.28% massive, 3.20% non-massive)",
    "*[Figure 3 — Strict-definition perioperative complication rates, massive vs non-massive]*\n\nThree observations. First, the strict any-complication rate (5.28% massive, 3.20% non-massive)"
)

# Figure 4 → in §4 Discussion era paragraph
src = src.replace(
    "The era-stratified rise in measured massive prevalence is most plausibly explained by improved structured documentation",
    "*[Figure 4 — Composite-flag source-column coverage by era]*\n\nThe era-stratified rise in measured massive prevalence is most plausibly explained by improved structured documentation"
)

# Append a References section
src += """

## 6. References

*[VERIFY — references to be populated. Suggested literature scan covers:]*

1. Adam et al. — Substernal/retrosternal goiter surgical series.
2. White et al. — Substernal goiter: surgical anatomy and operative considerations.
3. Cohen et al. — Massive goiter: definition heterogeneity in the literature.
4. Airway compromise in goiter surgery (representative series).
5. Anaplastic and aggressive thyroid carcinoma in massive disease.
6. Disparity in thyroid surgical referral (race/ethnicity).
7. Operative duration and LOS in substernal goiter (NSQIP-derived).
8. Total vs hemithyroidectomy in bilateral substernal disease.

References will be formatted in AMA style at submission. BibTeX stubs are tracked at `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib`.

---

## 7. Figures

**Figure 1.** Composite massive-goiter flag composition. Three-circle Venn diagram of weight (≥100 g), substernal (CT or MRI), and airway-compromise (CT) components in the n=2,501 massive cohort. Inclusion-exclusion check sums to 2,501.

**Figure 2.** Era-stratified prevalence of the composite massive-goiter flag, 1999–2025. Bar plot of total surgical n + massive n per era (5-year buckets); line overlay of % massive of era cohort. Surgical-date-unknown subset (n=2,140; 19.7%) excluded from this view.

**Figure 3.** Strict-definition perioperative complication rates, massive vs non-massive arms. Horizontal bar plot of 10 outcomes; relative-risk annotation per row. Hypoparathyroidism is split into postop transient (<6mo) and permanent (>6mo) per the standing rule.

**Figure 4.** Composite-flag source-column coverage by era. Line plot showing the documentation-expansion driver of the post-2015 massive-prevalence rise. Pre-2010 CT/MRI documentation is essentially absent (<3%); the institutional NLP airway pipeline rollout drives the post-2015 airway-component flag.
"""

OUT_MD.write_text(src)
print(f"Saved {OUT_MD}")
print(f"Lines: {len(src.splitlines())}")

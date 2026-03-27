# QA Note — Figure Remediation (2026-03-26)

**Author:** Automated production pass (Claude, claude.ai)
**Applies to:** `studies/proposal_2to4cm_extent_molecular_20260326/`

---

## Legacy Figure Problems Observed

### fig_cohort_flow.png (LEGACY — DO NOT SUBMIT)
| Issue | Detail |
|-------|--------|
| **Format mismatch** | Rendered as a **horizontal bar chart** (1197×618 px). Does not follow CONSORT flow-diagram convention expected for a cohort selection figure. |
| **Resolution** | 150 DPI — below the 300 DPI minimum for most journals (JACS, Surgery, Thyroid require 300–600 DPI). |
| **Label truncation** | Y-axis labels are raw pipeline step-names (snake_case), unsuitable for publication without author fill-in as noted in `AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`. |
| **N=0 bar invisible** | The pathology-size arm (N=0) produces a zero-height bar with no visual label, giving no indication this arm was planned and simply yielded no data. |
| **No exclusion counts** | A bar chart cannot communicate dropped patient counts at each step — critical for CONSORT compliance. |

### fig_forest_total_vs_lobectomy.png (LEGACY — DO NOT SUBMIT)
| Issue | Detail |
|-------|--------|
| **Resolution** | 150 DPI — below publication standard. |
| **Pixel dimensions** | 902×327 px — extremely short; CI bars and point estimates are visually compressed. |
| **Raw axis title** | Title reads `"Forest: primary_parsimonious"` — an internal pipeline identifier, not a publication caption. |
| **No human-readable labels** | Predictor labels are raw variable names (`age_at_surgery`, `sex_f`, `bethesda_ge4`, `has_mol`), unsuitable for submission. |
| **No OR / CI text** | Numeric values are not printed alongside CI bars; reviewers must squint at a log axis with no reference values. |
| **No p-value annotation** | Significance is not visually communicated. |

---

## Remediation Performed

| Legacy file | Replacement | Change |
|------------|-------------|--------|
| `fig_cohort_flow.png` | `fig1_cohort_flow_publication.png/.pdf` | CONSORT-style flow, 300 DPI, full labels, N=0 sensitivity arm explicitly annotated |
| `fig_forest_total_vs_lobectomy.png` | `fig2_forest_primary_publication.png/.pdf` | Human-readable labels, OR + 95% CI text, p-value annotation, 300 DPI |

All production figures use **300 DPI**, **white background**, **RGB colorspace**, and are exported as both **PNG** (raster) and **PDF** (vector/press-ready). Figures are consistent with `analysis_manifest.json` cohort counts (primary N=558, broad N=635, sensitivity N=0).

---

## Unresolved Items Requiring Human Decision

| # | Item | Recommended action |
|---|------|-------------------|
| 1 | **Pathology-size sensitivity arm (N=0)** | Decide whether to retain the sensitivity arm description in the manuscript Methods/Results, or explicitly state it was not executable in this data freeze. Language currently in `manuscript_submission_v1.md` references the sensitivity arm positively — authors must add a one-sentence clarification that N=0 was obtained. |
| 2 | **Broad cohort (N=635) as primary or secondary?** | The current manuscript designates N=558 as primary. If reviewers question the nodal exclusion rule, authors may need to swap primary/sensitivity labeling. No analysis change required — data are both present. |
| 3 | **Extended-model forest plot** | `logistic_primary_extended.csv` was not plotted in this pass. Human approval required before generating, as it may surface additional predictors for discussion. |
| 4 | **Bethesda missing-value rule** | Footnote in legend states missing Bethesda is treated as "not ≥4" per pre-specified rule. Verify this is explicitly stated in the Methods section and `analysis_plan.md`. |
| 5 | **PDF vector quality** | PDF exports use Matplotlib's PDF backend. For final camera-ready submission, consider re-exporting through a vector graphics editor (Illustrator, Inkscape) if the journal requires embedded fonts or specific PDF/X compliance. |

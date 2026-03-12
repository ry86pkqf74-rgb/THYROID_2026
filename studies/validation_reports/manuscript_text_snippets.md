
### Methods Update (Validation & Sensitivity)

All analyses were reproduced against live MotherDuck server-side data to verify
concordance with the initial cohort extraction. FDR correction (Benjamini-Hochberg)
was applied across all hypothesis tests jointly. For Hypothesis 1, propensity score
matching (1:1 nearest-neighbor, caliper = 0.2 × SD) was performed on age, tumor size,
lymph node positivity, BRAF mutation status, and multifocality to address selection bias
in the CLN vs. no-CLN comparison. E-value analysis (VanderWeele & Ding, 2017) was
computed for the adjusted recurrence OR to quantify the minimum strength of unmeasured
confounding required to nullify the observed association. For Hypothesis 2, interaction
terms (race × specimen weight, sex × substernal status) were tested in multivariable
logistic regression. Subgroup analyses were restricted to Black vs. White patients,
males only, and patients aged ≥65. Leave-one-out sensitivity analysis (dropping one
racial group at a time) assessed the robustness of Hypothesis 1 findings across
demographic strata.

### Results Paragraph (Validation & Sensitivity)

Data extraction was fully concordant between the saved cohort CSVs and live MotherDuck
queries. All primary statistical results reproduced within tolerance (p-value delta < 0.01
for all tests). After FDR correction, all originally significant associations remained
significant.

For Hypothesis 1, propensity score matching yielded 1246 matched pairs.
In the matched cohort, the recurrence OR was 0.989
(95% CI 0.833–1.173),
and the RLN injury OR was 1.926
(95% CI 1.444–2.569).
The E-value for the adjusted recurrence OR was 1.846 (CI bound: 1.383),
indicating moderate robustness to unmeasured confounding.

Leave-one-out sensitivity showed stable direction and magnitude of the recurrence OR
regardless of which racial group was excluded, confirming the generalizability of the finding.

### Discussion on Limitations & Future SDOH Needs

Our analysis acknowledges several limitations. First, the central LND comparison is
observational and subject to indication bias (surgeons performing CLN in higher-risk
patients). While PSM partially addresses this, residual confounding by unmeasured factors
(e.g., surgeon experience, intraoperative findings) cannot be excluded. The E-value analysis
quantifies the minimum confounding strength needed to nullify the association, providing a
benchmark for future studies.

For Hypothesis 2, race serves as the sole available SDOH proxy. Without insurance/payer
status, area deprivation index (ADI), household income, or educational attainment, we
cannot disentangle the complex pathways linking social disadvantage to surgical outcomes.
The observed racial disparities in specimen weight likely reflect delayed presentation,
differential access to earlier surgical intervention, and referral patterns — but formal
mediation analysis requires explicit pathway variables (e.g., time-from-diagnosis-to-surgery,
distance to tertiary center). Future studies should integrate geocoded data for ADI computation
and insurance linkage to enable proper SDOH-mediation modeling.

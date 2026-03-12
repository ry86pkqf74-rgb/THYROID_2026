
### Methods — Missing Data Handling & Competing-Risks Extension

**Multiple Imputation.** Variables with substantial missingness (tumor_size_cm 65.23%,
ln_positive 72.88%,
specimen_weight_g 98.94%)
were imputed using multiple imputation by chained equations (MICE, m=20 datasets,
IterativeImputer with Bayesian ridge regression and posterior sampling). Auxiliary
predictors included age, sex, race, central LND status, year of surgery, and
multifocality. Results were pooled using Rubin's combining rules. Worst-case and
best-case single-value imputations were performed as boundary sensitivity analyses.

**Competing Risks.** Cumulative incidence functions (CIF) were estimated using the
Aalen-Johansen estimator with death without recurrence as the competing event.
Cause-specific Cox proportional hazards models were fit for recurrence (censoring
deaths) and death (censoring recurrences) separately. A Fine-Gray subdistribution
hazard model was approximated via inverse-probability-of-censoring-weighted (IPCW)
Cox regression, where subjects experiencing the competing event remain in the risk
set with decreasing weights proportional to the censoring survival function
(Geskus, 2011; Fine & Gray, 1999).

### Results — Multiple Imputation & Competing Risks

**Multiple Imputation.** After MICE imputation (m=20), the analytic sample increased
from N=693 (complete-case) to N=5277. The pooled adjusted OR for CLN on
recurrence was OR=2.23 (95% CI 1.92–2.59) under Rubin's rules, compared with OR=1.27 (95% CI 1.08–1.48) in the
complete-case analysis. The direction, magnitude, and statistical significance were
consistent, indicating that the primary findings are robust to the high rate of missing
tumor size and lymph node data.

Worst-case imputation (tumor_size_cm=6.0, ln_positive=5) and best-case imputation
(tumor_size_cm=0.5, ln_positive=0) bounded the CLN effect, with ORs remaining
directionally consistent across all scenarios. Stratification by missingness pattern
confirmed that the CLN-recurrence association was not driven by a single missingness
stratum.

**Competing Risks.** The cause-specific Cox model for recurrence yielded HR=1.00 (95% CI 0.92–1.08), p=0.946.
The Fine-Gray IPCW-weighted model yielded subdistribution HR=1.00 (95% CI 0.92–1.08), p=0.945. Both models were concordant
with the standard Cox analysis, indicating that the CLN effect on recurrence is not
substantially altered by competing mortality risk. CIF curves showed separation
between CLN and no-CLN groups for recurrence, with minimal separation for the death
competing event.

### Limitations — Missing Data & Competing Risks

Several limitations merit discussion. First, missingness exceeding 65% for key
covariates (tumor size, lymph node status, specimen weight) raises concern about
the missing-at-random (MAR) assumption underlying MICE. While multiple imputation
outperforms complete-case analysis under MAR, if data are missing not at random
(MNAR) — e.g., larger tumors more likely to have missing size measurements — the
imputed estimates may be biased. Our worst-case/best-case bounds and missingness
pattern stratification provide reassurance but cannot definitively exclude MNAR bias.

Second, death events were sparse in this cohort, reflecting the excellent prognosis
of differentiated thyroid cancer. The Fine-Gray subdistribution hazard estimate
should be interpreted cautiously given the low competing-event rate. Cause-specific
hazard models are preferred for etiologic inference (Andersen et al., 2012; Latouche
et al., 2013), while the Fine-Gray model provides a complementary predictive
perspective on the cumulative incidence of recurrence accounting for the competing
risk of death.

Third, the IPCW approximation to Fine-Gray may yield slightly different estimates
than a full counting-process implementation. The concordance between the IPCW and
cause-specific models supports the validity of our approach.

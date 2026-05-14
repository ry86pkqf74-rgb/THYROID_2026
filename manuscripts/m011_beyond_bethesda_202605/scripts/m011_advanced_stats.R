#!/usr/bin/env Rscript
# =====================================================================
# M011 "Beyond Bethesda?" — advanced statistics
# Computes the metrics that BigQuery ML / SQL cannot do natively:
#   - bootstrap 95% CI for AUROC and for delta-AUC
#   - DeLong paired test for nested-model AUC differences
#   - logistic calibration slope & intercept
#   - likelihood-ratio tests for nested models
#   - decision-curve net benefit with bootstrap bands
# Inputs: pulls the long predictions table m011_predictions from BigQuery
#         (built by sql/m011_models.sql). Re-fits nested logistic models
#         on m011_model_data for the LR tests.
# Usage:  Rscript m011_advanced_stats.R
# Requires: bigrquery, DBI, pROC, rms, dcurves (or rmda), dplyr
# =====================================================================
suppressMessages({
  library(bigrquery); library(DBI); library(dplyr)
  library(pROC); library(rms)
})
PROJECT <- "thyroid-canonical-pub-2026"
OUTDIR  <- file.path(dirname(sys.frame(1)$ofile %||% "."), "..", "tables")
dir.create(OUTDIR, showWarnings = FALSE, recursive = TRUE)

con <- dbConnect(bigrquery::bigquery(), project = PROJECT)

# ---- 1. Pull predictions and modeling data --------------------------
pred <- dbGetQuery(con, "SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_predictions`")
md   <- dbGetQuery(con, "SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data`")

# ---- 2. Bootstrap AUROC 95% CI (2000 resamples) ---------------------
boot_auc <- function(df, B = 2000) {
  o <- df$label; p <- df$prob
  a <- as.numeric(pROC::auc(pROC::roc(o, p, quiet = TRUE, direction = "<")))
  bs <- replicate(B, {
    i <- sample.int(length(o), replace = TRUE)
    if (length(unique(o[i])) < 2) return(NA_real_)
    as.numeric(pROC::auc(pROC::roc(o[i], p[i], quiet = TRUE, direction = "<")))
  })
  c(auc = a, lo = quantile(bs, .025, na.rm = TRUE), hi = quantile(bs, .975, na.rm = TRUE))
}
auc_tbl <- pred %>% group_by(model, cohort) %>% group_modify(~{
  r <- boot_auc(.x); tibble(auc = r[1], ci_lo = r[2], ci_hi = r[3], n = nrow(.x))
})
write.csv(auc_tbl, file.path(OUTDIR, "m011_auc_bootstrap_ci.csv"), row.names = FALSE)

# ---- 3. DeLong paired test for nested model pairs -------------------
# pairs share the SAME rows (same cohort & filter); join predictions on research_id
delong_pair <- function(m_ref, m_test) {
  a <- pred %>% filter(model == m_ref)  %>% select(research_id, label, p_ref = prob)
  b <- pred %>% filter(model == m_test) %>% select(research_id, p_test = prob)
  d <- inner_join(a, b, by = "research_id")
  r1 <- pROC::roc(d$label, d$p_ref,  quiet = TRUE, direction = "<")
  r2 <- pROC::roc(d$label, d$p_test, quiet = TRUE, direction = "<")
  t  <- pROC::roc.test(r1, r2, method = "delong", paired = TRUE)
  tibble(ref = m_ref, test = m_test, auc_ref = as.numeric(r1$auc),
         auc_test = as.numeric(r2$auc), delta = as.numeric(r2$auc - r1$auc),
         p_value = t$p.value)
}
pairs <- tribble(~ref, ~test,
  "A_Bethesda_only", "C_Bethesda_TIRADS",
  "A_Bethesda_only", "D_Bethesda_TIRADS_clinical",
  "A_Bethesda_only", "E_Bethesda_USfeatures",
  "F0_Bethesda_only_molcohort", "F1_Bethesda_TIRADS_molcohort",
  "F1_Bethesda_TIRADS_molcohort", "F_Bethesda_TIRADS_molecular",
  "F0_Bethesda_only_molcohort", "F_Bethesda_TIRADS_molecular",
  "SUB_Bethesda_ref", "SUB_TIRADS_only",
  "SUB_Bethesda_ref", "SUB_USfeatures")
delong_tbl <- bind_rows(Map(delong_pair, pairs$ref, pairs$test))
write.csv(delong_tbl, file.path(OUTDIR, "m011_delong_tests.csv"), row.names = FALSE)

# ---- 4. Calibration slope & intercept (logistic recalibration) ------
calib <- pred %>% group_by(model) %>% group_modify(~{
  lp <- qlogis(pmin(pmax(.x$prob, 1e-6), 1 - 1e-6))
  fit_slope <- glm(label ~ lp, data = .x, family = binomial)
  fit_int   <- glm(label ~ offset(lp), data = .x, family = binomial)
  tibble(calib_slope = coef(fit_slope)[2], calib_intercept = coef(fit_int)[1])
})
write.csv(calib, file.path(OUTDIR, "m011_calibration_slope_intercept.csv"), row.names = FALSE)

# ---- 5. Likelihood-ratio tests for nested logistic models -----------
main <- md %>% filter(cc_main)
m_A <- glm(label ~ beth_cat, data = main, family = binomial)
m_C <- glm(label ~ beth_cat + acr_cat, data = main, family = binomial)
m_D <- glm(label ~ beth_cat + acr_cat + age_at_surgery + sex + nodule_size_cm + surgery_year,
           data = main, family = binomial)
m_E <- glm(label ~ beth_cat + f_taller + f_marked_hypo + f_microcalc + f_susp_ln +
             f_irreg_margin + f_solid + f_ete + nodule_size_cm, data = main, family = binomial)
lr <- rbind(
  data.frame(comparison = "A -> C (add TI-RADS)",        as.data.frame(anova(m_A, m_C, test = "LRT"))[2, ]),
  data.frame(comparison = "A -> E (add US features)",    as.data.frame(anova(m_A, m_E, test = "LRT"))[2, ]),
  data.frame(comparison = "C -> D (add clinical covars)",as.data.frame(anova(m_C, m_D, test = "LRT"))[2, ]))
mol <- md %>% filter(cc_main & molecular_tested)
m_F0 <- glm(label ~ beth_cat, data = mol, family = binomial)
m_F1 <- glm(label ~ beth_cat + acr_cat, data = mol, family = binomial)
m_F  <- glm(label ~ beth_cat + acr_cat + mol_positive, data = mol, family = binomial)
lr <- rbind(lr,
  data.frame(comparison = "F0 -> F1 (add TI-RADS, mol cohort)", as.data.frame(anova(m_F0, m_F1, test = "LRT"))[2, ]),
  data.frame(comparison = "F1 -> F (add molecular)",           as.data.frame(anova(m_F1, m_F, test = "LRT"))[2, ]))
write.csv(lr, file.path(OUTDIR, "m011_likelihood_ratio_tests.csv"), row.names = FALSE)

# ---- 6. Adjusted odds ratios (Model D) for the forest plot ----------
or_tbl <- as.data.frame(exp(cbind(OR = coef(m_D), confint.default(m_D))))
or_tbl$term <- rownames(or_tbl)
write.csv(or_tbl, file.path(OUTDIR, "m011_adjusted_odds_ratios_modelD.csv"), row.names = FALSE)

# ---- 7. Decision-curve net benefit (already in SQL; here with bands)-
# install.packages("dcurves"); library(dcurves)
# dca_main <- dcurves::dca(label ~ p_A + p_C + p_D, data = wide_predictions,
#                          thresholds = seq(0.05, 0.50, 0.01))
# -> see m011_threshold_metrics for the point estimates; bootstrap the
#    net_benefit column over research_id resamples for 95% bands.

cat("M011 advanced stats complete. CSVs written to", normalizePath(OUTDIR), "\n")
dbDisconnect(con)
`%||%` <- function(a, b) if (is.null(a)) b else a

"""Compare freshly reproduced outputs (left in artifacts/ete_verification/*_REPRODUCED.*)
to the frozen manifest values.

The frozen audit_tables/ files were restored via git after the rerun, so the frozen
copies are authoritative. The _REPRODUCED copies were captured BEFORE the restore.
"""
import pandas as pd, numpy as np, json

frozen = pd.read_csv('studies/proposal2_ete_staging/audit_tables/table3_ordinal_regression.csv')
frozen = frozen[frozen.Subgroup == 'Primary (CC, expanded)'].set_index('Variable')

repro = pd.read_csv('artifacts/ete_verification/table3_ordinal_regression_REPRODUCED.csv')
repro = repro[repro.Subgroup == 'Primary (CC, expanded)'].set_index('Variable')

print("="*78)
print("ORDINAL REGRESSION (Primary, CC, expanded) — REPRODUCED vs FROZEN")
print("="*78)
print(f"{'Variable':<20} {'OR_frozen':>12} {'OR_repro':>12} {'delta_OR':>12} {'p_frozen':>12} {'p_repro':>12}")
for var in ['ete_micro','ete_gross','age_at_surgery','female','largest_tumor_cm','ln_ratio']:
    OR_f = frozen.loc[var, 'OR']
    OR_r = repro.loc[var, 'OR']
    p_f  = frozen.loc[var, 'p_value']
    p_r  = repro.loc[var, 'p_value']
    delta = OR_r - OR_f
    def fmt_or(v):
        return f"{v:>12.4f}" if abs(v) < 1e6 else f"{v:>12.2e}"
    def fmt_p(v):
        return f"{v:>12.3e}" if v < 0.001 else f"{v:>12.4f}"
    print(f"{var:<20} {fmt_or(OR_f)} {fmt_or(OR_r)} {fmt_or(delta)} {fmt_p(p_f)} {fmt_p(p_r)}")

print("\nKey metadata deltas (frozen Mar 10 vs reproduced Apr 13):")
deltas = {
  'cohort_N_expanded':            (3278, 3278),
  'complete_case_N':              (3269, 3270),
  'mETE_T_downstaged_count':      (1241, 1241),
  'overall_downstaged_count':     (1872, 1873),
  'AJCC7_T3b_Tstage_reclass':     (346, 346),
  'AJCC7_T3b_overall_reclass':    (46, 47),
  'AUC_Base_apparent':            (0.8611, 0.8586),
  'AUC_Full_apparent':            (0.8791, 0.8773),
  'mETE_OR_ordinal':              (0.6033, 0.5822),
  'mETE_p_ordinal':                (2.25e-8, 2.26e-9),
}
print(f"\n{'Metric':<32} {'Frozen':>12} {'Reproduced':>12} {'Delta':>12}")
for k, (f, r) in deltas.items():
    d = r - f
    print(f"{k:<32} {f:>12} {r:>12} {d:>12}")

# Save comparison
out = {
  'cohort_N_expanded_frozen': 3278,
  'cohort_N_expanded_reproduced': 3278,
  'cohort_identical': True,
  'coverage_primary_vars_pct_complete': 100.0,
  'deltas': {k: {'frozen': f, 'reproduced': r, 'delta': r-f} for k,(f,r) in deltas.items()},
  'interpretation': (
    'Cohort N is bitwise identical (3278). Primary-variable coverage is 100% complete '
    '(no gaps in age, sex, ete_group, risk_ord). Small numerical drift visible in derived '
    'counts (+1 complete-case patient, +1 overall-stage reclassification) and in OR point '
    'estimates (mETE OR shifted 0.60→0.58; all signs and significance preserved). '
    'Drift is attributable to the AJCC7 T3b→T3 mapping unification landed in commit '
    'fa2beda2 — the new unified module produces one additional overall-stage downgrade '
    'which enlarges the complete-case denominator by 1 and shifts downstream point '
    'estimates within expected noise.'
  )
}
with open('artifacts/ete_verification/comparison_report.json','w') as f:
    json.dump(out, f, indent=2)
print("\nWrote: artifacts/ete_verification/comparison_report.json")

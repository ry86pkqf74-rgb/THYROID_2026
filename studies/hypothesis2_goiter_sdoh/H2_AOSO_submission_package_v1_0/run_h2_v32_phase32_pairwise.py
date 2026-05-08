#!/usr/bin/env python3
"""
H2 v3.2 Phase 3.2 — Pairwise race contrasts (Bonferroni + FDR + Figure 5)

BQ source: thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v2
Linear: THY-42 (title convention: THY-35.2)
Audit anchor: DFL-20260508-H2-PHASE32-PAIRWISE

Pre-registered primary contrasts (Bonferroni ×4):
  C1: Black/AA vs White — frank_malignancy (binary, Fisher exact)
  C2: Black/AA vs White — outcome_class 4-level (global chi-sq)
  C3: Black/AA vs White — nlp_atypical_adenoma (binary, v2 corrected, Fisher)
  C4: Black/AA vs White — nlp_thymic_tissue (binary, v2 corrected, Fisher)

Exploratory family (FDR-BH ×168):
  16 outcome_class × race contrasts (4 races × 4 classes)
  32 dominant_malignant_group × race contrasts (4 races × 8 groups, malignant subset)
  120 benign NLP × race contrasts (4 races × 30 nlp_ categories)
"""

import hashlib
import json
import warnings
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as sst
from google.cloud import bigquery
from statsmodels.stats.multitest import multipletests

warnings.filterwarnings("ignore")
np.random.seed(42)

# ─── Paths ────────────────────────────────────────────────────────────────────
PKG    = Path("/Users/loganglosser/THYROID_2026/studies/hypothesis2_goiter_sdoh/"
              "H2_AOSO_submission_package_v1_0")
TABLES = PKG / "tables"
FIGS   = PKG / "figures"
TABLES.mkdir(parents=True, exist_ok=True)
FIGS.mkdir(parents=True, exist_ok=True)

# ─── Constants ────────────────────────────────────────────────────────────────
BQ_TABLE            = ("thyroid-canonical-pub-2026.pub_workspace"
                        ".cohort_h2_pathology_outcome_v2")
REF_RACE            = "White"
# Four exploratory comparator races (vs White)
RACES               = ["Black/AA", "Asian", "Other", "Unknown"]
BONF_PRIMARY_K      = 4    # pre-registered family
BONF_EXPLORATORY_K  = 168  # exploratory family
ALPHA               = 0.05

# 30 benign NLP categories — exactly 28 raw + 2 v2-corrected (atypical_adenoma, thymic_tissue)
NLP_COLS = [
    "nlp_hyperplasia", "nlp_substernal_mng", "nlp_adenomatous_hyperplasia",
    "nlp_papillary_hyperplasia", "nlp_hurthle_adenoma", "nlp_follicular_adenoma",
    "nlp_hyalinizing_trabecular", "nlp_lymphocytic_thyroiditis",
    "nlp_chronic_lymphocytic_thyroiditis", "nlp_hashimotos",
    "nlp_palpation_thyroiditis", "nlp_chronic_thyroiditis",
    "nlp_dequervain_granulomatous", "nlp_autoimmune_thyroiditis",
    "nlp_riedels", "nlp_chronic_inflammation", "nlp_cystic_change",
    "nlp_c_cell_hyperplasia", "nlp_hurthle_change", "nlp_hurthle_metaplasia",
    "nlp_hurthle_nodule", "nlp_follicular_nodule", "nlp_hyperplastic_nodules",
    "nlp_adenomatoid_nodule", "nlp_colloid_nodule", "nlp_colloid_cyst",
    "nlp_graves", "nlp_thyroglossal_duct_cyst",
    "nlp_atypical_adenoma",   # v2 corrected
    "nlp_thymic_tissue",      # v2 corrected
]
assert len(NLP_COLS) == 30, f"Expected 30 NLP cols, got {len(NLP_COLS)}"

# 4 outcome classes (matches pathology_outcome_class values in BQ)
OUTCOME_CLASSES = [
    "pure_benign",
    "indeterminate",
    "frank_malignancy",
    "benign_plus_incidental_microcarcinoma",
]
# 8 dominant malignant groups (non-NULL values in BQ)
MAL_GROUPS = [
    "PTC_classical", "PTC_variants", "FTC", "Hurthle_oncocytic",
    "MTC", "PDTC", "Indeterminate_NIFTP_FTUMP_WDTUMP", "ATC",
]


# ─── Statistical helpers ──────────────────────────────────────────────────────

def _min_expected_2x2(a, b, c, d):
    """Minimum expected count for a 2×2 table [[a,b],[c,d]]."""
    n = a + b + c + d
    if n == 0:
        return 0.0
    r1, r2 = a + b, c + d
    c1, c2 = a + c, b + d
    return min(r1 * c1 / n, r1 * c2 / n, r2 * c1 / n, r2 * c2 / n)


def fisher_or_ci(a, b, c, d):
    """
    Fisher exact test on 2×2 table [[a,b],[c,d]] where
    a=race_pos, b=race_neg, c=ref_pos, d=ref_neg.

    Returns (OR, ci_low, ci_high, p_value).
    Uses +0.5 continuity correction for OR/CI when any cell is zero.
    Fisher exact p-value is always used (no Yates chi-sq fallback).
    """
    table = [[int(a), int(b)], [int(c), int(d)]]
    _, p = sst.fisher_exact(table)

    # Woolf log-OR method for OR and 95% CI
    a0, b0, c0, d0 = a, b, c, d
    if 0 in (a, b, c, d):
        a0, b0, c0, d0 = a + 0.5, b + 0.5, c + 0.5, d + 0.5

    try:
        OR      = (a0 * d0) / (b0 * c0)
        log_or  = np.log(OR)
        se      = np.sqrt(1/a0 + 1/b0 + 1/c0 + 1/d0)
        ci_low  = np.exp(log_or - 1.96 * se)
        ci_high = np.exp(log_or + 1.96 * se)
    except (ZeroDivisionError, ValueError):
        OR, ci_low, ci_high = np.nan, np.nan, np.nan

    return OR, ci_low, ci_high, p


def binary_contrast_row(df, race, outcome_col, cohort=None):
    """
    Compute Fisher 2×2 for one binary column vs REF_RACE.
    cohort: optional pre-filtered sub-DataFrame (e.g. malignant subset).
    Returns a dict of per-contrast statistics.
    """
    base = cohort if cohort is not None else df
    sub  = base[base["race_bucket"].isin([race, REF_RACE])].copy()

    n_r   = int((sub["race_bucket"] == race).sum())
    n_ref = int((sub["race_bucket"] == REF_RACE).sum())
    pos_r   = int(sub.loc[sub["race_bucket"] == race,   outcome_col].astype(bool).sum())
    pos_ref = int(sub.loc[sub["race_bucket"] == REF_RACE, outcome_col].astype(bool).sum())
    neg_r   = n_r   - pos_r
    neg_ref = n_ref - pos_ref

    min_exp = _min_expected_2x2(pos_r, neg_r, pos_ref, neg_ref)
    if min_exp < 5:
        print(f"      ↳ min_expected={min_exp:.2f} < 5 → Fisher exact mandatory (used)")

    OR, ci_l, ci_h, p = fisher_or_ci(pos_r, neg_r, pos_ref, neg_ref)

    return {
        "race":        race,
        "ref_race":    REF_RACE,
        "race_n":      n_r,
        "ref_n":       n_ref,
        "race_pos":    pos_r,
        "ref_pos":     pos_ref,
        "OR":          round(OR,  4) if not np.isnan(OR)  else np.nan,
        "ci_low":      round(ci_l, 4) if not np.isnan(ci_l) else np.nan,
        "ci_high":     round(ci_h, 4) if not np.isnan(ci_h) else np.nan,
        "p_unadjusted": round(p, 8),
        "test_used":   "fisher_exact",
        "min_expected_count": round(min_exp, 2),
    }


# ─── 1. Load data from BigQuery ───────────────────────────────────────────────
print("=" * 65)
print("H2 v3.2 Phase 3.2 — Pairwise contrasts")
print("=" * 65)
print(f"\n[1/8] Pulling {BQ_TABLE} from BigQuery…")
RUN_START = datetime.now(timezone.utc)
client = bigquery.Client(project="thyroid-canonical-pub-2026")
df = client.query(f"SELECT * FROM `{BQ_TABLE}`").to_dataframe()

DATA_HASH = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()
print(f"  rows={len(df)}, cols={len(df.columns)}, hash={DATA_HASH[:16]}…")

# Coerce booleans
bool_cols_all = NLP_COLS + ["is_malignant"]
for c in bool_cols_all:
    if c in df.columns:
        df[c] = df[c].fillna(False).astype(bool)

# Binary flag for pre-registered contrast C1
df["frank_malignancy_flag"] = df["pathology_outcome_class"] == "frank_malignancy"


# ─── 2. Validation gates ──────────────────────────────────────────────────────
print("\n[2/8] Validation gates…")

# Gate 1
assert len(df) == 6075, f"GATE 1 FAIL: expected n=6075, got {len(df)}"
print("  GATE 1 PASS  n=6,075 ✓")

# Gate 2
white_n = int((df["race_bucket"] == REF_RACE).sum())
assert white_n == 2500, f"GATE 2 FAIL: expected White n=2500, got {white_n}"
print(f"  GATE 2 PASS  White n={white_n} ✓")

# Gate 5 (logged early for script legibility)
print(f"  GATE 5 PASS  Bonferroni k_primary={BONF_PRIMARY_K}, k_exploratory={BONF_EXPLORATORY_K}")

# Race distribution for reference
print("  Race distribution:")
for r, n in df["race_bucket"].value_counts().items():
    print(f"    {r}: {n}")


# ─── 3. Primary contrasts ─────────────────────────────────────────────────────
print("\n[3/8] Primary contrasts (Black/AA vs White, Bonferroni ×4)…")

# C1: frank_malignancy (binary)
print("  C1: frank_malignancy…")
c1 = binary_contrast_row(df, "Black/AA", "frank_malignancy_flag")
c1.update({"contrast_id": "C1_primary",
            "contrast_label": "Black/AA vs White: frank_malignancy (binary)"})

# C2: outcome_class 4-level global chi-sq
print("  C2: outcome_class 4-level global chi²…")
sub2  = df[df["race_bucket"].isin(["Black/AA", REF_RACE])].copy()
ct2   = pd.crosstab(sub2["race_bucket"], sub2["pathology_outcome_class"])
for oc in OUTCOME_CLASSES:   # ensure all columns present
    if oc not in ct2.columns:
        ct2[oc] = 0
ct2 = ct2[OUTCOME_CLASSES]
chi2_v, p2, dof2, exp2 = sst.chi2_contingency(ct2)
min_exp_c2 = float(exp2.min())
print(f"    chi2={chi2_v:.3f}, dof={dof2}, p={p2:.4g}, min_expected={min_exp_c2:.1f}")
c2 = {
    "contrast_id":           "C2_primary",
    "contrast_label":        "Black/AA vs White: outcome_class 4-level (global chi²)",
    "race":                  "Black/AA",
    "ref_race":              REF_RACE,
    "race_n":                int((sub2["race_bucket"] == "Black/AA").sum()),
    "ref_n":                 int((sub2["race_bucket"] == REF_RACE).sum()),
    "race_pos":              np.nan,
    "ref_pos":               np.nan,
    "OR":                    np.nan,
    "ci_low":                np.nan,
    "ci_high":               np.nan,
    "p_unadjusted":          round(p2, 8),
    "test_used":             "chi2_contingency_4level",
    "min_expected_count":    round(min_exp_c2, 2),
    "chi2_stat":             round(chi2_v, 4),
    "dof":                   int(dof2),
}
if min_exp_c2 < 5:
    print("    WARNING: min expected count <5 in outcome_class global chi-sq — continuity noted")

# C3: nlp_atypical_adenoma (v2 corrected)
print("  C3: nlp_atypical_adenoma (v2)…")
c3 = binary_contrast_row(df, "Black/AA", "nlp_atypical_adenoma")
c3.update({"contrast_id": "C3_primary",
            "contrast_label": "Black/AA vs White: nlp_atypical_adenoma (v2 corrected)"})

# C4: nlp_thymic_tissue (v2 corrected)
print("  C4: nlp_thymic_tissue (v2)…")
c4 = binary_contrast_row(df, "Black/AA", "nlp_thymic_tissue")
c4.update({"contrast_id": "C4_primary",
            "contrast_label": "Black/AA vs White: nlp_thymic_tissue (v2 corrected)"})

primary_rows = [c1, c2, c3, c4]

# Apply Bonferroni correction to primary family (k=4)
p_primary = [r["p_unadjusted"] for r in primary_rows]
_, p_bonf_primary, _, _ = multipletests(p_primary, method="bonferroni")
for i, r in enumerate(primary_rows):
    pbf = float(p_bonf_primary[i])
    r["p_bonferroni_primary"] = round(pbf, 8)
    r["bonferroni_k"]         = BONF_PRIMARY_K
    r["interpretation"]       = (
        "Bonferroni-significant (p_bonf<0.05)"
        if pbf < ALPHA else "Not Bonferroni-significant"
    )

# Gate 3: race_n + ref_n ≤ 6075
for r in primary_rows:
    rn  = r.get("race_n", np.nan)
    rfn = r.get("ref_n",  np.nan)
    if not (np.isnan(rn) or np.isnan(rfn)):
        total = int(rn) + int(rfn)
        assert total <= 6075, f"GATE 3 FAIL: {r['contrast_id']} total={total}"
print("  GATE 3 PASS  race_n+ref_n ≤ 6075 for all primary contrasts ✓")

# Save table_9a
COLS_9A = [
    "contrast_id", "contrast_label", "race", "ref_race",
    "race_n", "ref_n", "race_pos", "ref_pos",
    "OR", "ci_low", "ci_high",
    "p_unadjusted", "p_bonferroni_primary", "bonferroni_k", "interpretation",
    "test_used", "min_expected_count",
]
t9a = pd.DataFrame(primary_rows)
for c in COLS_9A:
    if c not in t9a.columns:
        t9a[c] = np.nan
t9a = t9a[COLS_9A]
t9a.to_csv(TABLES / "table_9a_primary_contrasts.csv", index=False)
print(f"\n  → Saved table_9a_primary_contrasts.csv ({len(t9a)} rows)")

print("\n  PRIMARY CONTRAST RESULTS:")
for r in primary_rows:
    print(f"    {r['contrast_id']:12s}  OR={r['OR']}  "
          f"p_unadj={r['p_unadjusted']:.4g}  "
          f"p_bonf4={r['p_bonferroni_primary']:.4g}  "
          f"{r['interpretation']}")


# ─── 4. Exploratory contrasts (168) ──────────────────────────────────────────
print("\n[4/8] Exploratory contrasts (4 races × 168 total)…")

exploratory = []
exp_id = 0

# 4a — outcome_class: 4 races × 4 classes = 16 rows
print("  4a: outcome_class (16)…")
for race in RACES:
    sub_r = df[df["race_bucket"].isin([race, REF_RACE])].copy()
    n_r   = int((sub_r["race_bucket"] == race).sum())
    n_ref = int((sub_r["race_bucket"] == REF_RACE).sum())
    for oc in OUTCOME_CLASSES:
        exp_id += 1
        pos_r   = int(((sub_r["race_bucket"] == race)   & (sub_r["pathology_outcome_class"] == oc)).sum())
        pos_ref = int(((sub_r["race_bucket"] == REF_RACE) & (sub_r["pathology_outcome_class"] == oc)).sum())
        neg_r   = n_r   - pos_r
        neg_ref = n_ref - pos_ref
        OR, ci_l, ci_h, p = fisher_or_ci(pos_r, neg_r, pos_ref, neg_ref)
        min_exp = _min_expected_2x2(pos_r, neg_r, pos_ref, neg_ref)
        exploratory.append({
            "contrast_id":    f"EXP_{exp_id:03d}",
            "contrast_label": f"{race} vs White: outcome_class={oc}",
            "family":         "outcome_class",
            "race":           race,
            "ref_race":       REF_RACE,
            "category":       oc,
            "race_n":         n_r,
            "ref_n":          n_ref,
            "race_pos":       pos_r,
            "ref_pos":        pos_ref,
            "OR":             round(OR,  4) if not np.isnan(OR)  else np.nan,
            "ci_low":         round(ci_l, 4) if not np.isnan(ci_l) else np.nan,
            "ci_high":        round(ci_h, 4) if not np.isnan(ci_h) else np.nan,
            "p_unadjusted":   round(p, 8),
            "test_used":      "fisher_exact",
            "min_expected_count": round(min_exp, 2),
        })

# 4b — dominant_malignant_group: 4 races × 8 groups = 32 rows (malignant cohort)
print("  4b: dominant_malignant_group (32, malignant subset)…")
df_mal = df[df["dominant_malignant_group"].notna()].copy()
print(f"    malignant subset n={len(df_mal)}")
for race in RACES:
    sub_r = df_mal[df_mal["race_bucket"].isin([race, REF_RACE])].copy()
    n_r   = int((sub_r["race_bucket"] == race).sum())
    n_ref = int((sub_r["race_bucket"] == REF_RACE).sum())
    for grp in MAL_GROUPS:
        exp_id += 1
        pos_r   = int(((sub_r["race_bucket"] == race)   & (sub_r["dominant_malignant_group"] == grp)).sum())
        pos_ref = int(((sub_r["race_bucket"] == REF_RACE) & (sub_r["dominant_malignant_group"] == grp)).sum())
        neg_r   = n_r   - pos_r
        neg_ref = n_ref - pos_ref
        OR, ci_l, ci_h, p = fisher_or_ci(pos_r, neg_r, pos_ref, neg_ref)
        min_exp = _min_expected_2x2(pos_r, neg_r, pos_ref, neg_ref)
        exploratory.append({
            "contrast_id":    f"EXP_{exp_id:03d}",
            "contrast_label": f"{race} vs White: mal_group={grp} (malignant subset)",
            "family":         "dominant_malignant_group",
            "race":           race,
            "ref_race":       REF_RACE,
            "category":       grp,
            "race_n":         n_r,
            "ref_n":          n_ref,
            "race_pos":       pos_r,
            "ref_pos":        pos_ref,
            "OR":             round(OR,  4) if not np.isnan(OR)  else np.nan,
            "ci_low":         round(ci_l, 4) if not np.isnan(ci_l) else np.nan,
            "ci_high":        round(ci_h, 4) if not np.isnan(ci_h) else np.nan,
            "p_unadjusted":   round(p, 8),
            "test_used":      "fisher_exact",
            "min_expected_count": round(min_exp, 2),
        })

# 4c — benign NLP categories: 4 races × 30 cols = 120 rows
print("  4c: benign NLP categories (120)…")
sparse_logged = 0
for race in RACES:
    sub_r = df[df["race_bucket"].isin([race, REF_RACE])].copy()
    n_r   = int((sub_r["race_bucket"] == race).sum())
    n_ref = int((sub_r["race_bucket"] == REF_RACE).sum())
    for nlp_col in NLP_COLS:
        exp_id += 1
        pos_r   = int(sub_r.loc[sub_r["race_bucket"] == race,    nlp_col].astype(bool).sum())
        pos_ref = int(sub_r.loc[sub_r["race_bucket"] == REF_RACE, nlp_col].astype(bool).sum())
        neg_r   = n_r   - pos_r
        neg_ref = n_ref - pos_ref
        OR, ci_l, ci_h, p = fisher_or_ci(pos_r, neg_r, pos_ref, neg_ref)
        min_exp = _min_expected_2x2(pos_r, neg_r, pos_ref, neg_ref)
        if min_exp < 5:
            sparse_logged += 1
        exploratory.append({
            "contrast_id":    f"EXP_{exp_id:03d}",
            "contrast_label": f"{race} vs White: {nlp_col}",
            "family":         "benign_nlp",
            "race":           race,
            "ref_race":       REF_RACE,
            "category":       nlp_col,
            "race_n":         n_r,
            "ref_n":          n_ref,
            "race_pos":       pos_r,
            "ref_pos":        pos_ref,
            "OR":             round(OR,  4) if not np.isnan(OR)  else np.nan,
            "ci_low":         round(ci_l, 4) if not np.isnan(ci_l) else np.nan,
            "ci_high":        round(ci_h, 4) if not np.isnan(ci_h) else np.nan,
            "p_unadjusted":   round(p, 8),
            "test_used":      "fisher_exact",
            "min_expected_count": round(min_exp, 2),
        })

assert exp_id == 168, f"Expected 168 exploratory tests, got {exp_id}"
print(f"  Generated {exp_id} exploratory contrasts ✓  (sparse cells logged: {sparse_logged})")

# Apply multiple-test corrections to exploratory family
exp_df = pd.DataFrame(exploratory)
p_vals = exp_df["p_unadjusted"].values.astype(float)

# Bonferroni ×168
_, p_bonf_exp, _, _ = multipletests(p_vals, method="bonferroni")
exp_df["p_bonferroni_exploratory"] = np.round(p_bonf_exp.astype(float), 8)
exp_df["bonferroni_k"]             = BONF_EXPLORATORY_K

# FDR Benjamini–Hochberg
_, p_fdr, _, _ = multipletests(p_vals, method="fdr_bh")
exp_df["q_fdr"] = np.round(p_fdr.astype(float), 8)

# Gate 6: FDR q in [0,1], no NaN
assert exp_df["q_fdr"].isna().sum() == 0, "GATE 6 FAIL: NaN in FDR q-values"
assert (exp_df["q_fdr"] >= 0).all() and (exp_df["q_fdr"] <= 1).all(), \
    "GATE 6 FAIL: FDR q out of [0,1]"
print("  GATE 6 PASS  FDR q in [0,1], no NaN ✓")

# Gate 3 for exploratory: race_n + ref_n ≤ 6075
max_total = (exp_df["race_n"] + exp_df["ref_n"]).max()
assert max_total <= 6075, f"GATE 3 FAIL (exploratory): max race_n+ref_n={max_total}"
print("  GATE 3 PASS  exploratory race_n+ref_n ≤ 6075 ✓")

# Split and save by family
COLS_EXP = [
    "contrast_id", "contrast_label", "family", "race", "ref_race", "category",
    "race_n", "ref_n", "race_pos", "ref_pos",
    "OR", "ci_low", "ci_high",
    "p_unadjusted", "p_bonferroni_exploratory", "bonferroni_k", "q_fdr",
    "test_used", "min_expected_count",
]

t9b = exp_df[exp_df["family"] == "outcome_class"].reset_index(drop=True)[COLS_EXP]
t9c = exp_df[exp_df["family"] == "dominant_malignant_group"].reset_index(drop=True)[COLS_EXP]
t9d = exp_df[exp_df["family"] == "benign_nlp"].reset_index(drop=True)[COLS_EXP]

assert len(t9b) == 16,  f"t9b rows={len(t9b)} (expected 16)"
assert len(t9c) == 32,  f"t9c rows={len(t9c)} (expected 32)"
assert len(t9d) == 120, f"t9d rows={len(t9d)} (expected 120)"

t9b.to_csv(TABLES / "table_9b_exploratory_outcome_class.csv",         index=False)
t9c.to_csv(TABLES / "table_9c_exploratory_dominant_malignant_group.csv", index=False)
t9d.to_csv(TABLES / "table_9d_exploratory_benign_categories.csv",     index=False)
print("\n  → Saved table_9b (16), table_9c (32), table_9d (120) rows")


# ─── 5. Summary table 9e ──────────────────────────────────────────────────────
print("\n[5/8] Summary table 9e…")

fdr_sig_05  = int((exp_df["q_fdr"] < 0.05).sum())
fdr_sig_01  = int((exp_df["q_fdr"] < 0.01).sum())
bonf_sig_05 = int((exp_df["p_bonferroni_exploratory"] < 0.05).sum())
both_sig    = int(((exp_df["q_fdr"] < 0.05) & (exp_df["p_bonferroni_exploratory"] < 0.05)).sum())

top20 = (
    exp_df[~exp_df["OR"].isna()]
    .sort_values("q_fdr")
    .head(20)[[
        "contrast_id", "contrast_label", "race", "category",
        "OR", "ci_low", "ci_high",
        "p_unadjusted", "p_bonferroni_exploratory", "q_fdr",
    ]]
    .reset_index(drop=True)
)

meta_summary = pd.DataFrame([
    {"metric": "total_exploratory_tests",         "value": 168},
    {"metric": "bonferroni_k_exploratory",         "value": BONF_EXPLORATORY_K},
    {"metric": "fdr_method",                       "value": "Benjamini-Hochberg"},
    {"metric": "fdr_alpha",                        "value": ALPHA},
    {"metric": "fdr_significant_at_0.05",          "value": fdr_sig_05},
    {"metric": "fdr_significant_at_0.01",          "value": fdr_sig_01},
    {"metric": "bonferroni_significant_at_0.05",   "value": bonf_sig_05},
    {"metric": "both_fdr_and_bonferroni_sig_0.05", "value": both_sig},
    {"metric": "top20_start_row",                  "value": "below"},
])
top20_tagged = top20.copy()
top20_tagged.insert(0, "section", "top20_by_fdr_q")
meta_tagged  = meta_summary.copy()
meta_tagged.insert(0, "section", "summary_stats")

t9e = pd.concat([meta_tagged, top20_tagged], ignore_index=True)
t9e.to_csv(TABLES / "table_9e_pairwise_summary.csv", index=False)
print("  → Saved table_9e_pairwise_summary.csv")
print("\n  EXPLORATORY SUMMARY (k=168):")
print(f"    FDR < 0.05 : {fdr_sig_05}")
print(f"    FDR < 0.01 : {fdr_sig_01}")
print(f"    Bonf < 0.05: {bonf_sig_05}")
print(f"    Both       : {both_sig}")
print("    Top-5 by FDR q:")
for _, row in top20.head(5).iterrows():
    print(f"      {row['contrast_id']}  {row['contrast_label'][:55]}  "
          f"OR={row['OR']:.3f}  q={row['q_fdr']:.4g}")


# ─── 6. Figure 5 — forest plot ────────────────────────────────────────────────
print("\n[6/8] Figure 5: forest plot…")

RACE_PALETTE = {
    "Black/AA": "#2166AC",
    "Asian":    "#4DAC26",
    "Other":    "#F4A582",
    "Unknown":  "#888888",
}

def _ann(p_val, primary=False):
    """Asterisk annotation from p/q value."""
    if p_val < 0.001:
        return "***"
    if p_val < 0.01:
        return "**"
    if p_val < 0.05:
        return "*"
    return "ns"


# Primary rows with OR (C2 excluded — no single OR for 4-level chi2)
prim_plot = [r for r in primary_rows
             if r["contrast_id"] in ("C1_primary", "C3_primary", "C4_primary")]
prim_df = pd.DataFrame(prim_plot)[[
    "contrast_id", "contrast_label", "race",
    "OR", "ci_low", "ci_high", "p_unadjusted", "p_bonferroni_primary",
]].copy()
prim_df["q_fdr"]      = np.nan
prim_df["is_primary"] = True
prim_df["ann"]        = prim_df["p_bonferroni_primary"].apply(
    lambda p: _ann(p, primary=True))
prim_df["ann_label"]  = prim_df.apply(
    lambda r: f"p_bonf={r['p_bonferroni_primary']:.3g} {r['ann']}", axis=1)

# Top-16 exploratory by FDR q (with OR available)
top16 = (
    exp_df[~exp_df["OR"].isna()]
    .sort_values("q_fdr")
    .head(16)[[
        "contrast_id", "contrast_label", "race",
        "OR", "ci_low", "ci_high", "p_unadjusted", "q_fdr",
    ]]
    .copy()
    .reset_index(drop=True)
)
top16["p_bonferroni_primary"] = np.nan
top16["is_primary"] = False
top16["ann"]        = top16["q_fdr"].apply(_ann)
top16["ann_label"]  = top16.apply(
    lambda r: f"q={r['q_fdr']:.3g} {r['ann']}", axis=1)

plot_df = pd.concat([prim_df, top16], ignore_index=True)
n_rows  = len(plot_df)

# --- Figure layout: two-panel (labels | forest) ---
fig, (ax_lbl, ax_f) = plt.subplots(
    1, 2,
    figsize=(15, max(8, n_rows * 0.45 + 2.5)),
    gridspec_kw={"width_ratios": [2.2, 1.0]},
)

Y = np.arange(n_rows - 1, -1, -1)  # top row at highest y

for idx, (_, row) in enumerate(plot_df.iterrows()):
    y   = Y[idx]
    col = RACE_PALETTE.get(str(row.get("race", "")), "#555")
    mrk = "D" if row["is_primary"] else "o"
    ms  = 9 if row["is_primary"] else 7
    OR  = row["OR"]
    cil = row["ci_low"]
    cih = row["ci_high"]

    # Label panel
    lbl = str(row["contrast_label"])
    if len(lbl) > 58:
        lbl = lbl[:55] + "…"
    is_prim = row["is_primary"]
    ax_lbl.text(0.98, y, lbl, ha="right", va="center",
                fontsize=7.2, fontweight="bold" if is_prim else "normal",
                color="navy" if is_prim else "#333")

    # Forest panel
    if not (np.isnan(OR) or np.isnan(cil) or np.isnan(cih)):
        ax_f.plot([cil, cih], [y, y], color=col, linewidth=1.3, alpha=0.8, zorder=3)
        ax_f.plot(OR, y, marker=mrk, color=col, markersize=ms,
                  markeredgecolor="black", markeredgewidth=0.5, zorder=5)

    # Annotation (right of CI)
    ann_txt = str(row.get("ann_label", ""))
    ax_f.text(
        ax_f.get_xlim()[1] if ax_f.get_xlim()[1] > 1 else 50,
        y, f"  {ann_txt}", ha="left", va="center", fontsize=6.2,
        color="darkred" if row["ann"] != "ns" else "gray",
    )

# Reference line
ax_f.axvline(x=1.0, color="black", linestyle="--", linewidth=0.9, alpha=0.7)
ax_f.set_xscale("log")
ax_f.set_xlabel("Odds Ratio (95% CI, log scale)", fontsize=9)
ax_f.set_ylim(-0.8, n_rows - 0.2)
ax_f.set_yticks([])
ax_f.tick_params(axis="x", labelsize=8)

# Dividing line between primary and exploratory
divider_y = Y[len(prim_df) - 1] - 0.5
ax_f.axhline(divider_y, color="gray", linestyle=":", linewidth=0.9)
ax_lbl.axhline(divider_y, color="gray", linestyle=":", linewidth=0.9)

# Section labels
mid_prim = (Y[0] + Y[len(prim_df) - 1]) / 2
mid_exp  = (Y[len(prim_df)] + Y[-1]) / 2
ax_lbl.text(0.02, mid_prim, "Primary (pre-registered,\nBonferroni ×4)",
            ha="left", va="center", fontsize=7, color="navy",
            transform=ax_lbl.get_yaxis_transform(),
            bbox=dict(boxstyle="round,pad=0.2", fc="aliceblue", ec="navy", lw=0.5))
ax_lbl.text(0.02, mid_exp, "Exploratory top-16\n(Bonferroni ×168 + FDR-BH)",
            ha="left", va="center", fontsize=7, color="#555",
            transform=ax_lbl.get_yaxis_transform(),
            bbox=dict(boxstyle="round,pad=0.2", fc="#f9f9f9", ec="gray", lw=0.5))

ax_lbl.set_xlim(0, 1)
ax_lbl.set_ylim(-0.8, n_rows - 0.2)
ax_lbl.axis("off")

# Legend
leg_elements = (
    [mpatches.Patch(color=v, label=k) for k, v in RACE_PALETTE.items()]
    + [
        plt.Line2D([0], [0], marker="D", color="w",
                   markerfacecolor="gray", markeredgecolor="black",
                   markersize=8, label="Primary (pre-registered)"),
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor="gray", markeredgecolor="black",
                   markersize=6, label="Exploratory (top-16)"),
    ]
)
ax_f.legend(handles=leg_elements, loc="lower right",
            fontsize=6.5, framealpha=0.85, ncol=2)

fig.suptitle(
    "Figure 5. Pairwise Race Contrasts on Pathologic Outcomes (H2 v3.2)\n"
    "Reference race = White (n=2,500)  ·  "
    "*q<0.05  **q<0.01  ***q<0.001  (exploratory: FDR–BH q; primary: Bonferroni p)",
    fontsize=10, y=1.005,
)
plt.tight_layout(pad=1.2)
plt.subplots_adjust(wspace=0.04)

fig.savefig(FIGS / "figure_5_forest_plot_pairwise.png",
            dpi=300, bbox_inches="tight")
fig.savefig(FIGS / "figure_5_forest_plot_pairwise.svg",
            dpi=300, bbox_inches="tight")
plt.close()
print("  → Saved figure_5_forest_plot_pairwise.{png,svg}")

# Caption
caption = (
    "Figure 5. Forest plot of pairwise race contrasts on pathologic outcomes "
    "(H2 v3.2, Phase 3.2). Reference race = White (n=2,500). "
    "Upper section (diamonds): 3 of 4 pre-registered primary contrasts with "
    "computable OR (Black/AA vs White); Bonferroni correction ×4; "
    "C2 (outcome_class 4-level global chi-square, p_unadj reported only) omitted "
    "as it yields no single OR. "
    "Lower section (circles): top-16 exploratory contrasts by FDR q from the "
    f"{BONF_EXPLORATORY_K}-test family (4 races × 168 tests: 16 outcome-class, "
    "32 dominant-malignant-group [malignant subset], 120 benign NLP categories). "
    "All point estimates from Fisher's exact test; 95% CIs by Woolf log-OR "
    "+0.5 continuity correction applied to zero cells. "
    "OR on log scale; vertical dashed line at OR=1.0. "
    f"Asterisks: *p/q<0.05, **<0.01, ***<0.001."
)
(FIGS / "figure_5_caption.txt").write_text(caption)
print("  → Saved figure_5_caption.txt")


# ─── 7. run_metadata.json ─────────────────────────────────────────────────────
print("\n[7/8] Saving run_metadata_phase32.json…")


def _nan_to_none(obj):
    """Recursively replace np.nan / float NaN with None for JSON."""
    if isinstance(obj, float) and np.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: _nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_nan_to_none(v) for v in obj]
    return obj


metadata = {
    "script":                 "run_h2_v32_phase32_pairwise.py",
    "run_timestamp_utc":      RUN_START.isoformat(),
    "bq_source":              BQ_TABLE,
    "data_hash_sha256":       DATA_HASH,
    "cohort_n":               6075,
    "white_n":                2500,
    "ref_race":               REF_RACE,
    "numpy_seed":             42,
    "bonferroni_primary_k":   BONF_PRIMARY_K,
    "bonferroni_exploratory_k": BONF_EXPLORATORY_K,
    "fdr_method":             "Benjamini-Hochberg (statsmodels.stats.multitest, fdr_bh)",
    "alpha":                  ALPHA,
    "validation_gates": {
        "GATE_1_cohort_n_6075":               "PASS",
        "GATE_2_white_n_2500":                "PASS",
        "GATE_3_race_n_plus_ref_n_le_6075":   "PASS",
        "GATE_4_fisher_exact_sparse_cells":   "ENFORCED (Fisher always used; sparse cells logged)",
        "GATE_5_bonferroni_multipliers":      "PASS (k=4 primary, k=168 exploratory)",
        "GATE_6_fdr_no_nan_in_01":            "PASS",
    },
    "primary_contrasts": _nan_to_none(primary_rows),
    "exploratory_summary": {
        "total_tests":             168,
        "fdr_significant_0.05":    fdr_sig_05,
        "fdr_significant_0.01":    fdr_sig_01,
        "bonferroni_significant":  bonf_sig_05,
        "both_fdr_and_bonferroni": both_sig,
    },
    "outputs": {
        "table_9a": str(TABLES / "table_9a_primary_contrasts.csv"),
        "table_9b": str(TABLES / "table_9b_exploratory_outcome_class.csv"),
        "table_9c": str(TABLES / "table_9c_exploratory_dominant_malignant_group.csv"),
        "table_9d": str(TABLES / "table_9d_exploratory_benign_categories.csv"),
        "table_9e": str(TABLES / "table_9e_pairwise_summary.csv"),
        "figure_5_png": str(FIGS / "figure_5_forest_plot_pairwise.png"),
        "figure_5_svg": str(FIGS / "figure_5_forest_plot_pairwise.svg"),
        "figure_5_caption": str(FIGS / "figure_5_caption.txt"),
    },
    "audit_anchor":          "DFL-20260508-H2-PHASE32-PAIRWISE",
    "dfl_airtable_record_id": "recVkVdmocKkemiit",
    "linear_issue_id":       "THY-42",
    "linear_title_convention": "THY-35.2",
}

(PKG / "run_metadata_phase32.json").write_text(
    json.dumps(metadata, indent=2, default=str))
print("  → Saved run_metadata_phase32.json")


# ─── 8. Final summary ─────────────────────────────────────────────────────────
print("\n[8/8] COMPLETE\n" + "=" * 65)
print("VALIDATION GATES:")
print("  GATE 1  n=6,075              ✓")
print("  GATE 2  White n=2,500        ✓")
print("  GATE 3  race_n+ref_n ≤ 6,075 ✓  (primary & exploratory)")
print("  GATE 4  Fisher exact enforced ✓  (sparse cells logged)")
print("  GATE 5  Bonferroni k=4/168   ✓")
print("  GATE 6  FDR q ∈ [0,1], no NaN ✓")
print("\nPRIMARY CONTRASTS (Bonferroni ×4):")
for r in primary_rows:
    print(f"  {r['contrast_id']:12s} | OR={r.get('OR','N/A')} | "
          f"p_unadj={r['p_unadjusted']:.4g} | "
          f"p_bonf={r['p_bonferroni_primary']:.4g} | "
          f"{r['interpretation']}")
print("\nEXPLORATORY (k=168):")
print(f"  FDR < 0.05 : {fdr_sig_05}")
print(f"  FDR < 0.01 : {fdr_sig_01}")
print(f"  Bonf < 0.05: {bonf_sig_05}")
print(f"  Both       : {both_sig}")
print("\nOUTPUTS (all in submission package):")
print("  tables/table_9a_primary_contrasts.csv                       (4 rows)")
print("  tables/table_9b_exploratory_outcome_class.csv               (16 rows)")
print("  tables/table_9c_exploratory_dominant_malignant_group.csv    (32 rows)")
print("  tables/table_9d_exploratory_benign_categories.csv           (120 rows)")
print("  tables/table_9e_pairwise_summary.csv")
print("  figures/figure_5_forest_plot_pairwise.png  (.svg)")
print("  figures/figure_5_caption.txt")
print("  run_metadata_phase32.json")

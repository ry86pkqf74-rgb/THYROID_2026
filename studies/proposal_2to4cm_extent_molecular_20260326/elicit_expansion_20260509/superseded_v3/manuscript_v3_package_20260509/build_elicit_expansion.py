#!/usr/bin/env python3
"""
build_elicit_expansion.py — produces the 2026-05-09 Elicit-driven expansion package
for EXT2-4 manuscript on top of the BigQuery canonical layer.

Inputs: hardcoded aggregate counts from `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
        captured at 2026-05-09 (see sql/ subfolder for the underlying queries).
Outputs:
    tables/table1_cohort_overall_and_2to4cm.csv
    tables/table2_malignancy_by_bethesda_size_era.csv
    tables/table2b_surgical_extent_by_bethesda_size_era.csv
    tables/table3_diagnostic_performance_thyroseq_vs_afirma.csv
    tables/table4_recurrence_by_molecular_status.csv
    figures/fig_cohort_flow_bq_20260509.png
    executive_summary_elicit_alignment.md
    data_dictionary.md
    cohort_flow_bq.csv

All proportions reported with Wilson 95% CIs.
No PHI. research_id-grain only.
"""
import csv
import json
import os
from math import sqrt
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TABLES = ROOT / "tables"
FIGURES = ROOT / "figures"
SQL_DIR = ROOT / "sql"
TABLES.mkdir(exist_ok=True)
FIGURES.mkdir(exist_ok=True)
SQL_DIR.mkdir(exist_ok=True)


def wilson_ci(k, n, z=1.96):
    """Wilson score 95% CI for a binomial proportion."""
    if n == 0:
        return (None, None, None)
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = (z / denom) * sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (p, max(0.0, center - half), min(1.0, center + half))


def fmt_pct_ci(k, n):
    if n == 0:
        return "0/0 (—)"
    p, lo, hi = wilson_ci(k, n)
    return f"{k}/{n} ({100 * p:.1f}% [{100 * lo:.1f}–{100 * hi:.1f}])"


# -----------------------------------------------------------------------------
# TABLE 1 — Cohort characteristics
# -----------------------------------------------------------------------------
# Aggregate counts pulled from manuscript_cohort_v1 (2026-05-09).
# Cohort = surg_first_date in 1999–2025 AND surg_procedure_type IN
# ('total_thyroidectomy','hemithyroidectomy').
table1_strata = [
    # stratum, n, n_female, mean_age, median_age, p25, p75, n_with_preop_size,
    # mean_preop, median_preop, n_b1..b6, n_afirma, n_thyroseq, n_named, n_with_tirads,
    # n_tirads_ge4, n_pre2015, n_2015plus, n_total_thyroid, n_lobectomy, n_malig_histo
    ("Overall", 8368, 6572, 51.4, 52, 40, 63, 2038, 1.42, 1.33,
     164, 1644, 444, 489, 216, 964, 224, 273, 497, 2061, 1293,
     3756, 4612, 4559, 3809, 3093),
    ("Initial lobectomy", 3809, 3022, 52.5, 53, 41, 64, 994, 1.38, 1.29,
     114, 999, 280, 330, 57, 173, 110, 151, 261, 1006, 599,
     1837, 1972, 0, 3809, 996),
    ("Initial total thyroidectomy", 4559, 3550, 50.5, 51, 39, 62, 1044, 1.47, 1.37,
     50, 645, 164, 159, 159, 791, 114, 122, 236, 1055, 694,
     1919, 2640, 4559, 0, 2097),
    ("Preop nodule <2 cm", 1636, 1324, 53.3, 54, 42, 65, 1636, 1.17, 1.14,
     32, 464, 182, 144, 62, 249, 57, 151, 208, 1636, 888,
     62, 1574, 821, 815, 687),
    ("Preop nodule 2–4 cm", 400, 313, 53.5, 55, 42, 65, 400, 2.47, 2.34,
     15, 87, 43, 27, 25, 109, 12, 45, 57, 400, 387,
     8, 392, 222, 178, 232),
    ("Preop nodule >4 cm", 2, 1, 46.0, 37, 37, 55, 2, 4.25, 4.16,
     0, 1, 0, 0, 0, 1, 1, 0, 1, 2, 2,
     0, 2, 1, 1, 1),
    ("Era pre-2015", 3756, 2956, 50.4, 51, 39, 61, 70, 1.30, 1.19,
     78, 790, 90, 214, 65, 363, 3, 6, 9, 70, 31,
     3756, 0, 1919, 1837, 1215),
    ("Era 2015+", 4612, 3616, 52.3, 53, 41, 64, 1968, 1.43, 1.34,
     86, 854, 354, 275, 151, 601, 221, 267, 488, 1991, 1262,
     0, 4612, 2640, 1972, 1878),
]


def write_table1():
    cols = ["Stratum", "N", "Female n (%)", "Age median [IQR]",
            "Preop nodule cm median [n with size]",
            "Bethesda I", "Bethesda II", "Bethesda III", "Bethesda IV",
            "Bethesda V", "Bethesda VI",
            "Afirma n (%)", "ThyroSeq n (%)", "Any named platform n (%)",
            "TIRADS ≥4 n (% of patients with TIRADS)",
            "Pre-2015 n (%)", "2015+ n (%)",
            "Total thyroidectomy n (%)", "Lobectomy n (%)",
            "Malignant on final pathology n (%)"]
    rows = []
    for r in table1_strata:
        (stratum, n, nf, ma, med, p25, p75, nps, meanp, medp,
         b1, b2, b3, b4, b5, b6, na, nt, nn, nti, nti4,
         npr, n15, ntot, nlob, nmal) = r
        bden = b1 + b2 + b3 + b4 + b5 + b6 if (b1 + b2 + b3 + b4 + b5 + b6) > 0 else 1
        row = {
            "Stratum": stratum,
            "N": n,
            "Female n (%)": f"{nf} ({100*nf/n:.1f})" if n else "0",
            "Age median [IQR]": f"{med} [{p25}–{p75}]",
            "Preop nodule cm median [n with size]": f"{medp:.2f} [n={nps}]",
            "Bethesda I": f"{b1} ({100*b1/bden:.1f})",
            "Bethesda II": f"{b2} ({100*b2/bden:.1f})",
            "Bethesda III": f"{b3} ({100*b3/bden:.1f})",
            "Bethesda IV": f"{b4} ({100*b4/bden:.1f})",
            "Bethesda V": f"{b5} ({100*b5/bden:.1f})",
            "Bethesda VI": f"{b6} ({100*b6/bden:.1f})",
            "Afirma n (%)": f"{na} ({100*na/n:.1f})",
            "ThyroSeq n (%)": f"{nt} ({100*nt/n:.1f})",
            "Any named platform n (%)": f"{nn} ({100*nn/n:.1f})",
            "TIRADS ≥4 n (% of patients with TIRADS)": f"{nti4} ({100*nti4/nti:.1f}, n={nti})" if nti else "—",
            "Pre-2015 n (%)": f"{npr} ({100*npr/n:.1f})",
            "2015+ n (%)": f"{n15} ({100*n15/n:.1f})",
            "Total thyroidectomy n (%)": f"{ntot} ({100*ntot/n:.1f})",
            "Lobectomy n (%)": f"{nlob} ({100*nlob/n:.1f})",
            "Malignant on final pathology n (%)": f"{nmal} ({100*nmal/n:.1f})",
        }
        rows.append(row)
    with open(TABLES / "table1_cohort_overall_and_2to4cm.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# -----------------------------------------------------------------------------
# TABLE 2 — Malignancy rate by Bethesda × size × era
# -----------------------------------------------------------------------------
# Pulled 2026-05-09 from manuscript_cohort_v1.
# Malignancy = histology_final populated with PTC/MTC/follicular ca/anaplastic/etc
# ("strict" — NIFTP & FTUMP excluded). Inclusive variant adds NIFTP & borderline.
# Rows with n_with_histology = 0 are reported as "no histology resolved".
table2_input = [
    # (bethesda, size_band, era, n_total, n_with_histology, n_malignant_strict, n_malignant_inclusive, n_niftp, n_borderline)
    (1, "lt2cm", "pre_2015", 1, 0, 0, 0, 0, 0),
    (1, "lt2cm", "2015_plus", 31, 7, 7, 7, 0, 0),
    (1, "2to4cm", "2015_plus", 15, 2, 2, 2, 0, 0),
    (1, "unknown", "pre_2015", 77, 31, 31, 31, 0, 0),
    (1, "unknown", "2015_plus", 40, 8, 7, 8, 1, 0),
    (2, "lt2cm", "pre_2015", 36, 7, 7, 7, 0, 0),
    (2, "lt2cm", "2015_plus", 428, 76, 69, 75, 6, 0),
    (2, "2to4cm", "pre_2015", 5, 1, 1, 1, 0, 0),
    (2, "2to4cm", "2015_plus", 82, 13, 11, 13, 2, 0),
    (2, "gt4cm", "2015_plus", 1, 0, 0, 0, 0, 0),
    (2, "unknown", "pre_2015", 749, 125, 124, 125, 0, 1),
    (2, "unknown", "2015_plus", 343, 67, 60, 66, 6, 0),
    (3, "lt2cm", "pre_2015", 1, 0, 0, 0, 0, 0),
    (3, "lt2cm", "2015_plus", 181, 94, 83, 94, 10, 1),
    (3, "2to4cm", "2015_plus", 43, 26, 25, 26, 1, 0),
    (3, "unknown", "pre_2015", 89, 42, 42, 42, 0, 0),
    (3, "unknown", "2015_plus", 130, 61, 57, 61, 4, 0),
    (4, "lt2cm", "pre_2015", 3, 0, 0, 0, 0, 0),
    (4, "lt2cm", "2015_plus", 141, 73, 66, 73, 7, 0),
    (4, "2to4cm", "2015_plus", 27, 10, 10, 10, 0, 0),
    (4, "unknown", "pre_2015", 211, 77, 76, 77, 1, 0),
    (4, "unknown", "2015_plus", 107, 59, 57, 59, 2, 0),
    (5, "lt2cm", "pre_2015", 2, 2, 2, 2, 0, 0),
    (5, "lt2cm", "2015_plus", 60, 56, 56, 56, 0, 0),
    (5, "2to4cm", "2015_plus", 25, 23, 21, 23, 2, 0),
    (5, "unknown", "pre_2015", 63, 52, 51, 52, 0, 1),
    (5, "unknown", "2015_plus", 66, 59, 58, 59, 1, 0),
    (6, "lt2cm", "pre_2015", 1, 0, 0, 0, 0, 0),
    (6, "lt2cm", "2015_plus", 248, 204, 202, 204, 2, 0),
    (6, "2to4cm", "pre_2015", 2, 2, 2, 2, 0, 0),
    (6, "2to4cm", "2015_plus", 107, 100, 99, 100, 1, 0),
    (6, "gt4cm", "2015_plus", 1, 1, 1, 1, 0, 0),
    (6, "unknown", "pre_2015", 360, 331, 331, 331, 0, 0),
    (6, "unknown", "2015_plus", 245, 213, 211, 213, 2, 0),
]


def write_table2():
    cols = ["Bethesda", "Size band", "Era", "n_total",
            "n_with_resolved_histology",
            "Malignant (strict, NIFTP excluded) n (%) [Wilson 95% CI]",
            "Malignant (inclusive, NIFTP-as-malignant) n (%) [Wilson 95% CI]",
            "NIFTP n", "Borderline (FTUMP/HTT) n"]
    with open(TABLES / "table2_malignancy_by_bethesda_size_era.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for (b, sz, era, ntot, nh, ms, mi, nf, nb) in table2_input:
            ms_disp = fmt_pct_ci(ms, nh) if nh else f"0/0 (—)"
            mi_disp = fmt_pct_ci(mi, nh) if nh else f"0/0 (—)"
            w.writerow([f"Bethesda {b}", sz, era, ntot, nh, ms_disp, mi_disp, nf, nb])


# Surgical extent by Bethesda × size × era
table2b_input = [
    # (bethesda, size_band, era, n_total, n_total_thyroid, n_lobectomy)
    (1, "lt2cm", "pre_2015", 1, 0, 1),
    (1, "lt2cm", "2015_plus", 31, 10, 21),
    (1, "2to4cm", "2015_plus", 15, 2, 13),
    (1, "unknown", "pre_2015", 77, 28, 49),
    (1, "unknown", "2015_plus", 40, 10, 30),
    (2, "lt2cm", "pre_2015", 36, 7, 29),
    (2, "lt2cm", "2015_plus", 428, 165, 263),
    (2, "2to4cm", "pre_2015", 5, 1, 4),
    (2, "2to4cm", "2015_plus", 82, 38, 44),
    (2, "gt4cm", "2015_plus", 1, 0, 1),
    (2, "unknown", "pre_2015", 749, 276, 473),
    (2, "unknown", "2015_plus", 343, 158, 185),
    (3, "lt2cm", "pre_2015", 1, 0, 1),
    (3, "lt2cm", "2015_plus", 181, 61, 120),
    (3, "2to4cm", "2015_plus", 43, 13, 30),
    (3, "unknown", "pre_2015", 89, 38, 51),
    (3, "unknown", "2015_plus", 130, 52, 78),
    (4, "lt2cm", "pre_2015", 3, 0, 3),
    (4, "lt2cm", "2015_plus", 141, 47, 94),
    (4, "2to4cm", "2015_plus", 27, 4, 23),
    (4, "unknown", "pre_2015", 211, 64, 147),
    (4, "unknown", "2015_plus", 107, 44, 63),
    (5, "lt2cm", "pre_2015", 2, 0, 2),
    (5, "lt2cm", "2015_plus", 60, 44, 16),
    (5, "2to4cm", "2015_plus", 25, 17, 8),
    (5, "unknown", "pre_2015", 63, 44, 19),
    (5, "unknown", "2015_plus", 66, 54, 12),
    (6, "lt2cm", "pre_2015", 1, 0, 1),
    (6, "lt2cm", "2015_plus", 248, 192, 56),
    (6, "2to4cm", "pre_2015", 2, 0, 2),
    (6, "2to4cm", "2015_plus", 107, 89, 18),
    (6, "gt4cm", "2015_plus", 1, 1, 0),
    (6, "unknown", "pre_2015", 360, 309, 51),
    (6, "unknown", "2015_plus", 245, 200, 45),
]


def write_table2b():
    cols = ["Bethesda", "Size band", "Era", "n_total",
            "Total thyroidectomy n (%) [Wilson 95% CI]",
            "Lobectomy n (%)"]
    with open(TABLES / "table2b_surgical_extent_by_bethesda_size_era.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for (b, sz, era, ntot, ntot_th, nlob) in table2b_input:
            tt = fmt_pct_ci(ntot_th, ntot) if ntot else "0/0 (—)"
            w.writerow([f"Bethesda {b}", sz, era, ntot, tt, f"{nlob}/{ntot} ({100*nlob/ntot:.1f}%)" if ntot else "—"])


# -----------------------------------------------------------------------------
# TABLE 3 — Diagnostic performance ThyroSeq vs Afirma in Bethesda III/IV
# -----------------------------------------------------------------------------
# Source: manuscript_cohort_v1 filter Bethesda IN (3,4) AND mol_platform IN ('Afirma','ThyroSeq')
# Histology truth: histology_final NULL among surgical patients => benign;
# malignant categories described in SQL; NIFTP & borderline split out.
# Molecular call: positive if BRAF/RAS/TERT positive OR molecular_risk_tier in
# {high, intermediate, low_intermediate}; else negative.
# IMPORTANT: this is a *post-surgical* cohort; molecular-negative patients who
# avoided surgery are not observable. Sensitivity/Specificity therefore reflect
# CONDITIONAL-ON-SURGERY operating characteristics, NOT population-level test
# performance. PPV is least biased among the four metrics.
table3_input = [
    # (bethesda, mol_platform, size_band, mol_call, histo_class, n)
    (3, "Afirma", "2to4cm", "negative", "malignant", 1),
    (3, "Afirma", "2to4cm", "positive", "benign", 1),
    (3, "Afirma", "2to4cm", "positive", "malignant", 1),
    (3, "Afirma", "lt2cm", "negative", "benign", 4),
    (3, "Afirma", "lt2cm", "negative", "malignant", 6),
    (3, "Afirma", "lt2cm", "positive", "benign", 3),
    (3, "Afirma", "lt2cm", "positive", "malignant", 3),
    (3, "Afirma", "lt2cm", "positive", "niftp", 1),
    (3, "Afirma", "unknown_size", "negative", "benign", 1),
    (3, "Afirma", "unknown_size", "negative", "malignant", 4),
    (3, "Afirma", "unknown_size", "positive", "benign", 13),
    (3, "Afirma", "unknown_size", "positive", "malignant", 6),
    (4, "Afirma", "2to4cm", "negative", "malignant", 1),
    (4, "Afirma", "2to4cm", "positive", "malignant", 1),
    (4, "Afirma", "lt2cm", "negative", "benign", 2),
    (4, "Afirma", "lt2cm", "negative", "malignant", 3),
    (4, "Afirma", "lt2cm", "positive", "benign", 3),
    (4, "Afirma", "lt2cm", "positive", "malignant", 8),
    (4, "Afirma", "unknown_size", "negative", "benign", 4),
    (4, "Afirma", "unknown_size", "negative", "malignant", 4),
    (4, "Afirma", "unknown_size", "positive", "benign", 10),
    (4, "Afirma", "unknown_size", "positive", "malignant", 13),
    (3, "ThyroSeq", "2to4cm", "negative", "benign", 9),
    (3, "ThyroSeq", "2to4cm", "negative", "malignant", 6),
    (3, "ThyroSeq", "2to4cm", "positive", "benign", 1),
    (3, "ThyroSeq", "2to4cm", "positive", "malignant", 6),
    (3, "ThyroSeq", "lt2cm", "negative", "benign", 29),
    (3, "ThyroSeq", "lt2cm", "negative", "borderline", 1),
    (3, "ThyroSeq", "lt2cm", "negative", "malignant", 20),
    (3, "ThyroSeq", "lt2cm", "negative", "niftp", 4),
    (3, "ThyroSeq", "lt2cm", "positive", "benign", 6),
    (3, "ThyroSeq", "lt2cm", "positive", "malignant", 13),
    (3, "ThyroSeq", "unknown_size", "negative", "benign", 11),
    (3, "ThyroSeq", "unknown_size", "negative", "malignant", 6),
    (3, "ThyroSeq", "unknown_size", "negative", "niftp", 1),
    (3, "ThyroSeq", "unknown_size", "positive", "benign", 2),
    (3, "ThyroSeq", "unknown_size", "positive", "malignant", 8),
    (4, "ThyroSeq", "2to4cm", "negative", "benign", 4),
    (4, "ThyroSeq", "2to4cm", "positive", "malignant", 2),
    (4, "ThyroSeq", "lt2cm", "negative", "benign", 18),
    (4, "ThyroSeq", "lt2cm", "negative", "malignant", 9),
    (4, "ThyroSeq", "lt2cm", "negative", "niftp", 1),
    (4, "ThyroSeq", "lt2cm", "positive", "benign", 4),
    (4, "ThyroSeq", "lt2cm", "positive", "malignant", 8),
    (4, "ThyroSeq", "unknown_size", "negative", "benign", 4),
    (4, "ThyroSeq", "unknown_size", "negative", "malignant", 3),
    (4, "ThyroSeq", "unknown_size", "positive", "benign", 2),
    (4, "ThyroSeq", "unknown_size", "positive", "malignant", 5),
]


def aggregate_2x2(rows, *, niftp_as_malignant=False):
    """Return dict keyed by (platform, bethesda_label, size_label) → (TP, FP, FN, TN)."""
    out = {}
    malig_labels = {"malignant", "borderline"}
    if niftp_as_malignant:
        malig_labels.add("niftp")
    for (beth, plat, size, call, histo, n) in rows:
        is_malig = histo in malig_labels
        for beth_key in [f"B{beth}", "B3+B4"]:
            for size_key in [size, "all_sizes"]:
                key = (plat, beth_key, size_key)
                if key not in out:
                    out[key] = [0, 0, 0, 0]  # TP FP FN TN
                if call == "positive" and is_malig:
                    out[key][0] += n
                elif call == "positive" and not is_malig:
                    out[key][1] += n
                elif call == "negative" and is_malig:
                    out[key][2] += n
                elif call == "negative" and not is_malig:
                    out[key][3] += n
    return out


def perf_metrics(tp, fp, fn, tn):
    out = {"TP": tp, "FP": fp, "FN": fn, "TN": tn,
           "n_total": tp + fp + fn + tn,
           "n_positive_call": tp + fp,
           "n_negative_call": fn + tn,
           "n_malignant": tp + fn,
           "n_benign": fp + tn}
    if tp + fn:
        p, lo, hi = wilson_ci(tp, tp + fn)
        out["Sensitivity (CI)"] = f"{100*p:.1f}% [{100*lo:.1f}–{100*hi:.1f}]"
    else:
        out["Sensitivity (CI)"] = "—"
    if tn + fp:
        p, lo, hi = wilson_ci(tn, tn + fp)
        out["Specificity (CI)"] = f"{100*p:.1f}% [{100*lo:.1f}–{100*hi:.1f}]"
    else:
        out["Specificity (CI)"] = "—"
    if tp + fp:
        p, lo, hi = wilson_ci(tp, tp + fp)
        out["PPV (CI)"] = f"{100*p:.1f}% [{100*lo:.1f}–{100*hi:.1f}]"
    else:
        out["PPV (CI)"] = "—"
    if tn + fn:
        p, lo, hi = wilson_ci(tn, tn + fn)
        out["NPV (CI)"] = f"{100*p:.1f}% [{100*lo:.1f}–{100*hi:.1f}]"
    else:
        out["NPV (CI)"] = "—"
    return out


def write_table3():
    rows_to_write = []
    for label, niftp_flag in [("Strict (NIFTP=benign)", False),
                              ("Inclusive (NIFTP=malignant)", True)]:
        agg = aggregate_2x2(table3_input, niftp_as_malignant=niftp_flag)
        for (plat, beth, size), (tp, fp, fn, tn) in sorted(agg.items()):
            m = perf_metrics(tp, fp, fn, tn)
            rows_to_write.append({
                "Histology rule": label,
                "Platform": plat,
                "Bethesda": beth,
                "Size band": size,
                "TP": tp, "FP": fp, "FN": fn, "TN": tn,
                "n_total": m["n_total"],
                "n_positive_call": m["n_positive_call"],
                "n_malignant": m["n_malignant"],
                "n_benign": m["n_benign"],
                "Sensitivity (95% CI)": m["Sensitivity (CI)"],
                "Specificity (95% CI)": m["Specificity (CI)"],
                "PPV (95% CI)": m["PPV (CI)"],
                "NPV (95% CI)": m["NPV (CI)"],
            })
    cols = list(rows_to_write[0].keys())
    with open(TABLES / "table3_diagnostic_performance_thyroseq_vs_afirma.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows_to_write:
            w.writerow(r)


# -----------------------------------------------------------------------------
# TABLE 4 — Path-proven recurrence by molecular status
# -----------------------------------------------------------------------------
# Per user direction: "include based on which patients had documented by biopsy or
# operative pathology" — corresponds exactly to canonical_recurrence_resolved_v1
# .recurrence_path_proven (TRUE = biopsy- or op-path-confirmed recurrence).
# Numerator: n_path_proven_recurrence. Denominator: malignant histology cases only.
# Reported with Wilson 95% CI. NOT survival/long-term outcomes (outside this scope).
table4_input = [
    ("Afirma", "high_risk_mutation", 64, 0, 31, 32),
    ("Afirma", "ras_only", 8, 0, 6, 6),
    ("Afirma", "wild_type", 65, 0, 38, 39),
    ("Other / historical / in-house", "high_risk_mutation", 86, 4, 48, 49),
    ("Other / historical / in-house", "no_result", 28, 2, 3, 5),
    ("Other / historical / in-house", "ras_only", 47, 1, 27, 27),
    ("Other / historical / in-house", "wild_type", 2377, 61, 678, 755),
    ("ThyroSeq", "high_risk_mutation", 32, 2, 24, 24),
    ("ThyroSeq", "no_result", 3, 0, 1, 2),
    ("ThyroSeq", "ras_only", 38, 1, 32, 33),
    ("ThyroSeq", "wild_type", 88, 1, 64, 65),
    ("Untested", "high_risk_mutation", 1, 0, 1, 1),
    ("Untested", "no_result", 2, 0, 0, 0),
    ("Untested", "ras_only", 1, 0, 1, 1),
    ("Untested", "wild_type", 253, 4, 10, 25),
]


def write_table4():
    cols = ["Molecular group", "Mutation class", "n_malignant",
            "Path-proven recurrence n (%) [Wilson 95% CI]",
            "Structural recurrence n (% of n_malignant)",
            "Any recurrence (path/imaging/biochemical) n (%)"]
    with open(TABLES / "table4_recurrence_by_molecular_status.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for (mg, mc, nm, npp, nsr, nany) in table4_input:
            pp = fmt_pct_ci(npp, nm) if nm else "—"
            sr = f"{nsr}/{nm} ({100*nsr/nm:.1f}%)" if nm else "—"
            an = f"{nany}/{nm} ({100*nany/nm:.1f}%)" if nm else "—"
            w.writerow([mg, mc, nm, pp, sr, an])


# -----------------------------------------------------------------------------
# COHORT FLOW
# -----------------------------------------------------------------------------
cohort_flow = [
    ("All patients in canonical_patient_master (1999–2025)", 10871),
    ("Patients with first surgery date resolved", 8731),
    ("Surgical extent ∈ {hemithyroidectomy, total_thyroidectomy}", 8368),
    ("    of which initial hemithyroidectomy", 3809),
    ("    of which initial total thyroidectomy", 4559),
    ("With Bethesda category resolved", 3921),
    ("    Bethesda III (AUS/FLUS)", 444),
    ("    Bethesda IV (FN/SFN)", 489),
    ("    Bethesda III+IV (indeterminate cytology)", 933),
    ("With named molecular platform (Afirma or ThyroSeq)", 497),
    ("Molecular head-to-head subset (Bethesda III/IV + Afirma|ThyroSeq + final histology)", 238),
    ("    Afirma B3+B4 + histology", 96),
    ("    ThyroSeq B3+B4 + histology", 142),
    ("Preop nodule 2.0–4.0 cm (any Bethesda)", 400),
    ("    Pre-2015 surgery", 8),
    ("    2015+ surgery", 392),
    ("    With named platform (Afirma|ThyroSeq)", 57),
]


def write_cohort_flow():
    with open(ROOT / "cohort_flow_bq.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Step", "n"])
        for label, n in cohort_flow:
            w.writerow([label, n])


def make_cohort_flow_figure():
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(11, 7), dpi=200)
    labels = [c[0] for c in cohort_flow]
    counts = [c[1] for c in cohort_flow]
    y_pos = list(range(len(labels)))[::-1]
    bars = ax.barh(y_pos, counts, color="#3b6ea5", edgecolor="white")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xscale("log")
    ax.set_xlabel("n (log scale)")
    ax.set_title("EXT2-4 Elicit-expansion cohort flow (BigQuery canonical layer, 2026-05-09)",
                 fontsize=10)
    for bar, n in zip(bars, counts):
        ax.text(n * 1.05, bar.get_y() + bar.get_height() / 2, f"n={n:,}",
                va="center", fontsize=7)
    plt.tight_layout()
    fig.savefig(FIGURES / "fig_cohort_flow_bq_20260509.png", dpi=200,
                bbox_inches="tight")
    plt.close(fig)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    write_table1()
    write_table2()
    write_table2b()
    write_table3()
    write_table4()
    write_cohort_flow()
    make_cohort_flow_figure()
    print(json.dumps({
        "wrote": [
            "tables/table1_cohort_overall_and_2to4cm.csv",
            "tables/table2_malignancy_by_bethesda_size_era.csv",
            "tables/table2b_surgical_extent_by_bethesda_size_era.csv",
            "tables/table3_diagnostic_performance_thyroseq_vs_afirma.csv",
            "tables/table4_recurrence_by_molecular_status.csv",
            "cohort_flow_bq.csv",
            "figures/fig_cohort_flow_bq_20260509.png",
        ],
    }, indent=2))

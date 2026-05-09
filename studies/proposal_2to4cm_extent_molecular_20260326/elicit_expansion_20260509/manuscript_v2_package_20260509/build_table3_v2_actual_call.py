#!/usr/bin/env python3
"""
build_table3_v2_actual_call.py — corrected Table 3 using the platform-reported
test call from canonical_molecular_genetics_v2 (not the derived call from
manuscript_cohort_v1's molecular_risk_tier + mutation flags).

v3 update (2026-05-09): cells refreshed after ThyroSeq band-coverage backfill
(mig_321). ThyroSeq unknown_or_excluded in Bethesda III/IV evaluable set
reduced from ~165 → 17. Run DFL: DFL-20260509-EXT2-4-THYROSEQ-BAND-BACKFILL.
Coverage gate: 83.8% (below 95% target; 141 rows remain manual_review).

Inputs: hardcoded aggregate counts from BQ canonical_molecular_genetics_v2
        captured 2026-05-09. SQL in sql/04b_table3_v2_actual_reported_call.sql.

Reported-call rules:
  Afirma test-positive  = overall_result_class IN ('suspicious','positive')
  Afirma test-negative  = overall_result_class = 'negative'
  Afirma not classifiable = 'other' / 'non_diagnostic' / NULL  → excluded from 2x2,
                             reported in a separate descriptive row
  ThyroSeq test-positive = rom_descriptor IN ('HIGH','INTERMEDIATE-HIGH','INTERMEDIATEHIGH')
                            OR overall_result_class = 'positive'
  ThyroSeq test-negative = rom_descriptor IN ('LOW','INTERMEDIATE-LOW')
                            OR overall_result_class = 'negative'
  ThyroSeq INTERMEDIATE   = rom_descriptor = 'INTERMEDIATE'  → THIRD CATEGORY,
                             reported separately (per user direction). Not pooled
                             into the binary 2x2.
  ThyroSeq not classifiable = rom_descriptor NULL AND overall_result_class NOT IN ('positive','negative')

When a patient has multiple molecular tests, the latest preoperative test is used
(test_date <= surg_first_date), falling back to the most recent test.

Outputs (in addition to existing files):
  tables/table3_v2_diagnostic_performance_actual_reported_call.csv
  tables/table3_v2_rom_pct_descriptive_stats.csv
  tables/superseded/  (old Table 3 moved here with a header note in
                      SUPERSEDED_NOTE.md explaining why)
"""
import csv
import os
import shutil
from math import sqrt
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TABLES = ROOT / "tables"
SUPERSEDED = TABLES / "superseded"
SUPERSEDED.mkdir(exist_ok=True, parents=True)


def wilson_ci(k, n, z=1.96):
    if n == 0:
        return (None, None, None)
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = (z / denom) * sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (p, max(0.0, center - half), min(1.0, center + half))


# Cells captured 2026-05-09 from canonical_molecular_genetics_v2 (post-join to
# manuscript_cohort_v1 surgical Bethesda III/IV cohort, latest preop test per
# patient via ROW_NUMBER).
#
# v3 update 2026-05-09: ThyroSeq band-coverage backfill applied (mig_321).
# ThyroSeq unknown_or_excluded: v2=~165 → v3=17 (reduction: 148 patients).
# DFL: DFL-20260509-EXT2-4-THYROSEQ-BAND-BACKFILL.
# Coverage gate: 83.8% frac_with_band (below 95% target; 141 rows unresolvable).
#
# Schema: (bethesda, platform, size_band, reported_call, histo_class, n,
#          mean_rom_pct, median_rom_pct, p25_rom_pct, p75_rom_pct, n_with_rom_pct)
# reported_call ∈ {positive, negative, intermediate, unknown_or_excluded}
# histo_class ∈ {malignant, niftp, borderline, benign, benign_adenoma}
cells = [
    # --- Afirma rows (unchanged from v2) ---
    (3, "Afirma", "2to4cm", "negative", "malignant", 1, None, None, None, None, 0),
    (3, "Afirma", "2to4cm", "positive", "malignant", 1, None, None, None, None, 0),
    (3, "Afirma", "lt2cm", "negative", "benign", 1, None, None, None, None, 0),
    (3, "Afirma", "lt2cm", "positive", "benign", 4, None, None, None, None, 0),
    (3, "Afirma", "lt2cm", "positive", "malignant", 7, None, None, None, None, 0),
    (3, "Afirma", "lt2cm", "unknown_or_excluded", "malignant", 1, None, None, None, None, 0),
    (3, "Afirma", "unknown_size", "negative", "benign", 3, None, None, None, None, 0),
    (3, "Afirma", "unknown_size", "negative", "malignant", 1, None, None, None, None, 0),
    (3, "Afirma", "unknown_size", "positive", "benign", 7, None, None, None, None, 0),
    (3, "Afirma", "unknown_size", "positive", "malignant", 9, None, None, None, None, 0),
    (3, "Afirma", "unknown_size", "unknown_or_excluded", "benign", 1, None, None, None, None, 0),
    (4, "Afirma", "2to4cm", "positive", "malignant", 2, None, None, None, None, 0),
    (4, "Afirma", "lt2cm", "negative", "malignant", 1, None, None, None, None, 0),
    (4, "Afirma", "lt2cm", "positive", "benign", 3, None, None, None, None, 0),
    (4, "Afirma", "lt2cm", "positive", "malignant", 10, None, None, None, None, 0),
    (4, "Afirma", "lt2cm", "unknown_or_excluded", "benign", 1, None, None, None, None, 0),
    (4, "Afirma", "unknown_size", "negative", "benign", 1, None, None, None, None, 0),
    (4, "Afirma", "unknown_size", "negative", "malignant", 2, None, None, None, None, 0),
    (4, "Afirma", "unknown_size", "positive", "benign", 10, None, None, None, None, 0),
    (4, "Afirma", "unknown_size", "positive", "malignant", 13, None, None, None, None, 0),
    (4, "Afirma", "unknown_size", "unknown_or_excluded", "benign", 2, None, None, None, None, 0),
    (4, "Afirma", "unknown_size", "unknown_or_excluded", "malignant", 1, None, None, None, None, 0),
    # --- ThyroSeq rows — REFRESHED v3 (post mig_321 band backfill 2026-05-09) ---
    (3, "ThyroSeq", "2to4cm", "intermediate", "benign", 3, 46.7, 50.0, 40.0, 50.0, 3),
    (3, "ThyroSeq", "2to4cm", "intermediate", "malignant", 4, 48.8, 50.0, 40.0, 50.0, 4),
    (3, "ThyroSeq", "2to4cm", "negative", "benign", 7, 3.0, 3.0, 3.0, 3.0, 7),
    (3, "ThyroSeq", "2to4cm", "negative", "malignant", 1, 35.0, 35.0, 35.0, 35.0, 1),
    (3, "ThyroSeq", "2to4cm", "positive", "benign", 1, 60.0, 60.0, 60.0, 60.0, 1),
    (3, "ThyroSeq", "2to4cm", "positive", "malignant", 11, 70.4, 70.0, 60.0, 75.0, 11),
    (3, "ThyroSeq", "2to4cm", "unknown_or_excluded", "malignant", 1, None, None, None, None, 0),
    (3, "ThyroSeq", "lt2cm", "intermediate", "benign", 8, 47.5, 50.0, 40.0, 50.0, 8),
    (3, "ThyroSeq", "lt2cm", "intermediate", "malignant", 4, 53.3, 50.0, 50.0, 60.0, 3),
    (3, "ThyroSeq", "lt2cm", "intermediate", "niftp", 1, 50.0, 50.0, 50.0, 50.0, 1),
    (3, "ThyroSeq", "lt2cm", "negative", "benign", 26, 5.9, 3.0, 3.0, 4.0, 20),
    (3, "ThyroSeq", "lt2cm", "negative", "malignant", 19, 4.6, 3.0, 3.0, 3.0, 12),
    (3, "ThyroSeq", "lt2cm", "negative", "niftp", 2, 3.3, 3.0, 3.0, 3.5, 2),
    (3, "ThyroSeq", "lt2cm", "positive", "benign", 12, 67.4, 70.0, 60.0, 70.0, 12),
    (3, "ThyroSeq", "lt2cm", "positive", "borderline", 1, 70.0, 70.0, 70.0, 70.0, 1),
    (3, "ThyroSeq", "lt2cm", "positive", "malignant", 24, 79.0, 70.0, 70.0, 90.0, 23),
    (3, "ThyroSeq", "lt2cm", "positive", "niftp", 4, 68.8, 70.0, 60.0, 70.0, 4),
    (3, "ThyroSeq", "lt2cm", "unknown_or_excluded", "benign", 4, None, None, None, None, 0),
    (3, "ThyroSeq", "lt2cm", "unknown_or_excluded", "malignant", 4, None, None, None, None, 0),
    (3, "ThyroSeq", "unknown_size", "intermediate", "benign", 4, 51.0, 50.0, 44.0, 50.0, 4),
    (3, "ThyroSeq", "unknown_size", "intermediate", "malignant", 3, 63.3, 70.0, 50.0, 70.0, 3),
    (3, "ThyroSeq", "unknown_size", "negative", "benign", 12, 3.3, 3.0, 3.0, 3.0, 8),
    (3, "ThyroSeq", "unknown_size", "negative", "malignant", 6, 4.3, 5.0, 3.0, 5.0, 3),
    (3, "ThyroSeq", "unknown_size", "positive", "benign", 7, 65.0, 70.0, 60.0, 70.0, 7),
    (3, "ThyroSeq", "unknown_size", "positive", "malignant", 11, 73.2, 70.0, 70.0, 75.0, 11),
    (3, "ThyroSeq", "unknown_size", "positive", "niftp", 2, 65.0, 60.0, 60.0, 70.0, 2),
    (3, "ThyroSeq", "unknown_size", "unknown_or_excluded", "benign", 1, None, None, None, None, 0),
    (3, "ThyroSeq", "unknown_size", "unknown_or_excluded", "malignant", 1, None, None, None, None, 0),
    (4, "ThyroSeq", "2to4cm", "negative", "benign", 5, 4.0, 3.0, 3.0, 3.5, 5),
    (4, "ThyroSeq", "2to4cm", "negative", "malignant", 1, 4.0, 4.0, 4.0, 4.0, 1),
    (4, "ThyroSeq", "2to4cm", "positive", "benign", 3, 61.7, 60.0, 55.0, 70.0, 3),
    (4, "ThyroSeq", "2to4cm", "positive", "malignant", 2, 89.5, 80.0, 80.0, 99.0, 2),
    (4, "ThyroSeq", "lt2cm", "intermediate", "malignant", 5, 53.0, 50.0, 50.0, 50.0, 5),
    (4, "ThyroSeq", "lt2cm", "intermediate", "niftp", 1, 35.0, 35.0, 35.0, 35.0, 1),
    (4, "ThyroSeq", "lt2cm", "negative", "benign", 20, 3.3, 3.0, 3.0, 3.5, 18),
    (4, "ThyroSeq", "lt2cm", "negative", "malignant", 4, 9.8, 3.0, 3.0, 3.0, 4),
    (4, "ThyroSeq", "lt2cm", "negative", "niftp", 1, 3.0, 3.0, 3.0, 3.0, 1),
    (4, "ThyroSeq", "lt2cm", "positive", "benign", 8, 67.5, 70.0, 55.0, 70.0, 8),
    (4, "ThyroSeq", "lt2cm", "positive", "malignant", 17, 72.4, 70.0, 70.0, 75.0, 17),
    (4, "ThyroSeq", "lt2cm", "positive", "niftp", 2, 62.5, 60.0, 60.0, 65.0, 2),
    (4, "ThyroSeq", "lt2cm", "unknown_or_excluded", "benign", 1, None, None, None, None, 0),
    (4, "ThyroSeq", "lt2cm", "unknown_or_excluded", "malignant", 2, None, None, None, None, 0),
    (4, "ThyroSeq", "unknown_size", "intermediate", "benign", 2, 45.0, 40.0, 40.0, 50.0, 2),
    (4, "ThyroSeq", "unknown_size", "intermediate", "malignant", 2, 50.0, 50.0, 50.0, 50.0, 2),
    (4, "ThyroSeq", "unknown_size", "negative", "benign", 4, 4.8, 4.0, 3.0, 7.5, 3),
    (4, "ThyroSeq", "unknown_size", "negative", "malignant", 2, None, None, None, None, 0),
    (4, "ThyroSeq", "unknown_size", "positive", "benign", 4, 72.5, 70.0, 70.0, 70.0, 4),
    (4, "ThyroSeq", "unknown_size", "positive", "malignant", 10, 71.9, 70.0, 60.0, 70.0, 8),
    (4, "ThyroSeq", "unknown_size", "positive", "niftp", 1, 75.0, 75.0, 75.0, 75.0, 1),
    (4, "ThyroSeq", "unknown_size", "unknown_or_excluded", "benign", 1, None, None, None, None, 0),
    (4, "ThyroSeq", "unknown_size", "unknown_or_excluded", "malignant", 2, None, None, None, None, 0),
]


def aggregate_2x2(rows, *, niftp_as_malignant=False):
    """Return TP/FP/FN/TN by (platform, bethesda_label, size_label).
    INTERMEDIATE and unknown_or_excluded calls are NOT counted in the 2x2.
    They are tracked separately in `extras`.
    """
    out = {}
    extras = {}  # (platform, beth, size) -> {'intermediate_n': X, 'intermediate_malig': Y, 'unclass_n': X, 'unclass_malig': Y}
    malig_labels = {"malignant", "borderline"}
    if niftp_as_malignant:
        malig_labels.add("niftp")
    for (beth, plat, size, call, histo, n, *_rest) in rows:
        is_malig = histo in malig_labels
        for beth_key in [f"B{beth}", "B3+B4"]:
            for size_key in [size, "all_sizes"]:
                key = (plat, beth_key, size_key)
                if key not in out:
                    out[key] = [0, 0, 0, 0]
                if key not in extras:
                    extras[key] = {"intermediate_n": 0, "intermediate_malig": 0,
                                   "unclass_n": 0, "unclass_malig": 0}
                if call == "positive" and is_malig:
                    out[key][0] += n
                elif call == "positive" and not is_malig:
                    out[key][1] += n
                elif call == "negative" and is_malig:
                    out[key][2] += n
                elif call == "negative" and not is_malig:
                    out[key][3] += n
                elif call == "intermediate":
                    extras[key]["intermediate_n"] += n
                    if is_malig:
                        extras[key]["intermediate_malig"] += n
                elif call == "unknown_or_excluded":
                    extras[key]["unclass_n"] += n
                    if is_malig:
                        extras[key]["unclass_malig"] += n
    return out, extras


def perf_row(plat, beth, size, tp, fp, fn, tn, ex):
    n_2x2 = tp + fp + fn + tn
    out = {
        "Platform": plat, "Bethesda": beth, "Size band": size,
        "TP (test-pos AND malignant)": tp,
        "FP (test-pos AND benign)": fp,
        "FN (test-neg AND malignant)": fn,
        "TN (test-neg AND benign)": tn,
        "n_2x2": n_2x2,
        "Intermediate-call n (% malig)": (
            f"{ex['intermediate_n']} ({100*ex['intermediate_malig']/ex['intermediate_n']:.1f}% malig)"
            if ex["intermediate_n"] else "0"
        ),
        "Not-classifiable-call n (% malig)": (
            f"{ex['unclass_n']} ({100*ex['unclass_malig']/ex['unclass_n']:.1f}% malig)"
            if ex["unclass_n"] else "0"
        ),
    }

    def fmt(p, lo, hi):
        if p is None:
            return "—"
        return f"{100*p:.1f}% [{100*lo:.1f}–{100*hi:.1f}]"

    p_se, lo, hi = wilson_ci(tp, tp + fn) if (tp + fn) else (None, None, None)
    out["Sensitivity (95% CI)"] = fmt(p_se, lo, hi) if p_se is not None else "—"
    p_sp, lo, hi = wilson_ci(tn, tn + fp) if (tn + fp) else (None, None, None)
    out["Specificity (95% CI)"] = fmt(p_sp, lo, hi) if p_sp is not None else "—"
    p_ppv, lo, hi = wilson_ci(tp, tp + fp) if (tp + fp) else (None, None, None)
    out["PPV (95% CI)"] = fmt(p_ppv, lo, hi) if p_ppv is not None else "—"
    p_npv, lo, hi = wilson_ci(tn, tn + fn) if (tn + fn) else (None, None, None)
    out["NPV (95% CI)"] = fmt(p_npv, lo, hi) if p_npv is not None else "—"
    return out


def write_table3_v2():
    rows_to_write = []
    for label, niftp_flag in [("Strict (NIFTP=benign)", False),
                              ("Inclusive (NIFTP=malignant)", True)]:
        agg, extras = aggregate_2x2(cells, niftp_as_malignant=niftp_flag)
        for (plat, beth, size), (tp, fp, fn, tn) in sorted(agg.items()):
            row = {"Histology rule": label}
            row.update(perf_row(plat, beth, size, tp, fp, fn, tn, extras[(plat, beth, size)]))
            rows_to_write.append(row)
    cols = ["Histology rule", "Platform", "Bethesda", "Size band",
            "TP (test-pos AND malignant)", "FP (test-pos AND benign)",
            "FN (test-neg AND malignant)", "TN (test-neg AND benign)",
            "n_2x2",
            "Intermediate-call n (% malig)",
            "Not-classifiable-call n (% malig)",
            "Sensitivity (95% CI)", "Specificity (95% CI)",
            "PPV (95% CI)", "NPV (95% CI)"]
    with open(TABLES / "table3_v2_diagnostic_performance_actual_reported_call.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows_to_write:
            w.writerow(r)


def write_rom_table():
    """ROM% descriptive stats by platform x reported_call x histo_class.
    Only ThyroSeq has rom_percent_point; Afirma rows show 'n/a — not reported'.
    """
    cols = ["Platform", "Reported call", "Histology class",
            "n", "n with numeric ROM%",
            "Mean ROM%", "Median ROM% [IQR]"]
    rows = []
    by_key = {}
    for (beth, plat, size, call, histo, n, mean, med, p25, p75, n_rom) in cells:
        key = (plat, call, histo)
        d = by_key.setdefault(key, {"n": 0, "n_rom": 0, "rom_means": []})
        d["n"] += n
        d["n_rom"] += n_rom or 0
        if mean is not None and n_rom:
            d["rom_means"].append((mean, n_rom, med, p25, p75))
    for (plat, call, histo), d in sorted(by_key.items()):
        if plat == "Afirma":
            mean_str = "n/a — Afirma reports binary call only"
            med_str = "n/a"
        elif d["n_rom"] == 0:
            mean_str = "—"
            med_str = "—"
        else:
            # Weighted mean across cells
            total = sum(m * n for m, n, *_ in d["rom_means"])
            denom = sum(n for _, n, *_ in d["rom_means"])
            mean_str = f"{total/denom:.1f}"
            # Approximate median as the median of cell-medians weighted by n (good enough for table)
            meds = []
            for (m, n, med, p25, p75) in d["rom_means"]:
                meds.extend([med] * n)
            meds.sort()
            if meds:
                mid = meds[len(meds) // 2]
                p25_v = meds[len(meds) // 4] if len(meds) >= 4 else meds[0]
                p75_v = meds[3 * len(meds) // 4] if len(meds) >= 4 else meds[-1]
                med_str = f"{mid} [{p25_v}–{p75_v}]"
            else:
                med_str = "—"
        rows.append({
            "Platform": plat,
            "Reported call": call,
            "Histology class": histo,
            "n": d["n"],
            "n with numeric ROM%": d["n_rom"],
            "Mean ROM%": mean_str,
            "Median ROM% [IQR]": med_str,
        })
    with open(TABLES / "table3_v2_rom_pct_descriptive_stats.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def supersede_old_table():
    src = TABLES / "table3_diagnostic_performance_thyroseq_vs_afirma.csv"
    dst = SUPERSEDED / "table3_diagnostic_performance_thyroseq_vs_afirma_DERIVED_CALL.csv"
    if src.exists():
        shutil.move(str(src), str(dst))
    note = """# SUPERSEDED — derived-call Table 3

This folder contains the original Table 3 from the 2026-05-09 EXT2-4 Elicit
expansion. It was built from a **derived** positive/negative call (`molecular_risk_tier`
plus BRAF/RAS/TERT mutation flags from `manuscript_cohort_v1`) rather than the
**actual platform-reported test call** from `canonical_molecular_genetics_v2`.

Logan flagged this on 2026-05-09. The corrected version using the actual reported
call (`overall_result_class` for Afirma; `rom_descriptor` + `rom_percent_point` for
ThyroSeq, with INTERMEDIATE-only as a third category per Logan's direction) is at:

  ../table3_v2_diagnostic_performance_actual_reported_call.csv
  ../table3_v2_rom_pct_descriptive_stats.csv

**Do NOT cite the file in this folder for diagnostic-performance claims.** It is
preserved here per the project's append-only / never-delete rule. The corresponding
Manuscript Feedback Log row is `MFL-20260509-EXT2-4-TABLE3-CORRECTION`
(`rec2RAsAFehw1zEHV`).

Caveats that drove the supersession:
- Afirma "positive" in the derived call = (BRAF or RAS or TERT positive) OR
  (molecular_risk_tier ∈ {high, intermediate, low_intermediate}). This conflates
  the GSC binary Suspicious vs Benign call with downstream Xpression-Atlas
  mutation findings, which are reported alongside but separately from the GSC
  classifier on commercial Afirma reports.
- ThyroSeq "positive" in the derived call used the same rule, conflating the
  ROM-band classification with mutation positivity, and silently pooled
  INTERMEDIATE with positive.

The corrected Table 3 separates these signals.
"""
    with open(SUPERSEDED / "SUPERSEDED_NOTE.md", "w") as f:
        f.write(note)


if __name__ == "__main__":
    write_table3_v2()
    write_rom_table()
    supersede_old_table()
    print("OK")

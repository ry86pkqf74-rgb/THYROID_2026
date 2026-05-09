#!/usr/bin/env python3
"""
M088 — generate Tables 1–4 as Markdown files.

Reads pre-computed CSV outputs from analysis/output/ and writes to tables/.
"""

from __future__ import annotations
import csv
import math
from pathlib import Path
from collections import defaultdict

ROOT    = Path(__file__).resolve().parent.parent
OUTPUT  = ROOT / "analysis" / "output"
TABLES  = ROOT / "tables"
TABLES.mkdir(exist_ok=True)

ENTITIES_DISPLAY = {
    "follicular_adenoma":           "Follicular adenoma",
    "hurthle_cell_adenoma":         "Hurthle cell / Oncocytic adenoma",
    "FTUMP":                        "FT-UMP",
    "atypical_follicular_adenoma":  "Atypical follicular adenoma",
    "NIFTP":                        "NIFTP",
    "FTC_NOS":                      "FTC, NOS",
    "FTC_minimally_invasive":       "FTC, minimally invasive",
    "FTC_oncocytic_warthin":        "FTC, oncocytic variant",
    "FTC_widely_invasive":          "FTC, widely invasive",
    "HCC":                          "Hurthle cell / Oncocytic carcinoma",
    "DHGTC":                        "DHGTC",
    "PDTC":                         "PDTC",
    "hyalinizing_trabecular_tumor": "Hyalinizing trabecular tumor",
}

ERAS = ["1990-1994","1995-1999","2000-2004","2005-2009","2010-2014","2015-2019","2020-2025"]

# ---------------------------------------------------------------------------- #
# Wilson 95% CI for proportions

def wilson(n_pos, n_total, z=1.96):
    if n_total == 0:
        return (0.0, 0.0, 0.0)
    p = n_pos / n_total
    denom = 1 + z*z/n_total
    centre = (p + z*z/(2*n_total)) / denom
    half = (z * math.sqrt(p*(1-p)/n_total + z*z/(4*n_total*n_total))) / denom
    return (p, max(0, centre - half), min(1, centre + half))

def fmt_pct(p, lo, hi):
    return f"{p*100:.1f}% ({lo*100:.1f}–{hi*100:.1f})"

def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))

# ---------------------------------------------------------------------------- #
# Table 1 — demographics × entity

def table1():
    rows = read_csv(OUTPUT / "table1_demographics.csv")
    out = ["# Table 1. Cohort demographics by historical (pre-2022 WHO) diagnosis",
           "",
           "Patient-level counts and demographics for the M088 follicular-patterned thyroid neoplasm cohort, n = 1,542 distinct patients.",
           "",
           "| Historical diagnosis | n | Female (%) | Male (%) | Mean age (SD) | Median age | Mean dominant tumor (cm) | n missing size |",
           "| -- | -- | -- | -- | -- | -- | -- | -- |"]
    # Order entities by n desc
    rows_sorted = sorted(rows, key=lambda r: -int(r["n"]))
    totals = defaultdict(float)
    for r in rows_sorted:
        n = int(r["n"])
        nf = int(r["n_female"])
        nm = int(r["n_male"])
        ma = r["mean_age"]
        sda = r["sd_age"] if r["sd_age"] else "—"
        meda = r["median_age"]
        ms = r["mean_dom_size_cm"] if r["mean_dom_size_cm"] else "—"
        n_size_miss = r["n_size_missing"]
        label = ENTITIES_DISPLAY.get(r["hist_group"], r["hist_group"])
        out.append(f"| {label} | {n} | {nf} ({nf/n*100:.1f}) | {nm} ({nm/n*100:.1f}) | "
                   f"{ma} ({sda}) | {meda} | {ms} | {n_size_miss} |")
        totals["n"]  += n
        totals["nf"] += nf
        totals["nm"] += nm
    out.append(f"| **Total** | **{int(totals['n'])}** | **{int(totals['nf'])} ({totals['nf']/totals['n']*100:.1f})** | "
               f"**{int(totals['nm'])} ({totals['nm']/totals['n']*100:.1f})** | — | — | — | — |")
    out.append("")
    out.append("Notes. Size missingness reflects the path_synoptics tumor_1 dimension field, which is sparsely populated for benign entities (follicular adenoma 700/707 missing, Hurthle cell adenoma 143/143 missing, atypical FA 33/33 missing). Sizes are reported only for entities where the field was populated for ≥10 patients.")
    p = TABLES / "table_1_demographics.md"
    p.write_text("\n".join(out))
    print(f"Wrote {p}")

# ---------------------------------------------------------------------------- #
# Table 2 — era × entity counts

def table2():
    rows = read_csv(OUTPUT / "cohort_entity_era.csv")
    # Aggregate by entity and era
    bigkey = lambda r: (r["diagnosis_primary"], r["variant"])
    counts = defaultdict(lambda: defaultdict(int))
    for r in rows:
        counts[bigkey(r)][r["era_5yr"]] += int(r["n"])
    # Display
    label_for = {
        ("follicular_adenoma","(null)"):                  "Follicular adenoma",
        ("hurthle_cell_adenoma","(null)"):                "Hurthle cell / Oncocytic adenoma",
        ("FTUMP","(null)"):                               "FT-UMP",
        ("atypical_follicular_adenoma","(null)"):         "Atypical follicular adenoma",
        ("NIFTP","(null)"):                               "NIFTP",
        ("FTC","(null)"):                                 "FTC, NOS",
        ("FTC","minimally_invasive"):                     "FTC, minimally invasive",
        ("FTC","oncocytic_warthin"):                      "FTC, oncocytic variant",
        ("FTC","widely_invasive"):                        "FTC, widely invasive",
        ("HCC","(null)"):                                 "Hurthle cell / Oncocytic carcinoma",
        ("DHGTC","(null)"):                               "DHGTC",
        ("PDTC","(null)"):                                "PDTC",
        ("hyalinizing_trabecular_tumor","(null)"):        "Hyalinizing trabecular tumor",
    }
    order = list(label_for.keys())
    out = ["# Table 2. Era × historical entity counts (1990–2025)",
           "",
           "Counts are distinct patients with the index diagnosis assigned in each 5-year era based on year of first surgery. Empty cells = 0.",
           "",
           "| Historical entity | " + " | ".join(ERAS) + " | Total |",
           "| -- | " + " | ".join(["--"] * len(ERAS)) + " | -- |"]
    era_totals = defaultdict(int)
    for k in order:
        if k not in counts:
            continue
        cells = []
        rowtotal = 0
        for e in ERAS:
            v = counts[k].get(e, 0)
            cells.append(str(v) if v else "")
            rowtotal += v
            era_totals[e] += v
        out.append(f"| {label_for[k]} | " + " | ".join(cells) + f" | **{rowtotal}** |")
    out.append("| **Total** | " + " | ".join(f"**{era_totals[e]}**" for e in ERAS) + f" | **{sum(era_totals.values())}** |")
    out.append("")
    out.append("Notes. NIFTP cases coded prior to the 2017 introduction (1 each in 1990–94, 1995–99, 2000–04) reflect retrospective re-coding rather than contemporaneous signout. The 2020–25 era is partially observed (data extracted ~Q1 2025); 2025 contributes only 5 cases across all entities.")
    p = TABLES / "table_2_era_by_entity.md"
    p.write_text("\n".join(out))
    print(f"Wrote {p}")

# ---------------------------------------------------------------------------- #
# Table 3 — reclassification matrix (H1, H2 strict/broad, H3 strict/moderate/broad)

def table3():
    h1 = read_csv(OUTPUT / "h1_family_by_era.csv")
    h2 = read_csv(OUTPUT / "h2_fa_ftump_by_era.csv")
    h3 = read_csv(OUTPUT / "h3_mi_ftc_ftump_by_era.csv")

    # H1: aggregate totals
    onco = sum(int(r["n"]) for r in h1 if r["who2022_family"] == "Oncocytic")
    conv = sum(int(r["n"]) for r in h1 if r["who2022_family"] == "Conventional follicular")
    cohort_n = onco + conv
    p, lo, hi = wilson(onco, cohort_n)

    # H2 totals
    fa_n      = sum(int(r["n_fa"])    for r in h2)
    h2_strict = sum(int(r["n_strict"]) for r in h2)
    h2_broad  = sum(int(r["n_broad"])  for r in h2)
    p2s, lo2s, hi2s = wilson(h2_strict, fa_n)
    p2b, lo2b, hi2b = wilson(h2_broad,  fa_n)

    # H3 totals
    mi_n        = sum(int(r["n_mi_ftc"])    for r in h3)
    h3_strict   = sum(int(r["n_strict"])    for r in h3)
    h3_moderate = sum(int(r["n_moderate"])  for r in h3)
    h3_broad    = sum(int(r["n_broad"])     for r in h3)
    p3s, lo3s, hi3s = wilson(h3_strict,   mi_n)
    p3m, lo3m, hi3m = wilson(h3_moderate, mi_n)
    p3b, lo3b, hi3b = wilson(h3_broad,    mi_n)

    out = ["# Table 3. 2022 WHO reclassification matrix (Tier A deterministic)",
           "",
           "Reclassification rates with 95% Wilson confidence intervals. Tier A is the deterministic vocabulary mapping; Tier B (blinded re-review on the borderline subset) is owned by sister manuscript M090 and is not included in v0.1.",
           "",
           "## Panel A — H1: Oncocytic Neoplasm family migration",
           "",
           "| Migration | Source N | Reclassified n | Rate (95% CI) |",
           "| -- | -- | -- | -- |",
           f"| Conventional follicular family → 2022 WHO Oncocytic Neoplasm family | {cohort_n} | {onco} | {fmt_pct(p, lo, hi)} |",
           "",
           "Drivers of H1 migration: oncocytic-variant FTC (n=209 reclassified to Oncocytic Carcinoma); Hurthle cell adenoma (n=143 reclassified to Oncocytic Adenoma); historical Hurthle cell carcinoma (n=2 reclassified to Oncocytic Carcinoma). Together these three sources contribute the bulk of the migration; FT-UMP and atypical FA cases with oncocytic morphology in synoptic comments contribute the remainder.",
           "",
           "## Panel B — H2: Follicular adenoma → FT-UMP migration (Tier A)",
           "",
           "| Bound | Source N (FA) | Reclassified n | Rate (95% CI) |",
           "| -- | -- | -- | -- |",
           f"| Strict (`indeterminate`/`suspected` finding_status, or strictly equivocal qualifier) | {fa_n} | {h2_strict} | {fmt_pct(p2s, lo2s, hi2s)} |",
           f"| Broad  (strict ∪ minimal/focal/partial/single-focus qualifier) | {fa_n} | {h2_broad} | {fmt_pct(p2b, lo2b, hi2b)} |",
           "",
           "H2 deterministic Tier A migration is essentially zero — the structured `canonical_invasion_events_v1` capsular field for follicular adenomas is overwhelmingly populated with `absent` or null, not equivocal. Any meaningful FA → FT-UMP migration estimate requires Tier B blinded re-review (sister manuscript M090).",
           "",
           "## Panel C — H3: Minimally invasive FTC → FT-UMP migration (Tier A)",
           "",
           "| Bound | Source N (MI-FTC) | Reclassified n | Rate (95% CI) |",
           "| -- | -- | -- | -- |",
           f"| Strict (`indeterminate` finding_status only, no vascular) | {mi_n} | {h3_strict} | {fmt_pct(p3s, lo3s, hi3s)} |",
           f"| Moderate (strict ∪ `minimal`/`minimally invasive` qualifier, no vascular) | {mi_n} | {h3_moderate} | {fmt_pct(p3m, lo3m, hi3m)} |",
           f"| Broad (moderate ∪ `focal`/`single focus`/`partial` qualifier, no vascular) | {mi_n} | {h3_broad} | {fmt_pct(p3b, lo3b, hi3b)} |",
           "",
           "H3 has a critical methodological subtlety. The diagnostic variant `minimally_invasive` is by construction associated with `evidence_qualifier='minimal'`/`'minimally invasive'` in the structured pathology synoptic — i.e., the moderate and broad bounds are partially circular. The strict bound (1.2%) is the only fully Tier A-deterministic estimate; the moderate/broad bounds (85%) reflect the size of the historical MI-FTC cohort that 2022 WHO criteria identify as borderline against the new threshold (unequivocal full-thickness capsular invasion, or unequivocal vascular invasion). The clinically meaningful estimate is Tier B re-review (M090).",
           ""]
    p = TABLES / "table_3_reclassification_matrix.md"
    p.write_text("\n".join(out))
    print(f"Wrote {p}")

# ---------------------------------------------------------------------------- #
# Table 4 — index-encounter management impact (H4)
# Compares observed historical management for MI-FTC, oncocytic FTC, FTC NOS, etc.
# against the counterfactual rate if the patients were managed as 2022 WHO target labels.

def table4():
    rows = read_csv(OUTPUT / "h4_management_by_group.csv")
    by_group = {r["historical_group"]: r for r in rows}

    # Definitive total at index = pct_total + pct_completion (since completion implies
    # the patient was upgraded post-hemi; both result in eventual total)
    def def_total(r):
        n = int(r["n_total"])
        # Definitive total = had a total OR had a completion (post hemi)
        n_def = int(r["n_total_thyr"]) + int(r["n_completion"])
        # Some patients may have both flagged; cap at n
        n_def = min(n_def, n)
        return wilson(n_def, n)

    def hemi_only(r):
        n = int(r["n_total"])
        n_hemi = int(r["n_hemi"])
        n_def_total = min(int(r["n_total_thyr"]) + int(r["n_completion"]), n)
        n_pure_hemi = max(0, n - n_def_total - (n - n_hemi - int(r["n_total_thyr"])))
        # Simpler: hemi-only = n_hemi who did NOT get completion
        n_pure_hemi = int(r["n_hemi"]) - int(r["n_completion"])
        n_pure_hemi = max(0, n_pure_hemi)
        return wilson(n_pure_hemi, n)

    # Comparisons:
    # H1: Oncocytic FTC (historical) vs hypothetical "Oncocytic Carcinoma" (same patients,
    #     management unchanged at the time but the LABEL changes; we report observed only)
    # H3: MI-FTC observed vs FT-UMP observed counterfactual
    #     i.e., what would these MI-FTC patients have received if labeled FT-UMP?
    # H2: FA observed vs FT-UMP observed counterfactual

    out = ["# Table 4. Index-encounter management impact under 2022 WHO reclassification",
           "",
           "Endpoints (per analysis_plan_v1.md §4.4): definitive total thyroidectomy at index (initial total OR initial hemi + completion) and pure hemi (no completion). RAI ordered/received at index has been dropped per Notable Finding NF-2026-05-09-rai-extraction-sparse-follicular-cohort.",
           "",
           "Counterfactual reasoning: for each historical → reclassified transition, we report (a) the observed rate in the historical group and (b) the observed rate in the reclassified-target group. The Δ rate is the absolute percentage-point difference and represents the management gap that the 2022 WHO criteria are intended to close.",
           "",
           "## Definitive total thyroidectomy at index (initial total OR initial hemi + completion)",
           "",
           "| Transition | n hist | Observed hist (95% CI) | n target | Observed target (95% CI) | Δ (pp) |",
           "| -- | -- | -- | -- | -- | -- |"]
    transitions = [
        ("MI-FTC → FT-UMP",            "FTC_minimally_invasive", "FTUMP"),
        ("Oncocytic FTC → Oncocytic carcinoma (label change only)", "FTC_oncocytic_warthin", "FTC_oncocytic_warthin"),
        ("HCA → OA (label change only)", "hurthle_cell_adenoma",  "hurthle_cell_adenoma"),
        ("FA → FT-UMP (Tier B candidate)", "follicular_adenoma",    "FTUMP"),
        ("Atypical FA → FT-UMP",       "atypical_follicular_adenoma", "FTUMP"),
        ("NIFTP comparator",           "NIFTP",                  "NIFTP"),
    ]
    for label, src, tgt in transitions:
        rh = by_group[src]
        rt = by_group[tgt]
        ph, loh, hih = def_total(rh)
        pt, lot, hit = def_total(rt)
        delta = (pt - ph) * 100
        out.append(f"| {label} | {rh['n_total']} | {fmt_pct(ph, loh, hih)} | {rt['n_total']} | {fmt_pct(pt, lot, hit)} | {delta:+.1f} |")
    out.append("")
    out.append("## Hemi-only (initial hemi without completion)")
    out.append("")
    out.append("| Transition | n hist | Observed hist (95% CI) | n target | Observed target (95% CI) | Δ (pp) |")
    out.append("| -- | -- | -- | -- | -- | -- |")
    for label, src, tgt in transitions:
        rh = by_group[src]
        rt = by_group[tgt]
        ph, loh, hih = hemi_only(rh)
        pt, lot, hit = hemi_only(rt)
        delta = (pt - ph) * 100
        out.append(f"| {label} | {rh['n_total']} | {fmt_pct(ph, loh, hih)} | {rt['n_total']} | {fmt_pct(pt, lot, hit)} | {delta:+.1f} |")
    out.append("")
    out.append("Caveats. (1) Counterfactual rates use observed management of the target-label cohort, which itself reflects era-mixed treatment decisions. The cleanest comparison is MI-FTC → FT-UMP because both labels exist in the cohort with comparable patient characteristics (mean age 52.7 vs 52.2; female-predominant). (2) Oncocytic FTC → Oncocytic carcinoma is purely a label change — same patients, same management — and is reported here as a baseline. (3) Bootstrapped 95% CIs around Δ rates are reported in the supplementary file `table_4_management_impact_bootstrap.csv` (1000 iterations).")
    p = TABLES / "table_4_management_impact.md"
    p.write_text("\n".join(out))
    print(f"Wrote {p}")

    # Bootstrap CIs for Δ
    import random
    random.seed(42)
    boot_rows = []
    for label, src, tgt in transitions:
        rh = by_group[src]
        rt = by_group[tgt]
        nh = int(rh["n_total"])
        nt = int(rt["n_total"])
        kh = min(int(rh["n_total_thyr"]) + int(rh["n_completion"]), nh)
        kt = min(int(rt["n_total_thyr"]) + int(rt["n_completion"]), nt)
        dr_obs = kt/nt - kh/nh
        deltas = []
        for _ in range(1000):
            bh = sum(1 for _ in range(nh) if random.random() < kh/nh)
            bt = sum(1 for _ in range(nt) if random.random() < kt/nt)
            deltas.append(bt/nt - bh/nh)
        deltas.sort()
        lo = deltas[24]; hi = deltas[974]
        boot_rows.append({"transition": label, "delta_pp": round(dr_obs*100, 1),
                          "ci_lo_pp": round(lo*100, 1), "ci_hi_pp": round(hi*100, 1),
                          "n_hist": nh, "n_target": nt})
    bp = TABLES / "table_4_management_impact_bootstrap.csv"
    with open(bp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(boot_rows[0].keys()))
        w.writeheader()
        for r in boot_rows:
            w.writerow(r)
    print(f"Wrote {bp}")

# ---------------------------------------------------------------------------- #

if __name__ == "__main__":
    table1()
    table2()
    table3()
    table4()
    print("All tables generated.")

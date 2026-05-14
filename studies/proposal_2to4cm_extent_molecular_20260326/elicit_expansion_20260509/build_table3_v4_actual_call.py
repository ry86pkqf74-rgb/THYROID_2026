#!/usr/bin/env python3
"""
build_table3_v4_actual_call.py — Table 3 (reported-call) for EXT2-4 v4 cohort.

Cells from ext2_4_v4_derive_tables.py → tables/_v4_table3_cells.json (BigQuery snapshot).
Outputs:
  tables/table3_v4_diagnostic_performance_actual_reported_call.csv
  tables/table3_v4_rom_pct_descriptive_stats.csv
"""
import csv
import json
from math import sqrt
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TABLES = ROOT / "tables"


def wilson_ci(k, n, z=1.96):
    if n == 0:
        return None, None, None
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = (z / denom) * sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return p, max(0.0, center - half), min(1.0, center + half)


def load_cells():
    meta = json.loads((TABLES / "_v4_table3_cells.json").read_text(encoding="utf-8"))
    out = []
    for row in meta["cells"]:
        t = tuple(
            (
                int(row[0]),
                row[1],
                row[2],
                row[3],
                row[4],
                int(row[5]),
                float(row[6]) if row[6] is not None else None,
                float(row[7]) if row[7] is not None else None,
                float(row[8]) if row[8] is not None else None,
                float(row[9]) if row[9] is not None else None,
                int(row[10] or 0),
            )
        )
        out.append(t)
    return out


cells = load_cells()


def aggregate_2x2(rows, *, niftp_as_malignant=False):
    out = {}
    extras = {}
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
                    extras[key] = {
                        "intermediate_n": 0,
                        "intermediate_malig": 0,
                        "unclass_n": 0,
                        "unclass_malig": 0,
                    }
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
        "Platform": plat,
        "Bethesda": beth,
        "Size band": size,
        "TP (test-pos AND malignant)": tp,
        "FP (test-pos AND benign)": fp,
        "FN (test-neg AND malignant)": fn,
        "TN (test-neg AND benign)": tn,
        "n_2x2": n_2x2,
        "Intermediate-call n (% malig)": (
            f"{ex['intermediate_n']} ({100*ex['intermediate_malig']/ex['intermediate_n']:.1f}% malig)"
            if ex["intermediate_n"]
            else "0"
        ),
        "Not-classifiable-call n (% malig)": (
            f"{ex['unclass_n']} ({100*ex['unclass_malig']/ex['unclass_n']:.1f}% malig)"
            if ex["unclass_n"]
            else "0"
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


def write_table3_v4():
    rows_to_write = []
    for label, niftp_flag in [
        ("Strict (NIFTP=benign)", False),
        ("Inclusive (NIFTP=malignant)", True),
    ]:
        agg, extras = aggregate_2x2(cells, niftp_as_malignant=niftp_flag)
        for (plat, beth, size), (tp, fp, fn, tn) in sorted(agg.items()):
            row = {"Histology rule": label}
            row.update(
                perf_row(plat, beth, size, tp, fp, fn, tn, extras[(plat, beth, size)])
            )
            rows_to_write.append(row)
    cols = [
        "Histology rule",
        "Platform",
        "Bethesda",
        "Size band",
        "TP (test-pos AND malignant)",
        "FP (test-pos AND benign)",
        "FN (test-neg AND malignant)",
        "TN (test-neg AND benign)",
        "n_2x2",
        "Intermediate-call n (% malig)",
        "Not-classifiable-call n (% malig)",
        "Sensitivity (95% CI)",
        "Specificity (95% CI)",
        "PPV (95% CI)",
        "NPV (95% CI)",
    ]
    with (TABLES / "table3_v4_diagnostic_performance_actual_reported_call.csv").open(
        "w", newline="", encoding="utf-8"
    ) as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows_to_write:
            w.writerow(r)


def write_rom_table():
    cols = [
        "Platform",
        "Reported call",
        "Histology class",
        "n",
        "n with numeric ROM%",
        "Mean ROM%",
        "Median ROM% [IQR]",
    ]
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
            total = sum(m * n for m, n, *_ in d["rom_means"])
            denom = sum(n for _, n, *_ in d["rom_means"])
            mean_str = f"{total/denom:.1f}"
            meds = []
            for (_m, n, med, p25, p75) in d["rom_means"]:
                if med is not None:
                    meds.extend([med] * n)
            meds.sort()
            if meds:
                mid = meds[len(meds) // 2]
                p25_v = meds[len(meds) // 4] if len(meds) >= 4 else meds[0]
                p75_v = meds[3 * len(meds) // 4] if len(meds) >= 4 else meds[-1]
                med_str = f"{mid} [{p25_v}–{p75_v}]"
            else:
                med_str = "—"
        rows.append(
            {
                "Platform": plat,
                "Reported call": call,
                "Histology class": histo,
                "n": d["n"],
                "n with numeric ROM%": d["n_rom"],
                "Mean ROM%": mean_str,
                "Median ROM% [IQR]": med_str,
            }
        )
    with (TABLES / "table3_v4_rom_pct_descriptive_stats.csv").open(
        "w", newline="", encoding="utf-8"
    ) as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def print_headline(strict_niftp_benign: bool = True) -> None:
    """Emit copy-paste values for Figures (B3+B4, all_sizes)."""
    agg, _ = aggregate_2x2(cells, niftp_as_malignant=not strict_niftp_benign)
    print("Forest inputs (Wilson midpoint & bounds as %)")
    for plat in ("Afirma", "ThyroSeq"):
        key = (plat, "B3+B4", "all_sizes")
        tp, fp, fn, tn = agg[key]
        n_tot = tp + fp + fn + tn
        if n_tot == 0:
            continue

        def tri(k, d):
            p, lo, hi = wilson_ci(k, d)
            assert p is not None
            return 100 * p, 100 * lo, 100 * hi

        sens = tri(tp, tp + fn) if tp + fn else (0, 0, 100)
        spec = tri(tn, tn + fp) if tn + fp else (0, 0, 100)
        ppv = tri(tp, tp + fp) if tp + fp else (0, 0, 100)
        npv = tri(tn, tn + fn) if tn + fn else (0, 0, 100)
        print(
            plat,
            "n=",
            n_tot,
            "Sens",
            round(sens[0], 1),
            round(sens[1], 1),
            round(sens[2], 1),
            "Spec",
            round(spec[0], 1),
            round(spec[1], 1),
            round(spec[2], 1),
            "PPV",
            round(ppv[0], 1),
            "NPV",
            round(npv[0], 1),
        )


if __name__ == "__main__":
    write_table3_v4()
    write_rom_table()
    print_headline()
    print("OK table3 v4")

"""
M033 — Afirma vs ThyroSeq Molecular Platform Comparison (Final)
Comprehensive analysis with all 8 sections and validated outputs.

Usage:
    .venv/bin/python scripts/m033_platform_comparison_final.py
"""

import os, math, textwrap, warnings
from pathlib import Path
from datetime import datetime

import duckdb
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")

OUT_DIR = Path("studies/m033_platform_comparison")
OUT_DIR.mkdir(parents=True, exist_ok=True)
TOML_PATH = Path("motherduck.local.toml")
VIEW = "manuscript_workspace.m033_afirma_thyroseq_analytic_v1"
MD_OUT_TABLE = "manuscript_workspace.m033_platform_analysis_v1"
RUN_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Helpers ────────────────────────────────────────────────────────────────
def load_token() -> str:
    for k in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        v = os.environ.get(k, "")
        if v:
            print(f"  token: {k}=SET (len={len(v)})")
            return v
    if TOML_PATH.exists():
        import toml
        cfg = toml.load(TOML_PATH)
        for k in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
            v = cfg.get(k, "")
            if v:
                print(f"  token: {k}=SET from {TOML_PATH}")
                return v
    raise RuntimeError("No MotherDuck token found")


def wilson_ci(k: int, n: int, z: float = 1.96):
    if n == 0:
        return float("nan"), float("nan"), float("nan")
    p = k / n
    d = 1 + z**2 / n
    c = (p + z**2 / (2 * n)) / d
    h = (z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2))) / d
    return p, max(0.0, c - h), min(1.0, c + h)


def fmt_pct(p, lo, hi, d=1):
    if math.isnan(p): return "N/A"
    return f"{p*100:.{d}f}% ({lo*100:.{d}f}–{hi*100:.{d}f}%)"


def bool_sum(series):
    """Safe sum of boolean/nullable series."""
    return int(series.fillna(False).astype(bool).sum())


def _chi2_p(ct: pd.DataFrame) -> float:
    try:
        _, p, _, _ = stats.chi2_contingency(ct.values)
        return p
    except Exception:
        return float("nan")


# ── Connection ────────────────────────────────────────────────────────────
def connect():
    token = load_token()
    os.environ["MOTHERDUCK_TOKEN"] = token
    return duckdb.connect("md:thyroid_canonical_publication_v1_0")


# ── Load data ─────────────────────────────────────────────────────────────
def load_data(conn) -> pd.DataFrame:
    print("\n[0] Loading analytic view …")
    df = conn.execute(f"SELECT * FROM {VIEW}").df()
    print(f"    {len(df):,} rows, {len(df.columns)} columns")

    # Join Bethesda from CPM
    beth_df = conn.execute("""
        SELECT CAST(research_id AS VARCHAR) AS research_id,
               TRY_CAST(bethesda_final AS INTEGER) AS bethesda_final
        FROM main.canonical_patient_master
        WHERE bethesda_final IS NOT NULL
    """).df()
    df = df.merge(beth_df, on="research_id", how="left")
    print(f"    Bethesda: {df['bethesda_final'].notna().sum()} patients")

    # Clean date (filter plausible years 2005-2025)
    df["test_year"] = pd.to_datetime(df["mol_first_test_date"], errors="coerce").dt.year
    n_bad = df[(df["test_year"] < 2005) | (df["test_year"] > 2025)]["test_year"].notna().sum()
    if n_bad > 0:
        print(f"    Removed {n_bad} rows with implausible test years")
    df["test_year_clean"] = df["test_year"].where((df["test_year"] >= 2005) & (df["test_year"] <= 2025))

    # Test positive flag: high or intermediate molecular_risk_tier
    df["test_positive"] = df["molecular_risk_tier"].isin(["high", "intermediate"])
    df["test_negative"] = df["molecular_risk_tier"].isin(["wild_type", "low_intermediate"])
    df["risk_tier_known"] = df["molecular_risk_tier"].notna()

    # LN positive flag (INTEGER column)
    df["ln_positive_flag"] = df["ln_positive_final"].fillna(0) > 0

    return df


# ── Section 1: Platform diagnostic performance ────────────────────────────
def s1_diagnostic_performance(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[1] Diagnostic performance …")
    primary = df[df["mol_platform_family"].isin(["ThyroSeq", "Afirma"])].copy()

    rows = []
    for plat in ["ThyroSeq", "Afirma"]:
        sub = primary[primary["mol_platform_family"] == plat]
        n = len(sub)
        nm = bool_sum(sub["is_malignant"])
        p_rom, lo_rom, hi_rom = wilson_ci(nm, n)

        # Sensitivity/specificity using risk tier (among those with known tier)
        sub_k = sub[sub["risk_tier_known"]]
        nk = len(sub_k)
        tp = bool_sum((sub_k["test_positive"]) & sub_k["is_malignant"])
        fp = bool_sum((sub_k["test_positive"]) & ~sub_k["is_malignant"].fillna(False))
        tn = bool_sum((sub_k["test_negative"]) & ~sub_k["is_malignant"].fillna(False))
        fn = bool_sum((sub_k["test_negative"]) & sub_k["is_malignant"])

        sens_p, sens_lo, sens_hi = wilson_ci(tp, tp + fn)
        spec_p, spec_lo, spec_hi = wilson_ci(tn, tn + fp)
        npv_p, npv_lo, npv_hi   = wilson_ci(tn, tn + fn)
        ppv_p, ppv_lo, ppv_hi   = wilson_ci(tp, tp + fp)

        # Bethesda III ROM
        b3 = sub[sub["bethesda_final"] == 3]
        b3_rom, b3_lo, b3_hi = wilson_ci(bool_sum(b3["is_malignant"]), len(b3)) if len(b3) > 0 else (float("nan"),)*3

        # Bethesda IV ROM
        b4 = sub[sub["bethesda_final"] == 4]
        b4_rom, b4_lo, b4_hi = wilson_ci(bool_sum(b4["is_malignant"]), len(b4)) if len(b4) > 0 else (float("nan"),)*3

        print(f"    {plat}: N={n}, ROM={fmt_pct(p_rom,lo_rom,hi_rom)}")
        print(f"           N with known tier={nk}, Sens={fmt_pct(sens_p,sens_lo,sens_hi)}, Spec={fmt_pct(spec_p,spec_lo,spec_hi)}, NPV={fmt_pct(npv_p,npv_lo,npv_hi)}")
        print(f"           Bethesda III ROM: N={len(b3)}, {fmt_pct(b3_rom,b3_lo,b3_hi)}")
        print(f"           Bethesda IV ROM:  N={len(b4)}, {fmt_pct(b4_rom,b4_lo,b4_hi)}")

        rows.append({
            "platform": plat,
            "n_total": n,
            "n_malignant": nm,
            "rom_pct": round(p_rom * 100, 1),
            "rom_ci_lo": round(lo_rom * 100, 1),
            "rom_ci_hi": round(hi_rom * 100, 1),
            "n_with_risk_tier": nk,
            "tp": tp, "fp": fp, "tn": tn, "fn": fn,
            "sensitivity_pct": round(sens_p * 100, 1) if not math.isnan(sens_p) else None,
            "sensitivity_ci_lo": round(sens_lo * 100, 1) if not math.isnan(sens_lo) else None,
            "sensitivity_ci_hi": round(sens_hi * 100, 1) if not math.isnan(sens_hi) else None,
            "specificity_pct": round(spec_p * 100, 1) if not math.isnan(spec_p) else None,
            "specificity_ci_lo": round(spec_lo * 100, 1) if not math.isnan(spec_lo) else None,
            "specificity_ci_hi": round(spec_hi * 100, 1) if not math.isnan(spec_hi) else None,
            "npv_pct": round(npv_p * 100, 1) if not math.isnan(npv_p) else None,
            "npv_ci_lo": round(npv_lo * 100, 1) if not math.isnan(npv_lo) else None,
            "npv_ci_hi": round(npv_hi * 100, 1) if not math.isnan(npv_hi) else None,
            "ppv_pct": round(ppv_p * 100, 1) if not math.isnan(ppv_p) else None,
            "ppv_ci_lo": round(ppv_lo * 100, 1) if not math.isnan(ppv_lo) else None,
            "ppv_ci_hi": round(ppv_hi * 100, 1) if not math.isnan(ppv_hi) else None,
            "note_sensitivity": "molecular_risk_tier proxy: positive=high/intermediate",
            "n_bethesda3": len(b3),
            "n_bethesda3_malignant": bool_sum(b3["is_malignant"]),
            "bethesda3_rom_pct": round(b3_rom * 100, 1) if not math.isnan(b3_rom) else None,
            "bethesda3_rom_ci_lo": round(b3_lo * 100, 1) if not math.isnan(b3_lo) else None,
            "bethesda3_rom_ci_hi": round(b3_hi * 100, 1) if not math.isnan(b3_hi) else None,
            "n_bethesda4": len(b4),
            "n_bethesda4_malignant": bool_sum(b4["is_malignant"]),
            "bethesda4_rom_pct": round(b4_rom * 100, 1) if not math.isnan(b4_rom) else None,
            "bethesda4_rom_ci_lo": round(b4_lo * 100, 1) if not math.isnan(b4_lo) else None,
            "bethesda4_rom_ci_hi": round(b4_hi * 100, 1) if not math.isnan(b4_hi) else None,
        })

    # Append all platforms for complete ROM table
    for plat in sorted(df["mol_platform_family"].dropna().unique()):
        if plat in ["ThyroSeq", "Afirma"]:
            continue
        sub = df[df["mol_platform_family"] == plat]
        n = len(sub); nm = bool_sum(sub["is_malignant"])
        p, lo, hi = wilson_ci(nm, n)
        rows.append({
            "platform": plat, "n_total": n, "n_malignant": nm,
            "rom_pct": round(p * 100, 1), "rom_ci_lo": round(lo * 100, 1), "rom_ci_hi": round(hi * 100, 1),
        })

    result = pd.DataFrame(rows)
    result.to_csv(OUT_DIR / "platform_diagnostic_performance.csv", index=False)
    print(f"    → platform_diagnostic_performance.csv ({len(result)} rows)")
    return result


# ── Section 2: Mutation spectrum ──────────────────────────────────────────
def s2_mutation_spectrum(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[2] Mutation spectrum …")

    rows = []
    for plat in sorted(df["mol_platform_family"].dropna().unique()):
        sub = df[df["mol_platform_family"] == plat]
        n = len(sub)
        n_braf = bool_sum(sub["braf_positive_final"])
        n_ras  = bool_sum(sub["ras_positive_final"])
        n_fus  = bool_sum(sub["mol_has_fusion"])
        n_malig = bool_sum(sub["is_malignant"])

        # Risk tier distribution
        if "molecular_risk_tier" in sub.columns:
            td = sub["molecular_risk_tier"].value_counts().to_dict()
        else:
            td = {}

        median_genes = sub["mol_n_distinct_genes"].median() if "mol_n_distinct_genes" in sub.columns else None

        # RAS subtypes for ThyroSeq
        ras_types = sub[sub["ras_positive_final"].fillna(False).astype(bool)]["ras_subtype"].value_counts().to_dict() if "ras_subtype" in sub.columns else {}

        rows.append({
            "platform": plat,
            "n": n,
            "n_malignant": n_malig,
            "malignancy_rate_pct": round(n_malig / n * 100, 1) if n > 0 else None,
            "n_braf_positive": n_braf,
            "braf_rate_pct": round(n_braf / n * 100, 1) if n > 0 else None,
            "n_ras_positive": n_ras,
            "ras_rate_pct": round(n_ras / n * 100, 1) if n > 0 else None,
            "ras_NRAS": ras_types.get("NRAS", 0),
            "ras_HRAS": ras_types.get("HRAS", 0),
            "ras_KRAS": ras_types.get("KRAS", 0),
            "n_fusion": n_fus,
            "fusion_rate_pct": round(n_fus / n * 100, 1) if n > 0 else None,
            "median_genes_tested": median_genes,
            "tier_high": td.get("high", 0),
            "tier_intermediate": td.get("intermediate", 0),
            "tier_low_intermediate": td.get("low_intermediate", 0),
            "tier_wild_type": td.get("wild_type", 0),
            "tier_unknown_missing": n - sum(td.values()),
        })

    result = pd.DataFrame(rows)
    result.to_csv(OUT_DIR / "mutation_spectrum_by_platform.csv", index=False)

    # Detailed output
    for _, r in result.iterrows():
        print(f"    {r['platform']}: N={r['n']}, BRAF={r['n_braf_positive']}({r['braf_rate_pct']}%), "
              f"RAS={r['n_ras_positive']}({r['ras_rate_pct']}%), Fusion={r['n_fusion']}({r['fusion_rate_pct']}%)")

    print("    → mutation_spectrum_by_platform.csv")
    return result


# ── Section 3: Version comparison ─────────────────────────────────────────
def s3_version_comparison(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[3] Version comparison …")

    ver_map = {
        "Afirma_GEC": "Afirma GEC (pre-2017)",
        "Afirma_GSC": "Afirma GSC (post-2017)",
        "ThyroSeq_v2": "ThyroSeq v2 (pre-2018)",
        "ThyroSeq_v3": "ThyroSeq v3 (post-2018)",
        "Afirma_version_unknown": "Afirma (version unknown)",
        "ThyroSeq_version_unknown": "ThyroSeq (version unknown)",
    }

    rows = []
    for vk, label in ver_map.items():
        if "version_unknown" in vk:
            platform = vk.split("_version_unknown")[0]
            sub = df[
                (df["mol_platform_family"] == platform) &
                (df["mol_platform_resolved"].str.contains("version_unknown", na=False))
            ]
        else:
            sub = df[df["mol_platform_resolved"] == vk]
        n = len(sub)
        if n == 0:
            continue

        nm = bool_sum(sub["is_malignant"])
        p_rom, lo_rom, hi_rom = wilson_ci(nm, n)

        n_braf = bool_sum(sub["braf_positive_final"])
        n_fus  = bool_sum(sub["mol_has_fusion"])
        n_recur = bool_sum(sub["any_recurrence_flag"])

        # Specificity proxy (wild_type+low_intermediate among benign)
        benign = sub[~sub["is_malignant"].fillna(False)]
        nb = len(benign)
        tn_v = bool_sum(benign["test_negative"]) if "test_negative" in benign.columns else 0
        spec_p, spec_lo, spec_hi = wilson_ci(tn_v, nb) if nb > 0 else (float("nan"),)*3

        print(f"    {label}: N={n}, ROM={fmt_pct(p_rom,lo_rom,hi_rom)}, BRAF={n_braf}({n_braf/n*100:.1f}%), Spec≈{fmt_pct(spec_p,spec_lo,spec_hi)}")

        rows.append({
            "version_key": vk,
            "label": label,
            "n": n,
            "n_malignant": nm,
            "rom_pct": round(p_rom * 100, 1),
            "rom_ci_lo": round(lo_rom * 100, 1),
            "rom_ci_hi": round(hi_rom * 100, 1),
            "n_braf": n_braf,
            "braf_rate_pct": round(n_braf / n * 100, 1),
            "n_fusion": n_fus,
            "fusion_rate_pct": round(n_fus / n * 100, 1),
            "n_recurrent": n_recur,
            "recurrence_rate_pct": round(n_recur / n * 100, 1),
            "n_benign": nb,
            "specificity_proxy_pct": round(spec_p * 100, 1) if not math.isnan(spec_p) else None,
            "specificity_proxy_ci_lo": round(spec_lo * 100, 1) if not math.isnan(spec_lo) else None,
            "specificity_proxy_ci_hi": round(spec_hi * 100, 1) if not math.isnan(spec_hi) else None,
        })

    result = pd.DataFrame(rows)
    result.to_csv(OUT_DIR / "version_comparison.csv", index=False)
    print(f"    → version_comparison.csv ({len(result)} rows)")
    return result


# ── Section 4: Dual-platform concordance ──────────────────────────────────
def s4_dual_platform(df: pd.DataFrame, conn) -> pd.DataFrame:
    print("\n[4] Dual-platform concordance …")

    dual = df[df["mol_platform_family"] == "ThyroSeq+Afirma"].copy()
    n_dual = len(dual)
    nm = bool_sum(dual["is_malignant"])
    n_recur = bool_sum(dual["any_recurrence_flag"])
    n_braf = bool_sum(dual["braf_positive_final"])

    p_rom, lo_rom, hi_rom = wilson_ci(nm, n_dual)
    p_rec, lo_rec, hi_rec = wilson_ci(n_recur, n_dual)
    p_braf, lo_braf, hi_braf = wilson_ci(n_braf, n_dual)

    print(f"    N dual-platform: {n_dual}")
    print(f"    ROM: {fmt_pct(p_rom, lo_rom, hi_rom)}")
    print(f"    Recurrence: {fmt_pct(p_rec, lo_rec, hi_rec)}")
    print(f"    BRAF+: {fmt_pct(p_braf, lo_braf, hi_braf)}")

    # Discordance analysis
    if "braf_discordance_flag" in dual.columns:
        n_discord = bool_sum(dual["braf_discordance_flag"])
        n_concord = n_dual - n_discord
        print(f"    BRAF concordant: {n_concord} ({n_concord/n_dual*100:.1f}%), discordant: {n_discord} ({n_discord/n_dual*100:.1f}%)")

        discord_sub = dual[dual["braf_discordance_flag"].fillna(False).astype(bool)]
        n_disc_malig = bool_sum(discord_sub["is_malignant"])
        disc_rate = f"{n_disc_malig/len(discord_sub)*100:.1f}%" if len(discord_sub) > 0 else "N/A"
        print(f"    Discordant → malignant: {n_disc_malig}/{len(discord_sub)} ({disc_rate})")

    # Compare all three arms: ThyroSeq-only, Afirma-only, Dual
    comparison = []
    for label, sub in [
        ("ThyroSeq-only", df[df["mol_platform_family"] == "ThyroSeq"]),
        ("Afirma-only",   df[df["mol_platform_family"] == "Afirma"]),
        ("Dual-platform", dual),
        ("single_gene",   df[df["mol_platform_family"] == "single_gene"]),
    ]:
        n = len(sub)
        nm2 = bool_sum(sub["is_malignant"])
        nr = bool_sum(sub["any_recurrence_flag"])
        nb = bool_sum(sub["braf_positive_final"])
        p_r, lo_r, hi_r = wilson_ci(nm2, n)
        p_rec2, lo_rec2, hi_rec2 = wilson_ci(nr, n)
        comparison.append({
            "group": label, "n": n,
            "n_malignant": nm2, "rom_pct": round(p_r * 100, 1),
            "rom_ci_lo": round(lo_r * 100, 1), "rom_ci_hi": round(hi_r * 100, 1),
            "n_recurrent": nr,
            "recurrence_rate_pct": round(p_rec2 * 100, 1),
            "recurrence_ci_lo": round(lo_rec2 * 100, 1),
            "recurrence_ci_hi": round(hi_rec2 * 100, 1),
            "n_braf": nb,
            "braf_rate_pct": round(nb / n * 100, 1) if n > 0 else None,
        })

    result = pd.DataFrame(comparison)
    result.to_csv(OUT_DIR / "dual_platform_concordance.csv", index=False)
    print("    → dual_platform_concordance.csv")
    return result


# ── Section 5: Outcomes by platform ───────────────────────────────────────
def s5_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[5] Outcomes by platform …")

    primary = df[df["mol_platform_family"].isin(["ThyroSeq", "Afirma"])].copy()
    rows = []

    for plat in ["ThyroSeq", "Afirma"]:
        sub = primary[primary["mol_platform_family"] == plat]
        n = len(sub)

        n_recur = bool_sum(sub["any_recurrence_flag"])
        n_rai   = bool_sum(sub["rai_received_reconciled"])
        n_ln    = int((sub["ln_positive_final"].fillna(0) > 0).sum())
        n_comp  = bool_sum(sub["any_confirmed_complication_flag"])

        p_rec, lo_rec, hi_rec = wilson_ci(n_recur, n)
        p_rai, lo_rai, hi_rai = wilson_ci(n_rai, n)
        p_ln,  lo_ln,  hi_ln  = wilson_ci(n_ln, n)

        # Procedure distribution
        if "surg_procedure_type" in sub.columns:
            proc = sub["surg_procedure_type"].value_counts(normalize=True).mul(100).round(1)
        else:
            proc = pd.Series(dtype=float)

        # ATA distribution
        if "ata_risk_category" in sub.columns:
            ata = sub["ata_risk_category"].value_counts().to_dict()
        else:
            ata = {}

        print(f"    {plat}: Recurrence={fmt_pct(p_rec,lo_rec,hi_rec)}, RAI={fmt_pct(p_rai,lo_rai,hi_rai)}, LN+={fmt_pct(p_ln,lo_ln,hi_ln)}")

        rows.append({
            "platform": plat, "n": n,
            "n_recurrent": n_recur,
            "recurrence_rate_pct": round(p_rec * 100, 1),
            "recurrence_ci_lo": round(lo_rec * 100, 1), "recurrence_ci_hi": round(hi_rec * 100, 1),
            "n_rai": n_rai,
            "rai_rate_pct": round(p_rai * 100, 1),
            "rai_ci_lo": round(lo_rai * 100, 1), "rai_ci_hi": round(hi_rai * 100, 1),
            "n_ln_positive": n_ln,
            "ln_positive_rate_pct": round(p_ln * 100, 1),
            "ln_ci_lo": round(lo_ln * 100, 1), "ln_ci_hi": round(hi_ln * 100, 1),
            "n_complication": n_comp,
            "complication_rate_pct": round(n_comp / n * 100, 1) if n > 0 else None,
            "proc_total_thyroidectomy_pct": proc.get("total_thyroidectomy", proc.get("total", None)),
            "proc_hemi_pct": proc.get("hemithyroidectomy", proc.get("hemi", None)),
            "ata_high": ata.get("high", 0),
            "ata_intermediate": ata.get("intermediate", 0),
            "ata_low": ata.get("low", 0),
        })

    # Chi-square test for recurrence difference
    ct = pd.crosstab(
        primary["mol_platform_family"],
        primary["any_recurrence_flag"].fillna(False)
    )
    p_chi = _chi2_p(ct)
    print(f"    Chi-square p-value (recurrence ThyroSeq vs Afirma): {p_chi:.4f}")

    # Logistic regression
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler

        m = primary.copy()
        m["is_thyroseq"] = (m["mol_platform_family"] == "ThyroSeq").astype(int)
        m["y"] = m["any_recurrence_flag"].fillna(False).astype(int)
        feats = []
        if "age_at_surgery" in m.columns:
            m["age_f"] = m["age_at_surgery"].fillna(m["age_at_surgery"].median()); feats.append("age_f")
        if "sex" in m.columns:
            m["sex_m"] = (m["sex"].str.lower() == "male").astype(int); feats.append("sex_m")
        if "tumor_size_cm_dominant" in m.columns:
            m["sz_f"] = m["tumor_size_cm_dominant"].fillna(m["tumor_size_cm_dominant"].median()); feats.append("sz_f")
        m["malig_f"] = m["is_malignant"].fillna(False).astype(int); feats.append("malig_f")
        feats.append("is_thyroseq")

        cmp = m[feats + ["y"]].dropna()
        if len(cmp) >= 30 and cmp["y"].sum() >= 5:
            X = cmp[feats].values
            y = cmp["y"].values
            X_s = StandardScaler().fit_transform(X)
            lr = LogisticRegression(max_iter=1000, random_state=42)
            lr.fit(X_s, y)
            ors = {f: round(math.exp(c), 3) for f, c in zip(feats, lr.coef_[0])}
            adj_or = ors.get("is_thyroseq", None)
            print(f"    Adjusted OR (ThyroSeq vs Afirma for recurrence): {adj_or} (N={len(cmp)}, events={cmp['y'].sum()})")
            lr_row = {"metric": "adjusted_OR_thyroseq_recurrence", "value": adj_or, "n": len(cmp), "events": int(cmp["y"].sum()), "covariates": str(feats[:-1])}
        else:
            lr_row = {"metric": "insufficient_data"}
    except Exception as e:
        lr_row = {"metric": f"error: {e}"}
        print(f"    LR error: {e}")

    result = pd.DataFrame(rows)
    result.to_csv(OUT_DIR / "outcomes_by_platform.csv", index=False)
    pd.DataFrame([lr_row]).to_csv(OUT_DIR / "outcomes_logistic_regression.csv", index=False)
    print("    → outcomes_by_platform.csv")
    return result


# ── Section 6: Utilization trends ─────────────────────────────────────────
def s6_utilization(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[6] Utilization trends …")

    td = df.dropna(subset=["test_year_clean"]).copy()
    td["yr"] = td["test_year_clean"].astype(int)

    # Volume by year and platform
    pivot = td.groupby(["yr", "mol_platform_family"]).size().reset_index(name="n_tests")
    total_yr = td.groupby("yr").size().reset_index(name="n_total")
    pivot = pivot.merge(total_yr, on="yr")
    pivot["pct_of_year"] = (pivot["n_tests"] / pivot["n_total"] * 100).round(1)

    pivot.rename(columns={"yr": "year"}, inplace=True)
    pivot.to_csv(OUT_DIR / "utilization_trends.csv", index=False)

    # Summary
    print(f"    Years: {sorted(pivot['year'].unique())}")
    for plat in ["ThyroSeq", "Afirma"]:
        sub = pivot[pivot["mol_platform_family"] == plat]
        if len(sub) > 0:
            fy, ly = sub["year"].min(), sub["year"].max()
            py = sub.loc[sub["n_tests"].idxmax(), "year"]
            total = sub["n_tests"].sum()
            print(f"    {plat}: {fy}–{ly}, peak={py}, total={total}")

    # Afirma GEC→GSC transition
    gec = td[(td["mol_platform_family"] == "Afirma") & (td["yr"] < 2017)]
    gsc = td[(td["mol_platform_family"] == "Afirma") & (td["yr"] >= 2017)]
    print(f"    Afirma: GEC-era (pre-2017) N={len(gec)}, GSC-era (2017+) N={len(gsc)}")

    print(f"    → utilization_trends.csv ({len(pivot)} rows)")
    return pivot


# ── Section 7: BRAF detection ─────────────────────────────────────────────
def s7_braf_detection(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[7] BRAF detection methods …")

    braf_pos = df[df["braf_positive_final"].fillna(False).astype(bool)].copy()
    n_total = len(braf_pos)
    print(f"    BRAF+ patients: {n_total}")

    rows = []
    if "braf_detection_method_v11" in braf_pos.columns:
        method_dist = braf_pos["braf_detection_method_v11"].value_counts()
        for method, cnt in method_dist.items():
            print(f"      {method}: {cnt} ({cnt/n_total*100:.1f}%)")
            rows.append({"detection_method": method, "n_braf_positive": cnt,
                         "pct_of_all_braf_positive": round(cnt / n_total * 100, 1)})

    if "braf_audit_tier" in braf_pos.columns:
        tier_dist = braf_pos["braf_audit_tier"].value_counts()
        print("    Audit tier breakdown:")
        for tier, cnt in tier_dist.items():
            print(f"      {tier}: {cnt}")

    # BRAF by platform
    plat_braf = []
    for plat in sorted(df["mol_platform_family"].dropna().unique()):
        sub = df[df["mol_platform_family"] == plat]
        n = len(sub); nb = bool_sum(sub["braf_positive_final"])
        if nb == 0: continue
        # Detection method breakdown within this platform
        if "braf_detection_method_v11" in sub.columns:
            sub_pos = sub[sub["braf_positive_final"].fillna(False).astype(bool)]
            mdist = sub_pos["braf_detection_method_v11"].value_counts().to_dict()
        else:
            mdist = {}
        plat_braf.append({"platform": plat, "n_total": n, "n_braf": nb,
                           "braf_rate_pct": round(nb / n * 100, 1),
                           **{f"method_{k}": v for k, v in mdist.items()}})

    braf_by_plat = pd.DataFrame(plat_braf)
    braf_by_plat.to_csv(OUT_DIR / "braf_by_platform.csv", index=False)

    result = pd.DataFrame(rows) if rows else pd.DataFrame()
    result.to_csv(OUT_DIR / "braf_detection_analysis.csv", index=False)
    print("    → braf_detection_analysis.csv, braf_by_platform.csv")
    return result


# ── Section 8: LaTeX tables ────────────────────────────────────────────────
def s8_latex(df: pd.DataFrame):
    print("\n[8] Building LaTeX tables …")

    ts = df[df["mol_platform_family"] == "ThyroSeq"]
    af = df[df["mol_platform_family"] == "Afirma"]

    def r(sub, col, as_pct=True):
        n = len(sub)
        if n == 0: return "—"
        if col == "ln_positive_final":
            nm = int((sub[col].fillna(0) > 0).sum())
        else:
            nm = bool_sum(sub[col]) if col in sub.columns else 0
        p, lo, hi = wilson_ci(nm, n)
        if as_pct:
            return f"{nm} ({p*100:.1f}\\%)"
        return str(nm)

    def rom(sub):
        n = len(sub); nm = bool_sum(sub["is_malignant"])
        p, lo, hi = wilson_ci(nm, n)
        return f"{nm}/{n} ({p*100:.1f}\\%, {lo*100:.1f}--{hi*100:.1f})"

    latex = textwrap.dedent(r"""
    \begin{table}[!htbp]
    \centering
    \caption{M033 --- Afirma vs ThyroSeq Molecular Platform Comparison (Primary Cohort)}
    \label{tab:m033_primary}
    \begin{tabular}{lcc}
    \toprule
    \textbf{Characteristic} & \textbf{ThyroSeq} (n=""" + str(len(ts)) + r""") & \textbf{Afirma} (n=""" + str(len(af)) + r""") \\
    \midrule
    \multicolumn{3}{l}{\textit{Malignancy}} \\
    """)

    latex += f"~~Malignancy rate (ROM) & {rom(ts)} & {rom(af)} \\\\\n"

    # Bethesda III
    ts_b3 = ts[ts["bethesda_final"] == 3]; af_b3 = af[af["bethesda_final"] == 3]
    ts_b4 = ts[ts["bethesda_final"] == 4]; af_b4 = af[af["bethesda_final"] == 4]
    latex += f"~~Bethesda III ROM & {rom(ts_b3)} & {rom(af_b3)} \\\\\n"
    latex += f"~~Bethesda IV ROM & {rom(ts_b4)} & {rom(af_b4)} \\\\\n"

    latex += textwrap.dedent(r"""
    \midrule
    \multicolumn{3}{l}{\textit{Molecular Findings}} \\
    """)
    latex += f"~~BRAF positive & {r(ts,'braf_positive_final')} & {r(af,'braf_positive_final')} \\\\\n"
    latex += f"~~RAS positive  & {r(ts,'ras_positive_final')} & {r(af,'ras_positive_final')} \\\\\n"
    latex += f"~~Fusion detected & {r(ts,'mol_has_fusion')} & {r(af,'mol_has_fusion')} \\\\\n"

    latex += textwrap.dedent(r"""
    \midrule
    \multicolumn{3}{l}{\textit{Outcomes}} \\
    """)
    latex += f"~~Recurrence & {r(ts,'any_recurrence_flag')} & {r(af,'any_recurrence_flag')} \\\\\n"
    latex += f"~~RAI received & {r(ts,'rai_received_reconciled')} & {r(af,'rai_received_reconciled')} \\\\\n"
    latex += f"~~LN positive & {r(ts,'ln_positive_final')} & {r(af,'ln_positive_final')} \\\\\n"

    latex += textwrap.dedent(r"""
    \bottomrule
    \multicolumn{3}{l}{\small ROM = Risk of Malignancy; CI = Wilson 95\% confidence interval.} \\
    \multicolumn{3}{l}{\small Sensitivity/specificity based on molecular\_risk\_tier (positive = high/intermediate).} \\
    \end{tabular}
    \end{table}
    """)

    # Version comparison table
    ver_data = {
        "Afirma GEC": df[df["mol_platform_resolved"] == "Afirma_GEC"],
        "Afirma GSC": df[df["mol_platform_resolved"] == "Afirma_GSC"],
        "ThyroSeq v2": df[df["mol_platform_resolved"] == "ThyroSeq_v2"],
        "ThyroSeq v3": df[df["mol_platform_resolved"] == "ThyroSeq_v3"],
    }
    latex += textwrap.dedent(r"""
    \begin{table}[!htbp]
    \centering
    \caption{M033 --- Molecular Platform Version Comparison}
    \label{tab:m033_version}
    \begin{tabular}{lcccc}
    \toprule
    \textbf{Metric} & \textbf{Afirma GEC} & \textbf{Afirma GSC} & \textbf{ThyroSeq v2} & \textbf{ThyroSeq v3} \\
    \midrule
    """)
    latex += "N & " + " & ".join(str(len(v)) for v in ver_data.values()) + " \\\\\n"

    def rom_short(sub):
        n = len(sub); nm = bool_sum(sub["is_malignant"])
        if n == 0: return "—"
        p, lo, hi = wilson_ci(nm, n)
        return f"{p*100:.1f}\\% ({lo*100:.1f}--{hi*100:.1f})"

    latex += "ROM & " + " & ".join(rom_short(v) for v in ver_data.values()) + " \\\\\n"
    latex += "BRAF+ & " + " & ".join(
        f"{bool_sum(v['braf_positive_final'])} ({bool_sum(v['braf_positive_final'])/len(v)*100:.1f}\\%)" if len(v) > 0 else "—"
        for v in ver_data.values()
    ) + " \\\\\n"
    latex += "Fusions & " + " & ".join(
        f"{bool_sum(v['mol_has_fusion'])} ({bool_sum(v['mol_has_fusion'])/len(v)*100:.1f}\\%)" if len(v) > 0 else "—"
        for v in ver_data.values()
    ) + " \\\\\n"

    latex += textwrap.dedent(r"""
    \bottomrule
    \end{tabular}
    \end{table}
    """)

    (OUT_DIR / "platform_comparison_summary.tex").write_text(latex)
    print("    → platform_comparison_summary.tex")


# ── Section 9: Upload to MD ────────────────────────────────────────────────
def s9_upload(conn, df: pd.DataFrame):
    print(f"\n[9] Uploading {MD_OUT_TABLE} …")

    out_df = df.copy()
    out_df["platform_primary"] = df["mol_platform_family"].where(
        df["mol_platform_family"].isin(["ThyroSeq", "Afirma"]), other="other")
    out_df["is_dual_platform"] = (df["mol_platform_family"] == "ThyroSeq+Afirma")
    out_df["is_primary_comparison"] = df["mol_platform_family"].isin(["ThyroSeq", "Afirma"])
    out_df["m033_run_ts"] = RUN_TS

    try:
        conn.execute(f"DROP TABLE IF EXISTS {MD_OUT_TABLE}")
        conn.execute(f"CREATE TABLE {MD_OUT_TABLE} AS SELECT * FROM out_df")
        cnt = conn.execute(f"SELECT COUNT(*) FROM {MD_OUT_TABLE}").fetchone()[0]
        print(f"    Uploaded {cnt:,} rows → {MD_OUT_TABLE}")
    except Exception as e:
        print(f"    ERROR: {e}")


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("M033 — Molecular Platform Comparison (Final)")
    print(f"Run: {RUN_TS}")
    print("=" * 60)

    conn = connect()
    df   = load_data(conn)

    s1_diagnostic_performance(df)
    s2_mutation_spectrum(df)
    s3_version_comparison(df)
    s4_dual_platform(df, conn)
    s5_outcomes(df)
    s6_utilization(df)
    s7_braf_detection(df)
    s8_latex(df)
    s9_upload(conn, df)

    print("\n" + "=" * 60)
    print(f"OUTPUTS → {OUT_DIR}/")
    print("=" * 60)
    for f in sorted(OUT_DIR.glob("*")):
        kb = f.stat().st_size / 1024
        print(f"  {f.name:<45} {kb:6.1f} KB")

    print("\nM033 complete.")


if __name__ == "__main__":
    main()

"""
M033 — Afirma vs ThyroSeq Molecular Platform Comparison
Comprehensive diagnostic performance, mutation spectrum, version comparison,
dual-platform concordance, outcomes, utilization trends, and BRAF detection analysis.

Usage:
    .venv/bin/python scripts/m033_platform_comparison.py

Outputs → studies/m033_platform_comparison/
"""

import sys
import os
import math
import warnings
import textwrap
from pathlib import Path
from datetime import datetime

import duckdb
import pandas as pd
import numpy as np
from scipy import stats

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
OUT_DIR = Path("studies/m033_platform_comparison")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOML_PATH = Path("motherduck.local.toml")
VIEW = "manuscript_workspace.m033_afirma_thyroseq_analytic_v1"
MD_OUT_TABLE = "manuscript_workspace.m033_platform_analysis_v1"

RUN_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ──────────────────────────────────────────────
# Token loader
# ──────────────────────────────────────────────
def _load_token() -> str:
    for env_key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        v = os.environ.get(env_key, "")
        if v:
            print(f"  token: {env_key}=SET (len={len(v)})")
            return v
    if TOML_PATH.exists():
        import toml
        cfg = toml.load(TOML_PATH)
        for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
            v = cfg.get(key, "")
            if v:
                print(f"  token: {key}=SET from {TOML_PATH} (len={len(v)})")
                return v
    raise RuntimeError("No MotherDuck token found in env or motherduck.local.toml")


# ──────────────────────────────────────────────
# Wilson confidence interval helper
# ──────────────────────────────────────────────
def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    """Return (proportion, lower, upper) with 95% Wilson CI."""
    if n == 0:
        return (float("nan"), float("nan"), float("nan"))
    p = k / n
    denom = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denom
    half = (z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2))) / denom
    return (p, max(0, center - half), min(1, center + half))


def fmt_pct(p, lo, hi, decimals=1):
    if math.isnan(p):
        return "N/A"
    return f"{p*100:.{decimals}f}% ({lo*100:.{decimals}f}–{hi*100:.{decimals}f}%)"


# ──────────────────────────────────────────────
# Connection
# ──────────────────────────────────────────────
def connect() -> duckdb.DuckDBPyConnection:
    token = _load_token()
    os.environ["MOTHERDUCK_TOKEN"] = token
    conn = duckdb.connect(f"md:thyroid_canonical_publication_v1_0")
    return conn


# ──────────────────────────────────────────────
# Section 0 — Verify view / schema
# ──────────────────────────────────────────────
def verify_view(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    print("\n[0] Verifying analytic view …")
    try:
        df = conn.execute(f"SELECT * FROM {VIEW} LIMIT 5").df()
        total = conn.execute(f"SELECT COUNT(*) FROM {VIEW}").fetchone()[0]
        print(f"    View OK — {total:,} rows, {len(df.columns)} columns")
        print(f"    Columns: {', '.join(df.columns)}")
        return conn.execute(f"SELECT * FROM {VIEW}").df()
    except Exception as e:
        print(f"  ERROR accessing view: {e}")
        print("  Attempting fallback from canonical_patient_master …")
        # fallback: build minimal dataset from CPM + extracted_molecular_panel_v1
        fallback_sql = """
        SELECT
            CAST(cpm.research_id AS VARCHAR) AS research_id,
            cpm.age_at_surgery,
            LOWER(cpm.sex) AS sex,
            cpm.race,
            cpm.histology_final,
            cpm.is_malignant,
            cpm.ajcc8_stage_group,
            cpm.any_recurrence_flag,
            cpm.braf_positive_final,
            cpm.ras_positive_final,
            cpm.mol_platform_family,
            cpm.mol_platform_resolved,
            cpm.ata_risk_category,
            cpm.rai_received_reconciled
        FROM main.canonical_patient_master cpm
        WHERE cpm.mol_platform_family IS NOT NULL
        """
        df = conn.execute(fallback_sql).df()
        print(f"    Fallback: {len(df):,} rows")
        return df


# ──────────────────────────────────────────────
# Section 1 — Platform diagnostic performance
# ──────────────────────────────────────────────
def platform_diagnostic_performance(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[1] Platform diagnostic performance …")
    
    # Primary comparison: ThyroSeq-only vs Afirma-only
    primary = df[df["mol_platform_family"].isin(["ThyroSeq", "Afirma"])].copy()
    
    rows = []
    for platform in ["ThyroSeq", "Afirma"]:
        sub = primary[primary["mol_platform_family"] == platform].copy()
        n = len(sub)
        
        # Malignancy rate (ROM)
        n_malig = int(sub["is_malignant"].fillna(False).astype(bool).sum())
        p, lo, hi = wilson_ci(n_malig, n)
        
        # Use molecular_risk_tier as proxy for test result if available
        has_tier = "molecular_risk_tier" in sub.columns
        
        if has_tier:
            # positive = high or intermediate risk tier
            sub["test_positive"] = sub["molecular_risk_tier"].isin(["high", "intermediate"])
            tp = int(((sub["test_positive"]) & (sub["is_malignant"].fillna(False))).sum())
            fp = int(((sub["test_positive"]) & (~sub["is_malignant"].fillna(False))).sum())
            tn = int(((~sub["test_positive"]) & (~sub["is_malignant"].fillna(False))).sum())
            fn = int(((~sub["test_positive"]) & (sub["is_malignant"].fillna(False))).sum())
            
            sens_p, sens_lo, sens_hi = wilson_ci(tp, tp + fn)
            spec_p, spec_lo, spec_hi = wilson_ci(tn, tn + fp)
            npv_p, npv_lo, npv_hi = wilson_ci(tn, tn + fn)
            ppv_p, ppv_lo, ppv_hi = wilson_ci(tp, tp + fp)
        else:
            tp = fp = tn = fn = None
            sens_p = sens_lo = sens_hi = float("nan")
            spec_p = spec_lo = spec_hi = float("nan")
            npv_p = npv_lo = npv_hi = float("nan")
            ppv_p = ppv_lo = ppv_hi = float("nan")
        
        row = {
            "platform": platform,
            "n_total": n,
            "n_malignant": n_malig,
            "rom_pct": round(p * 100, 1),
            "rom_ci_lo": round(lo * 100, 1),
            "rom_ci_hi": round(hi * 100, 1),
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
        }
        rows.append(row)
        print(f"    {platform}: N={n}, ROM={p*100:.1f}% ({lo*100:.1f}–{hi*100:.1f}%)")
    
    # Also add all platform groups for completeness
    for platform in sorted(df["mol_platform_family"].dropna().unique()):
        if platform in ["ThyroSeq", "Afirma"]:
            continue
        sub = df[df["mol_platform_family"] == platform]
        n = len(sub)
        n_malig = int(sub["is_malignant"].fillna(False).astype(bool).sum())
        p, lo, hi = wilson_ci(n_malig, n)
        rows.append({
            "platform": platform, "n_total": n, "n_malignant": n_malig,
            "rom_pct": round(p * 100, 1), "rom_ci_lo": round(lo * 100, 1), "rom_ci_hi": round(hi * 100, 1),
            "tp": None, "fp": None, "tn": None, "fn": None,
            "sensitivity_pct": None, "sensitivity_ci_lo": None, "sensitivity_ci_hi": None,
            "specificity_pct": None, "specificity_ci_lo": None, "specificity_ci_hi": None,
            "npv_pct": None, "npv_ci_lo": None, "npv_ci_hi": None,
            "ppv_pct": None, "ppv_ci_lo": None, "ppv_ci_hi": None,
        })
    
    # Bethesda stratification if available
    if "bethesda_final" in df.columns:
        print("    Bethesda stratification (III/IV) …")
        beth_rows = []
        for platform in ["ThyroSeq", "Afirma"]:
            sub = df[(df["mol_platform_family"] == platform) & (df["bethesda_final"].isin([3, 4]))]
            n = len(sub)
            n_malig = int(sub["is_malignant"].fillna(False).astype(bool).sum())
            p, lo, hi = wilson_ci(n_malig, n)
            beth_rows.append({
                "platform": platform, "bethesda_cat": "III/IV",
                "n": n, "n_malignant": n_malig,
                "rom_pct": round(p * 100, 1), "rom_ci_lo": round(lo * 100, 1), "rom_ci_hi": round(hi * 100, 1),
            })
        pd.DataFrame(beth_rows).to_csv(OUT_DIR / "rom_by_bethesda.csv", index=False)
        print(f"    Bethesda ROM saved")
    
    result = pd.DataFrame(rows)
    result.to_csv(OUT_DIR / "platform_diagnostic_performance.csv", index=False)
    print(f"    Saved platform_diagnostic_performance.csv ({len(result)} rows)")
    return result


# ──────────────────────────────────────────────
# Section 2 — Mutation spectrum by platform
# ──────────────────────────────────────────────
def mutation_spectrum(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[2] Mutation spectrum by platform …")
    
    rows = []
    for platform in sorted(df["mol_platform_family"].dropna().unique()):
        sub = df[df["mol_platform_family"] == platform].copy()
        n = len(sub)
        
        # BRAF
        braf_col = "braf_positive_final" if "braf_positive_final" in sub.columns else None
        n_braf = int(sub[braf_col].fillna(False).astype(bool).sum()) if braf_col else None
        
        # RAS
        ras_col = "ras_positive_final" if "ras_positive_final" in sub.columns else None
        n_ras = int(sub[ras_col].fillna(False).astype(bool).sum()) if ras_col else None
        
        # Fusions
        fus_col = "mol_has_fusion" if "mol_has_fusion" in sub.columns else None
        n_fus = int(sub[fus_col].fillna(False).astype(bool).sum()) if fus_col else None
        
        # Risk tier distribution
        if "molecular_risk_tier" in sub.columns:
            tier_dist = sub["molecular_risk_tier"].value_counts().to_dict()
        else:
            tier_dist = {}
        
        # Median genes tested
        if "mol_n_distinct_genes" in sub.columns:
            median_genes = sub["mol_n_distinct_genes"].median()
        else:
            median_genes = None
        
        row = {
            "platform": platform,
            "n": n,
            "n_braf_positive": n_braf,
            "braf_rate_pct": round(n_braf / n * 100, 1) if n_braf is not None and n > 0 else None,
            "n_ras_positive": n_ras,
            "ras_rate_pct": round(n_ras / n * 100, 1) if n_ras is not None and n > 0 else None,
            "n_fusion": n_fus,
            "fusion_rate_pct": round(n_fus / n * 100, 1) if n_fus is not None and n > 0 else None,
            "median_genes_tested": median_genes,
            "tier_high": tier_dist.get("high", 0),
            "tier_intermediate": tier_dist.get("intermediate", 0),
            "tier_low": tier_dist.get("low", 0),
            "tier_wild_type": tier_dist.get("wild_type", tier_dist.get("negative", 0)),
            "tier_unknown": tier_dist.get("unknown", 0),
        }
        rows.append(row)
        print(f"    {platform}: N={n}, BRAF={n_braf}, RAS={n_ras}, Fusions={n_fus}")
    
    result = pd.DataFrame(rows)
    result.to_csv(OUT_DIR / "mutation_spectrum_by_platform.csv", index=False)
    print(f"    Saved mutation_spectrum_by_platform.csv")
    return result


# ──────────────────────────────────────────────
# Section 3 — Version comparison
# ──────────────────────────────────────────────
def version_comparison(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[3] Version comparison …")
    
    if "mol_platform_resolved" not in df.columns:
        print("    mol_platform_resolved column missing — skipping")
        return pd.DataFrame()
    
    version_groups = {
        "Afirma_GEC": "Afirma GEC (pre-2017)",
        "Afirma_GSC": "Afirma GSC (post-2017)",
        "ThyroSeq_v2": "ThyroSeq v2 (pre-2018)",
        "ThyroSeq_v3": "ThyroSeq v3 (post-2018)",
    }
    
    rows = []
    for version_key, version_label in version_groups.items():
        sub = df[df["mol_platform_resolved"] == version_key].copy()
        n = len(sub)
        if n == 0:
            continue
        
        n_malig = int(sub["is_malignant"].fillna(False).astype(bool).sum())
        p_rom, lo_rom, hi_rom = wilson_ci(n_malig, n)
        
        braf_col = "braf_positive_final" if "braf_positive_final" in sub.columns else None
        n_braf = int(sub[braf_col].fillna(False).astype(bool).sum()) if braf_col else None
        
        fus_col = "mol_has_fusion" if "mol_has_fusion" in sub.columns else None
        n_fus = int(sub[fus_col].fillna(False).astype(bool).sum()) if fus_col else None
        
        recur_col = "any_recurrence_flag" if "any_recurrence_flag" in sub.columns else None
        n_recur = int(sub[recur_col].fillna(False).astype(bool).sum()) if recur_col else None
        p_recur, lo_r, hi_r = wilson_ci(n_recur, n) if n_recur is not None else (float("nan"), float("nan"), float("nan"))
        
        # Specificity proxy: test-negative (low/wild_type) among benign
        if "molecular_risk_tier" in sub.columns:
            benign = sub[~sub["is_malignant"].fillna(False)]
            n_benign = len(benign)
            test_neg_benign = int(benign["molecular_risk_tier"].isin(["low", "wild_type", "negative"]).sum())
            spec_p, spec_lo, spec_hi = wilson_ci(test_neg_benign, n_benign)
        else:
            spec_p = spec_lo = spec_hi = float("nan")
            n_benign = None
        
        row = {
            "version": version_key,
            "label": version_label,
            "n": n,
            "n_malignant": n_malig,
            "rom_pct": round(p_rom * 100, 1),
            "rom_ci_lo": round(lo_rom * 100, 1),
            "rom_ci_hi": round(hi_rom * 100, 1),
            "n_braf": n_braf,
            "braf_rate_pct": round(n_braf / n * 100, 1) if n_braf is not None else None,
            "n_fusion": n_fus,
            "fusion_rate_pct": round(n_fus / n * 100, 1) if n_fus is not None else None,
            "n_recurrent": n_recur,
            "recurrence_rate_pct": round(p_recur * 100, 1) if not math.isnan(p_recur) else None,
            "n_benign": n_benign,
            "specificity_proxy_pct": round(spec_p * 100, 1) if not math.isnan(spec_p) else None,
            "specificity_proxy_ci_lo": round(spec_lo * 100, 1) if not math.isnan(spec_lo) else None,
            "specificity_proxy_ci_hi": round(spec_hi * 100, 1) if not math.isnan(spec_hi) else None,
        }
        rows.append(row)
        print(f"    {version_label}: N={n}, ROM={p_rom*100:.1f}%")
    
    # Also capture version_unknown groups
    for platform in ["Afirma", "ThyroSeq"]:
        sub = df[
            (df["mol_platform_family"] == platform) &
            (df["mol_platform_resolved"].str.contains("version_unknown", na=False))
        ].copy()
        if len(sub) == 0:
            continue
        n = len(sub)
        n_malig = int(sub["is_malignant"].fillna(False).astype(bool).sum())
        p_rom, lo_rom, hi_rom = wilson_ci(n_malig, n)
        rows.append({
            "version": f"{platform}_version_unknown",
            "label": f"{platform} (version unknown)",
            "n": n,
            "n_malignant": n_malig,
            "rom_pct": round(p_rom * 100, 1),
            "rom_ci_lo": round(lo_rom * 100, 1),
            "rom_ci_hi": round(hi_rom * 100, 1),
            "n_braf": None, "braf_rate_pct": None,
            "n_fusion": None, "fusion_rate_pct": None,
            "n_recurrent": None, "recurrence_rate_pct": None,
            "n_benign": None,
            "specificity_proxy_pct": None, "specificity_proxy_ci_lo": None, "specificity_proxy_ci_hi": None,
        })
    
    result = pd.DataFrame(rows)
    result.to_csv(OUT_DIR / "version_comparison.csv", index=False)
    print(f"    Saved version_comparison.csv ({len(result)} rows)")
    return result


# ──────────────────────────────────────────────
# Section 4 — Dual-platform concordance
# ──────────────────────────────────────────────
def dual_platform_concordance(df: pd.DataFrame, conn=None) -> pd.DataFrame:
    print("\n[4] Dual-platform concordance …")
    
    # Try to use dedicated m083 dual-platform view
    dual_m083 = None
    if conn is not None:
        try:
            dual_m083 = conn.execute("SELECT * FROM manuscript_workspace.m083_dual_platform_analytic_v1").df()
            print(f"    m083 dual-platform view: {len(dual_m083)} rows")
        except Exception as e:
            print(f"    m083 view unavailable ({e}), using m033 ThyroSeq+Afirma subset")
    
    dual = df[df["mol_platform_family"] == "ThyroSeq+Afirma"].copy()
    n_dual = len(dual)
    print(f"    Dual-platform patients: N={n_dual}")
    
    rows = []
    
    # Basic metrics
    n_malig = int(dual["is_malignant"].fillna(False).astype(bool).sum())
    p_rom, lo_rom, hi_rom = wilson_ci(n_malig, n_dual)
    
    n_braf = int(dual["braf_positive_final"].fillna(False).astype(bool).sum()) if "braf_positive_final" in dual.columns else None
    n_recur = int(dual["any_recurrence_flag"].fillna(False).astype(bool).sum()) if "any_recurrence_flag" in dual.columns else None
    
    rows.append({
        "metric": "Total dual-platform patients", "value": n_dual, "ci": ""
    })
    rows.append({
        "metric": "Malignancy rate (ROM)",
        "value": f"{p_rom*100:.1f}%",
        "ci": f"({lo_rom*100:.1f}–{hi_rom*100:.1f}%)"
    })
    if n_braf is not None:
        p_b, lo_b, hi_b = wilson_ci(n_braf, n_dual)
        rows.append({
            "metric": "BRAF positive rate",
            "value": f"{p_b*100:.1f}%",
            "ci": f"({lo_b*100:.1f}–{hi_b*100:.1f}%)"
        })
    
    # Discordance analysis if braf_discordance_flag available
    if "braf_discordance_flag" in dual.columns:
        n_discord = int(dual["braf_discordance_flag"].fillna(False).astype(bool).sum())
        n_concord = n_dual - n_discord
        rows.append({
            "metric": "BRAF concordant (both platforms agree)",
            "value": n_concord,
            "ci": f"({n_concord/n_dual*100:.1f}%)"
        })
        rows.append({
            "metric": "BRAF discordant",
            "value": n_discord,
            "ci": f"({n_discord/n_dual*100:.1f}%)"
        })
        
        # When discordant, does malignancy match either platform?
        discord_sub = dual[dual["braf_discordance_flag"].fillna(False).astype(bool)]
        if len(discord_sub) > 0:
            n_disc_malig = int(discord_sub["is_malignant"].fillna(False).astype(bool).sum())
            rows.append({
                "metric": "Discordant patients: malignant",
                "value": n_disc_malig,
                "ci": f"({n_disc_malig/len(discord_sub)*100:.1f}%)"
            })
    
    # Recurrence in dual vs single platform
    if n_recur is not None:
        p_r, lo_r, hi_r = wilson_ci(n_recur, n_dual)
        rows.append({
            "metric": "Recurrence rate (dual-platform)",
            "value": f"{p_r*100:.1f}%",
            "ci": f"({lo_r*100:.1f}–{hi_r*100:.1f}%)"
        })
    
    # Compare single ThyroSeq vs single Afirma vs dual
    ts_only = df[df["mol_platform_family"] == "ThyroSeq"]
    af_only = df[df["mol_platform_family"] == "Afirma"]
    
    for label, sub in [("ThyroSeq-only", ts_only), ("Afirma-only", af_only), ("Dual-platform", dual)]:
        n = len(sub)
        n_m = int(sub["is_malignant"].fillna(False).astype(bool).sum())
        p, lo, hi = wilson_ci(n_m, n)
        n_r = int(sub["any_recurrence_flag"].fillna(False).astype(bool).sum()) if "any_recurrence_flag" in sub.columns else 0
        p_rec, lo_rec, hi_rec = wilson_ci(n_r, n)
        rows.append({
            "metric": f"{label} — N",
            "value": n,
            "ci": ""
        })
        rows.append({
            "metric": f"{label} — ROM",
            "value": f"{p*100:.1f}%",
            "ci": f"({lo*100:.1f}–{hi*100:.1f}%)"
        })
        rows.append({
            "metric": f"{label} — Recurrence rate",
            "value": f"{p_rec*100:.1f}%",
            "ci": f"({lo_rec*100:.1f}–{hi_rec*100:.1f}%)"
        })
    
    result = pd.DataFrame(rows)
    result.to_csv(OUT_DIR / "dual_platform_concordance.csv", index=False)
    print(f"    Saved dual_platform_concordance.csv")
    return result


# ──────────────────────────────────────────────
# Section 5 — Outcomes by platform
# ──────────────────────────────────────────────
def outcomes_by_platform(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[5] Outcomes by platform …")
    
    primary = df[df["mol_platform_family"].isin(["ThyroSeq", "Afirma"])].copy()
    
    rows = []
    for platform in ["ThyroSeq", "Afirma"]:
        sub = primary[primary["mol_platform_family"] == platform].copy()
        n = len(sub)
        
        # Recurrence
        recur_col = "any_recurrence_flag" if "any_recurrence_flag" in sub.columns else None
        n_recur = int(sub[recur_col].fillna(False).astype(bool).sum()) if recur_col else None
        p_rec, lo_rec, hi_rec = wilson_ci(n_recur, n) if n_recur is not None else (float("nan"),)*3
        
        # RAI
        rai_col = "rai_received_reconciled" if "rai_received_reconciled" in sub.columns else None
        n_rai = int(sub[rai_col].fillna(False).astype(bool).sum()) if rai_col else None
        p_rai, lo_rai, hi_rai = wilson_ci(n_rai, n) if n_rai is not None else (float("nan"),)*3
        
        # Surgery type
        proc_col = "surg_procedure_type" if "surg_procedure_type" in sub.columns else None
        if proc_col:
            proc_dist = sub[proc_col].value_counts(normalize=True).mul(100).round(1).to_dict()
        else:
            proc_dist = {}
        
        # ATA risk
        ata_col = "ata_risk_category" if "ata_risk_category" in sub.columns else None
        if ata_col:
            ata_dist = sub[ata_col].value_counts().to_dict()
        else:
            ata_dist = {}
        
        # LN positive (INTEGER column — >0 means positive)
        ln_col = "ln_positive_final" if "ln_positive_final" in sub.columns else None
        n_ln = int((sub[ln_col].fillna(0) > 0).sum()) if ln_col else None
        p_ln, lo_ln, hi_ln = wilson_ci(n_ln, n) if n_ln is not None else (float("nan"),)*3
        
        row = {
            "platform": platform,
            "n": n,
            "n_recurrent": n_recur,
            "recurrence_rate_pct": round(p_rec * 100, 1) if not math.isnan(p_rec) else None,
            "recurrence_ci_lo": round(lo_rec * 100, 1) if not math.isnan(lo_rec) else None,
            "recurrence_ci_hi": round(hi_rec * 100, 1) if not math.isnan(hi_rec) else None,
            "n_rai": n_rai,
            "rai_rate_pct": round(p_rai * 100, 1) if not math.isnan(p_rai) else None,
            "rai_ci_lo": round(lo_rai * 100, 1) if not math.isnan(lo_rai) else None,
            "rai_ci_hi": round(hi_rai * 100, 1) if not math.isnan(hi_rai) else None,
            "n_ln_positive": n_ln,
            "ln_positive_rate_pct": round(p_ln * 100, 1) if not math.isnan(p_ln) else None,
            "proc_total_pct": proc_dist.get("total_thyroidectomy", proc_dist.get("total", None)),
            "proc_hemi_pct": proc_dist.get("hemithyroidectomy", proc_dist.get("hemi", None)),
            "ata_high": ata_dist.get("high", 0),
            "ata_intermediate": ata_dist.get("intermediate", 0),
            "ata_low": ata_dist.get("low", 0),
        }
        rows.append(row)
        print(f"    {platform}: Recurrence={p_rec*100:.1f}%, RAI={p_rai*100:.1f}%")
    
    # Logistic regression for recurrence adjusted for confounders
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        
        model_df = primary.copy()
        model_df["is_thyroseq"] = (model_df["mol_platform_family"] == "ThyroSeq").astype(int)
        model_df["y"] = model_df["any_recurrence_flag"].fillna(False).astype(int)
        
        # Build feature set
        feature_cols = []
        if "age_at_surgery" in model_df.columns:
            model_df["age_scaled"] = model_df["age_at_surgery"].fillna(model_df["age_at_surgery"].median())
            feature_cols.append("age_scaled")
        if "sex" in model_df.columns:
            model_df["sex_male"] = (model_df["sex"].str.lower() == "male").astype(int)
            feature_cols.append("sex_male")
        if "tumor_size_cm_dominant" in model_df.columns:
            model_df["size_filled"] = model_df["tumor_size_cm_dominant"].fillna(model_df["tumor_size_cm_dominant"].median())
            feature_cols.append("size_filled")
        if "is_malignant" in model_df.columns:
            model_df["malignant_int"] = model_df["is_malignant"].fillna(False).astype(int)
            feature_cols.append("malignant_int")
        feature_cols.append("is_thyroseq")
        
        complete = model_df[feature_cols + ["y"]].dropna()
        if len(complete) >= 20 and complete["y"].sum() >= 5:
            X = complete[feature_cols].values
            y = complete["y"].values
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            lr = LogisticRegression(max_iter=500, random_state=42)
            lr.fit(X_scaled, y)
            coefs = dict(zip(feature_cols, lr.coef_[0]))
            odds_ratios = {k: round(math.exp(v), 3) for k, v in coefs.items()}
            lr_row = {"metric": "logistic_regression_thyroseq_or_recurrence", **odds_ratios, "n_complete_cases": len(complete)}
            print(f"    LR complete: N={len(complete)}, ThyroSeq OR={odds_ratios.get('is_thyroseq','N/A')}")
        else:
            lr_row = {"metric": "logistic_regression_insufficient_data"}
            print(f"    LR: insufficient data ({len(complete)} rows, {complete['y'].sum() if 'y' in complete.columns else 0} events)")
    except ImportError:
        lr_row = {"metric": "logistic_regression_sklearn_unavailable"}
        print("    sklearn unavailable — LR skipped")
    except Exception as e:
        lr_row = {"metric": f"logistic_regression_error: {e}"}
        print(f"    LR error: {e}")
    
    result = pd.DataFrame(rows)
    # Append LR result as separate row
    lr_df = pd.DataFrame([lr_row])
    result.to_csv(OUT_DIR / "outcomes_by_platform.csv", index=False)
    lr_df.to_csv(OUT_DIR / "outcomes_logistic_regression.csv", index=False)
    print(f"    Saved outcomes_by_platform.csv")
    return result


# ──────────────────────────────────────────────
# Section 6 — Utilization trends
# ──────────────────────────────────────────────
def utilization_trends(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[6] Utilization trends …")
    
    date_col = "mol_first_test_date" if "mol_first_test_date" in df.columns else None
    if date_col is None:
        print("    mol_first_test_date missing — skipping")
        return pd.DataFrame()
    
    trend_df = df.copy()
    trend_df["test_date"] = pd.to_datetime(trend_df[date_col], errors="coerce")
    trend_df = trend_df.dropna(subset=["test_date"])
    trend_df["year"] = trend_df["test_date"].dt.year
    
    # Volume by year and platform family
    pivot = trend_df.groupby(["year", "mol_platform_family"]).size().reset_index(name="n_tests")
    total_by_year = trend_df.groupby("year").size().reset_index(name="n_total")
    pivot = pivot.merge(total_by_year, on="year")
    pivot["pct_of_year"] = (pivot["n_tests"] / pivot["n_total"] * 100).round(1)
    
    pivot.to_csv(OUT_DIR / "utilization_trends.csv", index=False)
    print(f"    Saved utilization_trends.csv ({len(pivot)} rows, years {pivot['year'].min()}-{pivot['year'].max()})")
    
    # Summary stats
    print(f"    Years covered: {sorted(pivot['year'].unique())}")
    for platform in ["ThyroSeq", "Afirma"]:
        sub = pivot[pivot["mol_platform_family"] == platform]
        if len(sub) > 0:
            first_year = sub["year"].min()
            last_year = sub["year"].max()
            peak_year = sub.loc[sub["n_tests"].idxmax(), "year"]
            print(f"    {platform}: {first_year}–{last_year}, peak in {peak_year}")
    
    return pivot


# ──────────────────────────────────────────────
# Section 7 — BRAF detection method analysis
# ──────────────────────────────────────────────
def braf_detection_analysis(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[7] BRAF detection method analysis …")
    
    braf_pos = df[df["braf_positive_final"].fillna(False).astype(bool)].copy()
    n_braf = len(braf_pos)
    print(f"    Total BRAF+ patients: {n_braf}")
    
    rows = []
    
    if "braf_detection_method_v11" in braf_pos.columns:
        method_dist = braf_pos["braf_detection_method_v11"].value_counts()
        for method, count in method_dist.items():
            rows.append({
                "detection_method": method,
                "n_braf_positive": count,
                "pct_of_braf_positive": round(count / n_braf * 100, 1),
            })
        print(f"    Detection methods: {method_dist.to_dict()}")
    else:
        print("    braf_detection_method_v11 not available")
    
    if "braf_audit_tier" in braf_pos.columns:
        tier_dist = braf_pos["braf_audit_tier"].value_counts()
        for tier, count in tier_dist.items():
            row = next((r for r in rows if r.get("detection_method") == tier), None)
            if row:
                row["audit_tier"] = tier
                row["audit_tier_count"] = count
            else:
                rows.append({
                    "detection_method": tier,
                    "n_braf_positive": count,
                    "pct_of_braf_positive": round(count / n_braf * 100, 1),
                    "audit_tier": tier,
                    "audit_tier_count": count,
                })
        print(f"    Audit tiers: {tier_dist.to_dict()}")
    
    # Platform breakdown of BRAF detection
    if "mol_platform_family" in braf_pos.columns:
        platform_braf = braf_pos.groupby("mol_platform_family").size().reset_index(name="n_braf")
        platform_total = df.groupby("mol_platform_family").size().reset_index(name="n_total")
        platform_braf_table = platform_braf.merge(platform_total, on="mol_platform_family")
        platform_braf_table["braf_rate_pct"] = (platform_braf_table["n_braf"] / platform_braf_table["n_total"] * 100).round(1)
        platform_braf_table.to_csv(OUT_DIR / "braf_by_platform.csv", index=False)
        print(f"    Saved braf_by_platform.csv")
    
    result = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["detection_method", "n_braf_positive", "pct_of_braf_positive"])
    result.to_csv(OUT_DIR / "braf_detection_analysis.csv", index=False)
    print(f"    Saved braf_detection_analysis.csv")
    return result


# ──────────────────────────────────────────────
# Section 8 — LaTeX summary table
# ──────────────────────────────────────────────
def build_latex_tables(df: pd.DataFrame):
    print("\n[8] Building LaTeX summary tables …")
    
    primary = df[df["mol_platform_family"].isin(["ThyroSeq", "Afirma"])].copy()
    
    def _fmt_n_pct(n, total):
        if total == 0:
            return "0"
        return f"{n} ({n/total*100:.1f}\\%)"
    
    rows_ts = primary[primary["mol_platform_family"] == "ThyroSeq"]
    rows_af = primary[primary["mol_platform_family"] == "Afirma"]
    
    def _rom(sub):
        n = len(sub)
        nm = int(sub["is_malignant"].fillna(False).astype(bool).sum())
        p, lo, hi = wilson_ci(nm, n)
        return f"{p*100:.1f}\\% ({lo*100:.1f}--{hi*100:.1f}\\%)"
    
    def _rate(sub, col):
        n = len(sub)
        if col not in sub.columns:
            return "N/A"
        nm = int(sub[col].fillna(False).astype(bool).sum())
        p, lo, hi = wilson_ci(nm, n)
        return f"{p*100:.1f}\\% ({lo*100:.1f}--{hi*100:.1f}\\%)"
    
    latex = textwrap.dedent(r"""
    \begin{table}[h]
    \centering
    \caption{M033 -- Afirma vs ThyroSeq Molecular Platform Comparison}
    \label{tab:m033_platform_comparison}
    \begin{tabular}{lcc}
    \toprule
    \textbf{Characteristic} & \textbf{ThyroSeq} & \textbf{Afirma} \\
    \midrule
    """)
    
    latex += f"N (\\%) & {len(rows_ts)} & {len(rows_af)} \\\\\n"
    latex += f"Malignancy rate (ROM) & {_rom(rows_ts)} & {_rom(rows_af)} \\\\\n"
    
    if "braf_positive_final" in df.columns:
        latex += f"BRAF positive & {_rate(rows_ts, 'braf_positive_final')} & {_rate(rows_af, 'braf_positive_final')} \\\\\n"
    if "ras_positive_final" in df.columns:
        latex += f"RAS positive & {_rate(rows_ts, 'ras_positive_final')} & {_rate(rows_af, 'ras_positive_final')} \\\\\n"
    if "mol_has_fusion" in df.columns:
        latex += f"Fusion detected & {_rate(rows_ts, 'mol_has_fusion')} & {_rate(rows_af, 'mol_has_fusion')} \\\\\n"
    if "any_recurrence_flag" in df.columns:
        latex += f"Recurrence & {_rate(rows_ts, 'any_recurrence_flag')} & {_rate(rows_af, 'any_recurrence_flag')} \\\\\n"
    if "rai_received_reconciled" in df.columns:
        latex += f"RAI received & {_rate(rows_ts, 'rai_received_reconciled')} & {_rate(rows_af, 'rai_received_reconciled')} \\\\\n"
    
    latex += textwrap.dedent(r"""
    \bottomrule
    \end{tabular}
    \end{table}
    """)
    
    # Version comparison table
    latex += textwrap.dedent(r"""
    \begin{table}[h]
    \centering
    \caption{M033 -- Platform Version Comparison}
    \label{tab:m033_version_comparison}
    \begin{tabular}{lcccc}
    \toprule
    \textbf{Metric} & \textbf{Afirma GEC} & \textbf{Afirma GSC} & \textbf{ThyroSeq v2} & \textbf{ThyroSeq v3} \\
    \midrule
    """)
    
    versions = ["Afirma_GEC", "Afirma_GSC", "ThyroSeq_v2", "ThyroSeq_v3"]
    ver_dfs = {}
    if "mol_platform_resolved" in df.columns:
        for v in versions:
            ver_dfs[v] = df[df["mol_platform_resolved"] == v]
    
    for v in versions:
        sub = ver_dfs.get(v, pd.DataFrame())
        n = len(sub)
        if n > 0:
            nm = int(sub["is_malignant"].fillna(False).astype(bool).sum())
            p, lo, hi = wilson_ci(nm, n)
            ver_dfs[v + "_str"] = f"{p*100:.1f}\\% ({lo*100:.1f}--{hi*100:.1f}\\%)"
        else:
            ver_dfs[v + "_str"] = "N/A"
    
    def _vs(v):
        return ver_dfs.get(v + "_str", "N/A")
    
    ns = {v: len(ver_dfs.get(v, pd.DataFrame())) for v in versions}
    latex += f"N & {ns['Afirma_GEC']} & {ns['Afirma_GSC']} & {ns['ThyroSeq_v2']} & {ns['ThyroSeq_v3']} \\\\\n"
    latex += f"ROM & {_vs('Afirma_GEC')} & {_vs('Afirma_GSC')} & {_vs('ThyroSeq_v2')} & {_vs('ThyroSeq_v3')} \\\\\n"
    
    latex += textwrap.dedent(r"""
    \bottomrule
    \end{tabular}
    \end{table}
    """)
    
    latex_path = OUT_DIR / "platform_comparison_summary.tex"
    latex_path.write_text(latex)
    print(f"    Saved platform_comparison_summary.tex")


# ──────────────────────────────────────────────
# Section 9 — Upload to MotherDuck
# ──────────────────────────────────────────────
def upload_to_motherduck(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    print(f"\n[9] Uploading patient-level analysis to {MD_OUT_TABLE} …")
    
    # Create enriched patient-level table with platform performance flags
    analysis_df = df.copy()
    
    # Add derived fields
    analysis_df["platform_primary"] = df["mol_platform_family"].where(
        df["mol_platform_family"].isin(["ThyroSeq", "Afirma"]), other="other"
    )
    analysis_df["is_dual_platform"] = (df["mol_platform_family"] == "ThyroSeq+Afirma").astype(bool)
    analysis_df["is_primary_comparison"] = df["mol_platform_family"].isin(["ThyroSeq", "Afirma"])
    analysis_df["m033_analysis_ts"] = RUN_TS
    
    try:
        conn.execute(f"DROP TABLE IF EXISTS {MD_OUT_TABLE}")
        conn.execute(f"CREATE TABLE {MD_OUT_TABLE} AS SELECT * FROM analysis_df")
        count = conn.execute(f"SELECT COUNT(*) FROM {MD_OUT_TABLE}").fetchone()[0]
        print(f"    Uploaded {count:,} rows to {MD_OUT_TABLE}")
    except Exception as e:
        print(f"    ERROR uploading to MotherDuck: {e}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    print("=" * 60)
    print("M033 — Molecular Platform Comparison")
    print(f"Run: {RUN_TS}")
    print("=" * 60)
    
    # Connect
    conn = connect()
    
    # Load data
    df = verify_view(conn)
    
    if df is None or len(df) == 0:
        print("ERROR: No data loaded. Exiting.")
        sys.exit(1)
    
    # Booleans from BOOLEAN type — already Python True/False/None
    # ln_positive_final is INTEGER — convert to bool for flag checks
    if "ln_positive_final" in df.columns:
        df["ln_positive_flag"] = df["ln_positive_final"].fillna(0) > 0
    
    # Try to join Bethesda from CPM
    try:
        beth_df = conn.execute("""
            SELECT CAST(research_id AS VARCHAR) AS research_id,
                   CAST(bethesda_final AS INTEGER) AS bethesda_final
            FROM main.canonical_patient_master
            WHERE bethesda_final IS NOT NULL
        """).df()
        if len(beth_df) > 0:
            df = df.merge(beth_df, on="research_id", how="left")
            n_beth = df["bethesda_final"].notna().sum()
            print(f"    Bethesda joined: {n_beth} patients have Bethesda")
    except Exception as e:
        print(f"    Bethesda join skipped: {e}")
    
    print(f"\nDataset: {len(df):,} patients")
    if "mol_platform_family" in df.columns:
        print(f"Platform distribution:\n{df['mol_platform_family'].value_counts().to_string()}")
    
    # Run all analyses
    perf_df = platform_diagnostic_performance(df)
    mut_df = mutation_spectrum(df)
    ver_df = version_comparison(df)
    dual_df = dual_platform_concordance(df, conn=conn)
    out_df = outcomes_by_platform(df)
    trend_df = utilization_trends(df)
    braf_df = braf_detection_analysis(df)
    build_latex_tables(df)
    upload_to_motherduck(conn, df)
    
    # Summary
    print("\n" + "=" * 60)
    print("OUTPUTS")
    print("=" * 60)
    for f in sorted(OUT_DIR.glob("*")):
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name} ({size_kb:.1f} KB)")
    
    print("\nM033 analysis complete.")


if __name__ == "__main__":
    main()

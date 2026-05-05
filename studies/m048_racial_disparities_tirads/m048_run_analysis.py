#!/usr/bin/env python3
"""
M048 — Racial Disparities in ACR TI-RADS Performance
Analysis pipeline: SQL execution, Wilson CIs, bootstrap AUC, chi-square tests.

Steps executed:
  1. Connect to MotherDuck (thyroid_canonical_publication_v1_0)
  2. Execute M048 SQL tables (views + tables)
  3. Dump all SQL-derived tables to CSV
  4. Compute Wilson 95% CIs for ROM, sens/spec/PPV/NPV
  5. Bootstrap 1,000 replicates × race × grain for AUC 95% CIs
  6. Chi-square + Bonferroni for 5 feature-score distributions
  7. Compute inflation (patient ROM − nodule ROM) per race × TR
  8. Write m048_run_snapshot.json

SCOPE: NO DOCX, NO PROSE, NO ABSTRACT, NO MANUSCRIPT.
QA gates are HARD gates — if any gate fails, the run does NOT sign off.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from math import sqrt

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency
from sklearn.metrics import roc_auc_score

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

STUDY_DIR = os.path.dirname(os.path.abspath(__file__))
VERIF_DIR = os.path.join(STUDY_DIR, "verification")
SQL_FILE = os.path.join(STUDY_DIR, "M048_motherduck_queries.sql")

os.makedirs(VERIF_DIR, exist_ok=True)

DB_NAME = "thyroid_canonical_publication_v1_0"
DB_TAG = "pub_v1_1"
MIG_ID = "mig_315"

# Expected M025 benchmark values (QA gate targets)
M025_PATIENT_AUC_EXPECTED = 0.6478
M025_NODULE_AUC_EXPECTED = 0.6399
M025_BLACK_N = 1535
M025_WHITE_N = 1382
M025_ASIAN_N = 204
M025_NODULE_STRICT_TOTAL = 3687
M025_PATIENT_TOTAL = 3375


# ---------------------------------------------------------------------------
# Wilson CI helpers (identical to m025_sensitivity_lib.py)
# ---------------------------------------------------------------------------
def wilson_ci(k: float | int, n: int, z: float = 1.96) -> tuple[float, float]:
    kf = float(k)
    if n <= 0 or kf < 0:
        return (float("nan"), float("nan"))
    p = min(1.0, kf / n)
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    halfw = (z / denom) * sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (max(0.0, centre - halfw), min(1.0, centre + halfw))


def pct_ci(k: float | int, n: int) -> tuple[float, float]:
    lo, hi = wilson_ci(k, n)
    return round(100 * lo, 2), round(100 * hi, 2)


def add_rom_cis(df: pd.DataFrame, k_col: str = "n_malignant", n_col: str = "n_total") -> pd.DataFrame:
    df = df.copy()
    los, his = [], []
    for _, r in df.iterrows():
        n = int(r[n_col]) if pd.notna(r[n_col]) else 0
        k = float(r[k_col]) if pd.notna(r[k_col]) else 0.0
        lo, hi = pct_ci(k, n)
        los.append(lo)
        his.append(hi)
    df["rom_lo_95"] = los
    df["rom_hi_95"] = his
    return df


def add_diag_cis(df: pd.DataFrame) -> pd.DataFrame:
    """Append Wilson CIs for sens / spec / PPV / NPV from TP/FP/FN/TN columns."""
    df = df.copy()
    for metric, num_col, denom_expr in [
        ("sens", "tp", "tp+fn"),
        ("spec", "tn", "tn+fp"),
        ("ppv", "tp", "tp+fp"),
        ("npv", "tn", "tn+fn"),
    ]:
        los, his = [], []
        for _, r in df.iterrows():
            tp = int(r.get("tp", 0) or 0)
            fp = int(r.get("fp", 0) or 0)
            fn = int(r.get("fn", 0) or 0)
            tn = int(r.get("tn", 0) or 0)
            num = tp if metric in ("sens", "ppv") else tn
            if denom_expr == "tp+fn":
                denom = tp + fn
            elif denom_expr == "tn+fp":
                denom = tn + fp
            elif denom_expr == "tp+fp":
                denom = tp + fp
            else:
                denom = tn + fn
            lo, hi = pct_ci(num, denom) if denom > 0 else (float("nan"), float("nan"))
            los.append(lo)
            his.append(hi)
        df[f"{metric}_lo_95"] = los
        df[f"{metric}_hi_95"] = his
    return df


# ---------------------------------------------------------------------------
# Bootstrap AUC helper
# ---------------------------------------------------------------------------
def bootstrap_auc(
    scores: np.ndarray,
    labels: np.ndarray,
    n_boot: int = 1000,
    seed: int = 42,
    ci_level: float = 0.95,
) -> tuple[float, float, float]:
    """Return (auc_point, ci_lo, ci_hi) with stratified bootstrap."""
    rng = np.random.default_rng(seed)
    pos_idx = np.where(labels == 1)[0]
    neg_idx = np.where(labels == 0)[0]
    if len(pos_idx) == 0 or len(neg_idx) == 0:
        return float("nan"), float("nan"), float("nan")
    auc_point = float(roc_auc_score(labels, scores))
    boots = []
    for _ in range(n_boot):
        bi_pos = rng.choice(pos_idx, size=len(pos_idx), replace=True)
        bi_neg = rng.choice(neg_idx, size=len(neg_idx), replace=True)
        bi = np.concatenate([bi_pos, bi_neg])
        bs, bl = scores[bi], labels[bi]
        if bl.sum() == 0 or bl.sum() == len(bl):
            continue
        boots.append(float(roc_auc_score(bl, bs)))
    if not boots:
        return auc_point, float("nan"), float("nan")
    alpha = 1 - ci_level
    lo = float(np.percentile(boots, 100 * alpha / 2))
    hi = float(np.percentile(boots, 100 * (1 - alpha / 2)))
    return round(auc_point, 4), round(lo, 4), round(hi, 4)


def tr_to_int(cat) -> int | float:
    if cat is None or (isinstance(cat, float) and np.isnan(cat)):
        return np.nan
    import re
    s = str(cat).strip().upper()
    m = re.search(r"TR\s*(\d+)", s)
    if m:
        return int(m.group(1))
    m2 = re.search(r"(\d+)", s)
    return int(m2.group(1)) if m2 else np.nan


# ---------------------------------------------------------------------------
# Cramér's V effect size
# ---------------------------------------------------------------------------
def cramers_v(chi2: float, n: int, k: int, r: int) -> float:
    denom = n * (min(k, r) - 1)
    if denom <= 0:
        return float("nan")
    return float(np.sqrt(chi2 / denom))


# ---------------------------------------------------------------------------
# MotherDuck connection
# ---------------------------------------------------------------------------
def get_connection():
    import duckdb
    from motherduck_client import get_token  # type: ignore[import]
    token = get_token()
    tok_len = len(token) if token else 0
    print(f"[MD] Token: {'SET' if token else 'MISSING'}, length={tok_len}")
    con = duckdb.connect(f"md:{DB_NAME}?motherduck_token={token}")
    con.execute(f"USE {DB_NAME};")
    con.execute("SET schema = 'manuscript_workspace';")
    return con


# ---------------------------------------------------------------------------
# Step 1: Execute SQL file (create views + tables)
# ---------------------------------------------------------------------------
def execute_sql_file(con) -> None:
    print("[SQL] Executing M048_motherduck_queries.sql ...")
    sql_raw = open(SQL_FILE).read()
    # Split on statement-ending semicolons (skip bare SELECT sanity checks that
    # the driver runs for QA but don't need to be re-executed here)
    # We execute the full file via duckdb execute_many approach:
    # Split on ";" but be careful about strings containing semicolons
    statements = []
    buf = []
    for line in sql_raw.splitlines():
        stripped = line.strip()
        # Skip pure comment lines
        if stripped.startswith("--"):
            continue
        buf.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            buf = []
    if buf:
        stmt = "\n".join(buf).strip().rstrip(";").strip()
        if stmt:
            statements.append(stmt)

    for i, stmt in enumerate(statements):
        if not stmt or stmt.upper().startswith("--"):
            continue
        try:
            con.execute(stmt)
            # Extract table/view name for feedback
            first_word = stmt.split()[0].upper() if stmt.split() else ""
            if first_word in ("CREATE", "INSERT"):
                print(f"  [{i+1:02d}] OK: {stmt[:80].replace(chr(10), ' ')}")
        except Exception as exc:
            # SELECT sanity checks may fail if columns not present — non-fatal
            first = stmt.split()[0].upper() if stmt.split() else ""
            if first == "SELECT":
                print(f"  [{i+1:02d}] WARN (SELECT): {exc!s:.120}")
            else:
                print(f"  [{i+1:02d}] ERROR: {exc!s:.200}")
                raise
    print("[SQL] Done.")


# ---------------------------------------------------------------------------
# Step 2: Fetch tables from MotherDuck and dump to CSV
# ---------------------------------------------------------------------------
TABLES = {
    "rom_patient": "m048_rom_by_race_patient_v1",
    "rom_nodule": "m048_rom_by_race_nodule_v1",
    "threshold_raw": "m048_threshold_metrics_v1",
    "auc_raw": "m048_auc_v1",
    "feature_dist_raw": "m048_feature_distribution_v1",
    "fna_compliance_raw": "m048_fna_compliance_v1",
    "bethesda_raw": "m048_bethesda_x_race_x_tr_v1",
}


def fetch_tables(con) -> dict[str, pd.DataFrame]:
    dfs = {}
    for key, tname in TABLES.items():
        q = f"SELECT * FROM manuscript_workspace.{tname}"
        df = con.execute(q).df()
        print(f"  {tname}: {len(df)} rows, {list(df.columns)}")
        dfs[key] = df
    return dfs


# ---------------------------------------------------------------------------
# Step 3: Wilson CIs for ROM tables
# ---------------------------------------------------------------------------
def build_rom_combined(df_pat: pd.DataFrame, df_nod: pd.DataFrame) -> pd.DataFrame:
    dp = add_rom_cis(df_pat.copy())
    dp["grain"] = "patient"
    dn = add_rom_cis(df_nod.copy())
    dn["grain"] = "nodule_strict"
    out = pd.concat([dp, dn], ignore_index=True)
    return out


# ---------------------------------------------------------------------------
# Step 4: Bootstrap AUC CIs
# ---------------------------------------------------------------------------
def build_auc_with_ci(con, n_boot: int = 1000) -> pd.DataFrame:
    print(f"[AUC] Bootstrapping {n_boot} replicates per race × grain ...")
    # Patient grain
    dfp = con.execute(
        "SELECT race_strat, max_tirads_category_ever, is_malignant "
        "FROM manuscript_workspace.m048_patient_master_v1 "
        "WHERE max_tirads_category_ever IS NOT NULL"
    ).df()
    dfp["score"] = dfp["max_tirads_category_ever"].apply(tr_to_int)
    dfp["y"] = dfp["is_malignant"].apply(
        lambda v: 1 if (v is True or str(v).lower() in ("true", "t", "1")) else 0
    )

    # Nodule grain
    dfn = con.execute(
        "SELECT race_strat, acr2017_tirads_category, nodule_path_proven_malignant "
        "FROM manuscript_workspace.m048_nodule_master_v1 "
        "WHERE analytic_eligible_strict_acr_pernodule = TRUE "
        "  AND acr2017_tirads_category IS NOT NULL"
    ).df()
    dfn["score"] = dfn["acr2017_tirads_category"].apply(tr_to_int)
    dfn["y"] = dfn["nodule_path_proven_malignant"].apply(
        lambda v: 1 if (v is True or str(v).lower() in ("true", "t", "1")) else 0
    )

    rows = []
    for grain, df0, race_col in [
        ("patient", dfp, "race_strat"),
        ("nodule_strict", dfn, "race_strat"),
    ]:
        for race in sorted(df0[race_col].dropna().unique()):
            sub = df0[df0[race_col] == race].dropna(subset=["score"])
            s = sub["score"].values.astype(float)
            y = sub["y"].values.astype(int)
            n = len(s)
            n_pos = int(y.sum())
            n_neg = n - n_pos
            if n < 2 or n_pos == 0 or n_neg == 0:
                auc_pt, ci_lo, ci_hi = float("nan"), float("nan"), float("nan")
            else:
                auc_pt, ci_lo, ci_hi = bootstrap_auc(s, y, n_boot=n_boot)
            above_chance = None if np.isnan(ci_lo) else (ci_hi < 0.5 or ci_lo > 0.5)
            ci_includes_05 = (not np.isnan(ci_lo)) and (ci_lo <= 0.5 <= ci_hi)
            rows.append({
                "grain": grain,
                "race_strat": race,
                "n": n,
                "n_positive": n_pos,
                "n_negative": n_neg,
                "auc": auc_pt,
                "auc_ci_lo_95": ci_lo,
                "auc_ci_hi_95": ci_hi,
                "ci_includes_0_5": ci_includes_05,
            })
        # Pooled (all races)
        sub_all = df0.dropna(subset=["score"])
        s_all = sub_all["score"].values.astype(float)
        y_all = sub_all["y"].values.astype(int)
        n_all = len(s_all)
        np_all = int(y_all.sum())
        nn_all = n_all - np_all
        if n_all < 2 or np_all == 0 or nn_all == 0:
            auc_pt, ci_lo, ci_hi = float("nan"), float("nan"), float("nan")
        else:
            auc_pt, ci_lo, ci_hi = bootstrap_auc(s_all, y_all, n_boot=n_boot)
        rows.append({
            "grain": grain,
            "race_strat": "POOLED",
            "n": n_all,
            "n_positive": np_all,
            "n_negative": nn_all,
            "auc": auc_pt,
            "auc_ci_lo_95": ci_lo,
            "auc_ci_hi_95": ci_hi,
            "ci_includes_0_5": (not np.isnan(ci_lo)) and (ci_lo <= 0.5 <= ci_hi),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 5: Threshold metrics with Wilson CIs
# ---------------------------------------------------------------------------
def build_threshold_metrics(df_raw: pd.DataFrame) -> pd.DataFrame:
    return add_diag_cis(df_raw.copy())


# ---------------------------------------------------------------------------
# Step 6: Chi-square + Bonferroni for feature distributions
# ---------------------------------------------------------------------------
def build_feature_chi_square(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    For each of the 5 ACR features, build a (race × score) contingency table
    restricted to the 3 primary strata (Black, White, Asian).
    Apply chi2_contingency with Yates correction.
    Bonferroni alpha = 0.05 / 5 = 0.01.
    """
    primary = ["Black", "White", "Asian"]
    features = ["composition", "echogenicity", "shape", "margin", "foci"]
    bonferroni_alpha = 0.05 / len(features)
    results = []

    for feat in features:
        sub = df_raw[
            (df_raw["feature"] == feat) & (df_raw["race_strat"].isin(primary))
        ].copy()
        # Build pivot: rows = race_strat, cols = score
        pivot = sub.pivot_table(index="race_strat", columns="score", values="n", aggfunc="sum", fill_value=0)
        pivot = pivot.reindex(index=[r for r in primary if r in pivot.index])
        if pivot.shape[0] < 2 or pivot.shape[1] < 2:
            results.append({
                "feature": feat,
                "chi2": float("nan"),
                "df": float("nan"),
                "p_raw": float("nan"),
                "p_bonferroni_adj": float("nan"),
                "cramers_v": float("nan"),
                "bonferroni_alpha": bonferroni_alpha,
                "significant_bonferroni": False,
                "n_race_strata": pivot.shape[0],
                "n_score_levels": pivot.shape[1],
            })
            continue
        chi2_stat, p_val, df_val, _ = chi2_contingency(pivot.values, correction=True)
        n_total = int(pivot.values.sum())
        cv = cramers_v(chi2_stat, n_total, pivot.shape[1], pivot.shape[0])
        results.append({
            "feature": feat,
            "chi2": round(chi2_stat, 4),
            "df": int(df_val),
            "p_raw": round(p_val, 6),
            "p_bonferroni_adj": round(min(p_val * len(features), 1.0), 6),
            "cramers_v": round(cv, 4),
            "bonferroni_alpha": bonferroni_alpha,
            "significant_bonferroni": bool(p_val * len(features) < 0.05),
            "n_race_strata": pivot.shape[0],
            "n_score_levels": pivot.shape[1],
        })
    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Step 7: Patient–nodule inflation by race × TR
# ---------------------------------------------------------------------------
def build_inflation(df_patient_rom: pd.DataFrame, df_nodule_rom: pd.DataFrame) -> pd.DataFrame:
    primary = ["Black", "White", "Asian"]
    tr_levels = ["TR4", "TR5"]
    rows = []
    for race in primary:
        for tr in tr_levels:
            pat_row = df_patient_rom[
                (df_patient_rom["race_strat"] == race) &
                (df_patient_rom["tr_category"] == tr)
            ]
            nod_row = df_nodule_rom[
                (df_nodule_rom["race_strat"] == race) &
                (df_nodule_rom["tr_category"] == tr)
            ]
            if pat_row.empty or nod_row.empty:
                rows.append({"race_strat": race, "tr_category": tr,
                              "patient_rom_pct": float("nan"),
                              "nodule_rom_pct": float("nan"),
                              "inflation_pp": float("nan")})
                continue
            pr = pat_row.iloc[0]
            nr = nod_row.iloc[0]
            p_rom = float(pr["rom_pct"]) if pd.notna(pr["rom_pct"]) else float("nan")
            n_rom = float(nr["rom_pct"]) if pd.notna(nr["rom_pct"]) else float("nan")
            inflation = (p_rom - n_rom) if (not np.isnan(p_rom) and not np.isnan(n_rom)) else float("nan")

            # Wilson CIs for inflation bounds (propagation: use separate CIs,
            # report as point estimate only per pre-specified plan)
            p_ci_lo, p_ci_hi = pct_ci(int(pr["n_malignant"]), int(pr["n_total"]))
            n_ci_lo, n_ci_hi = pct_ci(int(nr["n_malignant"]), int(nr["n_total"]))
            rows.append({
                "race_strat": race,
                "tr_category": tr,
                "patient_n_total": int(pr["n_total"]),
                "patient_n_malignant": int(pr["n_malignant"]),
                "patient_rom_pct": p_rom,
                "patient_rom_lo_95": p_ci_lo,
                "patient_rom_hi_95": p_ci_hi,
                "nodule_n_total": int(nr["n_total"]),
                "nodule_n_malignant": int(nr["n_malignant"]),
                "nodule_rom_pct": n_rom,
                "nodule_rom_lo_95": n_ci_lo,
                "nodule_rom_hi_95": n_ci_hi,
                "inflation_pp": round(inflation, 2) if not np.isnan(inflation) else float("nan"),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 8: QA gate checks (hard gates)
# ---------------------------------------------------------------------------
def run_qa_gates(
    con,
    dfs: dict[str, pd.DataFrame],
    df_auc: pd.DataFrame,
    df_rom_combined: pd.DataFrame,
) -> pd.DataFrame:
    gates = []

    def gate(name: str, passed: bool, actual, expected, note: str = ""):
        status = "PASS" if passed else "FAIL"
        gates.append({
            "gate": name,
            "status": status,
            "actual": str(actual),
            "expected": str(expected),
            "note": note,
        })
        icon = "✓" if passed else "✗"
        print(f"  [{icon}] {status:4s} | {name}: actual={actual}, expected={expected}")
        return passed

    all_pass = True

    # Gate 1: Black patient count
    try:
        n_black = int(con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.m048_patient_master_v1 WHERE race_strat='Black'"
        ).fetchone()[0])
        all_pass &= gate("patient_black_n", n_black == M025_BLACK_N, n_black, M025_BLACK_N)
    except Exception as e:
        gate("patient_black_n", False, f"ERROR: {e}", M025_BLACK_N)
        all_pass = False

    # Gate 2: White patient count
    try:
        n_white = int(con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.m048_patient_master_v1 WHERE race_strat='White'"
        ).fetchone()[0])
        all_pass &= gate("patient_white_n", n_white == M025_WHITE_N, n_white, M025_WHITE_N)
    except Exception as e:
        gate("patient_white_n", False, f"ERROR: {e}", M025_WHITE_N)
        all_pass = False

    # Gate 3: Asian patient count
    try:
        n_asian = int(con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.m048_patient_master_v1 WHERE race_strat='Asian'"
        ).fetchone()[0])
        all_pass &= gate("patient_asian_n", n_asian == M025_ASIAN_N, n_asian, M025_ASIAN_N)
    except Exception as e:
        gate("patient_asian_n", False, f"ERROR: {e}", M025_ASIAN_N)
        all_pass = False

    # Gate 4: Patient total
    try:
        n_total = int(con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.m048_patient_master_v1"
        ).fetchone()[0])
        all_pass &= gate("patient_total_n", n_total == M025_PATIENT_TOTAL, n_total, M025_PATIENT_TOTAL)
    except Exception as e:
        gate("patient_total_n", False, f"ERROR: {e}", M025_PATIENT_TOTAL)
        all_pass = False

    # Gate 5: Strict nodule total
    try:
        n_strict = int(con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.m048_nodule_master_v1 "
            "WHERE analytic_eligible_strict_acr_pernodule = TRUE"
        ).fetchone()[0])
        all_pass &= gate(
            "nodule_strict_total_n",
            n_strict == M025_NODULE_STRICT_TOTAL,
            n_strict, M025_NODULE_STRICT_TOTAL
        )
    except Exception as e:
        gate("nodule_strict_total_n", False, f"ERROR: {e}", M025_NODULE_STRICT_TOTAL)
        all_pass = False

    # Gate 6: Pooled patient AUC ≈ 0.6478 (within 0.0001)
    pooled_pat = df_auc[(df_auc["grain"] == "patient") & (df_auc["race_strat"] == "POOLED")]
    if not pooled_pat.empty:
        auc_val = float(pooled_pat.iloc[0]["auc"])
        diff = abs(auc_val - M025_PATIENT_AUC_EXPECTED)
        all_pass &= gate(
            "pooled_patient_auc_matches_m025",
            diff <= 0.0001,
            round(auc_val, 4),
            M025_PATIENT_AUC_EXPECTED,
            note=f"diff={diff:.6f}",
        )
    else:
        gate("pooled_patient_auc_matches_m025", False, "MISSING", M025_PATIENT_AUC_EXPECTED)
        all_pass = False

    # Gate 7: Pooled nodule AUC ≈ 0.6399 (within 0.0001)
    pooled_nod = df_auc[(df_auc["grain"] == "nodule_strict") & (df_auc["race_strat"] == "POOLED")]
    if not pooled_nod.empty:
        auc_val = float(pooled_nod.iloc[0]["auc"])
        diff = abs(auc_val - M025_NODULE_AUC_EXPECTED)
        all_pass &= gate(
            "pooled_nodule_auc_matches_m025",
            diff <= 0.0001,
            round(auc_val, 4),
            M025_NODULE_AUC_EXPECTED,
            note=f"diff={diff:.6f}",
        )
    else:
        gate("pooled_nodule_auc_matches_m025", False, "MISSING", M025_NODULE_AUC_EXPECTED)
        all_pass = False

    # Gate 8: Wilson CI bounds valid
    ci_violations = 0
    for col_lo, col_hi in [("rom_lo_95", "rom_hi_95")]:
        for df_name, df_check in [
            ("rom_patient", dfs.get("rom_patient", pd.DataFrame())),
            ("rom_nodule", dfs.get("rom_nodule", pd.DataFrame())),
        ]:
            if col_lo in df_check.columns and col_hi in df_check.columns:
                bad = df_check[
                    (df_check[col_lo] < 0) |
                    (df_check[col_hi] > 100) |
                    (df_check[col_lo] > df_check[col_hi])
                ]
                ci_violations += len(bad)
    all_pass &= gate(
        "wilson_ci_bounds_valid",
        ci_violations == 0,
        ci_violations,
        0,
        note="lo>=0, hi<=100, lo<=hi",
    )

    df_gates = pd.DataFrame(gates)
    print(f"\n[QA] Result: {'ALL PASS' if all_pass else 'SOME FAILURES'}")
    return df_gates, all_pass


# ---------------------------------------------------------------------------
# Step 9: Diagnostic performance table (comprehensive)
# ---------------------------------------------------------------------------
def build_diagnostic_performance(df_threshold: pd.DataFrame) -> pd.DataFrame:
    return df_threshold.copy()


# ---------------------------------------------------------------------------
# Step 10: Run snapshot
# ---------------------------------------------------------------------------
def get_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


def write_run_snapshot(dfs: dict[str, pd.DataFrame], git_sha: str, qa_pass: bool) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    snap = {
        "study_id": "M048",
        "title": "Racial Disparities in ACR TI-RADS Performance",
        "run_timestamp_utc": now,
        "db_name": DB_NAME,
        "db_tag": DB_TAG,
        "mig_id": MIG_ID,
        "git_sha": git_sha,
        "n_boot_auc": 1000,
        "qa_gates_pass": qa_pass,
        "row_counts": {k: len(v) for k, v in dfs.items()},
        "scope_note": "NO MANUSCRIPT. Data + stats only.",
    }
    snap_path = os.path.join(STUDY_DIR, "m048_run_snapshot.json")
    with open(snap_path, "w") as f:
        json.dump(snap, f, indent=2)
    print(f"[SNAP] Written: {snap_path}")
    return snap


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("M048 Racial Disparities in ACR TI-RADS — Analysis Pipeline")
    print("=" * 70)
    print(f"SCOPE: Statistics + Tables + Figures ONLY. No manuscript, no prose.")
    print(f"QA GATES: HARD — failures block sign-off.")
    print()

    git_sha = get_git_sha()
    print(f"[INFO] git SHA: {git_sha}")

    # Connect
    con = get_connection()
    print(f"[MD] Connected to {DB_NAME}")

    # Execute SQL
    execute_sql_file(con)

    # Fetch tables
    print("[FETCH] Loading SQL-derived tables ...")
    dfs = fetch_tables(con)

    # Add Wilson CIs to ROM tables
    df_rom_pat_ci = add_rom_cis(dfs["rom_patient"].copy())
    df_rom_nod_ci = add_rom_cis(dfs["rom_nodule"].copy())

    # Combined ROM CSV
    df_rom_combined = build_rom_combined(df_rom_pat_ci, df_rom_nod_ci)

    # Threshold metrics with Wilson CIs
    df_threshold = build_threshold_metrics(dfs["threshold_raw"])

    # Bootstrap AUC
    df_auc = build_auc_with_ci(con, n_boot=1000)

    # Chi-square + Bonferroni
    df_feature_chi = build_feature_chi_square(dfs["feature_dist_raw"])

    # Inflation
    df_inflation = build_inflation(df_rom_pat_ci, df_rom_nod_ci)

    # QA gates (hard)
    print("\n[QA] Running 8 QA gates ...")
    df_gates, qa_pass = run_qa_gates(con, {
        "rom_patient": df_rom_pat_ci,
        "rom_nodule": df_rom_nod_ci,
    }, df_auc, df_rom_combined)

    # Write CSVs
    print("\n[CSV] Writing output CSVs ...")
    csv_map = {
        "m048_diagnostic_performance.csv": df_threshold,
        "m048_rom_by_race_x_tr.csv": df_rom_combined,
        "m048_auc_by_race.csv": df_auc,
        "m048_threshold_metrics.csv": df_threshold,
        "m048_feature_distribution.csv": dfs["feature_dist_raw"],
        "m048_feature_chi_square.csv": df_feature_chi,
        "m048_fna_compliance_by_race.csv": dfs["fna_compliance_raw"],
        "m048_bethesda_x_race_x_tr.csv": dfs["bethesda_raw"],
        "m048_inflation_by_race.csv": df_inflation,
        "m048_qa_gates.csv": df_gates,
    }
    for fname, df_out in csv_map.items():
        out_path = os.path.join(STUDY_DIR, fname)
        df_out.to_csv(out_path, index=False)
        print(f"  Written: {fname} ({len(df_out)} rows)")

    # Also write reconciliation CSV to verification/
    df_race_recon = con.execute(
        "SELECT race_strat, COUNT(*) AS n FROM manuscript_workspace.m048_patient_master_v1 "
        "GROUP BY race_strat ORDER BY n DESC"
    ).df()
    df_race_recon.to_csv(os.path.join(VERIF_DIR, "m025_reconciliation.csv"), index=False)
    print(f"  Written: verification/m025_reconciliation.csv")

    # Run snapshot
    all_dfs = dict(dfs)
    all_dfs.update({
        "rom_combined": df_rom_combined,
        "auc_with_ci": df_auc,
        "threshold_with_ci": df_threshold,
        "feature_chi_square": df_feature_chi,
        "inflation": df_inflation,
        "qa_gates": df_gates,
    })
    snap = write_run_snapshot(all_dfs, git_sha, qa_pass)

    # Attach dfs to con for independent recompute (return)
    con.close()

    if not qa_pass:
        print("\n[FAIL] One or more QA gates FAILED. Sign-off blocked.")
        sys.exit(1)

    print("\n[DONE] All QA gates PASS. Proceeding to figures and handoff.")
    return snap, csv_map


if __name__ == "__main__":
    main()

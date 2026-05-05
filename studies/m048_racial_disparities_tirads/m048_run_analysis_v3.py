#!/usr/bin/env python3
"""
M048 v3 — full covariate-adjusted racial disparities analysis.
Executes SQL (optional), dumps v3 tables, runs Models 0–6, F, F-Nodule, B, I, M,
mediation, sensitivity A–G, disparity-direction table, covariate balance, QA gates.
SCOPE: statistics + CSV + snapshot + verification scaffolding only (no manuscript prose).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)
STUDY_DIR = os.path.dirname(os.path.abspath(__file__))
V3_DIR = os.path.join(STUDY_DIR, "v3")
VERIF_DIR = os.path.join(V3_DIR, "verification")
FIG_V3 = os.path.join(REPO_ROOT, "M048_submission_package", "figures", "v3")
SQL_FILE = os.path.join(STUDY_DIR, "M048_motherduck_queries.sql")

from m048_v3_stats_lib import (  # noqa: E402
    MEDIATORS,
    PRIMARY_RACES,
    bootstrap_mediation_product,
    fit_logit,
    normalize_tr_category,
    prepare_v3_frame,
    race_or_table,
    smd_binary,
    smd_continuous,
)

DB_NAME = "thyroid_canonical_publication_v1_0"
DB_TAG = "pub_v1_1"
MIG_ID = "mig_317"
M025_N = 3375


def get_connection():
    import duckdb
    from motherduck_client import get_token

    token = get_token()
    print(f"[MD] Token: {'SET' if token else 'MISSING'}, len={len(token) if token else 0}")
    con = duckdb.connect(f"md:{DB_NAME}?motherduck_token={token}")
    con.execute(f"USE {DB_NAME};")
    con.execute("SET schema = 'manuscript_workspace';")
    return con


def execute_sql_file(con) -> None:
    """Reuse line-based splitter from m048_run_analysis (semicolon statements)."""
    sql_raw = open(SQL_FILE).read()
    buf, statements = [], []
    for line in sql_raw.splitlines():
        stripped = line.strip()
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
        if not stmt:
            continue
        first = stmt.split()[0].upper()
        try:
            con.execute(stmt)
            if first != "SELECT":
                print(f"  [{i+1:02d}] OK {stmt[:72].replace(chr(10), ' ')}")
        except Exception as exc:
            if first == "SELECT":
                print(f"  [{i+1:02d}] WARN SELECT: {exc!s:.160}")
            else:
                print(f"  [{i+1:02d}] ERROR: {exc!s:.220}")
                raise


def dump_table(con, name: str, out_path: str) -> None:
    df = con.execute(f"SELECT * FROM manuscript_workspace.{name}").df()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"  dumped {name} -> {out_path} ({len(df)} rows)")


def tr_to_int(cat) -> float:
    if cat is None or (isinstance(cat, float) and np.isnan(cat)):
        return np.nan
    import re
    s = str(cat).strip().upper()
    m = re.search(r"TR\s*(\d+)", s)
    if m:
        return float(m.group(1))
    m2 = re.search(r"(\d+)", s)
    return float(m2.group(1)) if m2 else np.nan


def acr_rom_mid_high(tr: str) -> tuple[float, float]:
    """Literature-oriented ROM anchors by TR band (percent scale)."""
    if tr == "TR5":
        return 42.0, 55.0
    return 18.0, 28.0


def build_disparity_direction(
    df_bio: pd.DataFrame,
    df_rom: pd.DataFrame,
    df_cell: pd.DataFrame,
) -> pd.DataFrame:
    """race × TR4/TR5 interpretive table + rule-based signature."""
    df_rom = df_rom.copy()
    df_bio = df_bio.copy()
    df_cell = df_cell.copy()
    df_rom["tr_key"] = df_rom["tr_category"].map(normalize_tr_category)
    df_bio["tr_key"] = df_bio["tr_category"].map(normalize_tr_category)
    df_cell["tr_key"] = df_cell["max_tirads_category_ever"].map(normalize_tr_category)

    rows = []
    for tr in ["TR4", "TR5"]:
        mid, high = acr_rom_mid_high(tr)
        for race in PRIMARY_RACES:
            r_rom = df_rom[(df_rom["race_strat"] == race) & (df_rom["tr_key"] == tr)]
            r_bio = df_bio[(df_bio["race_strat"] == race) & (df_bio["tr_key"] == tr)]
            r_cell = df_cell[(df_cell["race_strat"] == race) & (df_cell["tr_key"] == tr)]
            rom_pct = float(r_rom.iloc[0]["rom_pct"]) if len(r_rom) else np.nan
            n_bio = int(r_bio.iloc[0]["n_malignant_in_cell"]) if len(r_bio) else 0
            mean_sz = float(r_bio.iloc[0]["mean_tumor_size_cm"]) if len(r_bio) else np.nan
            multifocal_pct = (
                100.0 * float(r_bio.iloc[0]["n_multifocal"]) / n_bio if n_bio else np.nan
            )
            pct_ete = float(r_cell.iloc[0]["pct_any_ete"]) if len(r_cell) else np.nan
            pct_ln = float(r_cell.iloc[0]["pct_ln_positive"]) if len(r_cell) else np.nan
            dom_hist = str(r_cell.iloc[0]["dominant_histology"]) if len(r_cell) else ""

            rows.append({
                "race_strat": race,
                "tr_category": tr,
                "n_malignant_cell": n_bio,
                "rom_pct": rom_pct,
                "mean_tumor_size_cm": mean_sz,
                "pct_multifocal": multifocal_pct,
                "pct_any_ete": pct_ete,
                "pct_ln_positive": pct_ln,
                "dominant_histology": dom_hist,
                "acr_rom_mid_ref": mid,
                "acr_rom_high_ref": high,
            })

    out = pd.DataFrame(rows)
    med_sz = float(np.nanmedian(out["mean_tumor_size_cm"]))
    med_ete = float(np.nanmedian(out["pct_any_ete"]))
    med_ln = float(np.nanmedian(out["pct_ln_positive"]))

    sigs = []
    for _, r in out.iterrows():
        tr = r["tr_category"]
        mid, high = acr_rom_mid_high(tr)
        rom, sz, ete, ln = r["rom_pct"], r["mean_tumor_size_cm"], r["pct_any_ete"], r["pct_ln_positive"]
        s = "calibrated"
        if (
            rom == rom
            and sz == sz
            and ete == ete
            and rom < mid
            and sz < med_sz
            and ete < med_ete
        ):
            s = "over_referral_signature"
        elif (
            rom == rom
            and sz == sz
            and ete == ete
            and ln == ln
            and rom > high
            and sz > med_sz
            and (ete > med_ete or (ln == ln and ln > med_ln))
        ):
            s = "under_referral_signature"
        sigs.append(s)
    out["signature"] = sigs
    return out


def run_bethesda_stratified(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for b in sorted(df["bethesda_bucket"].dropna().unique()):
        sub = df[df["bethesda_bucket"] == b].copy()
        sub = sub.dropna(subset=["max_tr_int"])
        if len(sub) < 30:
            continue
        formula = "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int"
        try:
            res = fit_logit(formula, sub, cluster_col=None)
            rt = race_or_table(res)
            for _, rr in rt.iterrows():
                rows.append({
                    "bethesda_bucket": b,
                    "race_level": rr["race_level"],
                    "or": rr["or"],
                    "ci_lo": rr["ci_lo"],
                    "ci_hi": rr["ci_hi"],
                    "p": rr["p"],
                    "n": len(sub),
                    "n_events": int(sub["is_malignant"].sum()),
                })
        except Exception as e:
            rows.append({
                "bethesda_bucket": b,
                "race_level": "ERROR",
                "or": np.nan,
                "ci_lo": np.nan,
                "ci_hi": np.nan,
                "p": np.nan,
                "n": len(sub),
                "n_events": int(sub["is_malignant"].sum()) if len(sub) else 0,
                "error": str(e),
            })
    return pd.DataFrame(rows)


def covariate_balance(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    covariates = [
        ("max_tr_int", "cont"),
        ("age_at_surgery", "cont"),
        ("surg_year", "cont"),
        ("n_nodules_total", "cont"),
        ("n_fnas_total", "cont"),
        ("days_us_to_surg_approx", "cont"),
        ("had_any_genetics", "bin"),
        ("had_any_nm", "bin"),
        ("had_any_fna", "bin"),
        ("had_repeat_fna", "bin"),
        ("has_clt", "bin"),
        ("has_mng", "bin"),
        ("has_graves", "bin"),
        ("has_niftp", "bin"),
        ("has_ftump", "bin"),
    ]
    for name, kind in covariates:
        if name not in df.columns:
            continue
        if kind == "cont":
            smd = smd_continuous(df[name], df["race_strat"])
        else:
            smd = smd_binary(df[name], df["race_strat"])
        for race, val in smd.items():
            rows.append({
                "variable": name,
                "versus_reference": "White",
                "race_strat": race,
                "smd": round(val, 4) if val == val else np.nan,
                "flag_gt_010": bool(val == val and abs(val) > 0.10),
            })
    if "bethesda_bucket" in df.columns:
        for lv in sorted(df["bethesda_bucket"].dropna().astype(str).unique()):
            col = (df["bethesda_bucket"].astype(str) == lv).astype(float)
            smd = smd_binary(col, df["race_strat"])
            for race, val in smd.items():
                rows.append({
                    "variable": f"bethesda_{lv}",
                    "versus_reference": "White",
                    "race_strat": race,
                    "smd": round(val, 4) if val == val else np.nan,
                    "flag_gt_010": bool(val == val and abs(val) > 0.10),
                })
    return pd.DataFrame(rows)


def write_handoff_readme_v3(v3_dir: str, git_sha: str, mig_id: str, ts: str) -> None:
    """Numbers + paths only (per Cowork v3 spec)."""
    out = os.path.join(v3_dir, "m048_handoff_README_v3.md")
    lines = [
        "# M048 v3 handoff (numbers + paths; no manuscript prose)",
        "",
        f"- git_sha: {git_sha}",
        f"- mig_id: {mig_id}",
        f"- run_timestamp_utc: {ts}",
        f"- outputs: `{v3_dir}`",
        "- figures: build via `m048_build_figures_v3.py` → `M048_submission_package/figures/v3/`",
        "",
        "## Framing guidance",
        "- If race effect attenuates **>70%** M0→M6 and Bethesda-stratified analysis removes the TR gradient: apparent disparity explained by access/FNA/multinodular pathway.",
        "- If **<30%** attenuation and Bethesda-stratified gradient persists: residual race × TI-RADS performance signal.",
        "- **30–70%**: lead with attenuation cascade; add Bethesda-stratified + disparity-direction quadrant as clinical interpretation.",
        "- Always report disparity-direction signatures (over- vs under-referral) for TR4/TR5 × race.",
        "",
    ]
    try:
        cas = pd.read_csv(os.path.join(v3_dir, "m048_v3_attenuation_cascade.csv"))

        def _or(step: str, race: str) -> str:
            sub = cas[(cas["model_step"] == step) & (cas["race_level"] == race)]
            if sub.empty:
                return "NA"
            r = sub.iloc[0]
            lo, hi = r.get("ci_lo", float("nan")), r.get("ci_hi", float("nan"))
            return f"OR {float(r['or']):.3f} ({float(lo):.3f}–{float(hi):.3f})"

        lines += [
            "## Race OR vs White (patient grain)",
            f"- Black M0: {_or('m0_race_only', 'Black')}",
            f"- Black M3 ( + genetics + NM, last pre-FNA step after Bug C drop): {_or('m3_genetics_nm', 'Black')}",
            f"- Black M6 (full v3): {_or('m6_full', 'Black')}",
            f"- Asian M6: {_or('m6_full', 'Asian')}",
            "",
        ]
    except Exception as exc:
        lines += [f"## (cascade missing: {exc})", ""]

    try:
        dd = pd.read_csv(os.path.join(v3_dir, "m048_v3_disparity_direction_table.csv"))
        lines += ["## Disparity-direction (TR4/TR5 × race)", "```", dd.to_csv(index=False).strip(), "```", ""]
    except Exception as exc:
        lines += [f"## (disparity table missing: {exc})", ""]

    try:
        med = pd.read_csv(os.path.join(v3_dir, "m048_v3_mediation.csv")).copy()
        med["abs_ie"] = med["indirect_mean"].abs()
        top3 = med.sort_values("abs_ie", ascending=False).head(3)
        lines += ["## Top mediators by |bootstrap IE| (Black vs White)", "```", top3.to_csv(index=False).strip(), "```", ""]
    except Exception as exc:
        lines += [f"## (mediation missing: {exc})", ""]

    try:
        inter = pd.read_csv(os.path.join(v3_dir, "m048_v3_interaction_race_x_tr.csv"))
        lines += ["## Race × TR interaction", "```", inter.to_csv(index=False).strip(), "```", ""]
    except Exception as exc:
        lines += [f"## (interaction missing: {exc})", ""]

    try:
        bst = pd.read_csv(os.path.join(v3_dir, "m048_v3_bethesda_stratified_TR_ROM.csv"))
        lines += ["## Bethesda-stratified Model B (first 24 rows)", "```", bst.head(24).to_csv(index=False).strip(), "```", ""]
    except Exception as exc:
        lines += [f"## (bethesda stratified missing: {exc})", ""]

    with open(out, "w") as f:
        f.write("\n".join(lines))
    print(f"[handoff] {out}")


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-sql", action="store_true")
    ap.add_argument("--mediation-boot", type=int, default=1000)
    args = ap.parse_args()

    os.makedirs(V3_DIR, exist_ok=True)
    os.makedirs(VERIF_DIR, exist_ok=True)
    os.makedirs(FIG_V3, exist_ok=True)

    git_sha = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, stderr=subprocess.DEVNULL
    ).decode().strip()

    con = get_connection()
    if not args.skip_sql:
        print("[SQL] M048_motherduck_queries.sql ...")
        execute_sql_file(con)

    v3_tables = [
        "m048_fna_pattern_by_race_v1",
        "m048_fna_to_surgery_interval_by_race_v1",
        "m048_fna_path_concordance_by_race_v1",
        "m048_tumor_biology_descriptors_by_race_v1",
        "m048_histology_subtype_by_race_v1",
        "m048_aggressive_features_by_race_v1",
        "m048_frozen_section_by_race_v1",
        "m048_us_to_surgery_interval_by_race_v1",
        "m048_v3_patient_master_v1",
        "m048_v3_nodule_master_v1",
        "m048_rom_by_race_patient_v1",
        "m048_nodule_count_by_race_v1",
        "m048_genetics_access_by_race_v1",
        "m048_v3_sql_qa_counts_v1",
        "m048_bethesda_x_race_x_tr_rom_v1",
    ]
    print("[DUMP] v3 CSV exports ...")
    rename_map = {
        "m048_fna_pattern_by_race_v1": "m048_v3_fna_pattern_by_race.csv",
        "m048_fna_to_surgery_interval_by_race_v1": "m048_v3_fna_to_surgery_interval.csv",
        "m048_fna_path_concordance_by_race_v1": "m048_v3_fna_path_concordance.csv",
        "m048_tumor_biology_descriptors_by_race_v1": "m048_v3_tumor_biology_descriptors.csv",
        "m048_histology_subtype_by_race_v1": "m048_v3_histology_subtype_by_race.csv",
        "m048_aggressive_features_by_race_v1": "m048_v3_aggressive_features_by_race.csv",
        "m048_frozen_section_by_race_v1": "m048_v3_frozen_section_by_race.csv",
        "m048_us_to_surgery_interval_by_race_v1": "m048_v3_us_to_surgery_interval.csv",
    }
    for t in v3_tables:
        if t in rename_map:
            dump_table(con, t, os.path.join(V3_DIR, rename_map[t]))
        elif t == "m048_v3_patient_master_v1":
            dump_table(con, t, os.path.join(V3_DIR, "m048_v3_patient_master_full.csv"))
        elif t == "m048_v3_nodule_master_v1":
            dump_table(con, t, os.path.join(V3_DIR, "m048_v3_nodule_master_full.csv"))
        elif t == "m048_v3_sql_qa_counts_v1":
            dump_table(con, t, os.path.join(V3_DIR, "m048_v3_sql_qa_counts.csv"))
        elif t == "m048_bethesda_x_race_x_tr_rom_v1":
            dump_table(con, t, os.path.join(V3_DIR, "m048_v3_bethesda_x_race_x_tr_rom.csv"))
        else:
            dump_table(con, t, os.path.join(V3_DIR, f"{t}.csv"))

    df_raw = con.execute("SELECT * FROM manuscript_workspace.m048_v3_patient_master_v1").df()
    df = prepare_v3_frame(df_raw)
    df_model = df.dropna(subset=["max_tr_int"]).copy()

    # NOTE (Bug C): has_clt/has_mng/has_graves are extracted from histology_final
    # ILIKE patterns and are all-zero in this cohort because histology_final only
    # carries malignant categorisations. has_niftp/has_ftump are perfect-separation
    # path-diagnostic indicators (NIFTP -> always benign, FTUMP -> always malignant)
    # that are derivative of is_malignant rather than valid predictors. All five
    # are dropped to avoid singular design matrices.
    controls_tail = (
        "C(nodule_burden_cat) + had_any_genetics + had_any_nm "
        "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) "
        "+ days_us_to_surg_approx + age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)"
    )

    cascade_specs = [
        ("m0_race_only", "is_malignant ~ C(race_strat, Treatment('White'))"),
        ("m1_tr", "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int"),
        ("m2_burden", "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat)"),
        (
            "m3_genetics_nm",
            "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
            "+ had_any_genetics + had_any_nm",
        ),
        # m4_background previously added has_clt/has_mng/has_graves/has_niftp/has_ftump
        # (Bug C: dropped — see controls_tail comment above). With those removed, M4
        # would be identical to M3, so we keep M3 as the last pre-FNA-pattern step
        # and skip the duplicate. m5_fna_path and m6_full retain the FNA-pattern and
        # demographic blocks but drop the same five problematic indicators.
        (
            "m5_fna_path",
            "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
            "+ had_any_genetics + had_any_nm "
            "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx",
        ),
        (
            "m6_full",
            "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
            "+ had_any_genetics + had_any_nm "
            "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx "
            "+ age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)",
        ),
    ]

    cascade_rows = []
    m0_black_or = np.nan
    for tag, formula in cascade_specs:
        try:
            res = fit_logit(formula, df_model)
            rt = race_or_table(res)
            for _, rr in rt.iterrows():
                cascade_rows.append({
                    "model_step": tag,
                    "formula": formula,
                    "race_level": rr["race_level"],
                    "or": rr["or"],
                    "ci_lo": rr["ci_lo"],
                    "ci_hi": rr["ci_hi"],
                    "p": rr["p"],
                    "n": len(df_model),
                })
                if tag == "m0_race_only" and rr["race_level"] == "Black":
                    m0_black_or = rr["or"]
        except Exception as e:
            cascade_rows.append({
                "model_step": tag,
                "formula": formula,
                "race_level": "ERROR",
                "or": np.nan,
                "error": str(e),
            })

    pd.DataFrame(cascade_rows).to_csv(os.path.join(V3_DIR, "m048_v3_attenuation_cascade.csv"), index=False)

    # m6_full is the last entry in cascade_specs (Bug C removed m4_background, so
    # the list is now [m0, m1, m2, m3, m5, m6] -> use [-1] rather than fixed index).
    full_formula = cascade_specs[-1][1]
    full_res = fit_logit(full_formula, df_model)
    params_tbl = pd.DataFrame({
        "param": full_res.params.index,
        "coef": full_res.params.values,
        "or": np.exp(full_res.params.values),
        "ci_lo": np.exp(full_res.conf_int().iloc[:, 0].values),
        "ci_hi": np.exp(full_res.conf_int().iloc[:, 1].values),
        "p": full_res.pvalues.values,
    })
    params_tbl.to_csv(os.path.join(V3_DIR, "m048_v3_full_model_OR.csv"), index=False)

    # Model I: race × TR
    inter_formula = (
        "is_malignant ~ C(race_strat, Treatment('White')) * max_tr_int + " + controls_tail
    )
    inter_res = fit_logit(inter_formula, df_model)
    inter_params = pd.DataFrame({
        "param": inter_res.params.index,
        "coef": inter_res.params.values,
        "p": inter_res.pvalues.values,
    })
    inter_terms = inter_params[inter_params["param"].str.contains(":") & inter_params["param"].str.contains("race")].copy()
    inter_terms["p_bonf"] = np.minimum(inter_terms["p"] * 4, 1.0)
    inter_terms.to_csv(os.path.join(V3_DIR, "m048_v3_interaction_race_x_tr.csv"), index=False)

    # Model M: race × nodule burden
    m_formula = (
        "is_malignant ~ C(race_strat, Treatment('White')) * C(nodule_burden_cat) + max_tr_int "
        "+ had_any_genetics + had_any_nm "
        "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx "
        "+ age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)"
    )
    m_res = fit_logit(m_formula, df_model)
    pd.DataFrame({
        "param": m_res.params.index,
        "coef": m_res.params.values,
        "or": np.exp(m_res.params.values),
        "p": m_res.pvalues.values,
    }).to_csv(os.path.join(V3_DIR, "m048_v3_interaction_race_x_nodulect.csv"), index=False)

    # Bethesda stratified (primary additive Model B)
    df_bstrat = run_bethesda_stratified(df_model)
    df_bstrat.to_csv(os.path.join(V3_DIR, "m048_v3_bethesda_stratified_TR_ROM.csv"), index=False)

    # Bethesda × TR interaction secondary model
    # Formula: is_malignant ~ C(race_strat, Treatment('White')) * max_tr_int
    # One fit per Bethesda stratum; Bonferroni-adjusted across (n_strata * 2 race interaction terms)
    from m048_v3_stats_lib import fit_logit_regularized  # noqa: E402 (already imported as fit_logit)
    bstr_strata = sorted(df_model["bethesda_bucket"].dropna().unique())
    n_bonf = len(bstr_strata) * 2  # 2 race interaction terms (Black, Asian) per stratum
    inter_rows = []
    for b in bstr_strata:
        sub = df_model[df_model["bethesda_bucket"] == b].dropna(subset=["max_tr_int"]).copy()
        if len(sub) < 30:
            continue
        formula_inter = "is_malignant ~ C(race_strat, Treatment('White')) * max_tr_int"
        try:
            res = fit_logit(formula_inter, sub)
        except Exception:
            try:
                res = fit_logit_regularized(formula_inter, sub)
            except Exception as e2:
                inter_rows.append({"bethesda_bucket": b, "interaction_term": "ERROR", "coef": np.nan,
                                    "or": np.nan, "ci_lo": np.nan, "ci_hi": np.nan, "p": np.nan,
                                    "p_bonf": np.nan, "n": len(sub), "n_events": int(sub["is_malignant"].sum()),
                                    "error": str(e2)})
                continue
        ci = res.conf_int()
        pv = getattr(res, "pvalues", pd.Series(dtype=float))
        for pname in res.params.index:
            if ":" not in pname or "race_strat" not in pname:
                continue
            coef = float(res.params[pname])
            lo = float(ci.loc[pname, 0]) if pname in ci.index else np.nan
            hi = float(ci.loc[pname, 1]) if pname in ci.index else np.nan
            p_raw = float(pv[pname]) if pname in pv.index else np.nan
            inter_rows.append({
                "bethesda_bucket": b,
                "interaction_term": pname,
                "coef": coef,
                "or": np.exp(coef),
                "ci_lo": np.exp(lo),
                "ci_hi": np.exp(hi),
                "p": p_raw,
                "p_bonf": float(min(p_raw * n_bonf, 1.0)) if np.isfinite(p_raw) else np.nan,
                "n": len(sub),
                "n_events": int(sub["is_malignant"].sum()),
            })
    pd.DataFrame(inter_rows).to_csv(
        os.path.join(V3_DIR, "m048_v3_bethesda_stratified_TR_interaction.csv"), index=False
    )

    # F-Nodule
    df_nod = con.execute("SELECT * FROM manuscript_workspace.m048_v3_nodule_master_v1").df()
    # Bug A equivalent for nodule grain: cast to int(0/1) so Patsy treats endog
    # as numeric, not 2-column categorical. Same fix Bug A applied to is_malignant.
    df_nod["nodule_path_proven_malignant"] = df_nod["nodule_path_proven_malignant"].apply(
        lambda v: 1 if v in (True, "true", "True", 1, "1") else 0
    ).astype(int)
    df_nod["acr2017_tirads_int"] = df_nod["acr2017_tirads_category"].apply(tr_to_int)
    mask_col = "analytic_eligible_strict_acr_pernodule"
    if mask_col in df_nod.columns:
        df_nod = df_nod[df_nod[mask_col] == True].copy()
    df_nod = df_nod.dropna(subset=["acr2017_tirads_int", "research_id"])
    df_nod["race_strat"] = df_nod["race_strat"].astype(str)
    df_nod = df_nod[df_nod["race_strat"].isin(PRIMARY_RACES)]
    for c in [
        "had_any_genetics", "had_any_nm", "has_clt", "has_mng", "has_graves", "has_niftp", "has_ftump",
        "had_any_fna", "had_repeat_fna",
    ]:
        if c in df_nod.columns:
            df_nod[c] = df_nod[c].apply(lambda v: 1 if v in (True, "true", 1, "1") else 0)
    # Issue 1 follow-up: the nodule master ships both nodule-grain bethesda_bucket
    # (per-FNA, computed from bethesda_2023_name) and patient_bethesda_bucket
    # (joined from the v3 patient master). The original rename collided -- pandas
    # ended up with two columns named bethesda_bucket, which Patsy then chokes on
    # with "categorical data cannot be >1-dimensional". Drop the nodule-grain copy
    # before renaming so the formula uses the patient-grain Bethesda (consistent
    # with all the other patient-level controls in this regression).
    if "patient_bethesda_bucket" in df_nod.columns:
        if "bethesda_bucket" in df_nod.columns:
            df_nod = df_nod.drop(columns=["bethesda_bucket"])
        df_nod.rename(columns={"patient_bethesda_bucket": "bethesda_bucket"}, inplace=True)
    nod_formula = (
        "nodule_path_proven_malignant ~ C(race_strat, Treatment('White')) + acr2017_tirads_int + "
        "C(nodule_burden_cat) + had_any_genetics + had_any_nm "
        "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) "
        "+ days_us_to_surg_approx + age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)"
    )
    try:
        # Issue 1 fix: pass outcome_col explicitly so fit_logit's dropna() targets
        # nodule_path_proven_malignant rather than the default is_malignant
        # (which doesn't exist on the nodule frame and was wiping every row).
        # Also drop rows where the nodule outcome is null before fitting.
        df_nod_fit = df_nod.dropna(subset=["nodule_path_proven_malignant"]).copy()
        nod_res = fit_logit(
            nod_formula,
            df_nod_fit,
            cluster_col="research_id",
            outcome_col="nodule_path_proven_malignant",
        )
        race_or_table(nod_res).to_csv(os.path.join(V3_DIR, "m048_v3_nodule_model_race_OR.csv"), index=False)
    except Exception as e:
        pd.DataFrame([{"error": str(e)}]).to_csv(os.path.join(V3_DIR, "m048_v3_nodule_model_race_OR.csv"), index=False)

    # Mediation: Black vs White AND Asian vs White indirect effects
    med_rows = []
    med_controls = (
        "max_tr_int + C(nodule_burden_cat) + had_any_genetics + had_any_nm "
        "+ had_repeat_fna + C(bethesda_bucket) + age_at_surgery "
        "+ C(sex) + surg_year + C(surg_procedure_type)"
    )
    race_targets = ("Black", "Asian")
    df_med_input = df_model.assign(is_malignant=df_model["is_malignant"].astype(int))
    for race_target in race_targets:
        scope = f"univariate_{race_target.lower()}_vs_white"
        for med, mtype in MEDIATORS:
            if med not in df_model.columns:
                continue
            r = bootstrap_mediation_product(
                df_med_input,
                med,
                mtype,
                "race_strat",
                "is_malignant",
                med_controls,
                n_boot=max(50, args.mediation_boot),
                seed=42,
                race_target=race_target,
            )
            med_rows.append({"mediator": med, "type": mtype, "race_target": r["race_target"], "scope": scope,
                              "indirect_mean": r["indirect_mean"],
                              "indirect_winsor_mean": r.get("indirect_winsor_mean", float("nan")),
                              "ci_lo": r["ci_lo"], "ci_hi": r["ci_hi"]})
    pd.DataFrame(med_rows).to_csv(os.path.join(V3_DIR, "m048_v3_mediation.csv"), index=False)

    # Sensitivity arms
    sens_rows: list[dict] = []

    def sens_fit(label: str, sub: pd.DataFrame) -> None:
        if len(sub) < 100:
            sens_rows.append({"arm": label, "n": len(sub), "error": "too_small"})
            return
        try:
            r = fit_logit(full_formula, prepare_v3_frame(sub))
            for _, row in race_or_table(r).iterrows():
                sens_rows.append({
                    "arm": label, "n": len(sub), "race_level": row["race_level"],
                    "or": row["or"], "ci_lo": row["ci_lo"], "ci_hi": row["ci_hi"], "p": row["p"],
                })
        except Exception as e:
            sens_rows.append({"arm": label, "n": len(sub), "error": str(e)})

    # Bug E: df_model already carries surg_first_date through prepare_v3_frame, so
    # the original merge produced surg_first_date_x/_y suffixes and the bare-name
    # lookup raised KeyError. Use df_model directly; fall back to a derived
    # mid-year datetime from surg_year if the date column happens to be absent.
    sdf = df_model.copy()
    if "surg_first_date" in sdf.columns:
        sdf["surg_dt"] = pd.to_datetime(sdf["surg_first_date"], errors="coerce")
    elif "surg_year" in sdf.columns:
        # surg_year is centred (Bug D); recover the absolute year by adding back
        # the median that prepare_v3_frame subtracted (median of original years
        # was 2020 for this cohort).
        sdf["surg_dt"] = pd.to_datetime(
            (sdf["surg_year"].astype(float) + 2020).round().astype("Int64").astype(str) + "-07-01",
            errors="coerce",
        )
    else:
        sdf["surg_dt"] = pd.NaT
    sens_fit("S048v2_A_post2017", sdf[sdf["surg_dt"] >= "2017-05-01"])
    sens_fit("S048v2_B_single_nodule", sdf[sdf["n_nodules_total"] == 1])
    sens_fit("S048v2_C_genetics_tested", sdf[sdf["had_any_genetics"] == 1])
    sens_fit("S048v2_D_no_CLT", sdf[sdf["has_clt"] == 0])
    vi_mask = sdf["bethesda_bucket"].astype(str).str.strip().str.upper().isin({"VI", "6"})
    sens_fit("S048v3_E_no_Bethesda_VI", sdf[~vi_mask])
    sens_fit("S048v3_F_TR4_only", sdf[sdf["max_tr_int"] == 4])
    sens_fit("S048v3_G_had_fna", sdf[sdf["had_any_fna"] == 1])

    pd.DataFrame(sens_rows).to_csv(os.path.join(V3_DIR, "m048_v3_sensitivity_arms.csv"), index=False)

    # Disparity cell stats
    q_cell = """
    WITH mal AS (
      SELECT v.research_id, v.race_strat, v.max_tirads_category_ever,
             CASE WHEN e.ete_grade IN ('microscopic', 'gross') THEN 1 ELSE 0 END AS any_ete,
             CASE WHEN ln.ln_any_positive IS TRUE THEN 1 ELSE 0 END AS ln_pos,
             v.histology_category
      FROM manuscript_workspace.m048_v3_patient_master_v1 v
      LEFT JOIN main.canonical_ete_event_resolved_v1 e
        ON CAST(e.research_id AS VARCHAR) = v.research_id
      LEFT JOIN manuscript_workspace.ln_master_rollup_v1 ln
        ON CAST(ln.research_id AS VARCHAR) = v.research_id
      WHERE v.is_malignant = TRUE
        AND TRY_CAST(regexp_extract(CAST(v.max_tirads_category_ever AS VARCHAR), '[0-9]+') AS INTEGER) IN (4, 5)
    )
    SELECT race_strat, max_tirads_category_ever,
           COUNT(*) AS n,
           AVG(any_ete) AS pct_any_ete,
           AVG(ln_pos) AS pct_ln_positive,
           MODE(histology_category) AS dominant_histology
    FROM mal
    GROUP BY 1, 2
    """
    df_cell = con.execute(q_cell).df()
    if len(df_cell) and "pct_any_ete" in df_cell.columns:
        df_cell["pct_any_ete"] = df_cell["pct_any_ete"] * 100.0
        df_cell["pct_ln_positive"] = df_cell["pct_ln_positive"] * 100.0
    df_bio = con.execute("SELECT * FROM manuscript_workspace.m048_tumor_biology_descriptors_by_race_v1").df()
    df_rom = con.execute("SELECT * FROM manuscript_workspace.m048_rom_by_race_patient_v1").df()
    df_dd = build_disparity_direction(df_bio, df_rom, df_cell)
    df_dd.to_csv(os.path.join(V3_DIR, "m048_v3_disparity_direction_table.csv"), index=False)

    df_bal = covariate_balance(df_model)
    df_bal.to_csv(os.path.join(V3_DIR, "m048_v3_covariate_balance.csv"), index=False)

    # QA gates (v1 + v2 + v3)
    gates = []
    n_master = len(df_raw)
    gates.append({"gate": "v3_master_rowcount", "status": "PASS" if n_master == M025_N else "FAIL", "actual": n_master, "expected": M025_N})

    with_fna = int(pd.to_numeric(df_raw["had_any_fna"], errors="coerce").fillna(0).sum()) if "had_any_fna" in df_raw.columns else -1
    pct_fna = round(100.0 * with_fna / n_master, 2) if n_master else 0.0
    gates.append({"gate": "fna_coverage_pct", "status": "PASS" if 65 <= pct_fna <= 80 else "WARN", "actual": pct_fna, "expected": "~70.5"})

    sub_fna = df_raw[pd.to_numeric(df_raw["had_any_fna"], errors="coerce").fillna(0) == 1] if "had_any_fna" in df_raw.columns else pd.DataFrame()
    if len(sub_fna) and "had_repeat_fna" in sub_fna.columns:
        rpt = 100.0 * float(pd.to_numeric(sub_fna["had_repeat_fna"], errors="coerce").fillna(0).mean())
        gates.append({
            "gate": "repeat_fna_pct_among_biopsied",
            "status": "PASS" if 10 <= rpt <= 35 else "WARN",
            "actual": round(rpt, 2),
            "expected": "~15-25",
        })

    mal_mask = df_raw["is_malignant"].apply(lambda v: v in (True, "true", "True", 1, "1"))
    mal_n = int(mal_mask.sum())
    if mal_n and "multifocal_flag" in df_raw.columns:
        mf = df_raw.loc[mal_mask, "multifocal_flag"]
        mf_pct = 100.0 * float(mf.apply(lambda v: v in (True, "true", 1, "1")).mean())
        gates.append({
            "gate": "multifocal_pct_malignant",
            "status": "PASS" if 50 <= mf_pct <= 75 else "WARN",
            "actual": round(mf_pct, 2),
            "expected": "~61",
        })
    if mal_n and "max_tumor_size_cm" in df_raw.columns:
        ts_known = 100.0 * float(df_raw.loc[mal_mask, "max_tumor_size_cm"].notna().mean())
        gates.append({
            "gate": "tumor_size_nonnull_pct_malignant",
            "status": "PASS" if ts_known >= 70 else "WARN",
            "actual": round(ts_known, 2),
            "expected": ">70",
        })

    try:
        sql_qa = con.execute("SELECT * FROM manuscript_workspace.m048_v3_sql_qa_counts_v1").df()
        row_match = sql_qa.loc[sql_qa["gate"] == "reconciles_v3_to_v2", "n"]
        gates.append({
            "gate": "v3_v2_row_reconcile_sql",
            "status": "PASS" if not row_match.empty and int(row_match.iloc[0]) == 1 else "FAIL",
            "actual": int(row_match.iloc[0]) if not row_match.empty else None,
            "expected": 1,
        })
    except Exception as e:
        gates.append({"gate": "v3_v2_row_reconcile_sql", "status": "SKIP", "actual": str(e), "expected": 1})

    tr4_tr5_mal = df_dd[df_dd["tr_category"].isin(["TR4", "TR5"])]["n_malignant_cell"].fillna(0)
    gates.append({
        "gate": "disparity_cells_ge10_malignant",
        "status": "PASS" if int((tr4_tr5_mal >= 10).sum()) >= 6 else "WARN",
        "actual": int((tr4_tr5_mal >= 10).sum()),
        "expected": ">=6 of 9 cells",
    })

    # New QA gate: mediation has Asian rows
    try:
        med_df = pd.read_csv(os.path.join(V3_DIR, "m048_v3_mediation.csv"))
        has_asian_med = "race_target" in med_df.columns and int((med_df["race_target"] == "Asian").sum()) >= 1
        gates.append({
            "gate": "mediation_has_asian_rows",
            "status": "PASS" if has_asian_med else "FAIL",
            "actual": int((med_df["race_target"] == "Asian").sum()) if "race_target" in med_df.columns else 0,
            "expected": ">=1 row with race_target==Asian",
        })
    except Exception as e:
        gates.append({"gate": "mediation_has_asian_rows", "status": "FAIL", "actual": str(e), "expected": ">=1 row"})

    # New QA gate: Bethesda × race × TR ROM table has reportable cells
    try:
        brom_df = pd.read_csv(os.path.join(V3_DIR, "m048_v3_bethesda_x_race_x_tr_rom.csv"))
        n_reportable = int((pd.to_numeric(brom_df["n"], errors="coerce").fillna(0) >= 10).sum())
        gates.append({
            "gate": "bethesda_rom_table_complete",
            "status": "PASS" if n_reportable >= 6 else "FAIL",
            "actual": n_reportable,
            "expected": ">=6 cells with n>=10",
        })
    except Exception as e:
        gates.append({"gate": "bethesda_rom_table_complete", "status": "FAIL", "actual": str(e), "expected": ">=6 cells"})

    pd.DataFrame(gates).to_csv(os.path.join(V3_DIR, "m048_v3_qa_gates.csv"), index=False)

    # M025 cohort reconciliation (patient master; PHI-safe aggregates only)
    m6b = next(
        (r["or"] for r in cascade_rows if r.get("model_step") == "m6_full" and r.get("race_level") == "Black"),
        np.nan,
    )
    atten_pct = round(100.0 * (1 - m6b / m0_black_or), 2) if (m0_black_or == m0_black_or and m6b == m6b and m0_black_or) else np.nan

    snap = {
        "study_id": "M048_v3",
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "db_name": DB_NAME,
        "db_tag": DB_TAG,
        "mig_id": MIG_ID,
        "git_sha": git_sha,
        "n_patient_v3": n_master,
        "attenuation_pct_black_m0_to_m6": atten_pct,
        "paths": {"v3_dir": V3_DIR, "figures_v3": FIG_V3},
    }
    with open(os.path.join(V3_DIR, "m048_v3_run_snapshot.json"), "w") as f:
        json.dump(snap, f, indent=2)

    try:
        rec = con.execute(
            """
            SELECT race_strat,
                   COUNT(*)::BIGINT AS n_patients,
                   SUM(CAST(is_malignant AS INTEGER))::BIGINT AS n_malignant
            FROM manuscript_workspace.m048_patient_master_v1
            GROUP BY 1 ORDER BY 1
            """
        ).df()
        rec.to_csv(os.path.join(VERIF_DIR, "m025_reconciliation_v3.csv"), index=False)
    except Exception as e:
        pd.DataFrame([{"error": str(e)}]).to_csv(os.path.join(VERIF_DIR, "m025_reconciliation_v3.csv"), index=False)

    write_handoff_readme_v3(V3_DIR, git_sha, MIG_ID, snap["run_timestamp_utc"])

    con.close()
    print(f"[DONE] v3 outputs under {V3_DIR}; run m048_build_figures_v3.py for figures.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Full study pipeline: extract, cohorts, stats, figures, manuscript, validation."""
from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
STUDY = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(STUDY))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

import cohort_logic as cl  # noqa: E402

RNG = 42
np.random.seed(RNG)
pd.options.mode.copy_on_write = True


def connect():
    return MotherDuckClient(MotherDuckConfig()).connect_rw()


def qdf(con, sql: str) -> pd.DataFrame:
    return con.execute(sql).df()


def load_core(
    con,
    research_ids: np.ndarray | None = None,
) -> dict[str, pd.DataFrame]:
    """Pull MotherDuck slices (small/medium tables).

    When *research_id* filter is provided, narrow large FNA / imaging pulls.
    """
    out: dict[str, pd.DataFrame] = {}
    out["spl"] = qdf(
        con,
        """
        SELECT research_id, surgery_episode_id, path_size_cm, path_surgery_id,
               linkage_confidence_tier, score_rank
        FROM surgery_pathology_linkage_v3
        WHERE score_rank = 1
        """,
    )
    out["tumor_ep_1"] = qdf(
        con,
        """
        SELECT research_id, surgery_episode_id, tumor_size_cm, primary_histology,
               gross_ete, nodal_disease_positive_count, extranodal_extension,
               multifocality_flag, margin_status, histology_variant
        FROM tumor_episode_master_v2
        WHERE tumor_ordinal = 1
        """,
    )
    out["tumor_pathology"] = qdf(
        con,
        """
        SELECT research_id, histology_1_largest_tumor_cm, histology_1_type,
               surgery_date, tumor_1_size_cm,
               histology_1_ln_any_positive, primary_ln_ln_any_positive,
               tumor_1_extrathyroidal_ext, tumor_1_gross_ete
        FROM tumor_pathology
        """,
    )
    out["img_long"] = qdf(
        con,
        """
        SELECT research_id, nodule_id, imaging_exam_id, size_cm_max,
               resolved_exam_date, laterality, tirads_score, suspicious_node_flag,
               linked_fna_episode_id, dominant_nodule_flag
        FROM imaging_nodule_long_v2
        """,
    )
    imsql = """
        SELECT research_id, nodule_id, exam_date, max_dimension_cm, laterality,
               linked_fna_episode_id, tirads_category, suspicious_flag,
               tirads_acr_recalculated
        FROM imaging_nodule_master_v1
        """
    if research_ids is not None and len(research_ids) > 0:
        con.register("_cohort_ids_im", pd.DataFrame({"research_id": research_ids}))
        out["img_master"] = qdf(
            con,
            imsql + " INNER JOIN _cohort_ids_im USING (research_id)",
        )
        con.unregister("_cohort_ids_im")
    else:
        out["img_master"] = qdf(con, imsql)
    out["ifna_v3"] = qdf(
        con,
        """
        SELECT research_id, nodule_id, imaging_exam_id, fna_episode_id,
               score_rank, linkage_score, analysis_eligible_link_flag
        FROM imaging_fna_linkage_v3
        """,
    )
    out["exam_sum"] = qdf(con, "SELECT * FROM imaging_exam_summary_v2")
    fna_sql = """
        SELECT research_id, fna_episode_id, resolved_fna_date, bethesda_category,
               pathology_diagnosis, specimen_site_raw, laterality
        FROM fna_episode_master_v2
        """
    if research_ids is not None and len(research_ids) > 0:
        con.register("_cohort_ids", pd.DataFrame({"research_id": research_ids}))
        out["fna"] = qdf(
            con,
            fna_sql
            + " INNER JOIN _cohort_ids USING (research_id)",
        )
        con.unregister("_cohort_ids")
    else:
        out["fna"] = qdf(con, fna_sql)
    out["mol"] = qdf(
        con,
        """
        SELECT research_id, molecular_episode_id, platform, overall_result_class,
               resolved_test_date, linked_fna_episode_id, inadequate_flag, cancelled_flag,
               high_risk_marker_flag, braf_flag, ras_flag, ret_fusion_flag,
               tert_flag, ntrk_flag
        FROM molecular_test_episode_v2
        """,
    )
    out["pls"] = qdf(
        con,
        """
        SELECT research_id, age_at_surgery, sex, surgery_date
        FROM patient_level_summary_mv
        """,
    )
    out["ct"] = qdf(
        con,
        """
        SELECT TRY_CAST(research_id AS BIGINT) AS research_id,
               pathologic_lymph_nodes, date_of_exam
        FROM ct_imaging
        """,
    )
    out["mri"] = qdf(
        con,
        """
        SELECT TRY_CAST(research_id AS BIGINT) AS research_id,
               pathologic_lymph_nodes, date_of_exam
        FROM mri_imaging
        """,
    )
    out["path_syn"] = qdf(
        con,
        """
        SELECT research_id, thyroid_procedure, surg_date, completion
        FROM path_synoptics
        """,
    )
    return out


def histology_malignant_flag(h: pd.Series) -> pd.Series:
    def one(x: object) -> float:
        if pd.isna(x) or str(x).strip() in ("", "None"):
            return np.nan
        s = str(x).lower()
        if "niftp" in s:
            return 0.0
        if s.strip() in ("ptc",) or s.startswith("ptc "):
            return 1.0
        malign = ("carcinoma", "metastatic", "lymphoma", "sarcoma", "malignant")
        return 1.0 if any(k in s for k in malign) else 0.0

    return h.map(one)


def aggressive_pathology(row: pd.Series) -> bool:
    ge = row.get("gross_ete")
    gross = (not pd.isna(ge)) and str(ge).lower() in ("gross", "yes", "present", "extensive")
    nodal = pd.to_numeric(row.get("nodal_disease_positive_count"), errors="coerce")
    nodal_pos = float(nodal) > 0 if pd.notna(nodal) else False
    ene_raw = row.get("extranodal_extension")
    if pd.isna(ene_raw):
        ene = False
    else:
        ene = bool(ene_raw) if not isinstance(ene_raw, str) else ene_raw.lower() in ("true", "1", "yes")
    var = str(row.get("histology_variant") or "").lower()
    ag_var = any(
        k in var
        for k in ("tall cell", "hobnail", "diffuse sclerosing", "columnar", "pdtc", "atc")
    )
    return gross or nodal_pos or ene or ag_var


def fit_lr(
    df: pd.DataFrame, outcome: str, predictors: list[str], label: str
) -> dict:
    try:
        import statsmodels.api as sm
    except ImportError:
        return {"label": label, "error": "no statsmodels"}

    sub = df[[outcome] + predictors].dropna().copy()
    if len(sub) < 30:
        return {"label": label, "error": f"n={len(sub)} too small"}
    y = sub[outcome].astype(int)
    X = sub[predictors].apply(pd.to_numeric, errors="coerce")
    X = sm.add_constant(X.astype(float), has_constant="add")
    with np.errstate(invalid="ignore", divide="ignore"):
        try:
            model = sm.Logit(y, X).fit(disp=0, maxiter=200)
        except Exception as e:
            return {"label": label, "error": str(e)}
    tbl = pd.DataFrame(
        {
            "predictor": model.params.index,
            "coef": model.params.values,
            "OR": np.exp(model.params.values),
            "p": model.pvalues.values,
            "ci_low": np.exp(model.conf_int()[0].values),
            "ci_high": np.exp(model.conf_int()[1].values),
        }
    )
    if np.any(np.isinf(model.params.values)):
        return {
            "label": label,
            "error": "separation",
            "n": int(len(sub)),
            "or_table": tbl,
        }
    return {
        "label": label,
        "n": int(len(sub)),
        "pseudo_r2": float(model.prsquared),
        "or_table": tbl,
        "warnings": [],
    }


def fit_lr_sklearn(df: pd.DataFrame, outcome: str, predictors: list[str], label: str) -> dict:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    sub = df[[outcome] + predictors].dropna().copy()
    if len(sub) < 30:
        return {"label": label, "error": f"n={len(sub)} too small"}
    y = sub[outcome].astype(int)
    X = sub[predictors].apply(pd.to_numeric, errors="coerce").astype(float)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    lr = LogisticRegression(
        penalty="l2", C=1.0, max_iter=500, random_state=RNG
    )
    lr.fit(Xs, y)
    coef = np.concatenate([[lr.intercept_[0]], lr.coef_[0]])
    names = ["const"] + list(sub[predictors].columns)
    ors = np.exp(coef)
    tbl = pd.DataFrame({"predictor": names, "OR_penalized": ors})
    return {"label": label, "n": len(sub), "or_table": tbl, "engine": "sklearn_l2"}


def concordance_tables(patient_df: pd.DataFrame) -> pd.DataFrame:
    """2x2 summaries: molecular high-risk vs final malignant; by platform; neoplasm; mutations."""

    def _counts(ss: pd.DataFrame, label: str) -> dict:
        tp = int((ss["mol_pos"] & (ss["path_mal"] == 1.0)).sum())
        fn = int((~ss["mol_pos"] & (ss["path_mal"] == 1.0)).sum())
        fp = int((ss["mol_pos"] & (ss["path_mal"] == 0.0)).sum())
        tn = int((~ss["mol_pos"] & (ss["path_mal"] == 0.0)).sum())
        return {"framework": label, "tp": tp, "fn": fn, "fp": fp, "tn": tn, "n": len(ss)}

    rows: list[dict] = []
    d = patient_df.copy()
    d["mol_pos"] = d["preop_mol_positive"].fillna(False).astype(bool)
    d["path_mal"] = d["final_malignant"]
    dsub = d[d["preop_molecular_tested"]].copy()
    ss = dsub[dsub["path_mal"].isin([0.0, 1.0])]
    if len(ss) >= 5:
        rows.append(_counts(ss, "malignant_concordance_2x2"))
    # Neoplasm-or-malignancy vs molecular high-risk
    h = dsub["final_primary_histology"].fillna("").astype(str).str.lower()
    path_pos = (dsub["path_mal"] == 1.0) | h.str.contains(
        r"niftp|nifgp|follicular adenoma|hurthle cell adenoma|adenoma|follicular neoplasm|neoplasm",
        regex=True,
    )
    path_neg = (dsub["path_mal"] == 0.0) & ~path_pos
    ss_neo = dsub[path_pos | path_neg]
    if len(ss_neo) >= 5:
        mol = ss_neo["mol_pos"]
        pos = path_pos.loc[ss_neo.index]
        tp = int((mol & pos).sum())
        fn = int((~mol & pos).sum())
        fp = int((mol & ~pos).sum())
        tn = int((~mol & ~pos).sum())
        rows.append(
            {
                "framework": "neoplasm_or_malignancy_vs_mol_high",
                "tp": tp,
                "fn": fn,
                "fp": fp,
                "tn": tn,
                "n": len(ss_neo),
            }
        )
    d_ag = d[d["preop_molecular_tested"]].copy()
    if len(d_ag) >= 5 and "aggressive_pathology" in d_ag.columns:
        mol_hr = d_ag["high_risk_marker_flag"].fillna(False).astype(bool) | d_ag["mol_pos"]
        truth_ag = d_ag["aggressive_pathology"].fillna(False).astype(bool) | (
            d_ag["path_mal"] == 1.0
        )
        rows.append(
            {
                "framework": "aggressive_or_malignant_vs_highrisk_mol",
                "tp": int((mol_hr & truth_ag).sum()),
                "fn": int((~mol_hr & truth_ag).sum()),
                "fp": int((mol_hr & ~truth_ag).sum()),
                "tn": int((~mol_hr & ~truth_ag).sum()),
                "n": len(d_ag),
            }
        )
    for plat in ("ThyroSeq", "Afirma"):
        ss3 = dsub[(dsub["preop_mol_platform"] == plat) & dsub["path_mal"].isin([0.0, 1.0])]
        if len(ss3) >= 5:
            rows.append(_counts(ss3, f"platform_{plat}"))
    for mut_col, label in (
        ("braf_flag", "mutation_braf"),
        ("ras_flag", "mutation_ras"),
        ("ret_fusion_flag", "mutation_ret_fusion"),
        ("tert_flag", "mutation_tert"),
        ("ntrk_flag", "mutation_ntrk"),
    ):
        if mut_col not in dsub.columns:
            continue
        mc = dsub[mut_col].fillna(False).astype(bool)
        ss_m = dsub[mc & dsub["path_mal"].isin([0.0, 1.0])]
        if len(ss_m) >= 3:
            rows.append(_counts(ss_m, label))
    return pd.DataFrame(rows)


def univariable_screen(pdata: pd.DataFrame, outcome: str = "initial_total") -> pd.DataFrame:
    """Chi-square / Fisher for categorical; Wilcoxon for age (preop-only vars)."""
    try:
        from scipy import stats as sp_stats
    except ImportError:
        return pd.DataFrame([{"variable": "error", "test": "scipy missing", "p": np.nan}])

    out_rows: list[dict] = []
    y = pdata[outcome].astype(int)
    # age
    ag = pdata[["age_at_surgery", outcome]].dropna()
    if len(ag) >= 10:
        g0 = ag.loc[ag[outcome] == 0, "age_at_surgery"]
        g1 = ag.loc[ag[outcome] == 1, "age_at_surgery"]
        stat, p = sp_stats.mannwhitneyu(g0, g1, alternative="two-sided")
        out_rows.append(
            {
                "variable": "age_at_surgery",
                "test": "Mann-Whitney U",
                "statistic": float(stat),
                "p_value": float(p),
            }
        )
    for feat, kind in [
        ("sex_f", "cat"),
        ("bethesda_ge4", "cat"),
        ("has_mol", "cat"),
        ("bilateral_nodule_indicator", "cat"),
    ]:
        if feat not in pdata.columns:
            continue
        ct = pd.crosstab(pdata[feat].fillna(-1), y)
        if ct.shape[0] < 2 or ct.shape[1] < 2:
            continue
        exp = sp_stats.contingency.expected_freq(ct.values)
        use_fisher = ct.shape == (2, 2) and (exp < 5).any()
        if use_fisher:
            _, p = sp_stats.fisher_exact(ct.values)
            name = "Fisher exact"
        else:
            _, p, _, _ = sp_stats.chi2_contingency(ct.values)
            name = "Chi-square"
        out_rows.append({"variable": feat, "test": name, "statistic": np.nan, "p_value": float(p)})
    return pd.DataFrame(out_rows)


def assemble_patient_dataframe(
    preop_slice: pd.DataFrame,
    pls_dedup: pd.DataFrame,
    first_clean: pd.DataFrame,
    comp_df: pd.DataFrame,
    ult: pd.DataFrame,
    pre_mol: pd.DataFrame,
    pre_fna: pd.DataFrame,
    te_idx: pd.DataFrame,
    img_master: pd.DataFrame,
    path_comp_df: pd.DataFrame | None = None,
    oed_later_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build patient-level analytic frame from a preop lesion-level cohort slice."""
    x = preop_slice.copy()
    x["research_id"] = pd.to_numeric(x["research_id"], errors="coerce").astype("Int64")
    patient_df = x.merge(pls_dedup, on="research_id", how="left")

    patient_df["initial_total"] = (
        patient_df["procedure_normalized"] == "total_thyroidectomy"
    ).astype(int)
    patient_df["initial_lobectomy"] = (
        patient_df["procedure_normalized"] == "hemithyroidectomy"
    ).astype(int)
    patient_df["surgery_year"] = pd.to_datetime(
        patient_df["index_surgery_date"], errors="coerce"
    ).dt.year

    patient_df = patient_df.merge(comp_df, on="research_id", how="left")
    patient_df = patient_df.merge(
        ult[["research_id", "ultimate_total"]], on="research_id", how="left"
    )
    if path_comp_df is not None and not path_comp_df.empty:
        patient_df = patient_df.merge(path_comp_df, on="research_id", how="left")
    if oed_later_df is not None and not oed_later_df.empty:
        patient_df = patient_df.merge(oed_later_df, on="research_id", how="left")

    _path_cols = [
        "path_completion_definite_flag",
        "path_completion_days",
        "path_completion_within_30",
        "path_completion_within_90",
        "path_completion_within_365",
        "path_synoptic_any_later_row_flag",
        "path_completion_ambiguous_later_only_flag",
    ]
    for c in _path_cols:
        if c in patient_df.columns:
            if c == "path_completion_days":
                continue
            patient_df[c] = patient_df[c].fillna(False)
    if "path_completion_days" in patient_df.columns:
        patient_df["path_completion_days"] = patient_df["path_completion_days"].astype(float)
    if "oed_any_later_episode_flag" in patient_df.columns:
        patient_df["oed_any_later_episode_flag"] = patient_df[
            "oed_any_later_episode_flag"
        ].fillna(False)

    patient_df["chart_any_later_thyroid_surgery_flag"] = (
        patient_df.get("oed_any_later_episode_flag", pd.Series(False, index=patient_df.index))
        .fillna(False)
        .astype(bool)
        | patient_df.get(
            "path_synoptic_any_later_row_flag",
            pd.Series(False, index=patient_df.index),
        )
        .fillna(False)
        .astype(bool)
    )
    _pc_def = patient_df.get(
        "path_completion_definite_flag",
        pd.Series(False, index=patient_df.index),
    ).fillna(False).astype(bool)
    patient_df["chart_completion_ambiguous_only_flag"] = (
        patient_df["chart_any_later_thyroid_surgery_flag"]
        & ~(patient_df["completion_total_flag"].fillna(False).astype(bool))
        & ~_pc_def
    )
    patient_df["completion_path_synoptic_definite_flag"] = _pc_def

    patient_df = patient_df.merge(
        pre_fna[
            ["research_id", "fna_episode_id", "bethesda_category"]
        ].rename(columns={"fna_episode_id": "preop_fna_episode_id"}),
        on="research_id",
        how="left",
    )
    pm = pre_mol.rename(
        columns={
            "molecular_episode_id": "preop_molecular_episode_id",
            "platform": "preop_mol_platform",
            "overall_result_class": "preop_mol_result_class",
        }
    )
    patient_df = patient_df.merge(
        pm[
            [
                "research_id",
                "preop_molecular_episode_id",
                "preop_mol_platform",
                "preop_mol_result_class",
                "high_risk_marker_flag",
                "braf_flag",
                "ras_flag",
                "ret_fusion_flag",
                "tert_flag",
                "ntrk_flag",
            ]
        ],
        on="research_id",
        how="left",
    )

    rc = patient_df["preop_mol_result_class"].fillna("").astype(str).str.lower()
    hr = patient_df["high_risk_marker_flag"].fillna(False).astype(bool)
    patient_df["preop_molecular_tested"] = patient_df["preop_molecular_episode_id"].notna()
    patient_df["preop_mol_positive"] = rc.isin(
        ("suspicious", "positive", "high_risk")
    ) | hr.astype(bool)

    patient_df = patient_df.merge(
        te_idx[
            [
                "surgery_episode_id",
                "final_primary_histology",
                "gross_ete",
                "nodal_disease_positive_count",
                "extranodal_extension",
                "multifocality_flag",
                "margin_status",
                "histology_variant",
            ]
        ],
        on="surgery_episode_id",
        how="left",
    )
    patient_df["final_malignant"] = histology_malignant_flag(
        patient_df["final_primary_histology"]
    )
    patient_df["aggressive_pathology"] = patient_df.apply(aggressive_pathology, axis=1)

    if "tirads_acr_recalculated" in patient_df.columns:
        patient_df["tirads_score"] = pd.to_numeric(
            patient_df["tirads_acr_recalculated"], errors="coerce"
        )

    sx_dates = patient_df[["research_id", "index_surgery_date"]].copy()
    sx_dates["index_surgery_date"] = pd.to_datetime(
        sx_dates["index_surgery_date"], errors="coerce"
    )
    img_m = img_master.merge(sx_dates, on="research_id")
    img2 = img_m.rename(columns={"exam_date": "_ex"})
    img2["resolved_exam_date"] = pd.to_datetime(img2["_ex"], errors="coerce")
    img2 = img2[
        (img2["resolved_exam_date"] <= img2["index_surgery_date"])
        & img2["resolved_exam_date"].notna()
    ]
    side_ct = (
        img2.groupby("research_id")["laterality"]
        .nunique()
        .reset_index(name="preop_distinct_lateralities")
    )
    patient_df = patient_df.merge(side_ct, on="research_id", how="left")
    patient_df["bilateral_nodule_indicator"] = (
        patient_df["preop_distinct_lateralities"].fillna(0) >= 2
    ).astype(int)

    return patient_df


def write_validation_report(
    con,
    patient_df: pd.DataFrame,
    study_dir: Path,
) -> None:
    """Check CSV vs DataFrame; MotherDuck id registration overlap; flag >0.1% mismatch."""
    path_csv = study_dir / "patient_level_dataset.csv"
    file_df = pd.read_csv(path_csv, low_memory=False)
    n_file = len(file_df)
    n_mem = len(patient_df)
    id_file = set(pd.to_numeric(file_df["research_id"], errors="coerce").dropna().astype(int))
    id_mem = set(patient_df["research_id"].dropna().astype(int))
    sym_diff = id_file.symmetric_difference(id_mem)
    pct = 100.0 * len(sym_diff) / max(len(id_mem), 1)

    lines = [
        "# Validation report",
        "",
        f"- patient_level_dataset.csv rows: {n_file}",
        f"- in-memory patient_df rows: {n_mem}",
        f"- distinct research_id (file): {len(id_file)}",
        f"- distinct research_id (memory): {len(id_mem)}",
        f"- symmetric ID difference count: {len(sym_diff)}",
        f"- mismatch pct vs cohort (memory): {pct:.4f}%",
    ]
    if pct > 0.1:
        lines.append("- **⚠ FLAG**: row count or ID mismatch exceeds 0.1%")

    if id_mem:
        reg = pd.DataFrame({"research_id": sorted(id_mem)})
        con.register("_val_cohort", reg)
        n_mv = con.execute(
            """
            SELECT COUNT(*) FROM _val_cohort v
            INNER JOIN operative_episode_detail_v2 o ON o.research_id = v.research_id
            WHERE o.procedure_normalized IN ('hemithyroidectomy','total_thyroidectomy')
            """
        ).fetchone()[0]
        con.unregister("_val_cohort")
        lines.append(
            f"- MotherDuck operative rows matching cohort IDs (any hemi/total row): {n_mv}"
        )
        lines.append(
            f"- ratio operative_rows_over_cohort_ids: {n_mv / max(len(id_mem), 1):.4f}"
        )
    (study_dir / "validation_report.md").write_text("\n".join(lines) + "\n")


def export_flow(flow_rows: list[dict], path_md: Path, path_csv: Path) -> None:
    pd.DataFrame(flow_rows).to_csv(path_csv, index=False)
    lines = ["# Cohort flow", ""]
    for r in flow_rows:
        lines.append(f"- **{r['step']}**: n = {r['n']}")
        if r.get("note"):
            lines.append(f"  - {r['note']}")
    path_md.write_text("\n".join(lines))


def run() -> None:
    con = connect()
    ops = qdf(
        con,
        """
        SELECT research_id, surgery_episode_id, resolved_surgery_date,
               procedure_normalized, procedure_raw, laterality
        FROM operative_episode_detail_v2
        """,
    )
    ops["research_id"] = pd.to_numeric(ops["research_id"], errors="coerce").astype("Int64")

    # All potential first surgeries including unknown/other for flow
    first_all = cl.first_qualifying_surgeries(
        ops,
        frozenset({"hemithyroidectomy", "total_thyroidectomy", "unknown", "other"}),
    )
    first_clean = cl.first_qualifying_surgeries(
        ops, frozenset({"hemithyroidectomy", "total_thyroidectomy"})
    )
    id_arr = first_clean["research_id"].dropna().astype(int).unique()
    data = load_core(con, research_ids=id_arr)
    data["operative"] = ops

    flow: list[dict] = []
    flow.append(
        {
            "step": "all_patients_first_thyroid_procedure_lobe_or_total_or_unknown",
            "n": int(len(first_all)),
        }
    )
    flow.append(
        {
            "step": "hemithyroidectomy_or_total_only",
            "n": int(len(first_clean)),
        }
    )

    path_first = cl.add_pathology_size(
        first_clean.copy(),
        data["spl"],
        data["tumor_ep_1"],
        data["tumor_pathology"],
    )
    path_cohort = path_first[
        (path_first["path_defined_size_cm"] >= 2.0)
        & (path_first["path_defined_size_cm"] <= 4.0)
    ].copy()
    flow.append(
        {
            "step": "pathology_defined_size_2_to_4_cm",
            "n": int(len(path_cohort)),
        }
    )

    preop_pick, preop_all_candidates = cl.preop_imaging_size_cohort(
        first_clean.copy(),
        data["img_long"],
        data["ifna_v3"],
        img_master=data["img_master"],
    )
    preop_cohort = preop_pick.copy()
    flow.append(
        {
            "step": "preop_imaging_nodule_size_2_to_4_cm",
            "n": int(len(preop_cohort)),
        }
    )

    strict_fl = cl.strict_ln_exclusion(first_clean.copy(), data["ct"], data["mri"], data["fna"])
    broad_fl = cl.broad_suspicious_node(
        first_clean.copy(), data["exam_sum"], data["img_long"]
    )
    # img_long has no sizes live; exam-level flags still used

    strict_cols = [
        c
        for c in strict_fl.columns
        if c != "index_surgery_date"
    ]
    broad_cols = [
        c
        for c in broad_fl.columns
        if c != "index_surgery_date"
    ]

    def apply_excl(df: pd.DataFrame, broad: bool = False) -> pd.DataFrame:
        m = df.merge(strict_fl[strict_cols], on="research_id", how="left")
        m = m.merge(broad_fl[broad_cols], on="research_id", how="left")
        if not broad:
            m = m[~m["strict_ln_positive_any"].fillna(False)]
        else:
            m = m[~m["broad_suspicious_any"].fillna(False)]
        return m

    path_primary = apply_excl(path_cohort, broad=False)
    preop_primary = apply_excl(preop_cohort, broad=False)
    preop_broad = apply_excl(preop_cohort, broad=True)
    flow.append(
        {
            "step": "after_strict_preop_ln_exclusion_path_cohort",
            "n": int(len(path_primary)),
        }
    )
    flow.append(
        {
            "step": "after_strict_preop_ln_exclusion_preop_cohort",
            "n": int(len(preop_primary)),
        }
    )
    flow.append(
        {
            "step": "after_broad_suspicious_node_exclusion_preop_cohort",
            "n": int(len(preop_broad)),
        }
    )

    _pls = data["pls"].copy()
    _pls["research_id"] = pd.to_numeric(_pls["research_id"], errors="coerce").astype("Int64")
    _pls = _pls.drop_duplicates(subset=["research_id"], keep="first")

    comp_df = cl.completion_after_lobectomy(ops, first_clean)
    path_comp = cl.path_synoptic_completion_after_lobectomy(data["path_syn"], first_clean)
    oed_later = cl.oed_any_later_episode_after_index(ops, first_clean)
    ult = cl.ultimate_extent_total(first_clean, comp_df, path_comp)
    me = cl.molecular_meaningful_mask(data["mol"])
    pre_mol = cl.attach_preop_molecular(first_clean, data["mol"], me)
    pre_fna = cl.attach_preop_fna(first_clean, data["fna"])
    te_idx = (
        data["tumor_ep_1"]
        .sort_values(["surgery_episode_id", "tumor_size_cm"], ascending=[True, False])
        .groupby("surgery_episode_id", as_index=False)
        .first()
        .rename(columns={"primary_histology": "final_primary_histology"})
    )

    preop_primary["research_id"] = pd.to_numeric(
        preop_primary["research_id"], errors="coerce"
    ).astype("Int64")
    preop_broad["research_id"] = pd.to_numeric(
        preop_broad["research_id"], errors="coerce"
    ).astype("Int64")

    # Primary analytic set = preop-defined (strict nodal exclusion)
    patient_df = assemble_patient_dataframe(
        preop_primary,
        _pls,
        first_clean,
        comp_df,
        ult,
        pre_mol,
        pre_fna,
        te_idx,
        data["img_master"],
        path_comp,
        oed_later,
    )
    patient_df_broad = assemble_patient_dataframe(
        preop_broad,
        _pls,
        first_clean,
        comp_df,
        ult,
        pre_mol,
        pre_fna,
        te_idx,
        data["img_master"],
        path_comp,
        oed_later,
    )

    # Sensitivity: path cohort strict
    path_sens = apply_excl(path_cohort, broad=False)

    # Audits
    amb = preop_all_candidates.groupby("research_id").size().reset_index(name="n_lesion_candidates")
    amb_audit = amb[amb["n_lesion_candidates"] > 1]

    extent_audit = cl.path_synoptic_procedure_audit(first_clean, data["path_syn"])

    # Lesion-level export
    lesion_df = preop_pick.merge(
        first_clean[["research_id", "surgery_episode_id", "procedure_normalized"]],
        on="research_id",
        how="inner",
    )

    # Note snippets (exploratory): keyword filter
    note_snip = qdf(
        con,
        """
        SELECT research_id, note_date, LEFT(note_text, 120) AS note_snippet
        FROM clinical_notes_long
        WHERE note_text ILIKE '%thyroidectomy%'
           OR note_text ILIKE '%lobectomy%'
           OR note_text ILIKE '%total thyroid%'
        LIMIT 2000
        """,
    )
    cov_ids = patient_df["research_id"].astype(int).tolist()
    if cov_ids:
        note_snip = note_snip[note_snip["research_id"].isin(cov_ids)]

    # Save
    STUDY.mkdir(parents=True, exist_ok=True)
    patient_df.to_csv(STUDY / "patient_level_dataset.csv", index=False)
    patient_df_broad.to_csv(
        STUDY / "patient_level_dataset_broad_nodal_exclusion.csv", index=False
    )
    lesion_df.to_csv(STUDY / "lesion_level_dataset.csv", index=False)
    note_snip.to_csv(
        STUDY / "exploratory_note_rationale_snippets.csv", index=False
    )
    amb_audit.to_csv(STUDY / "ambiguity_audit.csv", index=False)
    extent_audit.to_csv(STUDY / "surgery_extent_audit.csv", index=False)
    comp_df.to_csv(STUDY / "completion_cases.csv", index=False)

    flow.append(
        {"step": "primary_preop_cohort_final_N", "n": int(len(patient_df))}
    )
    export_flow(flow, STUDY / "cohort_flow.md", STUDY / "cohort_flow.csv")

    # cohort build log
    (STUDY / "cohort_build_log.md").write_text(
        "\n".join(
            [
                "# Cohort build",
                "",
                "**Primary cohort:** Preoperative imaging-defined index nodule 2.0–4.0 cm "
                "(`imaging_nodule_long_v2.size_cm_max`, exam on/before index surgery). "
                "Rationale: reflects size available at surgical decision time.",
                "",
                "**Sensitivity cohort:** Pathology-defined size 2.0–4.0 cm from "
                "`surgery_pathology_linkage_v3.path_size_cm` with fallbacks documented in `cohort_logic.py`.",
                "",
                f"**Primary analytic N (strict nodal):** {len(patient_df)}",
                f"**Broad nodal exclusion preop cohort N:** {len(patient_df_broad)} "
                "(see `patient_level_dataset_broad_nodal_exclusion.csv`).",
                f"**Pathology-defined sensitivity N (strict LN exclusion):** {len(path_sens)}",
                "",
            ]
        )
    )

    # Missingness
    miss = patient_df.isna().mean().reset_index()
    miss.columns = ["column", "missing_prop"]
    miss.to_csv(STUDY / "missingness_summary.csv", index=False)

    # Table 1
    g0 = patient_df[patient_df["initial_lobectomy"] == 1]
    g1 = patient_df[patient_df["initial_total"] == 1]
    rows_t1 = []
    for name, func in [
        ("n", lambda: (len(g0), len(g1))),
        (
            "age_mean",
            lambda: (g0["age_at_surgery"].mean(), g1["age_at_surgery"].mean()),
        ),
        ("female_pct", lambda: ((g0["sex"] == "Female").mean(), (g1["sex"] == "Female").mean())),
        (
            "bethesda_mean",
            lambda: (g0["bethesda_category"].mean(), g1["bethesda_category"].mean()),
        ),
        (
            "molecular_tested_pct",
            lambda: (g0["preop_molecular_tested"].mean(), g1["preop_molecular_tested"].mean()),
        ),
    ]:
        a, b = func()
        rows_t1.append({"variable": name, "lobectomy": a, "total": b})
    pd.DataFrame(rows_t1).to_csv(STUDY / "table1_by_initial_extent.csv", index=False)

    # Models (preop-only)
    pdata = patient_df.copy()
    pdata["sex_f"] = (pdata["sex"] == "Female").astype(int)
    pdata["bethesda_ge4"] = (pd.to_numeric(pdata["bethesda_category"], errors="coerce") >= 4).astype(
        float
    )
    pdata["bethesda_ge4"] = pdata["bethesda_ge4"].fillna(0).astype(int)
    pdata["has_mol"] = pdata["preop_molecular_tested"].astype(int)

    univariable_screen(pdata).to_csv(STUDY / "univariable_tests.csv", index=False)

    parsimonious = ["age_at_surgery", "sex_f", "bethesda_ge4", "has_mol"]
    if "tirads_score" not in pdata.columns:
        pdata["tirads_score"] = np.nan
    pdata["tirads_score"] = pd.to_numeric(pdata["tirads_score"], errors="coerce")
    extended = parsimonious + ["bilateral_nodule_indicator", "tirads_score"]

    r_main = fit_lr(pdata, "initial_total", parsimonious, "primary_parsimonious")
    r_ext = fit_lr(pdata, "initial_total", extended, "primary_extended")
    tables = [r_main, r_ext]

    pdata_br = patient_df_broad.copy()
    pdata_br["sex_f"] = (pdata_br["sex"] == "Female").astype(int)
    pdata_br["bethesda_ge4"] = (
        pd.to_numeric(pdata_br["bethesda_category"], errors="coerce") >= 4
    ).astype(float)
    pdata_br["bethesda_ge4"] = pdata_br["bethesda_ge4"].fillna(0).astype(int)
    pdata_br["has_mol"] = pdata_br["preop_molecular_tested"].astype(int)
    if "tirads_score" not in pdata_br.columns:
        pdata_br["tirads_score"] = np.nan
    pdata_br["tirads_score"] = pd.to_numeric(pdata_br["tirads_score"], errors="coerce")
    r_broad = fit_lr(
        pdata_br, "initial_total", parsimonious, "broad_nodal_parsimonious"
    )
    if len(pdata_br) >= 30:
        tables.append(r_broad)
    if r_main.get("error") == "separation":
        tables.append(fit_lr_sklearn(pdata, "initial_total", parsimonious, "primary_sklearn"))

    mol_sub = pdata[pdata["preop_molecular_tested"]].copy()
    mol_sub["plat_thyroseq"] = (mol_sub["preop_mol_platform"] == "ThyroSeq").astype(int)
    mol_sub["plat_afirma"] = (mol_sub["preop_mol_platform"] == "Afirma").astype(int)
    r_mol = None
    if len(mol_sub) >= 30:
        r_mol = fit_lr(
            mol_sub,
            "initial_total",
            ["age_at_surgery", "sex_f", "bethesda_ge4", "plat_thyroseq", "plat_afirma"],
            "molecular_subset",
        )
        tables.append(r_mol)

    ts = pdata[pdata["preop_mol_platform"] == "ThyroSeq"]
    af = pdata[pdata["preop_mol_platform"] == "Afirma"]
    r_ts = None
    r_af = None
    if len(ts) >= 30:
        r_ts = fit_lr(ts, "initial_total", ["age_at_surgery", "sex_f", "bethesda_ge4"], "thyroseq_only")
        tables.append(r_ts)
    if len(af) >= 30:
        r_af = fit_lr(af, "initial_total", ["age_at_surgery", "sex_f", "bethesda_ge4"], "afirma_only")
        tables.append(r_af)

    lob_only = pdata[pdata["initial_lobectomy"] == 1]
    if len(lob_only) >= 40:
        lob_only = lob_only.copy()
        lob_only["completion_event"] = lob_only["completion_total_flag"].fillna(False).astype(int)
        tables.append(
            fit_lr(
                lob_only,
                "completion_event",
                ["age_at_surgery", "sex_f", "bethesda_ge4", "has_mol"],
                "completion_after_lobe",
            )
        )

    # Write model tables
    for r in tables:
        if "or_table" in r and isinstance(r["or_table"], pd.DataFrame):
            slug = re.sub(r"[^a-z0-9]+", "_", r["label"].lower()).strip("_")
            r["or_table"].to_csv(STUDY / f"logistic_{slug}.csv", index=False)

    t2_parts = []
    for x in tables:
        ot = x.get("or_table")
        if isinstance(ot, pd.DataFrame) and "OR" in ot.columns:
            t2_parts.append(ot.assign(model=x["label"], n=x.get("n")))
    if t2_parts:
        pd.concat(t2_parts, ignore_index=True).to_csv(
            STUDY / "table2_multivariable_total_vs_lobectomy.csv", index=False
        )
    if r_mol and isinstance(r_mol.get("or_table"), pd.DataFrame):
        r_mol["or_table"].to_csv(STUDY / "table3_molecular_tested_subset.csv", index=False)
    if r_ts and isinstance(r_ts.get("or_table"), pd.DataFrame):
        r_ts["or_table"].to_csv(STUDY / "table4_thyroseq_subgroup.csv", index=False)
    if r_af and isinstance(r_af.get("or_table"), pd.DataFrame):
        r_af["or_table"].to_csv(STUDY / "table5_afirma_subgroup.csv", index=False)

    conc = concordance_tables(patient_df)
    conc.to_csv(STUDY / "table6_molecular_pathology_concordance.csv", index=False)
    dsub = patient_df[patient_df["preop_molecular_tested"]][
        [
            "research_id",
            "preop_mol_platform",
            "preop_mol_result_class",
            "preop_mol_positive",
            "final_primary_histology",
            "final_malignant",
        ]
    ]
    dsub.to_csv(STUDY / "molecular_concordance_cases.csv", index=False)

    # completion table — OED pipeline vs path-synoptic integrated (see completion_audit_outputs/)
    lob = patient_df[patient_df["initial_lobectomy"] == 1]
    n_lob = int(len(lob))
    comp_rows = [
        {
            "group": "all_lobectomy",
            "n": n_lob,
            "completion_ever_oed_pipeline": lob["completion_total_flag"]
            .fillna(False)
            .mean(),
            "completion_30_oed_pipeline": lob["completion_within_30"]
            .fillna(False)
            .mean(),
            "completion_90_oed_pipeline": lob["completion_within_90"]
            .fillna(False)
            .mean(),
            "completion_365_oed_pipeline": lob["completion_within_365"]
            .fillna(False)
            .mean(),
            "completion_ever_path_synoptic_definite": lob[
                "path_completion_definite_flag"
            ]
            .fillna(False)
            .mean(),
            "completion_30_path_synoptic_definite": lob[
                "path_completion_within_30"
            ]
            .fillna(False)
            .mean(),
            "completion_90_path_synoptic_definite": lob[
                "path_completion_within_90"
            ]
            .fillna(False)
            .mean(),
            "completion_365_path_synoptic_definite": lob[
                "path_completion_within_365"
            ]
            .fillna(False)
            .mean(),
            "n_patients_any_later_thyroid_surgery_oed_or_path": int(
                lob["chart_any_later_thyroid_surgery_flag"].fillna(False).sum()
            ),
            "n_patients_definite_path_synoptic_completion": int(
                lob["path_completion_definite_flag"].fillna(False).sum()
            ),
            "n_patients_ambiguous_later_only_not_oed_or_path_definite": int(
                lob["chart_completion_ambiguous_only_flag"].fillna(False).sum()
            ),
        }
    ]
    pd.DataFrame(comp_rows).to_csv(
        STUDY / "table7_completion_thyroidectomy.csv", index=False
    )

    supp = pd.DataFrame(
        [
            {
                "rule": "strict_ln_exclusion",
                "definition": "CT/MRI pathologic_lymph_nodes preop; or Bethesda-6 node-specimen FNA",
            },
            {
                "rule": "broad_sensitivity",
                "definition": "any_suspicious_node exam or nodule suspicious_node_flag preop",
            },
            {
                "rule": "initial_total",
                "definition": "procedure_normalized total_thyroidectomy first qualifying episode",
            },
            {
                "rule": "initial_lobectomy",
                "definition": "procedure_normalized hemithyroidectomy",
            },
        ]
    )
    supp.to_csv(STUDY / "supplement_exclusions_and_definitions.csv", index=False)

    # Figures
    fig, ax = plt.subplots()
    ax.bar(["lobectomy", "total"], [len(g0), len(g1)])
    ax.set_title("Initial extent (primary cohort)")
    fig.savefig(STUDY / "fig_bethesda_by_extent.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    fig2, ax2 = plt.subplots()
    mol_cls = pdata["preop_mol_result_class"].fillna("(no molecular)").astype(str)
    gmean = pdata.assign(_mc=mol_cls).groupby("_mc")["initial_total"].mean()
    if len(gmean) > 0:
        gmean.plot.bar(ax=ax2)
    ax2.set_title("Rate of initial total by molecular class")
    fig2.savefig(STUDY / "fig_molecular_result_by_extent.png", dpi=150, bbox_inches="tight")
    plt.close(fig2)

    if len(conc) > 0:
        fig3, ax3 = plt.subplots()
        ax3.bar(conc["framework"], conc["n"])
        ax3.set_title("Concordance framework N")
        fig3.savefig(STUDY / "fig_platform_specific_extent.png", dpi=150, bbox_inches="tight")
        plt.close(fig3)

    fig4, ax4 = plt.subplots(figsize=(8, 4))
    cr = pd.DataFrame(comp_rows)
    plot_mat = cr.set_index("group")[
        [
            "completion_ever_oed_pipeline",
            "completion_ever_path_synoptic_definite",
            "completion_30_oed_pipeline",
            "completion_30_path_synoptic_definite",
            "completion_90_oed_pipeline",
            "completion_90_path_synoptic_definite",
            "completion_365_oed_pipeline",
            "completion_365_path_synoptic_definite",
        ]
    ]
    plot_mat.T.plot(kind="barh", ax=ax4)
    ax4.set_title(
        "Completion thyroidectomy: OED pipeline vs path-synoptic definite (lobectomy N)"
    )
    ax4.legend(loc="lower right", fontsize=8)
    fig4.tight_layout()
    fig4.savefig(STUDY / "fig_completion_rates.png", dpi=150, bbox_inches="tight")
    plt.close(fig4)

    # Forest from first successful OR table
    forest_src = None
    for r in tables:
        if r.get("or_table") is not None and "error" not in r:
            forest_src = r
            break
    if forest_src and isinstance(forest_src["or_table"], pd.DataFrame):
        ot = forest_src["or_table"]
        ot = ot[ot["predictor"] != "const"]
        if len(ot) > 0 and "OR" in ot.columns:
            fig5, ax5 = plt.subplots(figsize=(6, max(2, len(ot) * 0.35)))
            y = np.arange(len(ot))
            ax5.errorbar(ot["OR"], y, xerr=[ot["OR"] - ot["ci_low"], ot["ci_high"] - ot["OR"]], fmt="o")
            ax5.set_yticks(y, labels=ot["predictor"])
            ax5.axvline(1, color="gray", lw=0.8)
            ax5.set_title(f"Forest: {forest_src['label']}")
            fig5.savefig(STUDY / "fig_forest_total_vs_lobectomy.png", dpi=150, bbox_inches="tight")
            plt.close(fig5)

    # Flow figure
    fig6, ax6 = plt.subplots()
    ax6.barh([f["step"][:35] for f in flow], [f["n"] for f in flow])
    fig6.savefig(STUDY / "fig_cohort_flow.png", dpi=150, bbox_inches="tight")
    plt.close(fig6)

    # Initial vs ultimate extent (lobectomy completion flow)
    init_lab = np.where(
        patient_df["initial_total"] == 1, "Initial total", "Initial lobectomy"
    )
    ult_lab = np.where(
        patient_df["ultimate_total"].fillna(False).astype(bool),
        "Ultimate total-class",
        "Not ultimate total",
    )
    ct_tr = (
        pd.crosstab(
            pd.Series(init_lab, name="initial"),
            pd.Series(ult_lab, name="ultimate"),
        )
        .sort_index()
    )
    ct_tr.to_csv(STUDY / "initial_ultimate_extent_transition_counts.csv")
    if ct_tr.size > 0:
        fig7, ax7 = plt.subplots(figsize=(7, 4))
        ct_tr.plot(kind="bar", ax=ax7, rot=0)
        ax7.set_title("Initial surgical extent vs ultimate total classification")
        ax7.legend(title="Ultimate", bbox_to_anchor=(1.02, 1), loc="upper left")
        fig7.tight_layout()
        fig7.savefig(
            STUDY / "fig_initial_to_ultimate_extent.png", dpi=150, bbox_inches="tight"
        )
        plt.close(fig7)

    # Manifest refresh
    manifest = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "primary_cohort_n": len(patient_df),
        "broad_nodal_cohort_n": len(patient_df_broad),
        "path_sensitivity_n": len(path_sens),
        "duckdb": con.execute("SELECT version()").fetchone()[0],
    }
    with (STUDY / "analysis_manifest.json").open("w") as fh:
        json.dump(manifest, fh, indent=2)

    # README / analysis_plan stubs if missing
    (STUDY / "README.md").write_text(
        "# Proposal: 2–4 cm extent + molecular\n\n"
        "Run: `.venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/study_pipeline.py`\n\n"
        "Requires `MOTHERDUCK_TOKEN`.\n"
    )
    (STUDY / "analysis_plan.md").write_text(
        "# Analysis plan\n\n"
        "STROBE observational study; preoperative predictors vs initial total thyroidectomy.\n"
        "Primary cohort: preop imaging nodule size 2–4 cm. "
        "Sensitivity: pathology-defined size. "
        "Nodal: strict vs broad exclusions documented in supplement CSV.\n"
    )

    # Manuscript skeleton with filled N
    n_lob = int(patient_df["initial_lobectomy"].sum())
    n_tot = int(patient_df["initial_total"].sum())
    n_comp = int(
        patient_df.loc[patient_df["initial_lobectomy"] == 1, "completion_total_flag"]
        .fillna(False)
        .sum()
    )
    manuscript = f"""# Initial thyroidectomy extent among 2.0–4.0 cm thyroid nodules

## Abstract

**Background:** For 2–4 cm thyroid nodules, the choice of lobectomy versus total thyroidectomy varies.\n
**Methods:** Retrospective cohort from integrated thyroid research database (MotherDuck). "
Primary cohort used preoperative ultrasound nodule size 2.0–4.0 cm; sensitivity analysis used pathology size. "
Patients with definite preoperative lymph-node involvement (imaging or malignant node FNA) were excluded (strict rule).\n
**Results:** Primary analytic cohort N={len(patient_df)} (lobectomy n={n_lob}, initial total n={n_tot}). "
Completion thyroidectomy after lobectomy: n={n_comp}. "
Multivariable associations with initial total thyroidectomy used preoperative variables only.\n
**Conclusions:** Findings describe practice patterns; residual confounding and single-center bias apply.
"""
    (STUDY / "manuscript_full_draft.md").write_text(manuscript)
    (STUDY / "abstract_only.md").write_text(manuscript.split("## Methods")[0])
    (STUDY / "journal_style_results.md").write_text(
        f"Results: Among {len(patient_df)} patients, "
        f"{n_tot} ({100*n_tot/max(len(patient_df),1):.1f}%) underwent initial total thyroidectomy.\n"
    )
    (STUDY / "figure_legends.md").write_text("# Figure legends\n\nFlow, forest, and bar charts correspond to CSV tables in this folder.\n")
    (STUDY / "supplement.md").write_text("# Supplement\n\nOperational definitions in supplement_exclusions_and_definitions.csv.\n")

    # Claims ledger
    claims = pd.DataFrame(
        [
            {
                "claim": "primary_cohort_N",
                "source": "study_pipeline.py:preop_primary",
                "output_file": "patient_level_dataset.csv",
                "denominator": len(patient_df),
                "manuscript_ref": "Abstract/Results",
            },
            {
                "claim": "initial_total_count",
                "source": "sum(initial_total)",
                "output_file": "patient_level_dataset.csv",
                "denominator": n_tot,
                "manuscript_ref": "Results",
            },
            {
                "claim": "broad_nodal_exclusion_cohort_N",
                "source": "study_pipeline.py:preop_broad",
                "output_file": "patient_level_dataset_broad_nodal_exclusion.csv",
                "denominator": len(patient_df_broad),
                "manuscript_ref": "Methods/Sensitivity",
            },
        ]
    )
    claims.to_csv(STUDY / "claims_ledger.csv", index=False)

    write_validation_report(con, patient_df, STUDY)

    print("Done. N primary:", len(patient_df))


if __name__ == "__main__":
    run()

"""Cohort construction helpers: first surgery, size cohorts, exclusions, completion."""
from __future__ import annotations

import re

import numpy as np
import pandas as pd

# Path-synoptic completion cues (aligned with run_completion_audit_motherduck.py)
_TP_COMPLETION_RE = re.compile(
    r"(completion\s+thyroidectomy|compl\.?\s*thyroidectomy|complete\s+thyroidectomy|"
    r"completion\s+of\s+(the\s+)?thyroid|second\s*[- ]stage\s+thyroidectomy|"
    r"re-?operative\s+thyroidectomy|total\s+thyroidectomy\s*\(?\s*completion|"
    r"thyroidectomy\s*,\s*completion)",
    re.I,
)


def _classify_path_synoptic_completion_row(
    thyroid_procedure: object, completion_col: object
) -> str:
    """definite_completion | ambiguous_likely_staged | not_completion_indicator | unknown"""
    tp = str(thyroid_procedure or "").strip()
    comp = (
        str(completion_col).strip().lower()
        if completion_col is not None and str(completion_col).strip() != ""
        else ""
    )
    if comp in ("yes", "y"):
        return "definite_completion"
    if _TP_COMPLETION_RE.search(tp):
        return "definite_completion"
    tl = tp.lower()
    if "lobectomy" in tl or "hemithyroidectomy" in tl or "thyroidectomy" in tl:
        if comp in ("no", "n", "false", "0"):
            return "not_completion_indicator"
        if comp == "" or pd.isna(completion_col):
            return "ambiguous_likely_staged"
        return "ambiguous_likely_staged"
    return "unknown"

RANDOM_STATE = 42
np.random.seed(RANDOM_STATE)


def first_qualifying_surgeries(
    ops: pd.DataFrame,
    include_procedure: frozenset[str] | None = None,
) -> pd.DataFrame:
    """Earliest operative episode per patient among allowed procedure_normalized values."""
    if include_procedure is None:
        include_procedure = frozenset({"hemithyroidectomy", "total_thyroidectomy"})
    d = ops[ops["procedure_normalized"].isin(include_procedure)].copy()
    d["resolved_surgery_date"] = pd.to_datetime(d["resolved_surgery_date"], errors="coerce")
    d = d.sort_values(["research_id", "resolved_surgery_date", "surgery_episode_id"])
    d = d.groupby("research_id", as_index=False).head(1)
    return d.rename(columns={"resolved_surgery_date": "index_surgery_date"})


def add_pathology_size(
    first: pd.DataFrame,
    spl_rank1: pd.DataFrame,
    tumor_e1: pd.DataFrame,
    tumor_pathology: pd.DataFrame,
) -> pd.DataFrame:
    """Coalesce path size: linkage path_size_cm, tumor_episode tumor_size_cm, tumor_pathology."""
    sp = spl_rank1.copy()
    if "linkage_confidence_tier" in sp.columns:
        sp["_tier"] = sp["linkage_confidence_tier"].astype(str)
        sp = sp.sort_values(["surgery_episode_id", "_tier"])
    sp = sp.groupby("surgery_episode_id", as_index=False).first()
    te_one = tumor_e1.sort_values(
        ["surgery_episode_id", "tumor_size_cm"], ascending=[True, False]
    ).groupby("surgery_episode_id", as_index=False).first()

    out = first.merge(
        sp[["surgery_episode_id", "path_size_cm", "path_surgery_id", "linkage_confidence_tier"]],
        on="surgery_episode_id",
        how="left",
    )
    te = te_one[
        [
            "surgery_episode_id",
            "tumor_size_cm",
            "primary_histology",
            "gross_ete",
            "nodal_disease_positive_count",
            "extranodal_extension",
            "multifocality_flag",
            "margin_status",
            "histology_variant",
        ]
    ].rename(columns={"tumor_size_cm": "te_tumor_size_cm"})
    out = out.merge(te, on="surgery_episode_id", how="left")
    tp = tumor_pathology.drop_duplicates(subset=["research_id"], keep="first")[
        ["research_id", "histology_1_largest_tumor_cm", "histology_1_type", "surgery_date"]
    ].copy()
    tp["research_id"] = pd.to_numeric(tp["research_id"], errors="coerce").astype("Int64")
    out["research_id"] = pd.to_numeric(out["research_id"], errors="coerce").astype("Int64")
    out = out.merge(tp, on="research_id", how="left", suffixes=("", "_tp"))
    out["path_defined_size_cm"] = np.where(
        pd.notna(out["path_size_cm"]),
        out["path_size_cm"],
        np.where(
            pd.notna(out["te_tumor_size_cm"]),
            out["te_tumor_size_cm"],
            pd.to_numeric(out["histology_1_largest_tumor_cm"], errors="coerce"),
        ),
    )
    return out


def preop_imaging_size_cohort(
    first: pd.DataFrame,
    img_long: pd.DataFrame,
    ifna_v3: pd.DataFrame | None = None,
    img_master: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Lesion-level preop nodules 2–4 cm with exam before index surgery; patient-level pick + ambiguity.

    Prefer *img_master* (``imaging_nodule_master_v1.max_dimension_cm``) when present — live DB may have
    all-null ``size_cm_max`` on ``imaging_nodule_long_v2``.
    """
    sx = first[
        ["research_id", "surgery_episode_id", "index_surgery_date", "procedure_normalized"]
    ].copy()
    sx["index_surgery_date"] = pd.to_datetime(sx["index_surgery_date"], errors="coerce")

    if img_master is not None and not img_master.empty:
        img = img_master.copy()
        img["resolved_exam_date"] = pd.to_datetime(img["exam_date"], errors="coerce")
        img["size_cm_max"] = pd.to_numeric(img["max_dimension_cm"], errors="coerce")
    else:
        img = img_long.copy()
        img["resolved_exam_date"] = pd.to_datetime(img["resolved_exam_date"], errors="coerce")
        img["size_cm_max"] = pd.to_numeric(img["size_cm_max"], errors="coerce")

    m = img.merge(sx, on="research_id", how="inner")
    m = m[
        (m["resolved_exam_date"].notna())
        & (m["index_surgery_date"].notna())
        & (m["resolved_exam_date"] <= m["index_surgery_date"])
        & (m["size_cm_max"] >= 2.0)
        & (m["size_cm_max"] <= 4.0)
    ].copy()

    if ifna_v3 is not None and not ifna_v3.empty:
        iv = ifna_v3
        if "analysis_eligible_link_flag" in iv.columns:
            iv = iv[iv["analysis_eligible_link_flag"].fillna(True).astype(bool)]
        irank = iv[iv["score_rank"] == 1][
            ["research_id", "nodule_id", "linkage_score", "fna_episode_id"]
        ].drop_duplicates()
        if "nodule_id" in m.columns:
            m = m.merge(irank, on=["research_id", "nodule_id"], how="left")
        else:
            m["fna_episode_id"] = np.nan
            m["linkage_score"] = np.nan

    link_col = "linked_fna_episode_id" if "linked_fna_episode_id" in m.columns else None
    m["has_fna_link"] = (m[link_col].notna() if link_col else pd.Series(False, index=m.index)) | m.get(
        "fna_episode_id", pd.Series(np.nan, index=m.index)
    ).notna()
    m["sort_dom"] = (
        m["has_fna_link"].astype(int) * 1000
        + m["size_cm_max"].fillna(0) * 10
        + m["resolved_exam_date"].astype("int64").fillna(0) / 1e15
    )
    m = m.sort_values(
        ["research_id", "has_fna_link", "size_cm_max", "resolved_exam_date"],
        ascending=[True, False, False, False],
    )
    amb = m.groupby("research_id").size().reset_index(name="n_candidates")
    amb_ids = set(amb.loc[amb["n_candidates"] > 1, "research_id"].astype(int))

    top = m.groupby("research_id", as_index=False).head(1)
    top["preop_ambiguous_lesion"] = top["research_id"].isin(amb_ids)
    return top, m


def strict_ln_exclusion(
    first: pd.DataFrame,
    ct: pd.DataFrame,
    mri: pd.DataFrame,
    fna: pd.DataFrame,
) -> pd.DataFrame:
    """Flags per patient: ct_ln, mri_ln, malignant_node_fna."""
    sx = first[["research_id", "index_surgery_date"]].copy()
    sx["index_surgery_date"] = pd.to_datetime(sx["index_surgery_date"], errors="coerce")

    flags = sx.copy()
    flags["strict_ct_pathologic_ln"] = False
    flags["strict_mri_pathologic_ln"] = False
    flags["strict_malignant_node_fna"] = False

    if not ct.empty and "pathologic_lymph_nodes" in ct.columns:
        c = ct.copy()
        c["research_id"] = pd.to_numeric(c["research_id"], errors="coerce")
        c["date_of_exam"] = pd.to_datetime(c["date_of_exam"], errors="coerce")
        c = c[c["pathologic_lymph_nodes"] == True]
        c = c.merge(sx, on="research_id", how="inner")
        c = c[
            (c["date_of_exam"].notna())
            & (c["index_surgery_date"].notna())
            & (c["date_of_exam"] <= c["index_surgery_date"])
        ]
        hit = c["research_id"].drop_duplicates()
        flags.loc[flags["research_id"].isin(hit), "strict_ct_pathologic_ln"] = True

    if not mri.empty and "pathologic_lymph_nodes" in mri.columns:
        r = mri.copy()
        r["research_id"] = pd.to_numeric(r["research_id"], errors="coerce")
        r["date_of_exam"] = pd.to_datetime(r["date_of_exam"], errors="coerce")
        r = r[r["pathologic_lymph_nodes"] == True]
        r = r.merge(sx, on="research_id", how="inner")
        r = r[
            (r["date_of_exam"].notna())
            & (r["index_surgery_date"].notna())
            & (r["date_of_exam"] <= r["index_surgery_date"])
        ]
        hit = r["research_id"].drop_duplicates()
        flags.loc[flags["research_id"].isin(hit), "strict_mri_pathologic_ln"] = True

    fn = fna.copy()
    fn["resolved_fna_date"] = pd.to_datetime(fn["resolved_fna_date"], errors="coerce")
    fn = fn.merge(sx, on="research_id", how="inner")
    fn = fn[
        (fn["resolved_fna_date"].notna())
        & (fn["index_surgery_date"].notna())
        & (fn["resolved_fna_date"] <= fn["index_surgery_date"])
    ]
    site = (
        fn["specimen_site_raw"]
        .fillna("")
        .str.lower()
        .str.contains(r"node|lymph|neck", regex=True)
    )
    pdx = (
        fn["pathology_diagnosis"]
        .fillna("")
        .str.lower()
        .str.contains(r"lymph node|nodal|metastasis", regex=True)
    )
    mal = fn["bethesda_category"].isin([6]) & (site | pdx)
    hit_f = fn.loc[mal, "research_id"].drop_duplicates()
    flags.loc[flags["research_id"].isin(hit_f), "strict_malignant_node_fna"] = True

    flags["strict_ln_positive_any"] = (
        flags["strict_ct_pathologic_ln"]
        | flags["strict_mri_pathologic_ln"]
        | flags["strict_malignant_node_fna"]
    )
    return flags


def broad_suspicious_node(
    first: pd.DataFrame,
    exam_sum: pd.DataFrame,
    img_long: pd.DataFrame,
) -> pd.DataFrame:
    sx = first[["research_id", "index_surgery_date"]].copy()
    sx["index_surgery_date"] = pd.to_datetime(sx["index_surgery_date"], errors="coerce")

    ex = exam_sum.copy()
    ex["resolved_exam_date"] = pd.to_datetime(ex["resolved_exam_date"], errors="coerce")
    m = ex.merge(sx, on="research_id", how="inner")
    m = m[
        (m["resolved_exam_date"].notna())
        & (m["index_surgery_date"].notna())
        & (m["resolved_exam_date"] <= m["index_surgery_date"])
    ]
    def _tobool(x: object) -> bool:
        if isinstance(x, bool):
            return x
        s = str(x).strip().lower()
        return s in ("true", "1", "t", "yes")

    m["_susp_ex"] = m["any_suspicious_node"].map(_tobool)
    ex_flag = m.groupby("research_id")["_susp_ex"].max().reset_index()
    ex_flag = ex_flag.rename(columns={"_susp_ex": "broad_exam_suspicious_node"})

    im = img_long.copy()
    im["resolved_exam_date"] = pd.to_datetime(im["resolved_exam_date"], errors="coerce")
    m2 = im.merge(sx, on="research_id", how="inner")
    m2 = m2[
        (m2["resolved_exam_date"].notna())
        & (m2["index_surgery_date"].notna())
        & (m2["resolved_exam_date"] <= m2["index_surgery_date"])
    ]
    nd = m2.groupby("research_id")["suspicious_node_flag"].max().reset_index()
    nd = nd.rename(columns={"suspicious_node_flag": "broad_nodule_suspicious_node"})

    out = sx[["research_id"]].drop_duplicates().merge(ex_flag, how="left", on="research_id")
    out = out.merge(nd, how="left", on="research_id")
    out["broad_suspicious_any"] = (
        out["broad_exam_suspicious_node"].fillna(False).astype(bool)
        | out["broad_nodule_suspicious_node"].fillna(False).astype(bool)
    )
    return out


def completion_after_lobectomy(
    ops: pd.DataFrame, first: pd.DataFrame
) -> pd.DataFrame:
    """Subsequent total thyroidectomy after initial hemithyroidectomy; days to completion."""
    sx_map = first.set_index("research_id")["index_surgery_date"].to_dict()
    init_type = first.set_index("research_id")["procedure_normalized"].to_dict()

    rows = []
    all_ops = ops.copy()
    all_ops["resolved_surgery_date"] = pd.to_datetime(
        all_ops["resolved_surgery_date"], errors="coerce"
    )
    for rid, grp in all_ops.groupby("research_id"):
        if init_type.get(rid) != "hemithyroidectomy":
            continue
        anchor = sx_map.get(rid)
        if anchor is None or pd.isna(anchor):
            continue
        later = grp[grp["resolved_surgery_date"] > anchor].sort_values("resolved_surgery_date")
        tot = later[
            later["procedure_normalized"].isin(
                ("total_thyroidectomy", "completion_thyroidectomy")
            )
        ]
        if tot.empty:
            rows.append(
                {
                    "research_id": rid,
                    "completion_total_flag": False,
                    "completion_days": np.nan,
                    "completion_within_30": False,
                    "completion_within_90": False,
                    "completion_within_365": False,
                }
            )
            continue
        t0 = tot.iloc[0]["resolved_surgery_date"]
        delta = (t0 - pd.Timestamp(anchor)).days
        rows.append(
            {
                "research_id": rid,
                "completion_total_flag": True,
                "completion_days": float(delta),
                "completion_within_30": delta <= 30,
                "completion_within_90": delta <= 90,
                "completion_within_365": delta <= 365,
            }
        )
    return pd.DataFrame(rows)


def oed_any_later_episode_after_index(ops: pd.DataFrame, first: pd.DataFrame) -> pd.DataFrame:
    """Any operative_episode_detail_v2 row strictly after index surgery (lobectomy cohort)."""
    sx_map = first.set_index("research_id")["index_surgery_date"].to_dict()
    init_type = first.set_index("research_id")["procedure_normalized"].to_dict()
    all_ops = ops.copy()
    all_ops["resolved_surgery_date"] = pd.to_datetime(
        all_ops["resolved_surgery_date"], errors="coerce"
    )
    rows: list[dict] = []
    for rid, grp in all_ops.groupby("research_id"):
        if init_type.get(rid) != "hemithyroidectomy":
            continue
        anchor = sx_map.get(rid)
        if anchor is None or pd.isna(anchor):
            rows.append({"research_id": rid, "oed_any_later_episode_flag": False})
            continue
        later = grp[grp["resolved_surgery_date"] > anchor]
        rows.append(
            {"research_id": rid, "oed_any_later_episode_flag": bool(len(later) > 0)}
        )
    return pd.DataFrame(rows)


def path_synoptic_completion_after_lobectomy(
    path_syn: pd.DataFrame,
    first: pd.DataFrame,
) -> pd.DataFrame:
    """Later path_synoptics dated after index lobectomy; definite completion from synoptic cues.

    One row per patient with first qualifying hemithyroidectomy in *first*.
    Definite completion: synoptic ``completion`` in {yes, y} and/or procedure text matching
    completion-thyroidectomy patterns on a later-dated synoptic row.
    """
    sx_map = first.set_index("research_id")["index_surgery_date"].to_dict()
    init_type = first.set_index("research_id")["procedure_normalized"].to_dict()
    ps = path_syn.copy()
    if "research_id" not in ps.columns:
        return pd.DataFrame(
            columns=[
                "research_id",
                "path_completion_definite_flag",
                "path_completion_days",
                "path_completion_within_30",
                "path_completion_within_90",
                "path_completion_within_365",
                "path_synoptic_any_later_row_flag",
                "path_completion_ambiguous_later_only_flag",
            ]
        )
    ps["research_id"] = pd.to_numeric(ps["research_id"], errors="coerce").astype("Int64")
    if "completion" not in ps.columns:
        ps["completion"] = np.nan
    ps["surg_date"] = pd.to_datetime(ps["surg_date"], errors="coerce")
    rows: list[dict] = []
    for rid, grp in ps.groupby("research_id"):
        if init_type.get(rid) != "hemithyroidectomy":
            continue
        anchor = sx_map.get(rid)
        if anchor is None or pd.isna(anchor):
            rows.append(
                {
                    "research_id": rid,
                    "path_completion_definite_flag": False,
                    "path_completion_days": np.nan,
                    "path_completion_within_30": False,
                    "path_completion_within_90": False,
                    "path_completion_within_365": False,
                    "path_synoptic_any_later_row_flag": False,
                    "path_completion_ambiguous_later_only_flag": False,
                }
            )
            continue
        anchor_ts = pd.Timestamp(anchor)
        later = grp[grp["surg_date"] > anchor_ts].sort_values("surg_date")
        if later.empty:
            rows.append(
                {
                    "research_id": rid,
                    "path_completion_definite_flag": False,
                    "path_completion_days": np.nan,
                    "path_completion_within_30": False,
                    "path_completion_within_90": False,
                    "path_completion_within_365": False,
                    "path_synoptic_any_later_row_flag": False,
                    "path_completion_ambiguous_later_only_flag": False,
                }
            )
            continue
        classes = [
            _classify_path_synoptic_completion_row(r["thyroid_procedure"], r["completion"])
            for _, r in later.iterrows()
        ]
        definite_mask = [c == "definite_completion" for c in classes]
        ambiguous_mask = [c == "ambiguous_likely_staged" for c in classes]
        definite_any = any(definite_mask)
        ambiguous_any = any(ambiguous_mask) or any(
            c == "unknown" for c in classes
        )
        if definite_any:
            idx = int(np.argmax(definite_mask))
            t0 = later.iloc[idx]["surg_date"]
            delta = (pd.Timestamp(t0) - anchor_ts).days
            rows.append(
                {
                    "research_id": rid,
                    "path_completion_definite_flag": True,
                    "path_completion_days": float(delta),
                    "path_completion_within_30": delta <= 30,
                    "path_completion_within_90": delta <= 90,
                    "path_completion_within_365": delta <= 365,
                    "path_synoptic_any_later_row_flag": True,
                    "path_completion_ambiguous_later_only_flag": False,
                }
            )
        else:
            rows.append(
                {
                    "research_id": rid,
                    "path_completion_definite_flag": False,
                    "path_completion_days": np.nan,
                    "path_completion_within_30": False,
                    "path_completion_within_90": False,
                    "path_completion_within_365": False,
                    "path_synoptic_any_later_row_flag": True,
                    "path_completion_ambiguous_later_only_flag": bool(
                        ambiguous_any and not definite_any
                    ),
                }
            )
    return pd.DataFrame(rows)


def ultimate_extent_total(
    first: pd.DataFrame,
    completion_df: pd.DataFrame,
    path_completion: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Initial vs ultimate total (after OED- and/or path-synoptic–detected completion)."""
    out = first[
        ["research_id", "procedure_normalized", "index_surgery_date"]
    ].copy()
    out["initial_total"] = out["procedure_normalized"] == "total_thyroidectomy"
    comp = completion_df.set_index("research_id")
    out["completion_total_flag"] = (
        out["research_id"].map(comp["completion_total_flag"]).fillna(False).astype(bool)
    )
    if path_completion is not None and not path_completion.empty:
        pc = path_completion.set_index("research_id")
        out["completion_path_synoptic_definite_flag"] = (
            out["research_id"]
            .map(pc["path_completion_definite_flag"])
            .fillna(False)
            .astype(bool)
        )
    else:
        out["completion_path_synoptic_definite_flag"] = False
    out["ultimate_total"] = (
        out["initial_total"]
        | out["completion_total_flag"]
        | out["completion_path_synoptic_definite_flag"]
    )
    return out


def molecular_meaningful_mask(mol: pd.DataFrame) -> pd.Series:
    """Exclude inadequate, cancelled, and stub 'Other/other' rows."""
    plat = mol["platform"].astype(str)
    orc = mol["overall_result_class"].astype(str).str.lower()
    bad_other = (plat == "Other") & (orc == "other")
    inad = mol["inadequate_flag"].fillna(False).astype(bool)
    canc = mol["cancelled_flag"].fillna(False).astype(bool)
    return ~(bad_other | inad | canc)


def attach_preop_molecular(
    first: pd.DataFrame,
    mol: pd.DataFrame,
    meaningful: pd.Series,
) -> pd.DataFrame:
    """Closest pre-index molecular episode per patient (deterministic)."""
    sx = first[["research_id", "index_surgery_date", "surgery_episode_id"]].copy()
    sx["index_surgery_date"] = pd.to_datetime(sx["index_surgery_date"], errors="coerce")
    m = mol.loc[meaningful].copy()
    m["resolved_test_date"] = pd.to_datetime(m["resolved_test_date"], errors="coerce")
    m = m.merge(sx, on="research_id", how="inner")
    m = m[
        (m["resolved_test_date"].notna())
        & (m["index_surgery_date"].notna())
        & (m["resolved_test_date"] <= m["index_surgery_date"])
    ]
    if m.empty:
        return pd.DataFrame(
            columns=[
                "research_id",
                "molecular_episode_id",
                "platform",
                "overall_result_class",
                "high_risk_marker_flag",
            ]
        )
    m = m.sort_values(
        ["research_id", "resolved_test_date", "molecular_episode_id"], ascending=[True, False, True]
    )
    return m.groupby("research_id", as_index=False).head(1)


def attach_preop_fna(first: pd.DataFrame, fna: pd.DataFrame) -> pd.DataFrame:
    """Closest pre-index FNA per patient."""
    sx = first[["research_id", "index_surgery_date"]].copy()
    sx["index_surgery_date"] = pd.to_datetime(sx["index_surgery_date"], errors="coerce")
    f = fna.copy()
    f["resolved_fna_date"] = pd.to_datetime(f["resolved_fna_date"], errors="coerce")
    m = f.merge(sx, on="research_id", how="inner")
    m = m[
        (m["resolved_fna_date"].notna())
        & (m["index_surgery_date"].notna())
        & (m["resolved_fna_date"] <= m["index_surgery_date"])
    ]
    m = m.sort_values(
        ["research_id", "resolved_fna_date", "fna_episode_id"], ascending=[True, False, True]
    )
    return m.groupby("research_id", as_index=False).head(1)


def path_synoptic_procedure_audit(
    first: pd.DataFrame, psyn: pd.DataFrame
) -> pd.DataFrame:
    out = first[["research_id", "procedure_normalized", "index_surgery_date"]].copy()
    out["index_surgery_date"] = pd.to_datetime(out["index_surgery_date"], errors="coerce")
    p = psyn[["research_id", "thyroid_procedure", "surg_date"]].copy()
    p["surg_date"] = pd.to_datetime(p["surg_date"], errors="coerce")
    m = out.merge(p, on="research_id", how="left")
    m["date_diff_days"] = (m["index_surgery_date"] - m["surg_date"]).dt.days.abs()
    m = m.dropna(subset=["date_diff_days"])
    if m.empty:
        return pd.DataFrame(
            {
                "research_id": first["research_id"].values,
                "procedure_normalized": first["procedure_normalized"].values,
                "thyroid_procedure": np.nan,
                "extent_audit_category": "synoptic_missing",
            }
        )
    idx = m.groupby("research_id")["date_diff_days"].idxmin()
    best = m.loc[idx]

    def _norm_ext(row: pd.Series) -> str:
        t = str(row.get("thyroid_procedure") or "").lower()
        pn = row["procedure_normalized"]
        syn_total = "total" in t or "near" in t
        op_total = pn == "total_thyroidectomy"
        if pd.isna(row.get("thyroid_procedure")) or row.get("thyroid_procedure") == "":
            return "synoptic_missing"
        if syn_total == op_total:
            return "concordant"
        return "discordant"

    best["extent_audit_category"] = best.apply(_norm_ext, axis=1)
    return best[
        [
            "research_id",
            "procedure_normalized",
            "thyroid_procedure",
            "extent_audit_category",
        ]
    ]

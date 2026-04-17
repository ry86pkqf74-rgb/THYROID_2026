"""5-patient overshoot probe — find the patients with patient-dominant
T-stage in (T3b, T4a, T4b) but NOT in CPM ete_grade_final_v2='gross'.

Mirrors 266b's updated Phase 3 classifier exactly (Fix A + B + C applied).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import connect_locked  # type: ignore

GROSS_KEYWORDS = (
    "gross", "strap muscle", "sternothyroid", "sternohyoid", "omohyoid",
    "macroscopic extension", "visible gross", "invading trachea",
    "invading esophagus", "recurrent laryngeal nerve invasion",
)
MICRO_KEYWORDS = (
    "microscopic", "microscopiic",
    "minimal extension", "minimal",
    "focally extending", "focal perithyroidal", "focal",
    "perithyroidal soft tissue",
    "perithyroidal fibroadipose", "perithyroidal fat",
)
ABSENT_KEYWORDS = ("absent", "none", "no extension", "negative", "not present")
CPM_OVERLAY_GRADES = {"gross": True, "microscopic": False, "absent": False}


def classify_ete(stl_raw, adj, is_dom, cpm_grade):
    if pd.notna(stl_raw) and stl_raw is not None and str(stl_raw).strip():
        text = str(stl_raw).strip().lower()
        if any(k in text for k in GROSS_KEYWORDS):
            return text, True, "stl_per_tumor:gross"
        if any(k in text for k in MICRO_KEYWORDS):
            return text, False, "stl_per_tumor:microscopic"
        if any(k in text for k in ABSENT_KEYWORDS):
            return text, False, "stl_per_tumor:absent"
        if (is_dom and pd.notna(cpm_grade) and cpm_grade is not None
                and str(cpm_grade).strip().lower() in CPM_OVERLAY_GRADES):
            cg = str(cpm_grade).strip().lower()
            return cg, CPM_OVERLAY_GRADES[cg], f"cpm_patient_level:broadcast_to_dominant:{cg}"
        return text, False, "stl_per_tumor:unclassified_present"
    if pd.notna(adj) and adj is not None and str(adj).strip():
        a = str(adj).strip().lower()
        if a == "gross":
            return a, True, "ete_adjudication_v1:gross"
        if a == "microscopic":
            return a, False, "ete_adjudication_v1:microscopic"
        if a == "absent":
            return a, False, "ete_adjudication_v1:absent"
        return a, None, "ete_adjudication_v1:unable_to_determine"
    return None, None, "uncalculable:no_stl_no_adjudication"


def compute_t_stage(row):
    size = row.get("tumor_size_cm")
    ete = str(row.get("ete_grade", "") or "").lower()
    gross_ete = row.get("gross_ete_flag") is True
    is_t4a = False; is_t4b = False
    if any(k in ete for k in ("trachea", "tracheal", "cricoid",
                               "esophag",
                               "recurrent laryngeal", "rln",
                               "invading trachea", "invading esophagus",
                               "recurrent laryngeal nerve invasion")):
        is_t4a = True
    if any(k in ete for k in ("prevertebral", "carotid",
                               "mediastinal vessels", "unresectable")):
        is_t4b = True
    if row.get("oed_trachea_flag") is True or row.get("oed_esophag_flag") is True:
        is_t4a = True
    if pd.isna(size) or size is None:
        if is_t4b: return "T4b"
        if is_t4a: return "T4a"
        if gross_ete or "gross" in ete: return "T3b"
        return None
    size = float(size)
    if is_t4b: return "T4b"
    if is_t4a: return "T4a"
    if gross_ete or "gross" in ete: return "T3b"
    if size <= 1.0: return "T1a"
    elif size <= 2.0: return "T1b"
    elif size <= 4.0: return "T2"
    elif size > 4.0: return "T3a"
    return None


def main():
    con = connect_locked()
    df = con.execute("""
        WITH stl_real AS (
          SELECT * FROM main.synoptic_tumor_long_v1
          WHERE size_greatest_dimension_cm IS NOT NULL
             OR histologic_type IS NOT NULL
             OR extrathyroidal_extension IS NOT NULL
             OR margin_status IS NOT NULL
        ),
        per_tumor AS (
          SELECT
            CAST(ctc.research_id AS VARCHAR)               AS research_id,
            ctc.surgery_episode_id, ctc.tumor_ordinal, ctc.synoptic_row_ix,
            ctc.size_greatest_dimension_cm                  AS tumor_size_cm,
            stl.extrathyroidal_extension                    AS stl_ete_text,
            eaj.adjudicated_grade                           AS adjudicated_ete_grade,
            oed.strap_muscle_involvement_flag               AS oed_strap_flag,
            oed.tracheal_involvement_flag                   AS oed_trachea_flag,
            oed.esophageal_involvement_flag                 AS oed_esophag_flag
          FROM main.canonical_tumor_characteristics_v1 ctc
          LEFT JOIN stl_real AS stl
            ON CAST(ctc.research_id AS VARCHAR) = CAST(stl.research_id AS VARCHAR)
           AND ctc.synoptic_row_ix = stl.synoptic_row_ix
          LEFT JOIN main.ete_adjudication_v1 AS eaj
            ON CAST(ctc.research_id AS VARCHAR) = CAST(eaj.research_id AS VARCHAR)
          LEFT JOIN main.operative_episode_detail_v2 AS oed
            ON CAST(ctc.research_id AS VARCHAR) = CAST(oed.research_id AS VARCHAR)
           AND ctc.surgery_episode_id = oed.surgery_episode_id
        ),
        cpm_anchor AS (
          SELECT research_id, ete_grade_final_v2 AS cpm_ete_grade_final_v2
          FROM main.canonical_patient_master
        )
        SELECT pt.*, ca.cpm_ete_grade_final_v2
        FROM per_tumor pt
        LEFT JOIN cpm_anchor ca ON ca.research_id = pt.research_id
    """).fetchdf()

    # Mark dominant per pandas (size_greatest_dimension_cm only)
    _sorted = df.sort_values(
        ["research_id", "tumor_size_cm", "tumor_ordinal"],
        ascending=[True, False, True], na_position="last", kind="mergesort",
    )
    _dom = _sorted.groupby("research_id", sort=False).head(1).index
    df["is_dominant_tumor"] = df.index.isin(_dom)

    # Classify
    triples = df.apply(lambda r: classify_ete(
        r["stl_ete_text"], r["adjudicated_ete_grade"],
        r["is_dominant_tumor"], r["cpm_ete_grade_final_v2"]
    ), axis=1)
    df["ete_grade"] = [t[0] for t in triples]
    df["gross_ete_flag"] = [t[1] for t in triples]
    df["ete_source"] = [t[2] for t in triples]
    df["t_stage_ajcc8"] = df.apply(compute_t_stage, axis=1)

    # Patient-dominant tumors only
    dom = df[df["is_dominant_tumor"]].drop_duplicates(subset=["research_id"]).copy()
    print(f"per-patient dominants: {len(dom)}")
    dist = dom["t_stage_ajcc8"].value_counts(dropna=False).to_dict()
    print(f"per-patient T-stage dist: {dist}")

    upstaged = dom[dom["t_stage_ajcc8"].isin(["T3b", "T4a", "T4b"])]
    print(f"per-patient T3b/T4a/T4b: {len(upstaged)}")

    # CPM gross set
    cpm_gross_rids = set(con.execute(
        "SELECT research_id FROM main.canonical_patient_master "
        "WHERE ete_grade_final_v2 = 'gross'"
    ).fetchdf()["research_id"])
    print(f"CPM gross patients: {len(cpm_gross_rids)}")

    overshoot = upstaged[~upstaged["research_id"].isin(cpm_gross_rids)]
    print(f"\n=== Overshoot patients ({len(overshoot)}): t_stage in T3b/T4a/T4b but NOT in CPM gross ===")

    # Pull all CPM ETE-related fields for context
    rids = list(overshoot["research_id"])
    if rids:
        ph = ",".join(repr(r) for r in rids)
        cpm_extra = con.execute(f"""
            SELECT research_id, ete_grade_final_v2, ete_grade,
                   gross_ete_flag,
                   path_gross_ete_flag IS NOT NULL AS has_path_gross_col,
                   ete_op_note_grade IS NOT NULL AS has_op_note_col
            FROM main.canonical_patient_master WHERE research_id IN ({ph})
        """).fetchdf()
        # Some columns may not exist; handle gracefully
        merged = overshoot.merge(cpm_extra, on="research_id", how="left")
        for _, r in merged.iterrows():
            adj = con.execute(
                f"SELECT adjudicated_grade FROM main.ete_adjudication_v1 "
                f"WHERE CAST(research_id AS VARCHAR) = '{r['research_id']}'"
            ).fetchone()
            adj_g = adj[0] if adj else None
            print(f"  rid={r['research_id']}")
            print(f"    cpm.ete_grade_final_v2 = {r['ete_grade_final_v2']!r}")
            print(f"    cpm.ete_grade          = {r['ete_grade_y'] if 'ete_grade_y' in r else r.get('ete_grade')!r}")
            print(f"    cpm.gross_ete_flag     = {r['gross_ete_flag_y'] if 'gross_ete_flag_y' in r else r.get('gross_ete_flag')!r}")
            print(f"    dominant t_stage_ajcc8 = {r['t_stage_ajcc8']}")
            print(f"    dominant ete_source    = {r['ete_source']}")
            print(f"    dominant stl_ete_text  = {r['stl_ete_text']!r}")
            print(f"    OED strap/trachea/esoph= {r['oed_strap_flag']}/{r['oed_trachea_flag']}/{r['oed_esophag_flag']}")
            print(f"    ete_adjudication_v1.grade = {adj_g!r}")

    # Source breakdown
    print()
    print("=== Source breakdown of overshoot patients ===")
    print(overshoot["ete_source"].value_counts().to_dict())


if __name__ == "__main__":
    main()

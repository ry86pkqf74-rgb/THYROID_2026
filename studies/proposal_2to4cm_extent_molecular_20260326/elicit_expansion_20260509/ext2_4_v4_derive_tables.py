#!/usr/bin/env python3
"""
EXT2-4 v4 cohort table derivation via BigQuery (read-only).

Cohort v4 = surgical (lobectomy|total TT) 1999–2025 AND EXISTS preop US nodule with
canonical_us_nodule_v2.size_cm_max BETWEEN 2.0 AND 4.0 (exam_date <= surg_first_date).

Writes:
  - tables/table1_v4_cohort_characteristics.csv
  - tables/table2_v4_malignancy_by_bethesda_era.csv
  - tables/table2b_v4_surgical_extent_by_bethesda_era.csv
  - tables/_v4_table3_cells.json       (machine input for build_table3_v4)
  - tables/table4_v4_recurrence_by_molecular_status.csv
"""
from __future__ import annotations

import csv
import json
import math
from pathlib import Path

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent
TABLES = ROOT / "tables"
TABLES.mkdir(exist_ok=True)

PROJECT = "thyroid-canonical-pub-2026"
MC = f"`{PROJECT}.pub_canonical.manuscript_cohort_v1`"
USNOD = f"`{PROJECT}.pub_canonical.canonical_us_nodule_v2`"


def cohort_ctes() -> str:
    return f"""
WITH surgical AS (
  SELECT
    CAST(research_id AS STRING) AS rid_s,
    fna_bethesda_final AS bethesda,
    imaging_nodule_size_cm AS imaging_nodule_size_cm_index,
    surg_first_date,
    histology_final,
    surg_total_thyroidectomy,
    surg_hemithyroidectomy,
    surg_procedure_type,
    age_at_surgery,
    sex,
    mol_platform,
    molecular_risk_tier,
    braf_positive_final,
    ras_positive_final,
    tert_positive_final,
    any_recurrence_flag,
    structural_recurrence_flag,
    imaging_tirads_worst,
    EXTRACT(YEAR FROM surg_first_date) AS surgery_year
  FROM {MC}
  WHERE surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025
    AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
),
cohort_v4_pts AS (
  SELECT DISTINCT s.rid_s
  FROM surgical s
  JOIN {USNOD} n ON CAST(n.research_id AS STRING) = s.rid_s
   AND n.exam_date <= DATE(s.surg_first_date)
  WHERE n.size_cm_max BETWEEN 2.0 AND 4.0
),
ct_susp AS (
  SELECT CAST(research_id AS STRING) AS rid_s,
         MIN(exam_date) AS earliest_ct_susp_date
  FROM `{PROJECT}.pub_canonical.canonical_ct_lymph_node_v1`
  WHERE suspicious_flag = TRUE
  GROUP BY research_id
),
mri_susp AS (
  SELECT CAST(research_id AS STRING) AS rid_s,
         MIN(exam_date) AS earliest_mri_susp_date
  FROM `{PROJECT}.pub_canonical.canonical_mri_lymph_node_v1`
  WHERE suspicious_flag = TRUE
  GROUP BY research_id
),
bethesda6_ln_fna AS (
  SELECT DISTINCT CAST(research_id AS STRING) AS rid_s
  FROM `{PROJECT}.pub_canonical.canonical_fna_events_v1`
  WHERE bethesda_final_num = 6
    AND (LOWER(IFNULL(specimen_location,'')) LIKE '%node%'
         OR LOWER(IFNULL(specimen_location,'')) LIKE '%lymph%'
         OR LOWER(IFNULL(fna_site,'')) LIKE '%node%'
         OR LOWER(IFNULL(fna_site,'')) LIKE '%lymph%')
),
strict_ok AS (
  SELECT s.rid_s
  FROM surgical s
  JOIN cohort_v4_pts cv ON cv.rid_s = s.rid_s
  LEFT JOIN ct_susp c ON s.rid_s = c.rid_s
    AND DATE(c.earliest_ct_susp_date) <= DATE(s.surg_first_date)
  LEFT JOIN mri_susp m ON s.rid_s = m.rid_s
    AND DATE(m.earliest_mri_susp_date) <= DATE(s.surg_first_date)
  LEFT JOIN bethesda6_ln_fna b ON s.rid_s = b.rid_s
  WHERE c.rid_s IS NULL AND m.rid_s IS NULL AND b.rid_s IS NULL
),
v4_base AS (
  SELECT s.*, IF(st.rid_s IS NOT NULL, TRUE, FALSE) AS in_strict
  FROM surgical s
  INNER JOIN cohort_v4_pts cv ON cv.rid_s = s.rid_s
  LEFT JOIN strict_ok st ON st.rid_s = s.rid_s
)
"""


def malignancy_strict_expr(col: str = "histology_final") -> str:
    hl = f"LOWER(TRIM(IFNULL({col},'')))"
    return f"""(
      ({col} IS NOT NULL AND TRIM(IFNULL({col},'')) != '')
      AND NOT ({hl} LIKE '%niftp%' OR {hl} LIKE '%nifcp%' OR {hl} LIKE '%nifp%')
      AND (
        {hl} LIKE '%ptc%' OR {hl} LIKE '%papillary%' OR {hl} LIKE '%mtc%'
        OR {hl} LIKE '%follicular carcinoma%' OR {hl} LIKE '%medullary%'
        OR {hl} LIKE '%anaplastic%' OR {hl} LIKE '%poorly differentiated%'
        OR ({hl} LIKE '%ftump%' OR {hl} LIKE '%hyalinizing trabecular%')
      )
    )"""


def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    if n == 0:
        return (0.0, 0.0, 1.0)
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = (z / denom) * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (p, max(0.0, center - half), min(1.0, center + half))


def fmt_pct_ci(k: int, n: int) -> str:
    if n == 0:
        return "0/0 (—)"
    p, lo, hi = wilson_ci(k, n)
    return f"{k}/{n} ({100*p:.1f}% [{100*lo:.1f}–{100*hi:.1f}])"


def fetch_verification(client: bigquery.Client) -> dict:
    sql = cohort_ctes() + """
SELECT
  (SELECT COUNT(*) FROM surgical) AS n_surgical_total,
  (SELECT COUNT(*) FROM cohort_v4_pts) AS n_v4_pts,
  (SELECT COUNTIF(in_strict) FROM v4_base) AS n_v4_strict
"""
    r = next(iter(client.query(sql).result()))
    return dict(r)


def fetch_table1(client: bigquery.Client) -> list[dict]:
    mal_sql = malignancy_strict_expr()
    tmpl = cohort_ctes() + f"""
SELECT
  '{{LBL}}' AS stratum_label,
  COUNT(*) AS n_all,
  COUNTIF(in_strict) AS n_strict,
  COUNTIF(LOWER(IFNULL(sex,''))='female') AS nf_all,
  COUNTIF(in_strict AND LOWER(IFNULL(sex,''))='female') AS nf_strict,
  ROUND(AVG(age_at_surgery), 1) AS mean_age_all,
  APPROX_QUANTILES(age_at_surgery,100)[OFFSET(50)] AS med_age_all,
  APPROX_QUANTILES(age_at_surgery,100)[OFFSET(25)] AS p25_age_all,
  APPROX_QUANTILES(age_at_surgery,100)[OFFSET(75)] AS p75_age_all,
  ROUND(AVG(IF(in_strict, age_at_surgery, NULL)), 1) AS mean_age_strict,
  APPROX_QUANTILES(IF(in_strict, age_at_surgery, NULL),100)[OFFSET(50)] AS med_age_s,
  APPROX_QUANTILES(IF(in_strict, age_at_surgery, NULL),100)[OFFSET(25)] AS p25_age_s,
  APPROX_QUANTILES(IF(in_strict, age_at_surgery, NULL),100)[OFFSET(75)] AS p75_age_s,
  COUNTIF(imaging_nodule_size_cm_index IS NOT NULL) AS n_presz_all,
  COUNTIF(in_strict AND imaging_nodule_size_cm_index IS NOT NULL) AS n_presz_s,
  ROUND(AVG(imaging_nodule_size_cm_index), 2) AS mean_sz_all,
  APPROX_QUANTILES(imaging_nodule_size_cm_index,100)[OFFSET(50)] AS med_sz_all,
  COUNTIF(bethesda=1) AS b1_a, COUNTIF(bethesda=2) AS b2_a,
  COUNTIF(bethesda=3) AS b3_a, COUNTIF(bethesda=4) AS b4_a,
  COUNTIF(bethesda=5) AS b5_a, COUNTIF(bethesda=6) AS b6_a,
  COUNTIF(in_strict AND bethesda=1) AS b1_s, COUNTIF(in_strict AND bethesda=2) AS b2_s,
  COUNTIF(in_strict AND bethesda=3) AS b3_s, COUNTIF(in_strict AND bethesda=4) AS b4_s,
  COUNTIF(in_strict AND bethesda=5) AS b5_s, COUNTIF(in_strict AND bethesda=6) AS b6_s,
  COUNTIF(mol_platform='Afirma') AS na_a,
  COUNTIF(mol_platform='ThyroSeq') AS nt_a,
  COUNTIF(mol_platform IN ('Afirma','ThyroSeq')) AS nn_a,
  COUNTIF(in_strict AND mol_platform='Afirma') AS na_s,
  COUNTIF(in_strict AND mol_platform='ThyroSeq') AS nt_s,
  COUNTIF(in_strict AND mol_platform IN ('Afirma','ThyroSeq')) AS nn_s,
  COUNTIF(imaging_tirads_worst IS NOT NULL) AS ntir_all,
  COUNTIF(imaging_tirads_worst >= 4) AS ntir4_all,
  COUNTIF(in_strict AND imaging_tirads_worst IS NOT NULL) AS ntir_s,
  COUNTIF(in_strict AND imaging_tirads_worst >= 4) AS ntir4_s,
  COUNTIF(surgery_year < 2015) AS npre_a,
  COUNTIF(surgery_year >= 2015) AS np15_a,
  COUNTIF(in_strict AND surgery_year < 2015) AS npre_s,
  COUNTIF(in_strict AND surgery_year >= 2015) AS np15_s,
  COUNTIF(surg_total_thyroidectomy) AS ntot_a,
  COUNTIF(surg_hemithyroidectomy) AS nlob_a,
  COUNTIF(in_strict AND surg_total_thyroidectomy) AS ntot_s,
  COUNTIF(in_strict AND surg_hemithyroidectomy) AS nlob_s,
  COUNTIF({mal_sql}) AS nmal_all,
  COUNTIF(in_strict AND ({mal_sql})) AS nmal_s
FROM v4_base
WHERE {{PRED}}
"""
    strata = [
        ("Overall", "TRUE"),
        ("Initial lobectomy", "surg_hemithyroidectomy"),
        ("Initial total thyroidectomy", "surg_total_thyroidectomy"),
        ("Index preop size <2 cm",
         "(imaging_nodule_size_cm_index IS NOT NULL AND imaging_nodule_size_cm_index < 2)"),
        ("Index preop size 2–4 cm",
         "(imaging_nodule_size_cm_index BETWEEN 2.0 AND 4.0)"),
        ("Index preop size >4 cm",
         "(imaging_nodule_size_cm_index IS NOT NULL AND imaging_nodule_size_cm_index > 4)"),
        ("Era pre-2015 (surgery year)", "surgery_year < 2015"),
        ("Era 2015+ (surgery year)", "surgery_year >= 2015"),
    ]
    rows_out = []
    for lbl, pred in strata:
        q = tmpl.replace("{LBL}", lbl.replace("'", "\\'")).replace("{PRED}", pred)
        hit = client.query(q).result()
        rows_out.append(dict(next(iter(hit))))
    return rows_out


def write_table1_csv(rows: list[dict], path: Path) -> None:
    cols = [
        "Stratum",
        "N (v4 any preop US 2–4 cm)",
        "N (strict nodal exclusions)",
        "Female n (pct v4)",
        "Female n (pct strict)",
        "Age median IQR v4",
        "Age median IQR strict",
        "Index preop cm median [n]",
        "B1-B6 counts v4",
        "B1-B6 counts strict",
        "Afirma pct / ThyroSeq pct / Named pct (v4)",
        "Afirma pct / ThyroSeq pct / Named pct (strict)",
        "TIRADS ge4 of known v4",
        "TIRADS ge4 strict",
        "Pre2015 pct vs 2015plus pct v4",
        "same strict",
        "Total thyroid pct / Lobectomy pct v4",
        "same strict",
        "Malign strict histology pct (v4)",
        "Malign strict histology pct (strict)",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)

        def b6(prefix: str, r_: dict) -> str:
            ch = "a" if prefix == "v4" else "s"
            return ",".join(
                f"B{i}:{int(r_[f'b{i}_{ch}'])}"
                for i in range(1, 7)
            )

        for r in rows:
            n_a, n_s = int(r["n_all"]), int(r["n_strict"])
            w.writerow([
                r["stratum_label"],
                n_a,
                n_s,
                f"{r['nf_all']}/{n_a} ({100*r['nf_all']/n_a:.1f}%)" if n_a else "",
                f"{r['nf_strict']}/{n_s} ({100*r['nf_strict']/n_s:.1f}%)" if n_s else "",
                f"{r['med_age_all']} [{r['p25_age_all']}–{r['p75_age_all']}]",
                f"{r['med_age_s']} [{r['p25_age_s']}–{r['p75_age_s']}]" if n_s else "—",
                f"{float(r['med_sz_all'] or 0):.2f} [n={r['n_presz_all']}]"
                if r["n_presz_all"] else "—",
                b6("v4", r),
                b6("strict", r),
                "/".join(
                    f"{lbl}:{100*r[k]/n_a:.1f}%"
                    for lbl, k in [("Af", "na_a"), ("Ts", "nt_a"), ("Nm", "nn_a")]
                ) if n_a else "",
                "/".join(
                    f"{lbl}:{100*r[k]/n_s:.1f}%"
                    for lbl, k in [("Af", "na_s"), ("Ts", "nt_s"), ("Nm", "nn_s")]
                ) if n_s else "",
                f"{100*r['ntir4_all']/r['ntir_all']:.1f}% (n_known={r['ntir_all']})"
                if r["ntir_all"] else "—",
                f"{100*r['ntir4_s']/r['ntir_s']:.1f}% (n_known={r['ntir_s']})"
                if r["ntir_s"] else "—",
                f"{100*r['npre_a']/n_a:.1f}% / {100*r['np15_a']/n_a:.1f}%"
                if n_a else "",
                f"{100*r['npre_s']/n_s:.1f}% / {100*r['np15_s']/n_s:.1f}%"
                if n_s else "",
                f"{100*r['ntot_a']/n_a:.1f}% / {100*r['nlob_a']/n_a:.1f}%"
                if n_a else "",
                f"{100*r['ntot_s']/n_s:.1f}% / {100*r['nlob_s']/n_s:.1f}%"
                if n_s else "",
                f"{100*r['nmal_all']/n_a:.1f}% (n={r['nmal_all']})" if n_a else "",
                f"{100*r['nmal_s']/n_s:.1f}% (n={r['nmal_s']})" if n_s else "",
            ])


def fetch_table23(client: bigquery.Client) -> tuple[list, list]:
    q2 = cohort_ctes().rstrip() + """,
cls AS (
  SELECT
    b.bethesda,
    CASE WHEN b.surgery_year < 2015 THEN 'pre_2015' ELSE '2015_plus' END AS era,
    CASE
      WHEN b.histo_lower IS NULL OR b.histo_lower = '' THEN NULL
      WHEN b.histo_lower LIKE '%ptc%' OR b.histo_lower LIKE '%papillary%'
           OR b.histo_lower LIKE '%mtc%' OR b.histo_lower LIKE '%follicular carcinoma%'
           OR b.histo_lower LIKE '%anaplastic%' OR b.histo_lower LIKE '%medullary%'
           OR b.histo_lower LIKE '%poorly differentiated%' THEN 'malignant'
      WHEN b.histo_lower LIKE '%niftp%' THEN 'niftp'
      WHEN b.histo_lower LIKE '%ftump%' THEN 'borderline'
      ELSE 'benign_other'
    END AS malignancy_class
  FROM (
    SELECT v.*, LOWER(TRIM(IFNULL(v.histology_final,''))) AS histo_lower
    FROM v4_base v
  ) b
  WHERE bethesda IS NOT NULL
)
SELECT
  bethesda, era,
  COUNT(*) AS n_total,
  COUNTIF(malignancy_class IS NOT NULL) AS n_with_histology,
  COUNTIF(malignancy_class='malignant') AS n_malignant_strict,
  COUNTIF(malignancy_class IN ('malignant','niftp','borderline')) AS n_malign_incl,
  COUNTIF(malignancy_class='niftp') AS n_niftp,
  COUNTIF(malignancy_class='borderline') AS n_borderline
FROM cls
GROUP BY bethesda, era
ORDER BY bethesda, era
"""
    rows2 = list(client.query(q2).result())
    q2b = cohort_ctes() + """
SELECT
  bethesda,
  CASE WHEN surgery_year < 2015 THEN 'pre_2015' ELSE '2015_plus' END AS era,
  COUNT(*) AS n_total,
  COUNTIF(surg_total_thyroidectomy) AS n_total_thyroid,
  COUNTIF(surg_hemithyroidectomy) AS n_lobectomy
FROM v4_base
WHERE bethesda IS NOT NULL
GROUP BY bethesda, era
ORDER BY bethesda, era
"""
    rows2b = list(client.query(q2b).result())
    return rows2, rows2b


def write_table23_csv(rows2, rows2b) -> None:
    with (TABLES / "table2_v4_malignancy_by_bethesda_era.csv").open(
        "w", newline=""
    ) as f:
        w = csv.writer(f)
        w.writerow([
            "Bethesda",
            "Era",
            "n_total",
            "n_with_resolved_histology",
            "Malignant strict pct Wilson",
            "Malignant incl NIFTP pct Wilson",
            "NIFTP n",
            "Borderline n",
        ])
        for r in rows2:
            rr = dict(r)
            nh, ms, mi = int(rr["n_with_histology"]), int(
                rr["n_malignant_strict"]
            ), int(rr["n_malign_incl"])
            ms_disp = fmt_pct_ci(ms, nh) if nh else "—"
            mi_disp = fmt_pct_ci(mi, nh) if nh else "—"
            w.writerow([
                f"Bethesda {rr['bethesda']}",
                rr["era"],
                rr["n_total"],
                rr["n_with_histology"],
                ms_disp,
                mi_disp,
                rr["n_niftp"],
                rr["n_borderline"],
            ])
    with (TABLES / "table2b_v4_surgical_extent_by_bethesda_era.csv").open(
        "w", newline=""
    ) as f:
        w = csv.writer(f)
        w.writerow(["Bethesda", "Era", "n_total", "Total thyroidectomy Wilson",
                    "Lobectomy n pct"])
        for r in rows2b:
            rr = dict(r)
            nt = int(rr["n_total"])
            tt = int(rr["n_total_thyroid"])
            nl = int(rr["n_lobectomy"])
            w.writerow([
                f"Bethesda {rr['bethesda']}",
                rr["era"],
                nt,
                fmt_pct_ci(tt, nt) if nt else "—",
                f"{nl}/{nt} ({100*nl/nt:.1f}%)" if nt else "—",
            ])


def fetch_table3_cells(client: bigquery.Client) -> list:
    sql = cohort_ctes() + """
,mol AS (
  SELECT
    CAST(research_id AS STRING) AS rid_s,
    platform,
    overall_result_class,
    rom_descriptor,
    rom_percent_point,
    resolved_test_date,
    CASE
      WHEN platform='Afirma' AND overall_result_class IN ('suspicious','positive')
        THEN 'positive'
      WHEN platform='Afirma' AND overall_result_class='negative' THEN 'negative'
      WHEN platform='ThyroSeq'
           AND rom_descriptor IN ('HIGH','INTERMEDIATE-HIGH','INTERMEDIATEHIGH')
        THEN 'positive'
      WHEN platform='ThyroSeq'
           AND rom_descriptor IN ('LOW','INTERMEDIATE-LOW')
        THEN 'negative'
      WHEN platform='ThyroSeq' AND rom_descriptor='INTERMEDIATE' THEN 'intermediate'
      WHEN platform='ThyroSeq' AND overall_result_class='positive' THEN 'positive'
      WHEN platform='ThyroSeq' AND overall_result_class='negative' THEN 'negative'
      ELSE 'unknown_or_excluded'
    END AS reported_call
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2`
  WHERE platform IN ('Afirma','ThyroSeq')
    AND NOT (
      platform = 'ThyroSeq'
      AND (overall_result_class = 'superseded'
           OR IFNULL(platform_reclass_status,'')='superseded_by_afirma_row')
    )
    AND IFNULL(platform_reclass_status,'') != 'non_diagnostic_cancelled'
),
joined AS (
  SELECT
    s.bethesda,
    s.imaging_nodule_size_cm_index AS preop_size_cm,
    CASE
      WHEN s.imaging_nodule_size_cm_index BETWEEN 2.0 AND 4.0 THEN '2to4cm_index'
      WHEN s.imaging_nodule_size_cm_index IS NOT NULL
           AND s.imaging_nodule_size_cm_index < 2.0 THEN 'lt2cm_index'
      WHEN s.imaging_nodule_size_cm_index IS NOT NULL
           AND s.imaging_nodule_size_cm_index > 4.0 THEN 'gt4cm_index'
      ELSE 'unknown_size'
    END AS size_band_descriptive,
    m.platform,
    m.rom_percent_point,
    m.reported_call,
    ROW_NUMBER() OVER (
      PARTITION BY s.rid_s
      ORDER BY
        CASE WHEN m.resolved_test_date IS NOT NULL
              AND m.resolved_test_date <= DATE(s.surg_first_date) THEN 0 ELSE 1 END,
        m.resolved_test_date DESC
    ) AS rn,
    CASE
      WHEN s.histo_lower IS NULL OR s.histo_lower = '' THEN 'benign'
      WHEN s.histo_lower LIKE '%niftp%' OR s.histo_lower LIKE '%nifcp%' THEN 'niftp'
      WHEN s.histo_lower LIKE '%ftump%'
           OR s.histo_lower LIKE '%hyalinizing trabecular%' THEN 'borderline'
      WHEN s.histo_lower LIKE '%adenoma%' AND s.histo_lower NOT LIKE '%adenoid%'
        THEN 'benign_adenoma'
      ELSE 'malignant'
    END AS histo_class
  FROM (
    SELECT v.*,
      LOWER(TRIM(IFNULL(v.histology_final,''))) AS histo_lower
    FROM v4_base v
    WHERE bethesda IN (3, 4)
  ) s
  JOIN mol m ON m.rid_s = s.rid_s
)
SELECT
  bethesda, platform, size_band_descriptive, reported_call, histo_class,
  COUNT(*) AS n,
  ROUND(AVG(rom_percent_point), 1) AS mean_rom_pct,
  APPROX_QUANTILES(rom_percent_point,100)[OFFSET(50)] AS med_rom_pct,
  APPROX_QUANTILES(rom_percent_point,100)[OFFSET(25)] AS p25_rom_pct,
  APPROX_QUANTILES(rom_percent_point,100)[OFFSET(75)] AS p75_rom_pct,
  COUNTIF(rom_percent_point IS NOT NULL) AS n_with_rom_pct
FROM joined
WHERE rn = 1
GROUP BY 1,2,3,4,5
ORDER BY platform, bethesda, size_band_descriptive, reported_call, histo_class
"""
    return list(client.query(sql).result())


def fetch_table4(client: bigquery.Client) -> list:
    msq = malignancy_strict_expr("b.histology_final")
    sql = cohort_ctes() + f"""
,joined AS (
  SELECT
    CASE
      WHEN b.mol_platform IN ('Afirma','ThyroSeq') THEN b.mol_platform
      WHEN b.mol_platform = 'Other' THEN 'Other / historical / in-house'
      ELSE 'Untested'
    END AS molecular_group,
    CASE
      WHEN b.braf_positive_final OR b.tert_positive_final THEN 'high_risk_mutation'
      WHEN b.ras_positive_final THEN 'ras_only'
      WHEN b.molecular_risk_tier = 'wild_type' THEN 'wild_type'
      WHEN b.molecular_risk_tier IS NULL THEN 'no_result'
      ELSE 'other_intermediate'
    END AS mutation_class,
    CAST(rr.recurrence_path_proven AS BOOL) recurrence_path_proven,
    CAST(b.structural_recurrence_flag AS BOOL) structural_recurrence_flag,
    CAST(b.any_recurrence_flag AS BOOL) any_recurrence_flag
  FROM v4_base b
  LEFT JOIN `{PROJECT}.pub_canonical.canonical_recurrence_resolved_v1` rr
    ON CAST(rr.research_id AS STRING) = b.rid_s
  WHERE {msq}
)
SELECT
  molecular_group,
  mutation_class,
  COUNT(*) AS n_malignant,
  COUNTIF(recurrence_path_proven) AS n_path_proven_recurrence,
  COUNTIF(structural_recurrence_flag) AS n_structural_recurrence,
  COUNTIF(any_recurrence_flag) AS n_any_recurrence
FROM joined
GROUP BY 1,2
ORDER BY 1, 2
"""
    return list(client.query(sql).result())


def write_table4_csv(rows) -> None:
    p = TABLES / "table4_v4_recurrence_by_molecular_status.csv"
    cols = (
        "Molecular group",
        "Mutation class",
        "n_malignant",
        "Path-proven recurrence n pct Wilson",
        "Structural pct of n",
        "Any recurrence pct",
    )
    with p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            rr = dict(r)
            nm = int(rr["n_malignant"])
            npp, nsr, nany = map(int, [
                rr["n_path_proven_recurrence"],
                rr["n_structural_recurrence"],
                rr["n_any_recurrence"],
            ])
            w.writerow([
                rr["molecular_group"],
                rr["mutation_class"],
                nm,
                fmt_pct_ci(npp, nm) if nm else "—",
                f"{nsr}/{nm} ({100*nsr/nm:.1f}%)" if nm else "—",
                f"{nany}/{nm} ({100*nany/nm:.1f}%)" if nm else "—",
            ])


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    verif = fetch_verification(client)
    print("verification:", json.dumps(verif, indent=2, default=str))
    tbl1 = fetch_table1(client)
    write_table1_csv(tbl1, TABLES / "table1_v4_cohort_characteristics.csv")
    r2, r2b = fetch_table23(client)
    write_table23_csv(r2, r2b)
    cells = fetch_table3_cells(client)
    out_cells = []
    for r in cells:
        d = dict(r)
        out_cells.append(
            (
                int(d["bethesda"]),
                d["platform"],
                d["size_band_descriptive"].replace("_index", ""),
                d["reported_call"],
                d["histo_class"],
                int(d["n"]),
                float(d["mean_rom_pct"]) if d["mean_rom_pct"] is not None else None,
                float(d["med_rom_pct"]) if d["med_rom_pct"] is not None else None,
                float(d["p25_rom_pct"]) if d["p25_rom_pct"] is not None else None,
                float(d["p75_rom_pct"]) if d["p75_rom_pct"] is not None else None,
                int(d["n_with_rom_pct"] or 0),
            )
        )
    with (TABLES / "_v4_table3_cells.json").open("w") as fx:
        json.dump(
            {
                "n_surgical_verified": verif["n_surgical_total"],
                "n_v4": verif["n_v4_pts"],
                "n_v4_strict": verif["n_v4_strict"],
                "cells": [list(t) for t in out_cells],
            },
            fx,
            indent=2,
        )
    tbl4 = fetch_table4(client)
    write_table4_csv(tbl4)
    print(f"OK table1 rows={len(tbl1)}, table3 cell groups={len(out_cells)}, table4 rows={len(tbl4)}")


if __name__ == "__main__":
    main()

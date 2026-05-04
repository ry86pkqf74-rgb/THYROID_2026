#!/usr/bin/env python3
"""
build_m032_manuscript_md.py
===========================
Regenerates 02_manuscript_body.md from live MotherDuck numbers.
Also writes a CONSORT numbers summary for cross-checking.

Run from repo root:
    .venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_manuscript_md.py

Output: M032_submission_package_v1_0/08_analysis_outputs/M032_manuscript_numbers_YYYYMMDD.md
"""
import sys, os, datetime, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import duckdb
import pandas as pd

from motherduck_client import get_token

PKG_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(PKG_DIR, "08_analysis_outputs")
os.makedirs(OUT_DIR, exist_ok=True)

ERA_CASE = """
CASE
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
  ELSE 'F_unknown'
END AS surgery_era
"""
ERA_LABELS = {
    'A_1999_2004': '1999–2004',
    'B_2005_2009': '2005–2009',
    'C_2010_2014': '2010–2014',
    'D_2015_2019': '2015–2019',
    'E_2020_2025': '2020–2025',
}


def connect():
    tok = get_token()
    return duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={tok}")


def pull_numbers(con):
    """Pull all manuscript numbers into a dict for template fill-in."""
    # Cohort overall
    base = con.execute("""
        SELECT COUNT(*) AS n_total,
               COUNT(*) FILTER (WHERE is_malignant=TRUE) AS n_malig,
               ROUND(COUNT(*) FILTER (WHERE is_malignant=TRUE)*100.0/COUNT(*),1) AS pct_malig,
               ROUND(AVG(age_at_surgery::DOUBLE),1) AS age_mean,
               ROUND(STDDEV(age_at_surgery::DOUBLE),1) AS age_sd,
               MEDIAN(age_at_surgery) AS age_median,
               COUNT(*) FILTER (WHERE LOWER(sex)='female') AS n_female,
               COUNT(*) FILTER (WHERE is_malignant=TRUE AND LOWER(sex)='female') AS n_female_malig,
               COUNT(*) FILTER (WHERE LOWER(race) LIKE '%white%' OR LOWER(race) LIKE '%caucasian%') AS n_white,
               COUNT(*) FILTER (WHERE LOWER(race) LIKE '%black%' OR LOWER(race) LIKE '%african%') AS n_black,
               COUNT(*) FILTER (WHERE LOWER(race) LIKE '%asian%') AS n_asian,
               ROUND(MEDIAN(tumor_size_cm)::DOUBLE, 2) AS tumor_size_median,
               COUNT(*) FILTER (WHERE multifocal_flag=TRUE AND is_malignant=TRUE) AS n_multifocal_malig,
               COUNT(*) FILTER (WHERE LOWER(smoking_status_combined)='current') AS n_smoke_current,
               COUNT(*) FILTER (WHERE LOWER(smoking_status_combined)='former') AS n_smoke_former,
               COUNT(*) FILTER (WHERE LOWER(smoking_status_combined)='never') AS n_smoke_never,
               COUNT(*) FILTER (WHERE smoking_status_combined IS NOT NULL) AS n_smoke_known,
               COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid=TRUE) AS n_fhx_thyroid,
               COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid IS NOT NULL) AS n_fhx_known,
               ROUND(MEDIAN(followup_years)::DOUBLE, 1) AS fu_median,
               COUNT(*) FILTER (WHERE any_recurrence_flag=TRUE AND is_malignant=TRUE) AS n_recurrence,
               COUNT(*) FILTER (WHERE death_occurred=TRUE) AS n_death,
               COUNT(*) FILTER (WHERE rai_received_flag=TRUE AND is_malignant=TRUE) AS n_rai_malig
        FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
    """).fetchdf().iloc[0].to_dict()

    # Era table
    era = con.execute(f"""
        WITH b AS (SELECT *, {ERA_CASE}
                   FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
                   WHERE surgery_era != 'F_unknown')
        SELECT surgery_era,
               COUNT(*) AS n,
               COUNT(*) FILTER (WHERE is_malignant=TRUE) AS n_malig,
               ROUND(COUNT(*) FILTER (WHERE is_malignant=TRUE)*100.0/COUNT(*),1) AS pct_malig,
               ROUND(COUNT(*) FILTER (WHERE LOWER(smoking_status_combined)='current')*100.0
                 / NULLIF(COUNT(*) FILTER (WHERE smoking_status_combined IS NOT NULL),0),1) AS pct_current_of_known,
               ROUND(COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid=TRUE)*100.0
                 / NULLIF(COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid IS NOT NULL),0),1) AS pct_fhx_of_known
        FROM b GROUP BY surgery_era ORDER BY surgery_era
    """).fetchdf()
    era['era_label'] = era['surgery_era'].map(ERA_LABELS)

    return base, era


def render_md(base, era, out_path):
    n    = int(base['n_total'])
    nm   = int(base['n_malig'])
    pct  = base['pct_malig']
    age_mean = base['age_mean']
    age_sd   = base['age_sd']
    nf   = int(base['n_female'])
    pct_f = round(nf / n * 100, 1)
    nf_m = int(base['n_female_malig'])
    pct_fm = round(nf_m / nm * 100, 1)
    nw   = int(base['n_white'])
    nb   = int(base['n_black'])
    na_  = int(base['n_asian'])
    ts   = base['tumor_size_median']
    sc   = int(base['n_smoke_current'])
    sf   = int(base['n_smoke_former'])
    sn   = int(base['n_smoke_never'])
    sk   = int(base['n_smoke_known'])
    pct_sc = round(sc / sk * 100, 1) if sk else 0
    pct_sk_coh = round(sk / n * 100, 1)
    fhx_p = int(base['n_fhx_thyroid'])
    fhx_k = int(base['n_fhx_known'])
    pct_fhx = round(fhx_p / fhx_k * 100, 1) if fhx_k else 0
    fu    = base['fu_median']
    nrec  = int(base['n_recurrence'])
    ndeath = int(base['n_death'])
    nrai  = int(base['n_rai_malig'])

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        f"# M032 — Manuscript Numbers Sheet",
        f"",
        f"**Generated:** {now} | mig_290 | thyroid_canonical_publication_v1_0",
        f"**Cohort lock:** mig_281 + mig_285 (post-2026-05-04)",
        f"",
        f"---",
        f"",
        f"## Abstract Numbers",
        f"",
        f"- Total cohort: **{n:,}** patients",
        f"- Analytic malignant: **{nm:,}** ({pct}%)",
        f"- Female: {pct_f}% full cohort; {pct_fm}% malignant",
        f"- Mean age: {age_mean} ± {age_sd} years",
        f"- Median tumor size: {ts} cm",
        f"- Median follow-up: {fu} years",
        f"- Any recurrence: {nrec:,}; Deaths: {ndeath:,}",
        f"- RAI among malignant: {nrai:,}",
        f"",
        f"---",
        f"",
        f"## Table 1 — Demographics",
        f"",
        f"| Variable | Full Cohort (N={n:,}) | Malignant (N={nm:,}) |",
        f"|---|---|---|",
        f"| Female | {nf:,} ({pct_f}%) | {nf_m:,} ({pct_fm}%) |",
        f"| Age, mean ± SD | {age_mean} ± {age_sd} | [see XLSX] |",
        f"| White | {nw:,} | [see XLSX] |",
        f"| Black/African American | {nb:,} | [see XLSX] |",
        f"| Asian | {na_:,} | [see XLSX] |",
        f"| Median tumor size (cm) | — | {ts} |",
        f"",
        f"---",
        f"",
        f"## Table 5 — Smoking & Family Hx (Post-mig_281 NLP)",
        f"",
        f"### Cohort-wide",
        f"",
        f"| Status | n | % of known | % of cohort |",
        f"|---|---|---|---|",
        f"| Current smoker | {sc:,} | {pct_sc}% | {round(sc/n*100,2)}% |",
        f"| Former smoker  | {sf:,} | {round(sf/sk*100,1)}% | {round(sf/n*100,2)}% |",
        f"| Never smoked   | {sn:,} | {round(sn/sk*100,1)}% | {round(sn/n*100,2)}% |",
        f"| **Known**      | **{sk:,}** | — | **{pct_sk_coh}%** |",
        f"",
        f"| Family hx thyroid cancer | n | % of known |",
        f"|---|---|---|",
        f"| Present | {fhx_p:,} | {pct_fhx}% |",
        f"| Known | {fhx_k:,} | — |",
        f"",
        f"### By era",
        f"",
    ]

    era_rows = ["| Era | n | n_malig | %_malig | %_current_of_known | %_fhx_of_known |",
                "|---|---|---|---|---|---|"]
    for _, row in era.iterrows():
        if row.surgery_era == 'F_unknown':
            continue
        era_rows.append(
            f"| {row.era_label} | {int(row.n):,} | {int(row.n_malig):,} | "
            f"{row.pct_malig}% | {row.pct_current_of_known or '(sparse)'}% | {row.pct_fhx_of_known or '—'}% |"
        )
    lines += era_rows
    lines += [
        f"",
        f"---",
        f"",
        f"## Key Headlines (for Discussion draft)",
        f"",
        f"- Cohort grew ~4× over 25 years: 905 (1999–2004) → 3,935 (2020–2025)",
        f"- Malignancy rate rose {era.iloc[0].pct_malig}% → {era.iloc[-1 if len(era)>1 else 0].pct_malig}% (+{round(era.iloc[-1 if len(era)>1 else 0].pct_malig - era.iloc[0].pct_malig, 1)} pp)",
        f"- Smoking ever-prevalence in known patients: {pct_sc}% current in most recent era",
        f"- Family hx coverage now {pct_sk_coh}% cohort-wide (was ~4% pre-mig_281)",
        f"",
        f"---",
        f"",
        f"## Cross-reference to locked Cowork numbers (2026-05-04)",
        f"",
        f"| Metric | Live | Locked | Match? |",
        f"|---|---|---|---|",
        f"| n_total | {n:,} | 10,871 | {'✓' if n==10871 else '✗'} |",
        f"| n_malig | {nm:,} | 4,018 | {'✓' if nm==4018 else '✗ DIFF'} |",
        f"| n_smoke_current | {sc:,} | 212 | {'✓' if sc==212 else '✗ DIFF'} |",
        f"| n_smoke_known | {sk:,} | 3,022 | {'✓' if sk==3022 else '✗ DIFF'} |",
        f"| n_fhx_thyroid | {fhx_p:,} | 366 | {'✓' if fhx_p==366 else '✗ DIFF'} |",
        f"| n_fhx_known | {fhx_k:,} | 3,018 | {'✓' if fhx_k==3018 else '✗ DIFF'} |",
        f"",
    ]

    text = "\n".join(lines)
    with open(out_path, 'w') as f:
        f.write(text)
    print(f"[OK] {out_path}")
    return text


def main():
    print("Connecting to MotherDuck…")
    con = connect()
    print("Pulling numbers…")
    base, era = pull_numbers(con)

    date_str = datetime.date.today().strftime("%Y%m%d")
    out_path = os.path.join(OUT_DIR, f"M032_manuscript_numbers_{date_str}.md")
    render_md(base, era, out_path)

    # Also write JSON for downstream scripts
    json_path = os.path.join(OUT_DIR, f"M032_locked_numbers_{date_str}.json")
    payload = {k: (v if not pd.isna(v) else None) for k, v in base.items()}
    with open(json_path, 'w') as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"[OK] {json_path}")

    con.close()


if __name__ == "__main__":
    import json
    main()

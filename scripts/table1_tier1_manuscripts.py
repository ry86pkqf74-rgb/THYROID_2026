#!/usr/bin/env python3
"""
table1_tier1_manuscripts.py — Publication-style Table 1 for Tier-1 manuscripts (M025/M029/M032/M037/M047).

Connects to MotherDuck ``thyroid_canonical_publication_v1_0`` (token via motherduck_client.get_token),
joins each manuscript cohort view to ``main.canonical_patient_master``, and writes:
  studies/table1_outputs/{code}_table1.csv
  studies/table1_outputs/{code}_table1.tex

Run from repo root:
  .venv/bin/python scripts/table1_tier1_manuscripts.py
  .venv/bin/python scripts/table1_tier1_manuscripts.py --manuscript M032
  .venv/bin/python scripts/table1_tier1_manuscripts.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motherduck_client import get_token  # noqa: E402

OUT_DIR = ROOT / "studies" / "table1_outputs"
DEFAULT_DB = "thyroid_canonical_publication_v1_0"


def connect_publication_db(database: str = DEFAULT_DB) -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise RuntimeError(
            "No MotherDuck token (MD_SA_TOKEN / MOTHERDUCK_TOKEN / motherduck.local.toml)."
        )
    return duckdb.connect(f"md:{database}?motherduck_token={quote_plus(tok)}")


def _is_true_series(s: pd.Series) -> pd.Series:
    return s.map(lambda x: x is True or str(x).lower() in ("true", "t", "1", "yes"))


def _pct(n: int, d: int) -> float:
    return round(100.0 * n / d, 1) if d > 0 else 0.0


def summarize_continuous(s: pd.Series, n_total: int) -> dict:
    v = pd.to_numeric(s, errors="coerce")
    nmiss = int(v.isna().sum())
    v = v.dropna()
    if len(v) == 0:
        return {
            "display_mean_sd": "—",
            "display_median_iqr": "—",
            "n_nonmissing": 0,
            "missing_n": nmiss,
            "missing_pct": _pct(nmiss, n_total),
        }
    q25, med, q75 = v.quantile([0.25, 0.5, 0.75])
    return {
        "display_mean_sd": f"{v.mean():.1f} ± {v.std():.1f}",
        "display_median_iqr": f"{med:.1f} ({q25:.1f}–{q75:.1f})",
        "n_nonmissing": int(len(v)),
        "missing_n": nmiss,
        "missing_pct": _pct(nmiss, n_total),
    }


def summarize_categorical(s: pd.Series, n_total: int) -> list[dict]:
    rows = []
    vc = s.dropna().astype(str).value_counts()
    for val, ct in vc.items():
        rows.append(
            {
                "level": str(val),
                "n": int(ct),
                "pct": _pct(int(ct), n_total),
                "display": f"{int(ct)} ({_pct(int(ct), n_total)}%)",
            }
        )
    nmiss = int(s.isna().sum())
    rows.append(
        {
            "level": "Missing",
            "n": nmiss,
            "pct": _pct(nmiss, n_total),
            "display": f"{nmiss} ({_pct(nmiss, n_total)}%)",
        }
    )
    return rows


def summarize_boolean(s: pd.Series, n_total: int) -> dict:
    n_true = int(_is_true_series(s).sum())
    nmiss = int(s.isna().sum())
    return {
        "n_true": n_true,
        "pct_true": _pct(n_true, n_total),
        "display": f"{n_true} ({_pct(n_true, n_total)}%)",
        "missing_n": nmiss,
        "missing_pct": _pct(nmiss, n_total),
    }


def bethesda_label(v: object) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "Unknown"
    try:
        i = int(float(str(v)))
    except (TypeError, ValueError):
        return str(v)
    names = {1: "I", 2: "II", 3: "III", 4: "IV", 5: "V", 6: "VI"}
    return f"Bethesda {names.get(i, str(i))}"


def surgery_decade(y: object) -> str:
    if y is None or (isinstance(y, float) and np.isnan(y)):
        return "Unknown"
    try:
        yi = int(float(str(y)))
    except (TypeError, ValueError):
        return "Unknown"
    if yi < 1990:
        return "<1990"
    decade = (yi // 10) * 10
    return f"{decade}–{decade + 9}"


def tirads_label(row: pd.Series) -> str:
    # mig_260: SSOT = canonical_us_patient_master_VIEW_v2.max_tirads_category_ever
    # (exposed on cohort_m025 as max_tirads_category_ever and as tirads_worst_category_v12).
    for col in (
        "max_tirads_category_ever",
        "preop_tirads_category",
        "tirads_worst_category_v12",
        "tirads_best_category_v12",
    ):
        v = row.get(col)
        if v is not None and str(v).strip() and str(v).lower() not in ("nan", "none"):
            return str(v).strip()
    for col in ("tirads_worst_score_v12", "tirads_best_score_v12"):
        v = row.get(col)
        if v is not None and not (isinstance(v, float) and np.isnan(v)):
            try:
                return f"TR{int(float(v))}"
            except (TypeError, ValueError):
                continue
    return "Unknown"


@dataclass
class ManuscriptSpec:
    code: str
    cohort_view: str
    extra_select: str = ""
    extra_join: str = ""
    notes: str = ""


MANUSCRIPTS: dict[str, ManuscriptSpec] = {
    "M032": ManuscriptSpec(
        code="M032",
        cohort_view="manuscript_workspace.cohort_m032_descriptive_25yr_v1",
        notes="25-year descriptive; includes surgery decade distribution.",
    ),
    "M029": ManuscriptSpec(
        code="M029",
        cohort_view="manuscript_workspace.cohort_m029_fna_concordance_v1",
        notes="FNA concordance; stratify Bethesda + concordance.",
    ),
    "M025": ManuscriptSpec(
        code="M025",
        cohort_view="manuscript_workspace.cohort_m025_tirads_performance_v1",
        extra_select="""
        , co.max_tirads_category_ever
        , co.preop_tirads_category
        , co.tirads_best_category_v12
        , co.tirads_worst_category_v12
        , co.tirads_best_score_v12
        , co.tirads_worst_score_v12
        , co.dominant_nodule_size_cm AS cohort_dominant_nodule_size_cm
        , co.imaging_nodule_size_cm AS cohort_imaging_nodule_size_cm
        """,
        notes="TI-RADS performance; TI-RADS stratification + nodule size from cohort view.",
    ),
    "M037": ManuscriptSpec(
        code="M037",
        cohort_view="manuscript_workspace.cohort_m037_ln_metastasis_v1",
        notes="LN metastasis; LN+/− strata + LND resolution (when join present).",
    ),
    "M047": ManuscriptSpec(
        code="M047",
        cohort_view="manuscript_workspace.cohort_m047_frozen_section_v1",
        notes="Frozen section; stratify frozen_any_performed_flag.",
    ),
}


def base_cohort_sql(spec: ManuscriptSpec, *, lnd_column: str | None = None) -> str:
    """Join cohort to CPM; pull Table 1 analytic fields from CPM (+ manuscript extras)."""
    lnd_join = ""
    lnd_sel = ""
    if spec.code == "M037" and lnd_column:
        lnd_join = """
    LEFT JOIN manuscript_workspace.ln_dissection_lnd_resolved_v2 lnd
      ON CAST(pm.research_id AS VARCHAR) = CAST(lnd.research_id AS VARCHAR)
        """
        lnd_sel = f", lnd.{lnd_column} AS lnd_dissection_resolved"

    return f"""
    SELECT
      CAST(co.research_id AS VARCHAR) AS research_id
    , pm.age_at_surgery
    , pm.sex
    , pm.race
    , pm.bmi_combined
    , pm.surg_procedure_type
    , pm.is_malignant
    , pm.histology_final
    , pm.ajcc8_stage_group
    , COALESCE(pm.tumor_size_cm_dominant, pm.path_tumor_size_cm) AS tumor_size_cm_dominant
    , pm.ln_positive_final
    , COALESCE(CAST(pm.ete_grade_final AS VARCHAR), CAST(pm.ete_grade_final_v2 AS VARCHAR)) AS ete_grade_final
    , pm.any_recurrence_flag
    , pm.any_confirmed_complication_flag
    , pm.rai_received_reconciled
    , pm.molecular_tested_confirmed
    , pm.braf_positive_final
    , pm.surg_first_date
    , pm.frozen_any_performed_flag
    , pm.fna_path_concordance_category
    , pm.bethesda_final
    , pm.bethesda_final_name
    , pm.ln_rollup_total_examined
    , pm.ln_rollup_total_positive
    {spec.extra_select}
    {lnd_sel}
    FROM {spec.cohort_view} co
    INNER JOIN main.canonical_patient_master pm
      ON CAST(co.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    {lnd_join}
    {spec.extra_join}
    """


def _histology_ajcc_frame(df: pd.DataFrame) -> pd.DataFrame:
    if "is_malignant" not in df.columns:
        return df.iloc[0:0].copy()
    return df[_is_true_series(df["is_malignant"])].copy()


def _braf_frame(df: pd.DataFrame) -> pd.DataFrame:
    if "molecular_tested_confirmed" not in df.columns:
        return df.iloc[0:0].copy()
    return df[_is_true_series(df["molecular_tested_confirmed"])].copy()


def build_table1_records(
    df: pd.DataFrame,
    *,
    stratum: str = "Overall",
    manuscript_code: str = "",
    include_surgery_decade: bool = False,
    include_nodule_block: bool = False,
    lnd_dissection_col: str | None = None,
) -> list[dict]:
    """Long-format rows for CSV."""
    N = len(df)
    rows: list[dict] = [{"manuscript": manuscript_code, "stratum": stratum, "block": "Header", "variable": "N", "detail": "", "value": str(N)}]

    def add_row(block: str, variable: str, detail: str, value: str, extra: str = "") -> None:
        rows.append(
            {
                "manuscript": manuscript_code,
                "stratum": stratum,
                "block": block,
                "variable": variable,
                "detail": detail,
                "value": value,
                "extra": extra,
            }
        )

    # Continuous — age
    a = summarize_continuous(df["age_at_surgery"], N) if "age_at_surgery" in df.columns else None
    if a:
        add_row("Continuous", "Age at surgery (y)", "Mean ± SD", a["display_mean_sd"])
        add_row("Continuous", "Age at surgery (y)", "Median (IQR)", a["display_median_iqr"])
        add_row("Continuous", "Age at surgery (y)", "Non-missing / missing", f"n={a['n_nonmissing']}; missing {a['missing_n']} ({a['missing_pct']}%)")

    if include_surgery_decade and "surg_first_date" in df.columns:
        dt = pd.to_datetime(df["surg_first_date"], errors="coerce")
        dec = dt.dt.year.map(surgery_decade)
        add_row("Temporal", "Surgery decade", "", "")
        for r in summarize_categorical(dec, N):
            add_row("Temporal", "Surgery decade", r["level"], r["display"])

    # BMI
    if "bmi_combined" in df.columns:
        b = summarize_continuous(df["bmi_combined"], N)
        add_row("Continuous", "BMI", "Mean ± SD", b["display_mean_sd"])
        add_row("Continuous", "BMI", "Median (IQR)", b["display_median_iqr"])
        add_row("Continuous", "BMI", "Non-missing / missing", f"n={b['n_nonmissing']}; missing {b['missing_n']} ({b['missing_pct']}%)")

    for cat_col, title in (
        ("sex", "Sex"),
        ("race", "Race/ethnicity (as coded)"),
        ("surg_procedure_type", "Surgery procedure type"),
    ):
        if cat_col in df.columns:
            add_row("Categorical", title, "", "")
            for r in summarize_categorical(df[cat_col], N):
                add_row("Categorical", title, r["level"], r["display"])

    if "is_malignant" in df.columns:
        bo = summarize_boolean(df["is_malignant"], N)
        add_row("Binary", "Malignant", "", bo["display"])

    df_m = _histology_ajcc_frame(df)
    Nm = len(df_m)
    if Nm > 0 and "histology_final" in df_m.columns:
        add_row("Categorical", "Histology (malignant only)", "", f"N_malignant={Nm}")
        for r in summarize_categorical(df_m["histology_final"], Nm):
            add_row("Categorical", "Histology (malignant only)", r["level"], r["display"])

    if Nm > 0 and "ajcc8_stage_group" in df_m.columns:
        add_row("Categorical", "AJCC8 stage group (malignant only)", "", f"N_malignant={Nm}")
        for r in summarize_categorical(df_m["ajcc8_stage_group"].astype(str), Nm):
            add_row("Categorical", "AJCC8 stage group (malignant only)", r["level"], r["display"])

    if "tumor_size_cm_dominant" in df.columns:
        t = summarize_continuous(df["tumor_size_cm_dominant"], N)
        add_row("Continuous", "Tumor size, dominant (cm)", "Mean ± SD", t["display_mean_sd"])
        add_row("Continuous", "Tumor size, dominant (cm)", "Median (IQR)", t["display_median_iqr"])
        add_row(
            "Continuous",
            "Tumor size, dominant (cm)",
            "Non-missing / missing",
            f"n={t['n_nonmissing']}; missing {t['missing_n']} ({t['missing_pct']}%)",
        )

    if "ln_positive_final" in df.columns:
        ln = summarize_continuous(df["ln_positive_final"], N)
        add_row("Continuous", "LN positive count (final)", "Mean ± SD", ln["display_mean_sd"])
        add_row("Continuous", "LN positive count (final)", "Median (IQR)", ln["display_median_iqr"])

    if "ln_rollup_total_examined" in df.columns:
        ex = summarize_continuous(df["ln_rollup_total_examined"], N)
        add_row("Continuous", "LN rollup total examined", "Mean ± SD", ex["display_mean_sd"])
        add_row("Continuous", "LN rollup total examined", "Median (IQR)", ex["display_median_iqr"])

    if "ln_rollup_total_positive" in df.columns:
        tp = summarize_continuous(df["ln_rollup_total_positive"], N)
        add_row("Continuous", "LN rollup total positive", "Mean ± SD", tp["display_mean_sd"])
        add_row("Continuous", "LN rollup total positive", "Median (IQR)", tp["display_median_iqr"])

    if lnd_dissection_col and lnd_dissection_col in df.columns:
        add_row("Categorical", "LND dissection (resolved v2)", "", "")
        for r in summarize_categorical(df[lnd_dissection_col], N):
            add_row("Categorical", "LND dissection (resolved v2)", r["level"], r["display"])

    if "ete_grade_final" in df.columns:
        add_row("Categorical", "ETE grade (final)", "", "")
        for r in summarize_categorical(df["ete_grade_final"].astype(str), N):
            add_row("Categorical", "ETE grade (final)", r["level"], r["display"])

    for col, label in (
        ("any_recurrence_flag", "Any recurrence"),
        ("any_confirmed_complication_flag", "Any confirmed complication"),
        ("rai_received_reconciled", "RAI received (reconciled)"),
        ("molecular_tested_confirmed", "Molecular tested (confirmed)"),
    ):
        if col in df.columns:
            bo = summarize_boolean(df[col], N)
            add_row("Binary", label, "", bo["display"])

    df_t = _braf_frame(df)
    Nt = len(df_t)
    if Nt > 0 and "braf_positive_final" in df_t.columns:
        bo = summarize_boolean(df_t["braf_positive_final"], Nt)
        add_row("Binary", "BRAF positive (among molecular-tested)", "", f"{bo['display']} [N_tested={Nt}]")

    if include_nodule_block:
        if "cohort_dominant_nodule_size_cm" in df.columns:
            u = summarize_continuous(df["cohort_dominant_nodule_size_cm"], N)
            add_row("Nodule", "Dominant nodule size (cm), cohort view", "Mean ± SD", u["display_mean_sd"])
            add_row("Nodule", "Dominant nodule size (cm), cohort view", "Median (IQR)", u["display_median_iqr"])
        if "cohort_imaging_nodule_size_cm" in df.columns:
            u2 = summarize_continuous(df["cohort_imaging_nodule_size_cm"], N)
            add_row("Nodule", "Imaging nodule size (cm), cohort view", "Mean ± SD", u2["display_mean_sd"])
            add_row("Nodule", "Imaging nodule size (cm), cohort view", "Median (IQR)", u2["display_median_iqr"])

    return rows


def discover_lnd_dissection_column(con: duckdb.DuckDBPyConnection) -> str | None:
    try:
        d = con.execute(
            "DESCRIBE manuscript_workspace.ln_dissection_lnd_resolved_v2"
        ).fetchdf()
        colkey = "column_name" if "column_name" in d.columns else str(d.columns[0])
        raw = [c for c in d[colkey].astype(str).tolist() if "research_id" not in c.lower()]
        boring_prefixes = ("has_", "is_", "n_", "num_", "count_")
        rich = [n for n in raw if not any(n.lower().startswith(p) for p in boring_prefixes)]
        names = rich if rich else raw
    except Exception:
        return None
    priority = (
        "lnd_type_final",
        "dissection_side_final",
        "lateral_side_v10",
        "confidence_tier",
        "lnd_dissection_type",
        "lnd_type",
        "lnd_procedure_label",
        "lateral_neck_dissection_type",
        "dissection_type",
        "ln_dissection_class",
    )
    for p in priority:
        if p in names:
            return p
    for n in names:
        if any(k in n.lower() for k in ("dissection", "mrnd", "selective", "lnd", "neck")):
            return n
    return names[0] if names else None


def latex_escape(s: str) -> str:
    return (
        str(s)
        .replace("\\", "\\textbackslash{}")
        .replace("&", "\\&")
        .replace("%", "\\%")
        .replace("_", "\\_")
        .replace("#", "\\#")
    )


def records_to_latex(records: list[dict], caption: str, label: str) -> str:
    lines = [
        r"% Requires \usepackage{booktabs}",
        r"\begin{table}[htbp]",
        r"\centering",
        f"\\caption{{{latex_escape(caption)}}}",
        f"\\label{{{latex_escape(label)}}}",
        r"\begin{tabular}{lll}",
        r"\toprule",
        r"Block & Variable & Value \\",
        r"\midrule",
    ]
    for rec in records:
        if rec.get("variable") == "N" and rec.get("block") == "Header":
            lines.append(f"\\multicolumn{{3}}{{l}}{{\\textbf{{N = {latex_escape(rec['value'])}}}}} \\\\")
            continue
        b = latex_escape(str(rec.get("block", "")))
        v = latex_escape(str(rec.get("variable", "")))
        det = str(rec.get("detail", "")).strip()
        if det:
            v = f"{v} — {latex_escape(det)}"
        val = latex_escape(str(rec.get("value", "")))
        lines.append(f"{b} & {v} & {val} \\\\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}", ""])
    return "\n".join(lines)


def run_manuscript(
    con: duckdb.DuckDBPyConnection,
    code: str,
    *,
    lnd_col: str | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    spec = MANUSCRIPTS[code]
    lnd_for_sql = lnd_col if code == "M037" else None
    sql = base_cohort_sql(spec, lnd_column=lnd_for_sql)
    df = con.execute(sql).fetchdf()

    all_records: list[dict] = []

    if code == "M032":
        all_records.extend(
            build_table1_records(
                df,
                stratum="Overall",
                manuscript_code=code,
                include_surgery_decade=True,
            )
        )
    elif code == "M029":
        all_records.extend(
            build_table1_records(df, stratum="Overall", manuscript_code=code)
        )
        if "bethesda_final" in df.columns:
            for b, sub in df.groupby(df["bethesda_final"].map(bethesda_label)):
                all_records.extend(
                    build_table1_records(
                        sub,
                        stratum=f"Bethesda stratum: {b}",
                        manuscript_code=code,
                    )
                )
        if "fna_path_concordance_category" in df.columns:
            for ccat, sub in df.groupby(
                df["fna_path_concordance_category"].fillna("Unknown").astype(str)
            ):
                all_records.extend(
                    build_table1_records(
                        sub,
                        stratum=f"Concordance: {ccat}",
                        manuscript_code=code,
                    )
                )
    elif code == "M025":
        df = df.copy()
        df["_tirads_stratum"] = df.apply(tirads_label, axis=1)
        all_records.extend(
            build_table1_records(
                df,
                stratum="Overall",
                manuscript_code=code,
                include_nodule_block=True,
            )
        )
        for lab, sub in df.groupby("_tirads_stratum"):
            all_records.extend(
                build_table1_records(
                    sub,
                    stratum=f"TI-RADS: {lab}",
                    manuscript_code=code,
                    include_nodule_block=True,
                )
            )
    elif code == "M037":
        df = df.copy()
        df["_ln_stratum"] = np.where(
            pd.to_numeric(df.get("ln_positive_final"), errors="coerce").fillna(0) > 0,
            "LN positive",
            "LN negative or zero",
        )
        lnd_summary_col = "lnd_dissection_resolved" if "lnd_dissection_resolved" in df.columns else None

        all_records.extend(
            build_table1_records(
                df,
                stratum="Overall",
                manuscript_code=code,
                lnd_dissection_col=lnd_summary_col,
            )
        )
        for lab, sub in df.groupby("_ln_stratum"):
            all_records.extend(
                build_table1_records(
                    sub,
                    stratum=lab,
                    manuscript_code=code,
                    lnd_dissection_col=lnd_summary_col,
                )
            )
    elif code == "M047":
        df = df.copy()
        if "frozen_any_performed_flag" not in df.columns:
            df["_fz"] = "Column frozen_any_performed_flag missing"
        else:
            def _fz_bucket(x: object) -> str:
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return "Missing"
                if x is True or str(x).lower() in ("true", "t", "1", "yes"):
                    return "Frozen performed"
                if x is False or str(x).lower() in ("false", "f", "0", "no"):
                    return "Frozen not performed / unknown"
                return "Missing"

            df["_fz"] = df["frozen_any_performed_flag"].map(_fz_bucket)
        all_records.extend(build_table1_records(df, stratum="Overall", manuscript_code=code))
        for lab, sub in df.groupby("_fz"):
            all_records.extend(
                build_table1_records(sub, stratum=lab, manuscript_code=code)
            )
    else:
        all_records.extend(build_table1_records(df, stratum="Overall", manuscript_code=code))

    return df, all_records


def main() -> int:
    ap = argparse.ArgumentParser(description="Tier-1 manuscript Table 1 generator (MotherDuck publication DB).")
    ap.add_argument(
        "--manuscript",
        action="append",
        choices=sorted(MANUSCRIPTS.keys()),
        help="Repeat to restrict; default = all five.",
    )
    ap.add_argument("--database", default=os.environ.get("MOTHERDUCK_DATABASE", DEFAULT_DB))
    ap.add_argument("--dry-run", action="store_true", help="Print SQL + exit (no MotherDuck call).")
    args = ap.parse_args()
    codes = sorted(set(args.manuscript or list(MANUSCRIPTS.keys())))

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        for c in codes:
            lnd = None
            if c == "M037":
                lnd = "lnd_column_placeholder"
            print(f"=== {c} ===\n{base_cohort_sql(MANUSCRIPTS[c], lnd_column=lnd)}\n")
        return 0

    con = connect_publication_db(database=args.database)
    lnd_col = discover_lnd_dissection_column(con)
    if lnd_col:
        print(f"[table1] LND dissection column: {lnd_col}")
    else:
        print("[table1] LND table not found or no discernible column — M037 LND rows skipped.")

    for code in codes:
        _, records = run_manuscript(con, code, lnd_col=lnd_col)
        tdf = pd.DataFrame(records)
        csv_path = OUT_DIR / f"{code}_table1.csv"
        tdf.to_csv(csv_path, index=False)
        tex = records_to_latex(
            records,
            caption=f"Table 1 — cohort characteristics ({code})",
            label=f"tab:table1_{code.lower()}",
        )
        tex_path = OUT_DIR / f"{code}_table1.tex"
        tex_path.write_text(tex, encoding="utf-8")
        n_hdr = next(
            (
                int(r["value"])
                for r in records
                if r.get("block") == "Header" and r.get("variable") == "N" and r.get("stratum") == "Overall"
            ),
            0,
        )
        print(f"[table1] {code}: N={n_hdr}  -> {csv_path.name}, {tex_path.name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

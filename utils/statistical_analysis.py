"""
Statistical analysis toolkit for the THYROID_2026 research platform.

Provides publication-ready Table 1 generation, hypothesis testing with
multiple comparison correction, logistic/Cox regression with diagnostics,
forest plots, and correlation analysis — all tailored to the thyroid
cancer cohort schema.

Usage (standalone)::

    from utils.statistical_analysis import ThyroidStatisticalAnalyzer
    analyzer = ThyroidStatisticalAnalyzer(con)
    t1, meta = analyzer.generate_table_one("risk_enriched_mv", groupby_col="sex")

Usage (dashboard)::

    Consumed by ``app/statistical_analysis.py`` which wires interactive
    controls around these methods.
"""
from __future__ import annotations

import logging
import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd

try:
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

try:
    from tableone import TableOne
    HAS_TABLEONE = True
except ImportError:
    HAS_TABLEONE = False

try:
    import statsmodels.api as sm
    from statsmodels.stats.multitest import multipletests
    from statsmodels.stats.outliers_influence import variance_inflation_factor
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False

try:
    from scipy import stats as sp_stats
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

try:
    from lifelines import CoxPHFitter  # noqa: F401
    HAS_LIFELINES = True
except ImportError:
    HAS_LIFELINES = False

try:
    import pingouin as pg
    HAS_PINGOUIN = True
except ImportError:
    HAS_PINGOUIN = False


log = logging.getLogger(__name__)

# ── Thyroid-specific presets ──────────────────────────────────────────────

THYROID_TABLE1_PRESET: dict[str, list[str]] = {
    "continuous": [
        "age_at_surgery", "largest_tumor_cm",
        "ln_positive", "ln_examined", "ln_ratio",
    ],
    "categorical": [
        "sex", "histology_1_type", "overall_stage_ajcc8",
        "braf_positive", "tumor_1_extrathyroidal_ext",
        "recurrence_risk_band", "variant_standardized",
    ],
}

THYROID_OUTCOMES: list[str] = [
    "event_occurred", "recurrence_flag", "braf_positive",
    "structural_recurrence", "rai_need", "any_nsqip_complication",
]

THYROID_PREDICTORS: list[str] = [
    "age_at_surgery", "sex", "largest_tumor_cm", "ln_positive",
    "ln_ratio", "braf_positive", "tumor_1_gross_ete",
    "tumor_1_ete_microscopic_only", "overall_stage_ajcc8",
    "tert_positive", "variant_standardized",
]

THYROID_SURVIVAL: dict[str, str] = {
    "time_col": "time_to_event_days",
    "event_col": "event_occurred",
}

# ── NSQIP / Complications presets ─────────────────────────────────────────

THYROID_NSQIP_OUTCOMES: list[str] = [
    "any_nsqip_complication",
    "rln_injury",
    "hypocalcemia",
    "hypoparathyroidism",
    "seroma",
    "hematoma",
]

THYROID_NSQIP_PREDICTORS: list[str] = [
    "age_at_surgery",
    "sex",
    "braf_mutation_mentioned",
    "largest_tumor_cm",
    "ln_positive",
    "tumor_1_extrathyroidal_ext",
    "overall_stage_ajcc8",
    "malignant_surgery_type",
    "has_parathyroid",
]

# Raw complication column names from the `complications` table
NSQIP_COMPLICATION_COLUMNS: list[str] = [
    "rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy",
    "seroma",
    "hematoma",
    "hypocalcemia",
    "hypoparathyroidism",
]

# ── ETE subtype vocabulary ────────────────────────────────────────────────

ETE_SUBTYPES: dict[str, str] = {
    "none": "none",
    "microscopic": "yes, minimal",
    "gross": "yes, extensive",
    "present": "present",
}

# ── Longitudinal marker configuration ────────────────────────────────────

LONGITUDINAL_MARKERS: dict[str, dict] = {
    "tg": {
        "label": "Thyroglobulin (Tg)",
        "lab_type_filter": "thyroglobulin",
        "event_subtype_filter": "thyroglobulin",
        "units": "ng/mL",
        "log_transform": True,
    },
    "tsh": {
        "label": "TSH",
        "lab_type_filter": "tsh",
        "event_subtype_filter": "tsh",
        "units": "mIU/L",
        "log_transform": True,
    },
    "anti_tg": {
        "label": "Anti-Thyroglobulin (Anti-Tg)",
        "lab_type_filter": "anti_thyroglobulin",
        "event_subtype_filter": "anti-thyroglobulin",
        "units": "IU/mL",
        "log_transform": False,
    },
}

# ── Variable-specific clinical context for snippet generation ─────────────

_CLINICAL_CONTEXT: dict[str, str] = {
    "braf_positive": "BRAF V600E is common in aggressive PTC subtypes and associated with RAI refractoriness",
    "braf_mutation_mentioned": "BRAF V600E is associated with higher recurrence risk and RAI refractoriness",
    "tert_positive": "TERT promoter mutation marks dedifferentiation risk and aggressive clinical behavior",
    "tert_mutation_mentioned": "TERT promoter mutations co-occurring with BRAF markedly increase recurrence risk",
    "tumor_1_gross_ete": "Gross ETE upstages disease to AJCC T3b/T4 and confers worse prognosis",
    "tumor_1_ete_microscopic_only": "Microscopic ETE does not upstage per AJCC 8th Ed but warrants surveillance",
    "ln_positive": "LN involvement is a key driver of structural recurrence risk",
    "ln_ratio": "LN ratio >0.3 predicts regional recurrence independent of absolute count",
    "overall_stage_ajcc8": "AJCC 8th Ed stage dictates surveillance intensity and RAI eligibility",
    "age_at_surgery": "Age ≥55 is a staging threshold in AJCC 8th Ed with prognostic implications",
    "largest_tumor_cm": "Tumor size determines AJCC T stage cutoffs at 1, 2, and 4 cm",
    "ln_examined": "Higher LN examination count improves staging accuracy",
    "variant_standardized": "Histologic variant influences recurrence risk beyond classic PTC",
    "any_nsqip_complication": "Surgical complications directly impact post-operative quality of life",
    "hypocalcemia": "Hypocalcemia reflects parathyroid injury and requires vitamin D/calcium supplementation",
    "rln_injury": "RLN injury risk increases with central neck dissection and reoperation",
    "sex": "Female sex is the demographic norm in DTC but males may carry higher risk",
    "recurrence_risk_band": "ATA recurrence risk stratification guides RAI and surveillance decisions",
}

# Data source priority for view resolution
_VIEW_PRIORITY = [
    "risk_enriched_mv",
    "advanced_features_v3",
    "ptc_cohort",
]

# ── Plot layout (matches dashboard theme) ─────────────────────────────────

PL = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(14,18,25,0.8)",
    font=dict(family="DM Sans", color="#8892a4", size=12),
    title_font=dict(family="DM Serif Display", color="#f0f4ff", size=15),
    xaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", zerolinecolor="#1e2535"),
    yaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", zerolinecolor="#1e2535"),
    legend=dict(bgcolor="rgba(14,18,25,0.8)", bordercolor="#1e2535", borderwidth=1),
    margin=dict(l=16, r=16, t=36, b=16),
    colorway=["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#34d399", "#fb923c"],
    hoverlabel=dict(bgcolor="#141923", bordercolor="#1e2535", font_color="#f0f4ff"),
)

_TEAL = "#2dd4bf"
_GRAY = "#4a5568"
_ROSE = "#f43f5e"

# ── Interpretation helpers ────────────────────────────────────────────────

_EFFECT_SIZE_LABELS = {
    "d": [(0.2, "negligible"), (0.5, "small"), (0.8, "medium"), (float("inf"), "large")],
    "V": [(0.1, "negligible"), (0.3, "small"), (0.5, "medium"), (float("inf"), "large")],
    "r": [(0.1, "negligible"), (0.3, "small"), (0.5, "medium"), (float("inf"), "large")],
}


def _effect_label(value: float, kind: str = "d") -> str:
    for threshold, label in _EFFECT_SIZE_LABELS.get(kind, _EFFECT_SIZE_LABELS["d"]):
        if abs(value) < threshold:
            return label
    return "large"


def _interpret_hr(hr: float, label: str) -> str:
    if hr > 1:
        pct = (hr - 1) * 100
        return f"{label}: HR={hr:.2f} indicates {pct:.0f}% increased hazard"
    elif hr < 1:
        pct = (1 - hr) * 100
        return f"{label}: HR={hr:.2f} indicates {pct:.0f}% reduced hazard"
    return f"{label}: HR={hr:.2f} indicates no effect"


def _interpret_or(or_val: float, label: str) -> str:
    if or_val > 1:
        pct = (or_val - 1) * 100
        return f"{label}: OR={or_val:.2f} indicates {pct:.0f}% increased odds"
    elif or_val < 1:
        pct = (1 - or_val) * 100
        return f"{label}: OR={or_val:.2f} indicates {pct:.0f}% reduced odds"
    return f"{label}: OR={or_val:.2f} indicates no association"


# ── Core class ────────────────────────────────────────────────────────────

class ThyroidStatisticalAnalyzer:
    """Publication-ready statistical analysis engine for thyroid cancer cohorts.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB / MotherDuck connection (read-only sufficient).
    """

    def __init__(self, con: Any) -> None:
        self._con = con

    # ── Data loading ──────────────────────────────────────────────────────

    def _load_view(
        self,
        view: str,
        cols: str = "*",
        where: str | None = None,
    ) -> pd.DataFrame:
        """Load a view/table into a DataFrame."""
        clause = f" WHERE {where}" if where else ""
        sql = f"SELECT {cols} FROM {view}{clause}"
        try:
            return self._con.execute(sql).fetchdf()
        except Exception as exc:
            log.warning("Query on %s failed: %s", view, exc)
            return pd.DataFrame()

    def resolve_view(self, preferred: str | None = None) -> str | None:
        """Return the first available view from the priority list."""
        candidates = [preferred] if preferred else []
        candidates.extend(_VIEW_PRIORITY)
        for v in candidates:
            if v is None:
                continue
            try:
                row = self._con.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_name='{v}'"
                ).fetchone()
                if row and row[0] > 0:
                    return v
            except Exception:
                continue
        return None

    # ── Type detection ────────────────────────────────────────────────────

    @staticmethod
    def _auto_detect_types(
        df: pd.DataFrame,
        max_categorical_unique: int = 20,
    ) -> tuple[list[str], list[str]]:
        """Classify columns as continuous or categorical.

        Returns (continuous_vars, categorical_vars).
        """
        continuous: list[str] = []
        categorical: list[str] = []
        for col in df.columns:
            if col in ("research_id",):
                continue
            nuniq = df[col].nunique(dropna=True)
            if nuniq == 0:
                continue
            if pd.api.types.is_numeric_dtype(df[col]) and nuniq > max_categorical_unique:
                continuous.append(col)
            elif pd.api.types.is_bool_dtype(df[col]) or nuniq <= max_categorical_unique:
                categorical.append(col)
            elif pd.api.types.is_numeric_dtype(df[col]):
                continuous.append(col)
        return continuous, categorical

    # ── Missing data ──────────────────────────────────────────────────────

    @staticmethod
    def missing_data_summary(data: pd.DataFrame) -> pd.DataFrame:
        """Per-column missing count, percentage, and dtype."""
        n = len(data)
        rows = []
        for col in data.columns:
            n_miss = int(data[col].isna().sum())
            rows.append({
                "column": col,
                "n_total": n,
                "n_missing": n_miss,
                "pct_missing": round(100.0 * n_miss / n, 2) if n else 0.0,
                "dtype": str(data[col].dtype),
            })
        return pd.DataFrame(rows).sort_values("pct_missing", ascending=False)

    # ── Assumption checks ─────────────────────────────────────────────────

    @staticmethod
    def _check_assumptions(
        data: pd.DataFrame,
        variables: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Normality (Shapiro-Wilk) and homoscedasticity (Levene) tests."""
        if not HAS_SCIPY:
            return {}
        results: dict[str, dict[str, Any]] = {}
        for var in variables:
            col = data[var].dropna()
            if not pd.api.types.is_numeric_dtype(col) or len(col) < 8:
                continue
            sample = col.sample(min(len(col), 5000), random_state=42)
            stat, p = sp_stats.shapiro(sample)
            results[var] = {
                "shapiro_stat": round(float(stat), 4),
                "shapiro_p": round(float(p), 6),
                "is_normal": p > 0.05,
                "n": len(col),
            }
        return results

    # ── Table 1 ───────────────────────────────────────────────────────────

    def generate_table_one(
        self,
        view: str | None = None,
        data: pd.DataFrame | None = None,
        groupby_col: str | None = None,
        continuous_vars: list[str] | None = None,
        categorical_vars: list[str] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Generate a publication-ready Table 1 with automatic test selection.

        Parameters
        ----------
        view : str, optional
            Source view/table name. Ignored if *data* is provided.
        data : DataFrame, optional
            Pre-loaded cohort. Takes precedence over *view*.
        groupby_col : str, optional
            Stratification column (e.g. ``"sex"``, ``"braf_positive"``).
        continuous_vars, categorical_vars : list[str], optional
            Override auto-detection.  Falls back to ``THYROID_TABLE1_PRESET``
            filtered to columns present in the data.

        Returns
        -------
        (table1_df, metadata)
            ``table1_df`` contains the formatted table;
            ``metadata`` has N, missing %, test information.
        """
        if data is None:
            resolved = self.resolve_view(view)
            if resolved is None:
                return pd.DataFrame(), {"error": "No source view available"}
            data = self._load_view(resolved)
        if data.empty:
            return pd.DataFrame(), {"error": "Empty dataset"}

        available = set(data.columns)

        if continuous_vars is None:
            continuous_vars = [c for c in THYROID_TABLE1_PRESET["continuous"] if c in available]
        else:
            continuous_vars = [c for c in continuous_vars if c in available]

        if categorical_vars is None:
            categorical_vars = [c for c in THYROID_TABLE1_PRESET["categorical"] if c in available]
        else:
            categorical_vars = [c for c in categorical_vars if c in available]

        all_vars = continuous_vars + categorical_vars
        if not all_vars:
            auto_c, auto_cat = self._auto_detect_types(data)
            continuous_vars = auto_c[:10]
            categorical_vars = auto_cat[:10]
            all_vars = continuous_vars + categorical_vars

        if not all_vars:
            return pd.DataFrame(), {"error": "No usable variables found"}

        metadata: dict[str, Any] = {
            "n_total": len(data),
            "continuous_vars": continuous_vars,
            "categorical_vars": categorical_vars,
            "groupby": groupby_col,
        }

        if HAS_TABLEONE:
            nonnormal = self._detect_nonnormal(data, continuous_vars)
            t1 = TableOne(
                data,
                columns=all_vars,
                categorical=categorical_vars,
                groupby=groupby_col,
                nonnormal=nonnormal,
                pval=groupby_col is not None,
                smd=groupby_col is not None,  # SMD when stratifying (balance assessment)
                htest_name=True,
                missing=True,
            )
            t1_df = t1.tableone.copy()
            metadata["tableone_object"] = t1
            metadata["nonnormal"] = nonnormal
            metadata["smd_computed"] = groupby_col is not None
            return t1_df, metadata

        return self._manual_table_one(data, continuous_vars, categorical_vars, groupby_col, metadata)

    @staticmethod
    def _detect_nonnormal(df: pd.DataFrame, continuous_vars: list[str]) -> list[str]:
        """Identify continuous vars that fail Shapiro-Wilk at alpha=0.05."""
        if not HAS_SCIPY:
            return []
        nonnormal: list[str] = []
        for var in continuous_vars:
            col = df[var].dropna()
            if len(col) < 8:
                continue
            sample = col.sample(min(len(col), 5000), random_state=42)
            try:
                _, p = sp_stats.shapiro(sample)
                if p < 0.05:
                    nonnormal.append(var)
            except Exception:
                nonnormal.append(var)
        return nonnormal

    def _manual_table_one(
        self,
        data: pd.DataFrame,
        continuous_vars: list[str],
        categorical_vars: list[str],
        groupby_col: str | None,
        metadata: dict[str, Any],
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Fallback Table 1 when tableone is not installed."""
        rows: list[dict[str, Any]] = []
        total = len(data)
        rows.append({"Variable": "n", "Overall": str(total)})

        for var in continuous_vars:
            col = data[var].dropna()
            if col.empty:
                continue
            med = col.median()
            q1, q3 = col.quantile(0.25), col.quantile(0.75)
            n_miss = int(data[var].isna().sum())
            rows.append({
                "Variable": var,
                "Overall": f"{med:.1f} [{q1:.1f}, {q3:.1f}]",
                "Missing": f"{n_miss} ({100*n_miss/total:.1f}%)" if total else "",
            })

        for var in categorical_vars:
            col = data[var].dropna()
            if col.empty:
                continue
            n_miss = int(data[var].isna().sum())
            for cat in sorted(col.unique(), key=str):
                n = int((col == cat).sum())
                pct = 100 * n / total if total else 0
                rows.append({
                    "Variable": f"{var} = {cat}",
                    "Overall": f"{n} ({pct:.1f}%)",
                    "Missing": f"{n_miss} ({100*n_miss/total:.1f}%)" if total else "",
                })

        return pd.DataFrame(rows), metadata

    # ── Hypothesis testing ────────────────────────────────────────────────

    def run_hypothesis_tests(
        self,
        data: pd.DataFrame,
        target_var: str,
        feature_list: list[str],
        correction: Literal["bonferroni", "fdr_bh", "holm"] | None = None,
    ) -> pd.DataFrame:
        """Run automatic hypothesis tests for each feature against a target.

        Auto-selects the appropriate test based on:
        - Variable type (continuous vs categorical)
        - Number of groups (2 vs 3+)
        - Normality (Shapiro-Wilk)
        - Expected cell counts (Fisher exact for sparse tables)

        Parameters
        ----------
        data : DataFrame
            Cohort data.
        target_var : str
            Grouping variable (binary or multi-level categorical).
        feature_list : list[str]
            Variables to test against *target_var*.
        correction : str, optional
            Multiple comparison correction method.

        Returns
        -------
        DataFrame with columns: variable, test_used, statistic, p_value,
        p_adjusted, significant, effect_size, effect_label, interpretation.
        """
        if not HAS_SCIPY:
            return pd.DataFrame({"error": ["scipy required for hypothesis testing"]})

        if target_var not in data.columns:
            return pd.DataFrame({"error": [f"Target variable '{target_var}' not in data"]})

        groups = data[target_var].dropna().unique()
        n_groups = len(groups)
        if n_groups < 2:
            return pd.DataFrame({"error": [f"Target '{target_var}' has fewer than 2 groups"]})

        results: list[dict[str, Any]] = []

        for feat in feature_list:
            if feat not in data.columns or feat == target_var:
                continue
            result = self._test_single_feature(data, target_var, feat, groups, n_groups)
            if result:
                results.append(result)

        if not results:
            return pd.DataFrame()

        df_out = pd.DataFrame(results)

        if correction and HAS_STATSMODELS and len(df_out) > 1:
            raw_p = df_out["p_value"].values
            valid = ~np.isnan(raw_p)
            if valid.sum() > 1:
                _, p_adj, _, _ = multipletests(raw_p[valid], method=correction)
                df_out.loc[valid, "p_adjusted"] = p_adj
                df_out.loc[valid, "significant"] = p_adj < 0.05
            df_out["correction"] = correction
        else:
            df_out["p_adjusted"] = df_out["p_value"]
            df_out["correction"] = "none"

        return df_out.sort_values("p_value")

    def _test_single_feature(
        self,
        data: pd.DataFrame,
        target_var: str,
        feat: str,
        groups: np.ndarray,
        n_groups: int,
    ) -> dict[str, Any] | None:
        """Select and run the appropriate test for one feature."""
        is_numeric = pd.api.types.is_numeric_dtype(data[feat])
        nuniq = data[feat].nunique(dropna=True)
        is_categorical = (not is_numeric) or nuniq <= 10

        if is_categorical:
            return self._test_categorical(data, target_var, feat)
        return self._test_continuous(data, target_var, feat, groups, n_groups)

    @staticmethod
    def _test_continuous(
        data: pd.DataFrame,
        target_var: str,
        feat: str,
        groups: np.ndarray,
        n_groups: int,
    ) -> dict[str, Any] | None:
        group_data = []
        for g in groups:
            vals = data.loc[data[target_var] == g, feat].dropna()
            if len(vals) < 3:
                return None
            group_data.append(vals)

        is_normal = True
        for vals in group_data:
            sample = vals.sample(min(len(vals), 5000), random_state=42)
            try:
                _, p = sp_stats.shapiro(sample)
                if p < 0.05:
                    is_normal = False
                    break
            except Exception:
                is_normal = False
                break

        if n_groups == 2:
            if is_normal:
                stat, p = sp_stats.ttest_ind(group_data[0], group_data[1], equal_var=False)
                test_name = "Welch t-test"
            else:
                stat, p = sp_stats.mannwhitneyu(
                    group_data[0], group_data[1], alternative="two-sided"
                )
                test_name = "Mann-Whitney U"

            pooled_std = np.sqrt(
                (group_data[0].var() * (len(group_data[0]) - 1)
                 + group_data[1].var() * (len(group_data[1]) - 1))
                / (len(group_data[0]) + len(group_data[1]) - 2)
            )
            d = abs(group_data[0].mean() - group_data[1].mean()) / pooled_std if pooled_std > 0 else 0.0
            es_label = _effect_label(d, "d")

        else:
            if is_normal:
                stat, p = sp_stats.f_oneway(*group_data)
                test_name = "ANOVA"
            else:
                stat, p = sp_stats.kruskal(*group_data)
                test_name = "Kruskal-Wallis"
            n_total = sum(len(g) for g in group_data)
            d = np.sqrt(stat / (n_total - 1)) if n_total > 1 else 0.0
            es_label = _effect_label(d, "r")

        return {
            "variable": feat,
            "test_used": test_name,
            "statistic": round(float(stat), 4),
            "p_value": round(float(p), 6),
            "p_adjusted": np.nan,
            "significant": p < 0.05,
            "effect_size": round(float(d), 4),
            "effect_label": es_label,
            "interpretation": f"{test_name}; p={p:.4f}; {es_label} effect (d={d:.3f})",
        }

    @staticmethod
    def _test_categorical(
        data: pd.DataFrame,
        target_var: str,
        feat: str,
    ) -> dict[str, Any] | None:
        ct = pd.crosstab(data[feat].fillna("_missing"), data[target_var].fillna("_missing"))
        if ct.shape[0] < 2 or ct.shape[1] < 2:
            return None

        expected = sp_stats.contingency.expected_freq(ct.values)
        use_fisher = (expected < 5).any() and ct.shape == (2, 2)

        if use_fisher:
            stat, p = sp_stats.fisher_exact(ct.values)
            test_name = "Fisher exact"
        else:
            stat, p, _, _ = sp_stats.chi2_contingency(ct.values)
            test_name = "Chi-square"

        n = ct.values.sum()
        k = min(ct.shape) - 1
        v = np.sqrt(stat / (n * k)) if (n * k) > 0 and not use_fisher else 0.0
        es_label = _effect_label(v, "V")

        return {
            "variable": feat,
            "test_used": test_name,
            "statistic": round(float(stat), 4),
            "p_value": round(float(p), 6),
            "p_adjusted": np.nan,
            "significant": p < 0.05,
            "effect_size": round(float(v), 4),
            "effect_label": es_label,
            "interpretation": f"{test_name}; p={p:.4f}; {es_label} effect (V={v:.3f})",
        }

    # ── Logistic regression ───────────────────────────────────────────────

    def fit_logistic_regression(
        self,
        outcome: str,
        predictors: list[str],
        confounders: list[str] | None = None,
        data: pd.DataFrame | None = None,
        view: str | None = None,
    ) -> dict[str, Any]:
        """Fit a logistic regression model and return OR table + diagnostics.

        Returns dict with keys: or_table, summary_text, vif, n_obs,
        pseudo_r2, auc, warnings.
        """
        if not HAS_STATSMODELS:
            return {"error": "statsmodels required for logistic regression"}

        if data is None:
            resolved = self.resolve_view(view)
            if resolved is None:
                return {"error": "No source view available"}
            data = self._load_view(resolved)

        all_vars = [outcome] + predictors + (confounders or [])
        all_vars = list(dict.fromkeys(all_vars))
        present = [v for v in all_vars if v in data.columns]
        if outcome not in present:
            return {"error": f"Outcome '{outcome}' not in data"}

        sub = data[present].dropna().copy()
        sub[outcome] = pd.to_numeric(sub[outcome], errors="coerce")
        for c in present:
            if c != outcome:
                sub[c] = pd.to_numeric(sub[c], errors="coerce")
        sub = sub.dropna()

        if len(sub) < 20:
            return {"error": f"Only {len(sub)} complete cases — too few for regression (need >= 20)"}

        model_vars = [p for p in predictors if p in sub.columns]
        if confounders:
            model_vars.extend([c for c in confounders if c in sub.columns and c not in model_vars])

        if not model_vars:
            return {"error": "No valid predictor columns after cleaning"}

        X = sm.add_constant(sub[model_vars].astype(float))
        y = sub[outcome].astype(int)

        result_warnings: list[str] = []

        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                model = sm.Logit(y, X).fit(disp=0, maxiter=200)
                for w in caught:
                    result_warnings.append(str(w.message))
        except Exception as exc:
            return {"error": f"Model fitting failed: {exc}"}

        if np.any(np.isinf(model.params.values)):
            result_warnings.append(
                "Perfect or quasi-complete separation detected — "
                "some coefficients are infinite. Consider removing "
                "collinear or perfectly predictive variables."
            )

        coefs = model.params.values
        ci = model.conf_int()
        or_table = pd.DataFrame({
            "predictor": model.params.index,
            "coef": np.round(coefs, 4),
            "OR": np.round(np.exp(coefs), 4),
            "CI_lower": np.round(np.exp(ci[0].values), 4),
            "CI_upper": np.round(np.exp(ci[1].values), 4),
            "SE": np.round(model.bse.values, 4),
            "z": np.round(model.tvalues.values, 4),
            "p_value": np.round(model.pvalues.values, 6),
        })
        or_table["significant"] = or_table["p_value"] < 0.05
        or_table["interpretation"] = or_table.apply(
            lambda r: _interpret_or(r["OR"], r["predictor"]) if r["predictor"] != "const" else "",
            axis=1,
        )

        vif_df = pd.DataFrame()
        try:
            X_no_const = sub[model_vars].astype(float)
            if len(model_vars) > 1:
                vif_vals = [
                    variance_inflation_factor(X_no_const.values, i)
                    for i in range(X_no_const.shape[1])
                ]
                vif_df = pd.DataFrame({
                    "variable": model_vars,
                    "VIF": [round(float(v), 2) for v in vif_vals],
                    "concern": ["high" if v > 10 else "moderate" if v > 5 else "ok"
                                for v in vif_vals],
                })
                high_vif = [v for v in vif_vals if v > 5]
                if high_vif:
                    result_warnings.append(
                        f"Multicollinearity detected: {len(high_vif)} variable(s) with VIF > 5"
                    )
        except Exception:
            pass

        pred_probs = model.predict(X)
        try:
            from sklearn.metrics import roc_auc_score
            auc = round(float(roc_auc_score(y, pred_probs)), 4)
        except Exception:
            auc = None

        return {
            "or_table": or_table,
            "summary_text": str(model.summary()),
            "vif": vif_df,
            "n_obs": int(model.nobs),
            "pseudo_r2": round(float(model.prsquared), 4),
            "auc": auc,
            "aic": round(float(model.aic), 2),
            "bic": round(float(model.bic), 2),
            "warnings": result_warnings,
        }

    # ── Cox proportional hazards ──────────────────────────────────────────

    def fit_cox_ph(
        self,
        time_col: str,
        event_col: str,
        predictors: list[str],
        data: pd.DataFrame | None = None,
        view: str | None = None,
        penalizer: float = 0.01,
    ) -> dict[str, Any]:
        """Fit a Cox PH model and return HR table + Schoenfeld diagnostics.

        Returns dict with keys: hr_table, concordance, concordance_se,
        log_likelihood, schoenfeld_test, n_obs, n_events, warnings.
        """
        if not HAS_LIFELINES:
            return {"error": "lifelines required for Cox PH models"}

        if data is None:
            resolved = self.resolve_view(view)
            if resolved is None:
                return {"error": "No source view available"}
            data = self._load_view(resolved)

        required = [time_col, event_col] + predictors
        present = [v for v in required if v in data.columns]
        missing = set(required) - set(present)
        if missing:
            return {"error": f"Missing columns: {missing}"}

        sub = data[present].dropna().copy()
        for c in present:
            sub[c] = pd.to_numeric(sub[c], errors="coerce")
        sub = sub.dropna()

        if time_col == "time_to_event_days":
            sub["time_years"] = sub[time_col] / 365.25
            duration = "time_years"
        else:
            duration = time_col

        sub = sub[sub[duration] > 0]
        n_events = int(sub[event_col].sum())
        result_warnings: list[str] = []

        if n_events < 10:
            result_warnings.append(
                f"Only {n_events} events detected — Cox model requires >= 10 "
                "events for reliable estimates. Results may be unstable."
            )

        if len(sub) < 20:
            return {"error": f"Only {len(sub)} cases after cleaning — too few for Cox model"}

        fit_cols = [duration, event_col] + predictors
        try:
            cph = CoxPHFitter(penalizer=penalizer)
            cph.fit(sub[fit_cols], duration_col=duration, event_col=event_col)
        except Exception as exc:
            return {"error": f"Cox model fitting failed: {exc}"}

        summary = cph.summary.reset_index().rename(columns={"index": "covariate"})
        if "covariate" not in summary.columns and summary.index.name:
            summary = summary.reset_index()

        hr_table = pd.DataFrame({
            "covariate": summary.get("covariate", summary.iloc[:, 0]),
            "HR": summary.get("exp(coef)", pd.Series(dtype=float)).round(4),
            "CI_lower": summary.get("exp(coef) lower 95%", pd.Series(dtype=float)).round(4),
            "CI_upper": summary.get("exp(coef) upper 95%", pd.Series(dtype=float)).round(4),
            "coef": summary.get("coef", pd.Series(dtype=float)).round(4),
            "SE": summary.get("se(coef)", pd.Series(dtype=float)).round(4),
            "p_value": summary.get("p", pd.Series(dtype=float)).round(6),
        })
        hr_table["significant"] = hr_table["p_value"] < 0.05
        hr_table["interpretation"] = hr_table.apply(
            lambda r: _interpret_hr(r["HR"], r["covariate"]), axis=1
        )

        schoenfeld_df = pd.DataFrame()
        try:
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                sch = cph.check_assumptions(sub[fit_cols], p_value_threshold=0.05, show_plots=False)
                if isinstance(sch, list) and sch:
                    result_warnings.append(
                        "Proportional hazards assumption may be violated for: "
                        + ", ".join(str(s) for s in sch)
                    )
        except Exception:
            pass

        return {
            "hr_table": hr_table,
            "concordance": round(float(cph.concordance_index_), 4),
            "concordance_se": round(float(getattr(cph, "concordance_index_se_", 0)), 4),
            "log_likelihood": round(float(cph.log_likelihood_ratio_test().test_statistic), 4),
            "schoenfeld_test": schoenfeld_df,
            "n_obs": len(sub),
            "n_events": n_events,
            "aic": round(float(cph.AIC_partial_), 2),
            "warnings": result_warnings,
        }

    # ── Forest plot ───────────────────────────────────────────────────────

    @staticmethod
    def create_forest_plot(
        results_df: pd.DataFrame,
        title: str = "Forest Plot",
        reference_value: float = 1.0,
        log_scale: bool = True,
    ) -> "go.Figure":
        """Create a publication-ready horizontal forest plot.

        Parameters
        ----------
        results_df : DataFrame
            Must contain: label, estimate, ci_lower, ci_upper, p_value.
        title : str
            Plot title.
        reference_value : float
            Reference line (1.0 for HR/OR).
        log_scale : bool
            Use logarithmic x-axis.
        """
        if not HAS_PLOTLY:
            raise ImportError("plotly required for forest plots")

        df = results_df.copy()
        required = {"label", "estimate", "ci_lower", "ci_upper", "p_value"}
        if not required.issubset(df.columns):
            raise ValueError(f"results_df must contain columns: {required}")

        df = df.sort_values("estimate", ascending=True).reset_index(drop=True)

        fig = go.Figure()

        for i, row in df.iterrows():
            sig = row["p_value"] < 0.05
            color = _TEAL if sig else _GRAY
            fig.add_trace(go.Scatter(
                x=[row["ci_lower"], row["ci_upper"]],
                y=[i, i],
                mode="lines",
                line=dict(color=color, width=2),
                showlegend=False,
                hoverinfo="skip",
            ))
            p_str = f"p={row['p_value']:.4f}" if row["p_value"] >= 0.0001 else "p<0.0001"
            fig.add_trace(go.Scatter(
                x=[row["estimate"]],
                y=[i],
                mode="markers",
                marker=dict(size=10, color=color, symbol="diamond"),
                name=row["label"],
                showlegend=False,
                hovertemplate=(
                    f"<b>{row['label']}</b><br>"
                    f"Estimate: {row['estimate']:.3f}<br>"
                    f"95% CI: [{row['ci_lower']:.3f}, {row['ci_upper']:.3f}]<br>"
                    f"{p_str}<extra></extra>"
                ),
            ))

        fig.add_vline(
            x=reference_value, line_dash="dash",
            line_color=_ROSE, line_width=1, opacity=0.7,
        )

        annotations = []
        for i, row in df.iterrows():
            p_str = f"p={row['p_value']:.3f}" if row["p_value"] >= 0.001 else "p<0.001"
            annotations.append(dict(
                x=1.0, xref="paper", xanchor="left",
                y=i, yanchor="middle",
                text=f"  {row['estimate']:.2f} [{row['ci_lower']:.2f}, {row['ci_upper']:.2f}] {p_str}",
                font=dict(size=10, color="#8892a4", family="DM Mono"),
                showarrow=False,
            ))

        pl_base = {k: v for k, v in PL.items() if k not in ("xaxis", "yaxis", "margin")}
        fig.update_layout(
            **pl_base,
            title=title,
            yaxis=dict(
                tickvals=list(range(len(df))),
                ticktext=df["label"].tolist(),
                gridcolor="#1e2535",
                linecolor="#1e2535",
                zerolinecolor="#1e2535",
            ),
            xaxis=dict(
                title="Estimate (95% CI)",
                type="log" if log_scale else "linear",
                gridcolor="#1e2535",
                linecolor="#1e2535",
                zerolinecolor="#1e2535",
            ),
            height=max(300, 40 * len(df) + 100),
            margin=dict(l=16, r=280, t=50, b=40),
            annotations=annotations,
        )

        return fig

    # ── Correlation matrix ────────────────────────────────────────────────

    @staticmethod
    def correlation_matrix_with_pvalues(
        data: pd.DataFrame,
        vars_list: list[str],
        method: Literal["pearson", "spearman", "kendall"] = "spearman",
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Compute pairwise correlation matrix with p-values.

        Returns (corr_matrix, pvalue_matrix).
        """
        present = [v for v in vars_list if v in data.columns]
        if len(present) < 2:
            return pd.DataFrame(), pd.DataFrame()

        numeric = data[present].apply(pd.to_numeric, errors="coerce")

        if HAS_PINGOUIN:
            pw = pg.pairwise_corr(numeric, columns=present, method=method)
            corr = numeric.corr(method=method)
            pval = pd.DataFrame(np.ones_like(corr.values), index=corr.index, columns=corr.columns)
            p_col = "p_unc" if "p_unc" in pw.columns else "p-unc"
            for _, row in pw.iterrows():
                x, y = row["X"], row["Y"]
                if x in pval.index and y in pval.columns:
                    pval.loc[x, y] = row[p_col]
                    pval.loc[y, x] = row[p_col]
            return corr.round(4), pval.round(6)

        corr = numeric.corr(method=method)
        n = len(numeric.dropna())
        pval = pd.DataFrame(np.ones_like(corr.values), index=corr.index, columns=corr.columns)

        if HAS_SCIPY and n > 3:
            for i, c1 in enumerate(present):
                for j, c2 in enumerate(present):
                    if i >= j:
                        continue
                    pair = numeric[[c1, c2]].dropna()
                    if len(pair) < 4:
                        continue
                    if method == "pearson":
                        _, p = sp_stats.pearsonr(pair[c1], pair[c2])
                    elif method == "spearman":
                        _, p = sp_stats.spearmanr(pair[c1], pair[c2])
                    else:
                        _, p = sp_stats.kendalltau(pair[c1], pair[c2])
                    pval.loc[c1, c2] = p
                    pval.loc[c2, c1] = p

        return corr.round(4), pval.round(6)

    # ── Correlation heatmap ───────────────────────────────────────────────

    @staticmethod
    def create_correlation_heatmap(
        corr: pd.DataFrame,
        pval: pd.DataFrame | None = None,
        title: str = "Correlation Matrix",
    ) -> "go.Figure":
        """Create an annotated Plotly heatmap from a correlation matrix."""
        if not HAS_PLOTLY:
            raise ImportError("plotly required")

        text_matrix = corr.round(2).astype(str)
        if pval is not None:
            for r in corr.index:
                for c in corr.columns:
                    p = pval.loc[r, c]
                    star = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
                    text_matrix.loc[r, c] = f"{corr.loc[r, c]:.2f}{star}"

        fig = go.Figure(data=go.Heatmap(
            z=corr.values,
            x=corr.columns.tolist(),
            y=corr.index.tolist(),
            text=text_matrix.values,
            texttemplate="%{text}",
            colorscale=[[0, "#f43f5e"], [0.5, "#0e1219"], [1, "#2dd4bf"]],
            zmid=0,
            zmin=-1,
            zmax=1,
        ))
        fig.update_layout(**PL, title=title, height=max(400, 30 * len(corr) + 100))
        return fig

    # ── Longitudinal analysis (mixed-effects) ─────────────────────────────

    def longitudinal_summary(
        self,
        marker: str = "tg",
        view: str | None = None,
        stratify_by: str | None = None,
    ) -> dict[str, Any]:
        """Fit a linear mixed-effects model for longitudinal Tg/TSH trajectories.

        Loads from ``extracted_clinical_events_v4`` (primary, has days_from_surgery)
        or falls back to ``longitudinal_lab_view``.  Fits a random-intercept model:

            log(value + 0.01) ~ days_from_surgery + (1 | research_id)

        when log_transform is enabled (Tg, TSH), otherwise uses raw values.

        Parameters
        ----------
        marker : str
            One of ``"tg"``, ``"tsh"``, ``"anti_tg"``.
        view : str, optional
            Override source view/table.
        stratify_by : str, optional
            Optional column to stratify model (e.g. ``"braf_positive"``).

        Returns
        -------
        dict with keys:
            data, slope, slope_se, slope_ci, p_value, n_patients, n_obs,
            model_summary, per_patient_summary, warnings, model_type
        """
        if not HAS_STATSMODELS:
            return {"error": "statsmodels required for mixed-effects models"}

        marker_cfg = LONGITUDINAL_MARKERS.get(marker, LONGITUDINAL_MARKERS["tg"])
        log_transform = marker_cfg["log_transform"]
        label = marker_cfg["label"]
        result_warnings: list[str] = []

        # ── Load data ────────────────────────────────────────────────────
        data: pd.DataFrame | None = None
        time_col = "days_from_surgery"
        value_col = "event_value"
        id_col = "research_id"

        if view is None:
            view = "extracted_clinical_events_v4"

        primary_candidates = [view, "extracted_clinical_events_v4", "longitudinal_lab_view"]
        for candidate in primary_candidates:
            try:
                row = self._con.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{candidate}'"
                ).fetchone()
                if not (row and row[0] > 0):
                    continue
            except Exception:
                continue

            try:
                if "extracted_clinical_events" in candidate:
                    subtype = marker_cfg["event_subtype_filter"]
                    sql = (
                        f"SELECT research_id, "
                        f"TRY_CAST(event_value AS DOUBLE) AS event_value, "
                        f"TRY_CAST(days_from_surgery AS DOUBLE) AS days_from_surgery "
                        f"FROM {candidate} "
                        f"WHERE event_type = 'lab' "
                        f"AND LOWER(event_subtype) LIKE '%{subtype}%' "
                        f"AND event_value IS NOT NULL "
                        f"AND TRY_CAST(event_value AS DOUBLE) > 0 "
                        f"AND days_from_surgery IS NOT NULL"
                    )
                    time_col = "days_from_surgery"
                    value_col = "event_value"
                else:
                    lab_filter = marker_cfg["lab_type_filter"]
                    sql = (
                        f"SELECT research_id, "
                        f"TRY_CAST(numeric_result AS DOUBLE) AS event_value, "
                        f"TRY_CAST(days_from_first_lab AS DOUBLE) AS days_from_surgery "
                        f"FROM {candidate} "
                        f"WHERE LOWER(lab_type) LIKE '%{lab_filter}%' "
                        f"AND numeric_result IS NOT NULL "
                        f"AND TRY_CAST(numeric_result AS DOUBLE) > 0 "
                        f"AND days_from_first_lab IS NOT NULL"
                    )
                    time_col = "days_from_surgery"
                    value_col = "event_value"

                df_raw = self._con.execute(sql).fetchdf()
                if len(df_raw) >= 20:
                    data = df_raw
                    break
            except Exception as exc:
                log.warning("Longitudinal query on %s failed: %s", candidate, exc)
                continue

        if data is None or data.empty:
            return {
                "error": (
                    f"No longitudinal {label} data found. "
                    "Ensure extracted_clinical_events_v4 is materialized and "
                    "contains lab events."
                )
            }

        data = data.dropna(subset=[id_col, time_col, value_col]).copy()
        data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
        data[time_col] = pd.to_numeric(data[time_col], errors="coerce")
        data = data.dropna()

        if log_transform:
            data["outcome"] = np.log(data[value_col] + 0.01)
        else:
            data["outcome"] = data[value_col]

        # Need ≥2 obs per patient for random effects
        obs_per_patient = data.groupby(id_col)["outcome"].count()
        multi_obs_ids = obs_per_patient[obs_per_patient >= 2].index
        data = data[data[id_col].isin(multi_obs_ids)]

        n_patients = int(data[id_col].nunique())
        n_obs = len(data)

        if n_patients < 10:
            return {
                "error": (
                    f"Only {n_patients} patients with ≥2 {label} measurements. "
                    "Need ≥10 patients for mixed-effects model."
                )
            }

        if n_patients < 50:
            result_warnings.append(
                f"Only {n_patients} patients with repeated measures — "
                "mixed-effects estimates may be unstable."
            )

        # ── Per-patient slope summary (OLS per patient) ───────────────────
        per_patient_rows = []
        for pid, grp in data.groupby(id_col):
            if len(grp) < 2:
                continue
            x = grp[time_col].values
            y = grp["outcome"].values
            try:
                coeffs = np.polyfit(x, y, 1)
                slope = float(coeffs[0])
                intercept = float(coeffs[1])
                first_val = float(y[0]) if not log_transform else float(np.exp(y[0]))
                last_val = float(y[-1]) if not log_transform else float(np.exp(y[-1]))
            except Exception:
                continue
            per_patient_rows.append({
                "research_id": pid,
                "n_obs": len(grp),
                "slope_per_day": round(slope, 6),
                "first_value": round(first_val, 3),
                "last_value": round(last_val, 3),
                "rising": slope > 0,
            })

        per_patient_df = pd.DataFrame(per_patient_rows)

        # ── Mixed-effects model (random intercept) ────────────────────────
        try:
            import statsmodels.formula.api as smf  # type: ignore

            data["days_scaled"] = data[time_col] / 365.25
            data[id_col] = data[id_col].astype(str)

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                md = smf.mixedlm("outcome ~ days_scaled", data, groups=data[id_col])
                mdf = md.fit(reml=True, method="lbfgs")
                for w in caught:
                    if "ConvergenceWarning" in str(type(w.category)):
                        result_warnings.append("Mixed-effects model convergence warning — interpret carefully.")

            fe = mdf.params
            ci = mdf.conf_int()
            slope_raw = float(fe.get("days_scaled", np.nan))
            slope_ci_lo = float(ci.loc["days_scaled", 0]) if "days_scaled" in ci.index else np.nan
            slope_ci_hi = float(ci.loc["days_scaled", 1]) if "days_scaled" in ci.index else np.nan
            p_slope = float(mdf.pvalues.get("days_scaled", np.nan))

            unit_note = f"log({label})" if log_transform else label
            model_summary = (
                f"LME: {unit_note} ~ days_from_surgery/365 + (1|patient)\n"
                f"Slope β={slope_raw:.4f} [{slope_ci_lo:.4f}, {slope_ci_hi:.4f}] "
                f"p={p_slope:.4f}\n"
                f"N={n_patients} patients, {n_obs} obs"
            )
            model_type = "mixedlm"

        except Exception as exc:
            # Fallback to population OLS
            result_warnings.append(
                f"Mixed-effects model failed ({exc}); falling back to OLS."
            )
            x_scaled = data[time_col].values / 365.25
            y_out = data["outcome"].values
            coeffs = np.polyfit(x_scaled, y_out, 1)
            slope_raw = float(coeffs[0])
            slope_ci_lo = slope_ci_hi = slope_raw
            p_slope = np.nan
            model_summary = f"OLS fallback: slope={slope_raw:.4f} (MixedLM unavailable)"
            model_type = "ols_fallback"
            mdf = None

        # ── Slope direction label ─────────────────────────────────────────
        rising_pct = float(per_patient_df["rising"].mean() * 100) if not per_patient_df.empty else np.nan
        direction = "rising" if slope_raw > 0 else "falling"
        clinical_note = (
            f"{label} trajectory is {direction} (β={slope_raw:.4f}/year). "
            f"{rising_pct:.0f}% of patients show a rising slope. "
        )
        if marker == "tg" and slope_raw > 0:
            clinical_note += "Rising Tg post-thyroidectomy may indicate structural recurrence — "
            clinical_note += "correlate with imaging surveillance."
        elif marker == "tg":
            clinical_note += "Stable or falling Tg is consistent with successful treatment."

        return {
            "data": data,
            "marker": marker,
            "marker_label": label,
            "marker_units": marker_cfg["units"],
            "log_transform": log_transform,
            "slope": round(slope_raw, 6),
            "slope_se": round(float(mdf.bse.get("days_scaled", np.nan)) if model_type == "mixedlm" and mdf is not None else np.nan, 6),
            "slope_ci": (round(slope_ci_lo, 6), round(slope_ci_hi, 6)),
            "p_value": round(p_slope, 6) if not np.isnan(p_slope) else None,
            "n_patients": n_patients,
            "n_obs": n_obs,
            "model_summary": model_summary,
            "model_type": model_type,
            "per_patient_summary": per_patient_df,
            "rising_pct": round(rising_pct, 1),
            "clinical_note": clinical_note,
            "warnings": result_warnings,
        }

    # ── Power & sample-size helpers ───────────────────────────────────────

    @staticmethod
    def power_two_proportions(
        p1: float,
        p2: float,
        alpha: float = 0.05,
        power: float = 0.80,
        two_sided: bool = True,
    ) -> dict[str, Any]:
        """Required sample size per group for a two-proportion z-test.

        Implements the Fleiss (1981) formula, suitable for comparing
        event rates (e.g., recurrence) between BRAF+ and BRAF– groups.

        Parameters
        ----------
        p1, p2 : float
            Expected proportions in each group (0-1).
        alpha : float
            Type I error rate (default 0.05 two-sided).
        power : float
            Desired statistical power (default 0.80).
        two_sided : bool
            Use two-sided test (default True).

        Returns
        -------
        dict with n_per_group, n_total, effect_size (Cohen h), inputs
        """
        from scipy.stats import norm  # type: ignore

        alpha_adj = alpha / 2 if two_sided else alpha
        z_alpha = norm.ppf(1 - alpha_adj)
        z_beta = norm.ppf(power)

        # Fleiss formula
        p_bar = (p1 + p2) / 2
        n = (z_alpha * np.sqrt(2 * p_bar * (1 - p_bar))
             + z_beta * np.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2 / (p1 - p2) ** 2
        n = int(np.ceil(n))

        # Cohen's h
        h = 2 * np.arcsin(np.sqrt(p1)) - 2 * np.arcsin(np.sqrt(p2))

        return {
            "n_per_group": n,
            "n_total": 2 * n,
            "effect_size_h": round(abs(float(h)), 4),
            "effect_label": (
                "small" if abs(h) < 0.3 else "medium" if abs(h) < 0.5 else "large"
            ),
            "p1": p1, "p2": p2, "alpha": alpha, "power": power,
            "two_sided": two_sided,
            "formula": "Fleiss (1981) two-proportion z-test",
        }

    @staticmethod
    def power_logistic(
        p_event: float,
        or_detect: float,
        alpha: float = 0.05,
        power: float = 0.80,
        p_exposure: float = 0.5,
    ) -> dict[str, Any]:
        """Required sample size for detecting an OR in logistic regression.

        Implements the Hsieh, Block & Larsen (1998) formula for a single
        binary predictor.

        Parameters
        ----------
        p_event : float
            Baseline event rate (0-1).
        or_detect : float
            Minimum OR to detect (>1).
        alpha : float
            Type I error rate (default 0.05, two-sided).
        power : float
            Desired statistical power (default 0.80).
        p_exposure : float
            Prevalence of binary predictor (default 0.5).
        """
        from scipy.stats import norm  # type: ignore

        z_alpha = norm.ppf(1 - alpha / 2)
        z_beta = norm.ppf(power)

        # Hsieh et al. 1998 formula
        p1 = (p_event * or_detect) / (1 - p_event + p_event * or_detect)
        p_avg = p_exposure * p1 + (1 - p_exposure) * p_event
        numerator = (z_alpha + z_beta) ** 2
        denominator = p_avg * (1 - p_avg) * (np.log(or_detect)) ** 2
        n = int(np.ceil(numerator / denominator)) if denominator > 0 else None

        return {
            "n_total": n,
            "p_event_baseline": p_event,
            "p_event_exposed": round(float(p1), 4) if n else None,
            "or_detect": or_detect,
            "alpha": alpha,
            "power": power,
            "p_exposure": p_exposure,
            "formula": "Hsieh, Block & Larsen (1998)",
        }

    @staticmethod
    def sample_size_km(
        hr: float,
        alpha: float = 0.05,
        power: float = 0.80,
        event_rate: float = 0.10,
        allocation_ratio: float = 1.0,
    ) -> dict[str, Any]:
        """Required events (and total n) for a log-rank test (Cox/KM).

        Implements Schoenfeld (1981): d = (z_α + z_β)² / [log(HR)]².
        Total n derived from expected event rate.

        Parameters
        ----------
        hr : float
            Hazard ratio to detect (>1).
        alpha : float
            Type I error (default 0.05, two-sided).
        power : float
            Desired power (default 0.80).
        event_rate : float
            Overall event rate in cohort (default 0.10 = 10%).
        allocation_ratio : float
            n_group2 / n_group1 (default 1.0 = equal groups).
        """
        from scipy.stats import norm  # type: ignore

        z_alpha = norm.ppf(1 - alpha / 2)
        z_beta = norm.ppf(power)

        # Schoenfeld (1981)
        d = int(np.ceil((z_alpha + z_beta) ** 2 / (np.log(hr)) ** 2))
        n_total = int(np.ceil(d / event_rate)) if event_rate > 0 else None
        n_g1 = int(np.ceil(n_total / (1 + allocation_ratio))) if n_total else None
        n_g2 = int(n_total - n_g1) if (n_total and n_g1) else None

        return {
            "events_required": d,
            "n_total": n_total,
            "n_group1": n_g1,
            "n_group2": n_g2,
            "hr": hr,
            "alpha": alpha,
            "power": power,
            "event_rate": event_rate,
            "allocation_ratio": allocation_ratio,
            "formula": "Schoenfeld (1981) log-rank test",
        }

    # ── Clinical interpretation snippets ──────────────────────────────────

    @staticmethod
    def format_clinical_snippet(
        model_results: dict[str, Any],
        model_type: Literal["HR", "OR"] = "HR",
        outcome_label: str = "the outcome",
        significance_threshold: float = 0.05,
    ) -> str:
        """Generate plain-English clinical interpretation for significant findings.

        Parameters
        ----------
        model_results : dict
            Output of ``fit_cox_ph()`` or ``fit_logistic_regression()``.
        model_type : str
            ``"HR"`` for Cox, ``"OR"`` for logistic.
        outcome_label : str
            Human-readable outcome name (e.g., ``"disease recurrence"``).
        significance_threshold : float
            P-value threshold for highlighting (default 0.05).

        Returns
        -------
        str — formatted Markdown text suitable for ``st.info()`` or manuscript draft.
        """
        estimate_col = "HR" if model_type == "HR" else "OR"
        label_col = "covariate" if model_type == "HR" else "predictor"

        if estimate_col not in model_results.get(
            "hr_table" if model_type == "HR" else "or_table",
            pd.DataFrame(),
        ).columns:
            return "No significant associations detected."

        table_key = "hr_table" if model_type == "HR" else "or_table"
        df = model_results.get(table_key, pd.DataFrame())
        if df.empty:
            return "Model results unavailable."

        sig = df[
            (df["p_value"] < significance_threshold)
            & (df.get("predictor", df.get("covariate", pd.Series())) != "const")
        ].copy()

        if "predictor" not in sig.columns and "covariate" in sig.columns:
            sig = sig.rename(columns={"covariate": "predictor"})
        elif "covariate" not in sig.columns and "predictor" in sig.columns:
            pass  # predictor already correct

        if sig.empty:
            return (
                f"No individual predictors reached p<{significance_threshold} "
                f"for {outcome_label}. Consider relaxing significance threshold "
                f"or increasing sample size."
            )

        lines = [
            f"**Significant predictors of {outcome_label} "
            f"({'multivariable Cox PH' if model_type == 'HR' else 'multivariable logistic regression'}):**\n"
        ]

        for _, row in sig.sort_values("p_value").iterrows():
            var = str(row.get("predictor", ""))
            est = float(row[estimate_col])
            ci_lo = float(row["CI_lower"])
            ci_hi = float(row["CI_upper"])
            p = float(row["p_value"])
            p_str = f"p={p:.3f}" if p >= 0.001 else "p<0.001"
            dir_word = "associated with increased" if est > 1 else "associated with reduced"
            pct = abs(est - 1) * 100

            context = _CLINICAL_CONTEXT.get(var, "")
            context_str = f" ({context})" if context else ""
            var_display = var.replace("_", " ").title()

            lines.append(
                f"- **{var_display}**: {estimate_col}={est:.2f} "
                f"(95% CI {ci_lo:.2f}–{ci_hi:.2f}), {p_str} — "
                f"{dir_word} risk by {pct:.0f}%{context_str}"
            )

        meta_parts = []
        if "n_obs" in model_results:
            meta_parts.append(f"N={model_results['n_obs']:,}")
        if model_type == "HR" and "concordance" in model_results:
            meta_parts.append(f"concordance={model_results['concordance']:.3f}")
        elif "pseudo_r2" in model_results:
            meta_parts.append(f"pseudo-R²={model_results['pseudo_r2']:.3f}")
        if meta_parts:
            lines.append(f"\n*Model: {'; '.join(meta_parts)}*")

        return "\n".join(lines)

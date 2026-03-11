"""
Tests for utils/statistical_analysis.py

Uses synthetic DataFrames that mimic the thyroid cohort schema.
No MotherDuck or DuckDB connection required.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.statistical_analysis import (
    ThyroidStatisticalAnalyzer,
    HAS_STATSMODELS,
    HAS_SCIPY,
    HAS_LIFELINES,
    HAS_PLOTLY,
    _effect_label,
    _interpret_hr,
    _interpret_or,
)

np.random.seed(42)


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def thyroid_df() -> pd.DataFrame:
    """Synthetic cohort mimicking thyroid cancer data (200 patients)."""
    n = 200
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "research_id": range(1, n + 1),
        "age_at_surgery": rng.normal(48, 14, n).clip(18, 90).round(1),
        "sex": rng.choice(["Female", "Male"], n, p=[0.76, 0.24]),
        "largest_tumor_cm": rng.exponential(1.5, n).clip(0.1, 12).round(1),
        "ln_positive": rng.integers(0, 15, n),
        "ln_examined": rng.integers(1, 30, n),
        "ln_ratio": rng.uniform(0, 0.6, n).round(3),
        "histology_1_type": rng.choice(
            ["PTC", "FTC", "MTC", "ATC", "PDTC"], n,
            p=[0.70, 0.12, 0.08, 0.05, 0.05],
        ),
        "overall_stage_ajcc8": rng.choice(["I", "II", "III", "IV"], n, p=[0.55, 0.20, 0.15, 0.10]),
        "braf_positive": rng.choice([0, 1], n, p=[0.55, 0.45]).astype(int),
        "tert_positive": rng.choice([0, 1], n, p=[0.88, 0.12]).astype(int),
        "tumor_1_extrathyroidal_ext": rng.choice([0, 1], n, p=[0.70, 0.30]).astype(int),
        "tumor_1_gross_ete": rng.choice([0, 1], n, p=[0.85, 0.15]).astype(int),
        "tumor_1_ete_microscopic_only": rng.choice([0, 1], n, p=[0.80, 0.20]).astype(int),
        "recurrence_risk_band": rng.choice(["low", "intermediate", "high"], n, p=[0.40, 0.40, 0.20]),
        "variant_standardized": rng.choice(
            ["PTC_classic", "PTC_follicular_variant", "PTC_tall_cell", "FTC", "MTC"],
            n, p=[0.50, 0.20, 0.10, 0.10, 0.10],
        ),
        "event_occurred": rng.choice([0, 1], n, p=[0.85, 0.15]).astype(int),
        "time_to_event_days": rng.integers(30, 3650, n),
        "recurrence_flag": rng.choice([0, 1], n, p=[0.88, 0.12]).astype(int),
    })


@pytest.fixture
def small_df() -> pd.DataFrame:
    """Minimal DataFrame for edge-case testing (10 patients)."""
    return pd.DataFrame({
        "research_id": range(1, 11),
        "age_at_surgery": [45, 52, 38, 61, 55, 42, 67, 33, 48, 50],
        "sex": ["Female"] * 7 + ["Male"] * 3,
        "braf_positive": [1, 0, 1, 0, 1, 0, 1, 0, 1, 0],
        "event_occurred": [0, 0, 1, 0, 0, 1, 0, 0, 0, 1],
        "time_to_event_days": [365, 730, 180, 1095, 540, 90, 1460, 365, 200, 50],
        "largest_tumor_cm": [1.2, 2.5, 0.8, 3.1, 1.5, 4.2, 0.5, 2.0, 1.8, 3.5],
    })


@pytest.fixture
def analyzer_mock(thyroid_df):
    """Analyzer with a mock connection that returns the synthetic DataFrame."""
    class MockCon:
        def __init__(self, df):
            self._df = df
        def execute(self, sql):
            return self
        def fetchdf(self):
            return self._df
        def fetchone(self):
            return (1,)

    return ThyroidStatisticalAnalyzer(MockCon(thyroid_df))


# ── Interpretation helpers ────────────────────────────────────────────────

class TestInterpretation:
    def test_effect_label_small(self):
        assert _effect_label(0.3, "d") == "small"

    def test_effect_label_large(self):
        assert _effect_label(1.5, "d") == "large"

    def test_effect_label_negligible(self):
        assert _effect_label(0.05, "V") == "negligible"

    def test_interpret_hr_increased(self):
        s = _interpret_hr(1.84, "gross_ete")
        assert "increased hazard" in s
        assert "84%" in s

    def test_interpret_hr_reduced(self):
        s = _interpret_hr(0.5, "age")
        assert "reduced hazard" in s

    def test_interpret_or(self):
        s = _interpret_or(2.0, "braf_positive")
        assert "increased odds" in s


# ── Type detection ────────────────────────────────────────────────────────

class TestTypeDetection:
    def test_auto_detect_types(self, thyroid_df):
        cont, cat = ThyroidStatisticalAnalyzer._auto_detect_types(thyroid_df)
        assert "age_at_surgery" in cont
        assert "sex" in cat
        assert "research_id" not in cont + cat

    def test_auto_detect_binary(self, thyroid_df):
        _, cat = ThyroidStatisticalAnalyzer._auto_detect_types(thyroid_df)
        assert "braf_positive" in cat


# ── Missing data ──────────────────────────────────────────────────────────

class TestMissingData:
    def test_missing_summary_no_missing(self, thyroid_df):
        summary = ThyroidStatisticalAnalyzer.missing_data_summary(thyroid_df)
        assert len(summary) == len(thyroid_df.columns)
        assert summary["pct_missing"].max() == 0.0

    def test_missing_summary_with_nans(self, thyroid_df):
        df = thyroid_df.copy()
        df.loc[:19, "age_at_surgery"] = np.nan
        summary = ThyroidStatisticalAnalyzer.missing_data_summary(df)
        age_row = summary[summary["column"] == "age_at_surgery"].iloc[0]
        assert age_row["n_missing"] == 20
        assert age_row["pct_missing"] == 10.0


# ── Table 1 ───────────────────────────────────────────────────────────────

class TestTableOneGeneration:
    def test_table1_ungrouped(self, analyzer_mock, thyroid_df):
        t1, meta = analyzer_mock.generate_table_one(data=thyroid_df)
        assert not t1.empty
        assert meta["n_total"] == 200
        assert "error" not in meta

    def test_table1_grouped(self, analyzer_mock, thyroid_df):
        t1, meta = analyzer_mock.generate_table_one(data=thyroid_df, groupby_col="sex")
        assert not t1.empty
        assert meta["groupby"] == "sex"

    def test_table1_custom_vars(self, analyzer_mock, thyroid_df):
        t1, meta = analyzer_mock.generate_table_one(
            data=thyroid_df,
            continuous_vars=["age_at_surgery"],
            categorical_vars=["sex"],
        )
        assert not t1.empty
        assert meta["continuous_vars"] == ["age_at_surgery"]
        assert meta["categorical_vars"] == ["sex"]

    def test_table1_empty_data(self, analyzer_mock):
        t1, meta = analyzer_mock.generate_table_one(data=pd.DataFrame())
        assert t1.empty
        assert "error" in meta

    def test_table1_preset_filtering(self, thyroid_df, analyzer_mock):
        """Preset variables not in data are filtered out."""
        df = thyroid_df.drop(columns=["ln_ratio"])
        t1, meta = analyzer_mock.generate_table_one(data=df)
        assert "ln_ratio" not in meta.get("continuous_vars", [])


# ── Hypothesis testing ────────────────────────────────────────────────────

@pytest.mark.skipif(not HAS_SCIPY, reason="scipy required")
class TestHypothesisTests:
    def test_basic_tests(self, analyzer_mock, thyroid_df):
        results = analyzer_mock.run_hypothesis_tests(
            thyroid_df, "braf_positive",
            ["age_at_surgery", "largest_tumor_cm"],
        )
        assert not results.empty
        assert "test_used" in results.columns
        assert len(results) == 2

    def test_categorical_feature(self, analyzer_mock, thyroid_df):
        results = analyzer_mock.run_hypothesis_tests(
            thyroid_df, "braf_positive", ["histology_1_type"],
        )
        assert not results.empty
        assert results.iloc[0]["test_used"] in ("Chi-square", "Fisher exact")

    def test_fdr_correction(self, analyzer_mock, thyroid_df):
        results = analyzer_mock.run_hypothesis_tests(
            thyroid_df, "braf_positive",
            ["age_at_surgery", "largest_tumor_cm", "ln_positive", "ln_ratio"],
            correction="fdr_bh",
        )
        assert "p_adjusted" in results.columns
        assert results["correction"].iloc[0] == "fdr_bh"

    def test_missing_target(self, analyzer_mock, thyroid_df):
        results = analyzer_mock.run_hypothesis_tests(
            thyroid_df, "nonexistent_col", ["age_at_surgery"],
        )
        assert "error" in results.columns

    def test_single_group_target(self, analyzer_mock, thyroid_df):
        df = thyroid_df.copy()
        df["constant"] = 1
        results = analyzer_mock.run_hypothesis_tests(df, "constant", ["age_at_surgery"])
        assert "error" in results.columns

    def test_small_sample_guard(self, analyzer_mock, small_df):
        results = analyzer_mock.run_hypothesis_tests(
            small_df, "braf_positive", ["age_at_surgery"],
        )
        assert not results.empty


# ── Logistic regression ───────────────────────────────────────────────────

@pytest.mark.skipif(not HAS_STATSMODELS, reason="statsmodels required")
class TestLogisticRegression:
    def test_basic_logistic(self, analyzer_mock, thyroid_df):
        result = analyzer_mock.fit_logistic_regression(
            outcome="event_occurred",
            predictors=["age_at_surgery", "largest_tumor_cm", "braf_positive"],
            data=thyroid_df,
        )
        assert "error" not in result
        assert "or_table" in result
        assert result["n_obs"] > 0
        assert "pseudo_r2" in result

    def test_or_table_structure(self, analyzer_mock, thyroid_df):
        result = analyzer_mock.fit_logistic_regression(
            outcome="event_occurred",
            predictors=["age_at_surgery", "braf_positive"],
            data=thyroid_df,
        )
        or_table = result["or_table"]
        required_cols = {"predictor", "OR", "CI_lower", "CI_upper", "p_value", "significant"}
        assert required_cols.issubset(or_table.columns)

    def test_vif_computation(self, analyzer_mock, thyroid_df):
        result = analyzer_mock.fit_logistic_regression(
            outcome="event_occurred",
            predictors=["age_at_surgery", "largest_tumor_cm", "ln_positive"],
            data=thyroid_df,
        )
        vif = result.get("vif", pd.DataFrame())
        if not vif.empty:
            assert "VIF" in vif.columns
            assert "concern" in vif.columns

    def test_too_few_cases(self, analyzer_mock):
        tiny = pd.DataFrame({
            "outcome": [0, 1, 0, 1, 0],
            "x": [1.0, 2.0, 3.0, 4.0, 5.0],
        })
        result = analyzer_mock.fit_logistic_regression(
            outcome="outcome", predictors=["x"], data=tiny,
        )
        assert "error" in result

    def test_with_confounders(self, analyzer_mock, thyroid_df):
        result = analyzer_mock.fit_logistic_regression(
            outcome="event_occurred",
            predictors=["braf_positive"],
            confounders=["age_at_surgery", "largest_tumor_cm"],
            data=thyroid_df,
        )
        assert "error" not in result
        preds = result["or_table"]["predictor"].tolist()
        assert "age_at_surgery" in preds
        assert "braf_positive" in preds


# ── Cox PH ────────────────────────────────────────────────────────────────

@pytest.mark.skipif(not HAS_LIFELINES, reason="lifelines required")
class TestCoxPH:
    def test_basic_cox(self, analyzer_mock, thyroid_df):
        result = analyzer_mock.fit_cox_ph(
            time_col="time_to_event_days",
            event_col="event_occurred",
            predictors=["age_at_surgery", "braf_positive", "largest_tumor_cm"],
            data=thyroid_df,
        )
        assert "error" not in result
        assert "hr_table" in result
        assert result["n_obs"] > 0
        assert result["concordance"] > 0

    def test_hr_table_structure(self, analyzer_mock, thyroid_df):
        result = analyzer_mock.fit_cox_ph(
            time_col="time_to_event_days",
            event_col="event_occurred",
            predictors=["age_at_surgery", "braf_positive"],
            data=thyroid_df,
        )
        hr_table = result["hr_table"]
        required_cols = {"covariate", "HR", "CI_lower", "CI_upper", "p_value", "significant"}
        assert required_cols.issubset(hr_table.columns)

    def test_insufficient_events_warning(self, analyzer_mock):
        n = 50
        df = pd.DataFrame({
            "time_to_event_days": np.tile([100, 200, 300, 400, 500], 10),
            "event_occurred": [0] * 45 + [1] * 5,
            "x": np.random.randn(n),
        })
        result = analyzer_mock.fit_cox_ph(
            time_col="time_to_event_days",
            event_col="event_occurred",
            predictors=["x"],
            data=df,
        )
        if "error" not in result:
            assert any("events" in w.lower() for w in result.get("warnings", []))

    def test_missing_columns(self, analyzer_mock, thyroid_df):
        result = analyzer_mock.fit_cox_ph(
            time_col="nonexistent_time",
            event_col="event_occurred",
            predictors=["age_at_surgery"],
            data=thyroid_df,
        )
        assert "error" in result


# ── Forest plot ───────────────────────────────────────────────────────────

@pytest.mark.skipif(not HAS_PLOTLY, reason="plotly required")
class TestForestPlot:
    def test_basic_forest_plot(self):
        results_df = pd.DataFrame({
            "label": ["age", "braf", "ete"],
            "estimate": [1.02, 1.84, 2.10],
            "ci_lower": [0.98, 1.08, 1.30],
            "ci_upper": [1.06, 3.14, 3.40],
            "p_value": [0.30, 0.024, 0.003],
        })
        fig = ThyroidStatisticalAnalyzer.create_forest_plot(results_df)
        assert fig is not None
        assert len(fig.data) > 0

    def test_forest_plot_reference_line(self):
        results_df = pd.DataFrame({
            "label": ["x1", "x2"],
            "estimate": [0.5, 2.0],
            "ci_lower": [0.3, 1.5],
            "ci_upper": [0.8, 2.8],
            "p_value": [0.01, 0.04],
        })
        fig = ThyroidStatisticalAnalyzer.create_forest_plot(
            results_df, reference_value=1.0,
        )
        layout_shapes = fig.layout.shapes
        assert layout_shapes is not None

    def test_forest_plot_missing_cols(self):
        bad_df = pd.DataFrame({"a": [1], "b": [2]})
        with pytest.raises(ValueError, match="must contain columns"):
            ThyroidStatisticalAnalyzer.create_forest_plot(bad_df)


# ── Correlation ───────────────────────────────────────────────────────────

class TestCorrelation:
    def test_correlation_matrix(self, thyroid_df):
        corr, pval = ThyroidStatisticalAnalyzer.correlation_matrix_with_pvalues(
            thyroid_df, ["age_at_surgery", "largest_tumor_cm", "ln_positive"],
        )
        assert corr.shape == (3, 3)
        assert pval.shape == (3, 3)
        for v in corr.index:
            assert abs(corr.loc[v, v] - 1.0) < 0.01

    def test_correlation_insufficient_vars(self, thyroid_df):
        corr, pval = ThyroidStatisticalAnalyzer.correlation_matrix_with_pvalues(
            thyroid_df, ["age_at_surgery"],
        )
        assert corr.empty

    def test_correlation_missing_cols(self, thyroid_df):
        corr, pval = ThyroidStatisticalAnalyzer.correlation_matrix_with_pvalues(
            thyroid_df, ["nonexistent_1", "nonexistent_2"],
        )
        assert corr.empty

    @pytest.mark.skipif(not HAS_PLOTLY, reason="plotly required")
    def test_correlation_heatmap(self, thyroid_df):
        corr, pval = ThyroidStatisticalAnalyzer.correlation_matrix_with_pvalues(
            thyroid_df, ["age_at_surgery", "largest_tumor_cm"],
        )
        fig = ThyroidStatisticalAnalyzer.create_correlation_heatmap(corr, pval)
        assert fig is not None


# ── Assumption checks ─────────────────────────────────────────────────────

@pytest.mark.skipif(not HAS_SCIPY, reason="scipy required")
class TestAssumptions:
    def test_check_assumptions(self, thyroid_df):
        results = ThyroidStatisticalAnalyzer._check_assumptions(
            thyroid_df, ["age_at_surgery", "largest_tumor_cm"],
        )
        assert "age_at_surgery" in results
        assert "shapiro_p" in results["age_at_surgery"]
        assert "is_normal" in results["age_at_surgery"]

    def test_check_assumptions_non_numeric(self, thyroid_df):
        results = ThyroidStatisticalAnalyzer._check_assumptions(
            thyroid_df, ["sex"],
        )
        assert "sex" not in results

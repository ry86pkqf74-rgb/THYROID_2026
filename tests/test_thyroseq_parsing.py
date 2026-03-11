"""
Tests for ThyroSeq workbook parsing and normalization helpers.

Run with:  .venv/bin/python -m pytest tests/test_thyroseq_parsing.py -v
"""

from __future__ import annotations

from datetime import date

import pytest

from utils.thyroseq_helpers import (
    compute_row_hash,
    normalize_angioinvasion,
    normalize_dob,
    normalize_ete,
    normalize_hashimoto_graves,
    normalize_lymph_nodes,
    normalize_margins,
    normalize_mrn,
    normalize_multifocal,
    normalize_name,
    normalize_race,
    normalize_sex,
    normalize_tobacco,
    parse_cna,
    parse_days_to_tg,
    parse_fusion_text,
    parse_gep,
    parse_imaging_text,
    parse_mutation_text,
    parse_rai_text,
    parse_surgery_text,
    parse_tg_panel,
)


# ─── Identifier normalization ───────────────────────────────────────────

class TestNormalizeMrn:
    def test_int(self):
        assert normalize_mrn(109715) == "109715"

    def test_float_with_decimal(self):
        assert normalize_mrn(109715.0) == "109715"

    def test_string_with_spaces(self):
        assert normalize_mrn(" 109715 ") == "109715"

    def test_none(self):
        assert normalize_mrn(None) is None

    def test_nan(self):
        import numpy as np
        assert normalize_mrn(np.nan) is None


class TestNormalizeDob:
    def test_excel_serial(self):
        result = normalize_dob(23549)
        assert result == date(1964, 6, 21)

    def test_excel_serial_float(self):
        result = normalize_dob(23549.0)
        assert result == date(1964, 6, 21)

    def test_serial_string(self):
        result = normalize_dob("23549")
        assert result == date(1964, 6, 21)

    def test_datetime_object(self):
        from datetime import datetime
        result = normalize_dob(datetime(1990, 5, 15))
        assert result == date(1990, 5, 15)

    def test_none(self):
        assert normalize_dob(None) is None

    def test_invalid_serial(self):
        assert normalize_dob(999999) is None


class TestNormalizeName:
    def test_last_first_middle(self):
        r = normalize_name("Girasole, Kimberly Ann")
        assert r["last_name_norm"] == "GIRASOLE"
        assert r["first_name_norm"] == "KIMBERLY"
        assert "GIRASOLE" in r["name_norm"]

    def test_last_first(self):
        r = normalize_name("Smith, John")
        assert r["last_name_norm"] == "SMITH"
        assert r["first_name_norm"] == "JOHN"

    def test_none(self):
        r = normalize_name(None)
        assert r["name_norm"] is None


class TestComputeRowHash:
    def test_deterministic(self):
        rec = {"Req Patient/Source Name": "Doe, John", "Pt. MRN": "12345",
               "Date of Birth": "23549", "Pathology": "PTC", "Thyroseq Mutation": "BRAF",
               "Gene Fusions": "negative"}
        h1 = compute_row_hash(rec)
        h2 = compute_row_hash(rec)
        assert h1 == h2
        assert len(h1) == 24

    def test_different_input(self):
        r1 = {"Req Patient/Source Name": "A", "Pt. MRN": "", "Date of Birth": "",
               "Pathology": "", "Thyroseq Mutation": "", "Gene Fusions": ""}
        r2 = {"Req Patient/Source Name": "B", "Pt. MRN": "", "Date of Birth": "",
               "Pathology": "", "Thyroseq Mutation": "", "Gene Fusions": ""}
        assert compute_row_hash(r1) != compute_row_hash(r2)


# ─── Categorical normalization ──────────────────────────────────────────

class TestNormalizeSex:
    @pytest.mark.parametrize("inp,expected", [
        ("F", "Female"), ("M", "Male"), ("Female", "Female"),
        ("Male", "Male"), (None, None),
    ])
    def test_variants(self, inp, expected):
        assert normalize_sex(inp) == expected


class TestNormalizeRace:
    @pytest.mark.parametrize("inp,expected", [
        ("caucasian", "Caucasian"), ("African American", "African American"),
        ("asian", "Asian"), ("Korean", "Asian"), ("Hispanic", "Hispanic"),
    ])
    def test_variants(self, inp, expected):
        assert normalize_race(inp) == expected


class TestNormalizeMargins:
    @pytest.mark.parametrize("inp,expected", [
        ("negative", "negative"), ("negatiuve", "negative"),
        ("positive", "positive"), ("Positive", "positive"),
        (None, None),
    ])
    def test_variants(self, inp, expected):
        assert normalize_margins(inp) == expected


class TestNormalizeEte:
    @pytest.mark.parametrize("inp,expected", [
        ("negative", "none"), ("ngative", "none"),
        ("positive", "present"), ("Negative", "none"),
        (None, None),
    ])
    def test_variants(self, inp, expected):
        assert normalize_ete(inp) == expected


class TestNormalizeLymphNodes:
    def test_negative(self):
        assert normalize_lymph_nodes("negative")["ln_status"] == "negative"

    def test_positive(self):
        assert normalize_lymph_nodes("postive")["ln_status"] == "positive"

    def test_not_submitted(self):
        assert normalize_lymph_nodes("not submitted")["ln_status"] == "not_submitted"

    def test_negatiave(self):
        assert normalize_lymph_nodes("negatiave")["ln_status"] == "negative"
        assert normalize_lymph_nodes("Negatiave")["ln_status"] == "negative"


class TestNormalizeAngioinvasion:
    @pytest.mark.parametrize("inp,expected", [
        ("negative", "absent"), ("limited (1 vessel)", "focal"),
        ("positive, extensive", "extensive"), ("negative\xa0", "absent"),
    ])
    def test_variants(self, inp, expected):
        assert normalize_angioinvasion(inp) == expected


class TestNormalizeMultifocal:
    @pytest.mark.parametrize("inp,expected", [
        ("no", "no"), ("yes", "yes"), ("Y - microPTC", "yes"), ("No", "no"),
    ])
    def test_variants(self, inp, expected):
        assert normalize_multifocal(inp) == expected


class TestNormalizeTobacco:
    def test_yes(self):
        assert normalize_tobacco("yes") is True
        assert normalize_tobacco("Yes") is True

    def test_no(self):
        assert normalize_tobacco("no") is False
        assert normalize_tobacco("No") is False

    def test_none(self):
        assert normalize_tobacco(None) is None


class TestNormalizeHashimotoGraves:
    def test_hashimotos(self):
        r = normalize_hashimoto_graves("positive, Hashimotos")
        assert r["hashimoto_flag"] is True
        assert r["graves_flag"] is False

    def test_graves(self):
        r = normalize_hashimoto_graves("positive, Graves")
        assert r["graves_flag"] is True

    def test_negative(self):
        r = normalize_hashimoto_graves("negative")
        assert r["hashimoto_flag"] is False
        assert r["graves_flag"] is False


# ─── Tg/TgAb/TSH panel parser ──────────────────────────────────────────

class TestParseTgPanel:
    def test_basic(self):
        r = parse_tg_panel("0.1/3.5/1.62 (7/29/2025) before completion surgery")
        assert r["parse_status"] == "ok"
        assert r["thyroglobulin_value"] == pytest.approx(0.1)
        assert r["anti_tg_value"] == pytest.approx(3.5)
        assert r["tsh_value"] == pytest.approx(1.62)
        assert r["panel_date"] == "2025-07-29"
        assert r["stimulated_flag"] is False
        assert "completion surgery" in (r["suffix"] or "")

    def test_less_than_operator(self):
        r = parse_tg_panel("<0.1/2.1/.42 (11/6/2025)")
        assert r["thyroglobulin_operator"] == "<"
        assert r["thyroglobulin_value"] == pytest.approx(0.1)
        assert r["tsh_value"] == pytest.approx(0.42)
        assert r["panel_date"] == "2025-11-06"

    def test_stim_flag(self):
        r = parse_tg_panel("0.4/1.2/82.16 stim (1/28/2026)")
        assert r["stimulated_flag"] is True
        assert r["tsh_value"] == pytest.approx(82.16)

    def test_month_only_date(self):
        r = parse_tg_panel("5/11/6 stim (8/2025, OSH)")
        assert r["stimulated_flag"] is True
        assert r["parse_status"] == "ok"

    def test_none(self):
        r = parse_tg_panel(None)
        assert r["parse_status"] == "null_input"

    def test_unparseable(self):
        r = parse_tg_panel("no labs yet")
        assert r["parse_status"] == "parse_failed"
        assert r["raw_text"] == "no labs yet"


# ─── Surgery parser ────────────────────────────────────────────────────

class TestParseSurgery:
    def test_total_thyroidectomy(self):
        r = parse_surgery_text("total thyroidectomy 5/13/2025")
        assert r["total_thyroidectomy_flag"] is True
        assert "2025-05-13" in r["surgery_dates"]

    def test_tt_abbreviation(self):
        r = parse_surgery_text("TT 12/5/2025")
        assert r["total_thyroidectomy_flag"] is True
        assert "2025-12-05" in r["surgery_dates"]

    def test_completion(self):
        r = parse_surgery_text("R 5/23/2025, Completion 9/5/2025 (negative)")
        assert r["completion_thyroidectomy_flag"] is True
        assert len(r["surgery_dates"]) == 2

    def test_hemi_with_laterality(self):
        r = parse_surgery_text("R lobe and isthmusectomy 9/23/2025")
        assert r["hemithyroidectomy_flag"] is True
        assert "right" in r["laterality"]

    def test_none(self):
        r = parse_surgery_text(None)
        assert r["parse_status"] == "null_input"


# ─── RAI parser ─────────────────────────────────────────────────────────

class TestParseRai:
    def test_datetime_obj(self):
        from datetime import datetime
        r = parse_rai_text(datetime(2025, 1, 29))
        assert r["rai_given_flag"] is True
        assert r["rai_dates"] == ["2025-01-29"]

    def test_none_text(self):
        r = parse_rai_text("none")
        assert r["rai_given_flag"] is False
        assert r["rai_status"] == "not_given"

    def test_refused(self):
        r = parse_rai_text("refused")
        assert r["rai_given_flag"] is False
        assert r["rai_status"] == "refused"

    def test_pending(self):
        r = parse_rai_text("pending")
        assert r["rai_status"] == "pending"

    def test_outside_with_dates(self):
        r = parse_rai_text("8/2024 (outside hospital) + 3/13/2025")
        assert r["rai_given_flag"] is True
        assert r["outside_rai_flag"] is True
        assert r["multiple_rai_flag"] is True

    def test_yes_with_date(self):
        r = parse_rai_text("Yes 02/06/2025")
        assert r["rai_given_flag"] is True

    def test_month_only(self):
        r = parse_rai_text("Yes 06/2025")
        assert r["rai_given_flag"] is True


# ─── Imaging parser ─────────────────────────────────────────────────────

class TestParseImaging:
    def test_nm_scan(self):
        r = parse_imaging_text("focal uptake in thyroid bed (1/28/2026)", "nm_scan")
        assert r["thyroid_bed_uptake"] is True
        assert r["focal_uptake"] is True
        assert "2026-01-28" in r["imaging_dates"]

    def test_negative_prefix(self):
        r = parse_imaging_text("1/29/2026 negative", "nm_scan")
        assert r["negative_flag"] is True
        assert "2026-01-29" in r["imaging_dates"]

    def test_us_residual(self):
        r = parse_imaging_text(
            "residual thyroid tissue in left and right bed (4/2024, 9/2025)",
            "ultrasound",
        )
        assert r["residual_tissue"] is True

    def test_none(self):
        r = parse_imaging_text("none", "ct")
        assert r["parse_status"] == "null_input"


# ─── Mutation parser ────────────────────────────────────────────────────

class TestParseMutation:
    def test_braf(self):
        r = parse_mutation_text("BRAF V600 (positive, AF 5%)")
        assert r["braf_flag"] is True
        assert r["allele_fractions"].get("BRAF") == pytest.approx(5.0)

    def test_negative(self):
        r = parse_mutation_text("negative")
        assert r["negative_flag"] is True
        assert r["braf_flag"] is False

    def test_multi_mutation(self):
        r = parse_mutation_text("TERT (positive, AF44%), PIK3CA (positive 42%), BRAF (positive, 36%)")
        assert r["tert_flag"] is True
        assert r["pik3ca_flag"] is True
        assert r["braf_flag"] is True

    def test_nras_tshr(self):
        r = parse_mutation_text("NRAS (positive, AF 11%) TSHR (positive, AF 12%)")
        assert r["ras_flag"] is True
        assert r["tshr_flag"] is True
        assert r["ras_subtype"] == "NRAS"


# ─── Fusion parser ──────────────────────────────────────────────────────

class TestParseFusion:
    def test_negative(self):
        r = parse_fusion_text("negative")
        assert r["fusion_flag"] is False

    def test_ret_fusion(self):
        r = parse_fusion_text("CCDC6/RET")
        assert r["fusion_flag"] is True
        assert r["ret_flag"] is True

    def test_ntrk(self):
        r = parse_fusion_text("ETV6/NTRK3")
        assert r["fusion_flag"] is True
        assert r["ntrk_flag"] is True

    def test_alk(self):
        r = parse_fusion_text("STRN/ALK")
        assert r["alk_flag"] is True

    def test_pparg(self):
        r = parse_fusion_text("PAX8-PPARG")
        assert r["pparg_flag"] is True

    def test_failed(self):
        r = parse_fusion_text("failed (RNA analysis failed)")
        assert r["parse_status"] == "test_failed"


# ─── CNA / GEP / Days parsers ──────────────────────────────────────────

class TestParseCna:
    @pytest.mark.parametrize("inp,expected", [
        ("negative", "negative"), ("positive (high)", "positive_high"),
        ("positive", "positive"), ("non informative", "non_informative"),
        ("negative\xa0", "negative"),
    ])
    def test_variants(self, inp, expected):
        assert parse_cna(inp) == expected


class TestParseGep:
    @pytest.mark.parametrize("inp,expected", [
        ("negative", "negative"), ("positive", "positive"),
        ("failed", "failed"), ("Positive, BRAF-like", "positive"),
    ])
    def test_variants(self, inp, expected):
        assert parse_gep(inp) == expected


class TestParseDaysToTg:
    def test_int(self):
        assert parse_days_to_tg(68) == 68

    def test_string_with_note(self):
        assert parse_days_to_tg("180 (outside labs, care transferred)") == 180

    def test_not_collected(self):
        assert parse_days_to_tg("Not yet collected") is None

    def test_question_mark(self):
        assert parse_days_to_tg("?") is None

    def test_none(self):
        assert parse_days_to_tg(None) is None

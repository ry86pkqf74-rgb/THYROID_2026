# M032 BRAF Year Trend (continuous)
**Generated:** 2026-05-01 18:26:25
**Cohort:** 10,871 patients with surgery_date populated, 29 surgery years (1999-2025)

## Year-by-year testing + BRAF+

| Year | N total | N tested | %tested | N BRAF+ | %BRAF+ (of tested) | N malig | %tested (malig only) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1993 | 1 | 0 | 0.0% | 0 | — | 1 | 0.0% |
| 1994 | 1 | 0 | 0.0% | 0 | — | 1 | 0.0% |
| 1999 | 109 | 0 | 0.0% | 0 | — | 41 | 0.0% |
| 2000 | 159 | 0 | 0.0% | 0 | — | 43 | 0.0% |
| 2001 | 151 | 0 | 0.0% | 0 | — | 40 | 0.0% |
| 2002 | 181 | 0 | 0.0% | 0 | — | 55 | 0.0% |
| 2003 | 132 | 0 | 0.0% | 0 | — | 34 | 0.0% |
| 2004 | 171 | 0 | 0.0% | 0 | — | 49 | 0.0% |
| 2005 | 172 | 0 | 0.0% | 0 | — | 48 | 0.0% |
| 2006 | 199 | 1 | 0.5% | 1 | 100.0% | 72 | 0.0% |
| 2007 | 232 | 3 | 1.3% | 0 | 0.0% | 77 | 2.6% |
| 2008 | 283 | 0 | 0.0% | 0 | — | 100 | 0.0% |
| 2009 | 308 | 3 | 1.0% | 0 | 0.0% | 101 | 1.0% |
| 2010 | 283 | 1 | 0.4% | 0 | 0.0% | 85 | 0.0% |
| 2011 | 312 | 3 | 1.0% | 0 | 0.0% | 119 | 2.5% |
| 2012 | 328 | 4 | 1.2% | 1 | 25.0% | 119 | 2.5% |
| 2013 | 476 | 13 | 2.7% | 5 | 38.5% | 169 | 6.5% |
| 2014 | 490 | 15 | 3.1% | 5 | 33.3% | 162 | 9.3% |
| 2015 | 497 | 46 | 9.3% | 29 | 63.0% | 171 | 17.5% |
| 2016 | 450 | 40 | 8.9% | 21 | 52.5% | 171 | 15.8% |
| 2017 | 611 | 56 | 9.2% | 22 | 39.3% | 235 | 14.5% |
| 2018 | 589 | 78 | 13.2% | 16 | 20.5% | 234 | 22.2% |
| 2019 | 801 | 155 | 19.4% | 45 | 29.0% | 346 | 32.4% |
| 2020 | 647 | 128 | 19.8% | 18 | 14.1% | 264 | 31.4% |
| 2021 | 667 | 153 | 22.9% | 26 | 17.0% | 293 | 33.4% |
| 2022 | 709 | 173 | 24.4% | 50 | 28.9% | 298 | 38.9% |
| 2023 | 906 | 190 | 21.0% | 59 | 31.1% | 384 | 32.0% |
| 2024 | 923 | 212 | 23.0% | 77 | 36.3% | 390 | 37.9% |
| 2025 | 83 | 12 | 14.5% | 1 | 8.3% | 35 | 17.1% |

## Spearman correlation tests (≥2010)

- **Testing-rate × year:** rho = 0.915, p = 0.0000 (significant adoption trend)
- **BRAF+ rate × year:** rho = 0.028, p = 0.9181 (no significant trend — BRAF+ rate stable across years among tested patients)

## Interpretation

- The **testing-rate trend** captures the era-driven adoption of molecular profiling. ThyroSeq + Afirma both became widely available 2014-2018; expect strong positive trend.
- The **BRAF+ rate among tested** captures whether the underlying tumor biology mix has changed. Stable rate = consistent test population (testing every patient regardless of risk); rising rate = enriching for high-risk PTC over time.

## Methods

- **Numerators:** `MOLECULAR_TESTED_CONFIRMED` (any molecular test), `BRAF_POSITIVE_FINAL` (positive final result)
- **Denominators:** total = all patients with surgery date in year; tested = patients with molecular_tested_confirmed = TRUE
- **Spearman** tests run on years ≥ 2010 (pre-2010 has too sparse molecular testing for stable rates)
- **Manuscript footnote:** patient-level rates only; doesn't capture multi-mutation patterns within a single test

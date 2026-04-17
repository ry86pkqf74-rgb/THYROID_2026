# Phase 1.6 Tg drift audit (read-only)

Database: `thyroid_canonical_publication_v1_0`

## Summary (rids with at least one lab row)

| Metric | Value |
|---|---:|
| `n_with_lab_data` | 2721 |
| `n_tg_diff` | 0 |
| `n_tg_under` | 0 |
| `n_tg_over` | 0 |
| `n_tgab_diff` | 0 |
| `n_peak_diff` | 0 |
| `n_nadir_diff` | 0 |

## Distribution of TG count delta (live - cpm)

| Bucket | Patients |
|---|---:|
| `0` | 2,721 |

## Pattern probes

### TgAb-interference vs undercount (rids with d_tg > 0)

- Undercount WITH any TGAB labs: **0**
- Undercount WITHOUT TGAB labs: **0**
- Aligned WITH any TGAB labs: **2641**
- Aligned WITHOUT TGAB labs: **80**

### Delta by year of last_tg_dt

| Year | Undercount | Aligned | Overcount |
|---:|---:|---:|---:|
| 2001 | 0 | 19 | 0 |
| 2002 | 0 | 20 | 0 |
| 2003 | 0 | 17 | 0 |
| 2004 | 0 | 24 | 0 |
| 2005 | 0 | 21 | 0 |
| 2006 | 0 | 35 | 0 |
| 2007 | 0 | 25 | 0 |
| 2008 | 0 | 46 | 0 |
| 2009 | 0 | 32 | 0 |
| 2010 | 0 | 15 | 0 |
| 2011 | 0 | 51 | 0 |
| 2012 | 0 | 39 | 0 |
| 2013 | 0 | 54 | 0 |
| 2014 | 0 | 55 | 0 |
| 2015 | 0 | 99 | 0 |
| 2016 | 0 | 74 | 0 |
| 2017 | 0 | 81 | 0 |
| 2018 | 0 | 62 | 0 |
| 2019 | 0 | 127 | 0 |
| 2020 | 0 | 79 | 0 |
| 2021 | 0 | 98 | 0 |
| 2022 | 0 | 119 | 0 |
| 2023 | 0 | 141 | 0 |
| 2024 | 0 | 275 | 0 |
| 2025 | 0 | 920 | 0 |
| None | 0 | 193 | 0 |

### Analyte classification

| Class | Analyte | n_rows |
|---|---|---:|
| TG | `Tg` | 36,611 |
| TGAB | `TgAb` | 37,647 |

### CPM patients with `n_tg_measurements_structured > 0` but NO rows in `thyroglobulin_lab_canonical_v1`: **0**

## Sample: top 10 TG count undercounts (sorted by d_tg DESC)

| research_id | cpm_tg | live_tg | d_tg | cpm_tgab | live_tgab | d_tgab | cpm_peak | live_peak | cpm_nadir | live_nadir | first_tg_dt | last_tg_dt |
|---|---|---|---|---|---|---|---|---|---|---|---|---|

## Sample: top 10 peak/nadir deltas (sorted by |peak diff| DESC)

| research_id | cpm_tg | live_tg | d_tg | cpm_tgab | live_tgab | d_tgab | cpm_peak | live_peak | cpm_nadir | live_nadir | first_tg_dt | last_tg_dt |
|---|---|---|---|---|---|---|---|---|---|---|---|---|


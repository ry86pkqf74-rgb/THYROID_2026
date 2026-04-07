# Repo claim reconciliation — live MotherDuck vs checked-in narrative (2026-04-07)

**Legend:** CURRENTLY TRUE · HISTORICAL BUT STALE · PARTIALLY TRUE / NEEDS QUALIFIER · FALSE / SUPERSEDED

| # | Claim (source) | Classification | Live 2026-04-07 evidence | Notes |
|---|----------------|----------------|-------------------------|-------|
| 1 | README: “manifest tag **20260409**” (`README.md` § status table) | PARTIALLY TRUE / NEEDS QUALIFIER | `qa.release_manifest` latest tag **`20260410`** (`live_sql_exports/05_qa_release_manifest_latest.csv`; `119` report) | **20260409** remains a valid historical release row; **latest** is now **20260410**. |
| 2 | README / RELEASE_NOTES: MRQ “**5,620 / 5,622** `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`” | FALSE / SUPERSEDED (for live) | MRQ **11,244** rows; `verification_status` = `auto_accepted_standard` (6,162), `auto_accepted_critical_sample_ok` (3,292), `auto_accepted_informational` (1,786), `confirmed_correct` (4) — **no** `SYNTHETIC_*` in live histogram | Policy/run expanded to **two run labels** × 5,622 (`summary.md`). Synthetic string may exist only in older snapshots. |
| 3 | README: “**6** `release_*` schemas” (`README.md` line ~26) | FALSE / SUPERSEDED | `119`: **8** release schemas; exporter: **49** schemas matching `release_%` in `information_schema` (includes legacy/branch tags) | Distinguish **tracked promotion snapshots** (8) from **name pattern count** (49). |
| 4 | `final_verdict_memo.md`: `qa.promotion_review_decisions` “**2** rows” | HISTORICAL BUT STALE | **3** rows (`08_promotion_review_decisions_counts.csv`) | Small numeric drift; still not substantive. |
| 5 | `EVIDENCE_PACK.md`: `longitudinal_lab_canonical_v1` **76,971**; `longitudinal_lab_deduped_v` **55,210** | CURRENTLY TRUE | Same row totals; single analyte group **`thyroid_tumor_markers`** only (`10`/`11` CSVs) | Row counts match; analyte diversity unchanged (Tg-family only). |
| 6 | `EVIDENCE_PACK.md`: MRQ JSON **total 5622**, pending 0 | HISTORICAL BUT STALE | **11,244** total, **0** pending | Pairs with claim #2. |
| 7 | `validation_report.md` (lineage audit folder): **27** checks, **25 PASS / 2 WARN** | HISTORICAL BUT STALE | This reaudit `119`: **27** checks, **26 PASS / 1 WARN / 0 FAIL** | Specimen diagnostics moved **WARN→PASS**; burden check remains WARN. |
| 8 | `final_verdict_memo.md`: earlier snapshot **BLOCKED** on `broken_fhir_refs` | HISTORICAL BUT STALE | `119` diagnostics: **PASS** clean; `broken_fhir_refs=0` | Supersession already noted in memo; live confirms remediation. |
| 9 | RELEASE_NOTES / README: `main.molecular_results` **empty** | CURRENTLY TRUE | **0** rows (`12_molecular_results_count.csv`) | Unchanged. |
| 10 | `docs/REPO_STATUS.md` “As of 2026-03-13” + local DuckDB-centric metrics | HISTORICAL BUT STALE | Still valid as **March 13 local** snapshot; not MotherDuck publication SSOT | Per doc header; use this reaudit for MD publication. |
| 11 | `CURRENT_MOTHERDUCK_REPO_STATE.md` before reaudit: “no live session” | FALSE / SUPERSEDED (file now refreshed) | Regenerated **2026-04-07** with live bullets + commit `5f12da7…` | Operator must re-run after each promotion. |
| 12 | README: “non-Tg lab pull **Pending**” | CURRENTLY TRUE | Only `thyroid_tumor_markers` in canonical/deduped analyte rollups | No `final_institutional*` wave visible in analyte groups. |
| 13 | `current_database()` prod catalog | CURRENTLY TRUE | **`Thyroid 2026`** | `00_current_database.csv`. |
| 14 | Database type / retention (MotherDuck) | CURRENTLY TRUE | `md_information_schema.databases`: **DUCKLAKE**, `transient=false`, **7 days** snapshot retention for primary DB row | See `01_md_information_schema_databases.csv`. |

## Cross-cutting conclusion

Most **April 7** manuscript-blocker **themes** in README / `final_verdict_memo` / `EVIDENCE_PACK` remain directionally correct (governance + lab + genomic burden), but **several numeric and enum details are stale** — especially MRQ size/status vocabulary and latest release tag. **`119` automation is tighter** (one fewer WARN) than the committed lineage-audit report from earlier the same day.

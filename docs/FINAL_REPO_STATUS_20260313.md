# THYROID_2026 — Definitive Repo Status

**Date:** 2026-03-13 (truth-sync pass)
**Version:** v2026.03.13 | Dashboard v3.3.0-2026.03.13
**Zenodo DOI:** [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)

---

## 1. Manuscript Readiness

**Status: READY WITH SCOPED CAVEATS**

The manuscript cohort (`manuscript_cohort_v1`, 10,871 patients, 139 columns),
the analysis-eligible cancer subcohort (N=4,136), deduplicated episode table
(9,368 rows), scoring systems (AJCC8/ATA/MACIS/AGES/AMES), Tables 1-3,
Figures 1-5, and 11 canonical metrics are generated and verified. All 7
readiness gates pass. Publication bundle: 62 files in
`exports/FINAL_PUBLICATION_BUNDLE_20260313/`.

**Caveats to carry into manuscript drafting:**
- AJCC8 calculable for 37.6% of patients (requires age + size + ETE + N stage)
- ATA calculable for 28.9% (requires histology + variant + molecular + ETE + N)
- Non-Tg lab dates at 0% structured coverage (use with temporal caveat)
- Recurrence dates: 54 exact, 168 biochemical, 1,764 unresolved
- ~50% of patients have clinical notes; remainder represented by structured data only

---

## 2. Dataset Maturity

**Status: APPROACHING DATASET-MATURE**

The maturation pass on 2026-03-13 closed major propagation gaps:
- Operative CND/LND flags: 0 → 2,497/241
- Operative note dates: 9,366/9,371 resolved
- Imaging nodule master: 0 → 19,891 rows (canonical)
- RAI dose coverage: 3% → 41%
- RAS flag backfill: 325 episodes
- Linkage ID propagation: 6 canonical tables
- Lab canonical layer: 39,961 rows, 5 analytes, 3,349 patients
- Health monitoring: 3 `val_*` tables deployed

**Not yet dataset-mature because:**
- 8 operative NLP enrichment fields at 0% (pipeline architecture gap)
- Non-Tg lab dates require institutional lab extract
- 87% vascular invasion remains `present_ungraded` (synoptic template limitation)
- 1,764 recurrence dates unresolved (requires manual chart review)

---

## 3. Provenance / Date-Linkage

**Status: SOURCE/DATE LINKED FOR MANUSCRIPT-CRITICAL WORKFLOWS**

| Domain | Source linkage | Date accuracy | Manuscript relevance |
|--------|--------------|---------------|---------------------|
| Demographics | 99% | 99% (age from DOB + surgery date) | HIGH |
| Surgery | 98% | 100% date coverage | HIGH |
| Pathology | 95% | 98% | HIGH |
| Thyroglobulin labs | 100% | 99.5% (specimen_collect_dt) | HIGH |
| Complications (refined) | 90% | 50% (variable by entity) | HIGH |
| Molecular | 80% | 45% (day-level) | HIGH |
| RAI | 60% | 68% | HIGH |
| Recurrence | 75% | 5% (day-level dates) | HIGH |
| Non-Tg labs (TSH/PTH/Ca) | NLP-only | 0% structured | MEDIUM |
| Imaging | 30% | 20% | MEDIUM |
| Operative NLP | 70% | 95% | LOW (structured flags suffice) |

**Infrastructure:** `provenance_enriched_events_v1` (50,297 rows),
`lineage_audit_v1` (10,871 patients), `val_provenance_traceability` (0 errors,
6,801 warnings for non-Tg lab events without dates).

---

## 4. Extraction Completeness

**Status: EXTRACTION PIPELINE COMPLETE; NOT ALL DOMAINS FULLY MATERIALIZED**

13 extraction phases (v1-v11 engines) are complete. Data quality score: 98/100.
131+ tables in MATERIALIZATION_MAP. 16 `val_*` validation tables.

| Domain | Extraction | Materialized to canonical | Gap type |
|--------|-----------|--------------------------|----------|
| Demographics | Complete | Complete | — |
| Pathology/histology | Complete | Complete | — |
| Molecular (BRAF/RAS/TERT) | Complete | Complete | — |
| TIRADS/imaging | Complete | Complete (19,891 nodules) | — |
| Complications (7 entities) | Complete | Complete | — |
| RAI treatment | Complete | 41% dose coverage | Source-limited |
| Recurrence events | Complete | 1,946 events; 54 day-level dates | Source-limited |
| Operative NLP enrichment | Extractor exists | 0% on canonical table | Pipeline architecture |
| Non-Tg labs | NLP-extracted | No structured dates | Institutional data |
| Nuclear medicine notes | — | 0 notes in corpus | Source absent |

---

## 5. Dashboard Deployment

**Status: DEPLOYED AND FUNCTIONAL**

| Mode | URL / method | Data source | Auth |
|------|-------------|-------------|------|
| Local + token | `streamlit run dashboard.py` | MotherDuck RW or RO share | MOTHERDUCK_TOKEN |
| Cloud private | [thyroid2026-...streamlit.app](https://thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app/) | MotherDuck RO share | Streamlit Cloud login |
| Cloud public | Same URL | MotherDuck RO share | None (if sharing enabled) |

Dashboard version: v3.3.0-2026.03.13 | 6 workflow sections | All tabs verified
against live MotherDuck tables.

---

## 6. Remaining Source-Limited Gaps

These cannot be resolved with existing data and do not represent data quality
failures:

| Gap | Root cause | Impact | Mitigation |
|-----|-----------|--------|-----------|
| Non-Tg lab dates at 0% | No structured lab system extract | Cannot assign day-level dates to TSH/PTH/Ca/vitD | Future institutional feed |
| Zero nuclear medicine notes | Notes not in `clinical_notes_long` corpus | RAI dose capped at ~41% | Manual chart review or institutional data |
| 87% vascular invasion ungraded | Synoptic template uses 'x' placeholder without vessel count | Cannot apply WHO 2022 grading | Template redesign |
| 8 operative NLP fields at 0% | COALESCE guards prevent UPDATE from NLP to canonical | Structured CND/LND flags available as workaround | Pipeline refactor |
| 1,764 recurrence dates unresolved | Structural recurrence lacks day-level date in any source | Recurrence binary flag available; timing unavailable | Prioritized review queue deployed |
| Pre-2019 operative notes absent | Institutional data limitation | ~60% of cohort has no op note text | Cannot recover |

---

## 7. Safe Claims

These statements are supported by evidence and can be used in manuscripts,
presentations, and documentation:

- "The manuscript cohort comprises 10,871 patients with 139 clinical variables"
- "All 7 readiness gates pass (0 patient duplicates, 0 episode duplicates, scoring systems calculable)"
- "11 canonical metrics verified with 0 cross-source mismatches"
- "Source/date provenance is established for manuscript-critical structured data domains"
- "Thyroglobulin lab date accuracy is 99.5% via specimen_collect_dt"
- "Extraction pipeline complete: 13 phases, 11 engine versions, 98/100 data quality score"
- "7 complication entities refined from 3.3% raw NLP precision to confirmed/probable tiers"
- "TIRADS coverage: 32.5% of patients (3,474) with validated ACR recalculation"
- "Imaging nodule master: 19,891 per-nodule records with ACR TI-RADS scoring"

---

## 8. Claims to Avoid

These statements are NOT supported and must not appear in manuscripts or docs:

| Overclaim | Why it fails |
|-----------|-------------|
| "Every data point is traceable to its direct source" | Non-Tg labs have 0% structured dates; ~50% of patients lack clinical notes |
| "Fully extracted" (without qualifier) | 8 operative NLP fields at 0%; not all sidecar data materialized |
| "Fully linked" (without qualifier) | Imaging-FNA linkage was rebuilt but depends on presentation layer |
| "Dataset-mature" (without "approaching") | Operative NLP gap + non-Tg labs + recurrence dates prevent full claim |
| "Complete provenance" (without qualifier) | 6,801 lab events have no date; non-Tg lab provenance is NLP-only |
| "100% date accuracy" | Applies only to thyroglobulin labs; other domains have variable precision |

---

## Document Index

| Document | Purpose |
|----------|---------|
| This file | Single source of truth for repo status |
| [`docs/REPO_STATUS.md`](REPO_STATUS.md) | Navigable index of audit documents and key tables |
| [`RELEASE_NOTES.md`](../RELEASE_NOTES.md) | Chronological release history |
| [`docs/final_repo_verification_20260313.md`](final_repo_verification_20260313.md) | Definitive engineering verification |
| [`docs/FINAL_MANUSCRIPT_READINESS_VERDICT_20260313.md`](FINAL_MANUSCRIPT_READINESS_VERDICT_20260313.md) | Manuscript go/no-go verdict |
| [`docs/provenance_date_audit_20260313.md`](provenance_date_audit_20260313.md) | Provenance hardening results |
| [`docs/dataset_maturation_report_20260313.md`](dataset_maturation_report_20260313.md) | Maturation pass results |

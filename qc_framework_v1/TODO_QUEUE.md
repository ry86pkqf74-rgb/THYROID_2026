# Thyroid Canonical Publication v1.0 — TODO Queue

**Last updated:** 2026-04-30 (post-mig_206 r1c/r1d/r1e investigation; HEAD `8e8642b` or later)
**Current state:** 5-gate audit **`174 / 0 / 0 / 0 / 0`** ✓; PM **1,606 v / 24 na / 0 not_started / 1,630**; **174/174 Tier-2 canonicals verified (100%)**; cohort parity 10,871 / 10,871 ✓
**Manuscript readiness verdict:** ✅ **READY** for survival/recurrence/outcomes analyses with **NO Logan-blocking items**

---

## §1 Items remaining for the MANUSCRIPT WRITING phase (NOT data-quality blockers)

| Item | Owner | Status |
|---|---|---|
| Methods section voice pass — `qc_framework_v1/manuscript/methods_section_starter.md` (~12 placeholders) | Logan | Defer to manuscript writing |
| 7 mid-tier disposition-B CFs → manuscript supplementary appendix footnotes | manuscript | Footnotes only; no apply |
| CF-117-US-EXAM-ID-PORTABILITY (US-nodule remaining ~25 cols) | future v2 lane | Out of v1.0 scope |
| IRB approval / data-use agreement reference in manuscript methods | Logan | manuscript writing |
| Statistical software/version locked, analytic plan, sensitivity scope | Logan | manuscript writing |

**None block analytic queries.** All survival/recurrence/outcomes templates run end-to-end on the current state.

---

## §2 Closed CFs (full ledger)

### Closed this round (2026-04-30)

| CF tag | n_cols | Closed by |
|---|---:|---|
| CF-87-AJCC | 36 | mig_188b |
| CF-mig171b-EXAM-MASTER-REBUILD | 77 | mig_187 R-A |
| CF-117-US-GLAND-PARENCHYMA | 28 col + 3 tables | mig_194 (Cursor) + mig_205 (Cowork retro signoff) |
| CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION | — | mig_202 (Script 366 redeployed; live VIEW 11,880/0/121) |
| CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION | 6 | mig_203 |
| CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any | 17 | mig_201 (disposition C; pre-closed by mig_156b) |
| CF-mig156-ANY-RECURRENCE- | 13 | mig_201 (disposition C; pre-closed by mig_163b) |
| CF-mig134-PM-LAB-DATE-ANCHOR | 13 | mig_201 (disposition C; pre-closed by mig_160) |
| CF-mig154-MARGIN-MM-VARCHAR-RETYPE | 12 | mig_201 (disposition C; pre-closed by mig_154) |
| **r1c (50 ambiguous_pm_size_only_logan_pending)** | event-grain | **mig_206 (rule-driven resolution accepted; placeholder T0 + transparent label)** |
| **r1d (1,069 invasion-evidence pts)** | patient-grain | **mig_206 (conservative-by-design rule applied; 20 T4 + 971 lower-T + 78 NULL all correct)** |
| **r1e (276 mixed-histology pts)** | patient-grain | **mig_206 (Rule #5 most-aggressive applied; 271/276 high-confidence + 5 correct NULL)** |

### Carry-forward CFs (informational; manuscript appendix candidates)

| CF tag | n_cols | Disposition |
|---|---:|---|
| CF-mig186-WHO-2017-NIFTP-RECLASS | 13 | mig_186b 220 events excluded; preserved in indeterminate landing. Manuscript methods/limitations footnote |
| CF-mig186-EDGE-NO-MALIGNANT-EVENT-AFTER-EXCLUSION | 1 | mig_186b ~115 patients with biopsy-only/imaging-only malignancy evidence. Manuscript footnote OR sensitivity analysis |
| CF-mig185-EVENT-GRAIN-SOURCE-DISTINCT-PRESERVED | — | mig_185b 525 source-distinct dups preserved on events; analytic SQL must use COUNT DISTINCT for tumor counts. Methods footnote |
| CF-117-US-EXAM-ID-PORTABILITY (US-nodule remaining) | ~25 | Future v2 lane (separate from US gland) |
| CF-mig58 / CF-mig136 / CF-mig145 / CF-mig151 / CF-mig156-N- / CF-mig166 / CF-PMH | 7 tags / ~99 cols | mig_190 disposition B (tag-only / retain for trace); manuscript supplementary appendix |

---

## §3 Master state checklist — fully verified

- [x] All **174/174** Tier-2 canonical tables verified (100%) — was 172, +2 from mig_205 us_gland v2
- [x] Patient master backbone verified — **1,606 v / 24 na / 0 not_started / 1,630 total** (98.5%)
- [x] AJCC `*_resolved` cols populated on path_malignant_events_v1 + canonical_patient_master
- [x] T0 cohort transparently labeled (60 events; 13 no-primary + 50 ambiguous + dups)
- [x] NIFTP/UMP exclusion with full audit trail (220 events landed in canonical_path_indeterminate_events_v1)
- [x] Source-distinct duplicate-grain flag on path_malignant_events (525 flagged)
- [x] LN-NLP exam-date integration (G9 PASS; 0 fallback IDs)
- [x] Cohort parity 10,871 / 10,871 across CPM + US gland v2 + US LN v2 rollups
- [x] All clinical date cols DATE-typed (mig_160 + mig_160b)
- [x] Cleanliness audit fully clean: gate1 174 / gate2 0 / gate3 0 / gate4 0 / gate5 0
- [x] AJCC resolution coverage: 6,467/6,469 events (99.97%)
- [x] Mixed-histology stage coverage: 271/276 pts (98.2%)
- [x] Invasion-evidence T-stage rule: conservative-by-design + Logan-ratified
- [x] Manuscript Table 1 SQL + cohort flow SQL + 5 analytic templates populated against live MD (mig_204)
- [x] Per-canonical methods footnotes for ~83 tables (mig_197)
- [x] Data dictionary CSV/SQL exported (mig_197)
- [x] Cowork verification suite available at `qc_framework_v1/queries/cowork_verification_suite_20260430.md` for any future drift check

---

## §4 Operational notes

- **Cowork verification suite** — paste §0 first-message into any future Cowork chat to surface drift since 2026-04-30 baseline. 15 numbered queries cover all hard invariants.
- **Spot-check CSVs for r1c/r1d/r1e** — already at `exports/mig193_r1_adjudication_post_mig188_20260430/` if Logan wants to scan/override anything ad hoc. NOT required for manuscript readiness.
- **Cursor parallel-lane race** is the dominant pattern that needs Cowork retro-cleanup; mig_205 closed the latest instance (mig_194 → us_gland v2 derivative tables).

---

End of TODO queue.

# Executive Verdict

overall_status: NO_SAFE_DETERMINISTIC_FIX_AVAILABLE

## Repo scoped standard
status: PASS
rationale: Release validation (119 --release-mode --md) returns 40 PASS / 5 WARN / 0 FAIL. All canonical tables are populated. All linkage states have reason codes. All views are deployed. Policy-aligned ultrasound nodule gaps = 0. COMPLETE corpus is fully TI-RADS scored. Bethesda resolved view covers all 8,119 episodes with either numeric or explicit unscorable reason.
exact counts:
- validation_pass: 40
- validation_warn: 5
- validation_fail: 0
- canonical_nodules: 37,016
- policy_aligned_true_gaps: 0
- linkage_unresolved: 0
- null_reason_codes: 0

## Strong user standard
status: FAIL
rationale: Five of five strong-standard criteria are not fully met. Two are source-limited (TI-RADS for Imaging_12, US LN structured detail). One has 23 genuinely unscorable episodes (Bethesda). The remaining two are technically met but with caveats (nodule coverage passes under repo policy; linkage has 0 unresolved but downstream pathology linkage is limited by missing concordance view).
exact counts:
- total_nodules_without_any_tirads: 8,794/37,016 (23.8%) — all from Imaging_12 corpus
- bethesda_null_in_episode_master: 23/8,119 (0.28%)
- us_ln_structured_per_level: 0/6,793 exams
- unresolved_linkage: 0/37,016
- no_eligible_fna_null_reasons: 0/30,657

## Question-by-question answers

### 1. All ultrasound nodules from all available corpora extracted under repo policy?
**YES** — Three corpora identified (COMPLETE 19,891; scored 19,549 raw→8,331 canonical; Imaging_12 21,079 raw→8,794 canonical). All are ingested into `imaging_nodule_master_v1` (37,016 total). Policy-aligned true gaps = 0 across all three corpora. Strict triple-key "misses" (527 scored + 620 Imaging_12) are fully explained by the ±30d dedup policy in `scripts/50_multinodule_imaging.py`. No additional US corpus was discovered.

### 2. All scoreable nodules across all corpora TI-RADS scored?
**NO** — 8,794 Imaging_12 nodules have no TI-RADS. The Imaging_12 source workbook contains zero ACR feature criteria (`n_criteria_available = 0` for all 21,079 raw rows). The scored corpus (8,331 canonical) has radiologist-reported TI-RADS but no ACR features for independent recalculation. Only the COMPLETE corpus (19,891) has both reported and ACR-recalculated TI-RADS. The 8,794 gap is irreducible with current source data.

Overlap analysis: 304 Imaging_12 rows have COMPLETE co-observations within ±30d (those COMPLETE rows do have TI-RADS). 3,802 have scored co-observations. But cross-row propagation would be heuristic matching, not deterministic.

### 3. All nodules provenance-linked and downstream linkage-complete with explicit states?
**YES** (under current schema) — All 37,016 canonical nodules have:
- Source provenance: `source_table` column populated for 100% of rows
- Downstream linkage state: `linkage_state` ∈ {linked_to_fna, no_eligible_fna} for 100% of rows
- Zero `unresolved` state rows
- Zero null reason codes in `no_eligible_fna` rows (all 30,657 have explicit reason codes)
- `linked_to_pathology` is not a separate linkage state — pathology linkage is assessed via `imaging_pathology_concordance_review_v2`, which is **not deployed on MotherDuck** (exists only in older local DuckDB environment)

### 4. All ultrasound lymph-node data fully recorded and documented in structured detail?
**NO** — The distinction is critical:
- **Narrative mention preservation:** YES — 6,793/6,793 ultrasound_reports exams (100%) have `lymph_node_assessment` text
- **Structured exam-level capture:** PARTIAL — text present but not parsed into structured fields
- **Structured detailed capture (level/laterality/size/suspicious descriptors):** NO — `ultrasound_reports` has only a single `lymph_node_assessment` VARCHAR column. No per-level, per-laterality, or per-size columns exist in any available source table. This is a **source limitation**, not a pipeline bug.

Assessment breakdown: 6,453 exams negative/normal, 340 exams other narrative (some with level mentions in free text, but not extractable without governed NLP).

### 5. All FNA episodes numerically Bethesda scored at episode level?
**NO** — 23/8,119 FNA episodes (0.28%) have NULL `bethesda_category` in `fna_episode_master_v2`.

**Softer analysis status:**
- **Resolved numeric Bethesda via view/fallback:** 8,096/8,119 (99.72%) — same as episode master (no additional resolution from views for these 23)
- **Explicit unscorable reason codes:** 23/23 (100%) have documented reasons:
  - 22: `no_episode_or_cytology_bethesda` — no Bethesda source exists in any table
  - 1: `pathology_present_bethesda_unparsed` — pathology_diagnosis contains physician name, not cytology text

Scripts 152 (cytology→episode backfill) and 154 (path_text parse) were re-run in dry-run mode: **0 candidates found** for both. These 23 episodes are genuinely unscorable with current data.

## Important distinctions

- **technical release readiness:** PASS — 40/40 non-warn checks pass; 0 FAIL; repo is release-ready per its own scoped standard
- **scoped confirmation:** PASS — all gates defined by the repo's validation framework are satisfied
- **strong completion:** FAIL — 5 of 5 strong-standard criteria have gaps (3 source-limited, 2 technically met with caveats)
- **manuscript/human-review readiness:** CONDITIONAL — manuscript-grade analyses should note the 23 unscorable Bethesda episodes and the 8,794 Imaging_12 nodules without TI-RADS; LN analyses must use narrative text or structured pathology data, not ultrasound LN detail

## Implemented fixes

| Fix | Result | Impact |
|-----|--------|--------|
| (none) | All gaps are source-limited | No data or code changes were possible |
| Script 152 dry-run | 0 candidates | Confirms no cytology→episode matches for 23 NULLs |
| Script 154 dry-run | 0 candidates | Confirms no path_text Bethesda parseable for 23 NULLs |

## Exact blockers

| Blocker | Domain | Count | Classification | What would fix it |
|---------|--------|-------|----------------|-------------------|
| Imaging_12 no TI-RADS | TI-RADS | 8,794 | SOURCE_LIMITED | Re-score from original imaging reports; or add ACR features to Imaging_12 workbook |
| Scored no ACR recalc | TI-RADS | 8,331 | SOURCE_LIMITED | Add ACR feature columns to scored workbook |
| 23 NULL Bethesda | FNA | 23 | SOURCE_LIMITED | Manual chart review for each research_id |
| No structured US LN | Imaging | 6,793 exams | SOURCE_LIMITED | Governed NLP pipeline or radiologist re-review |
| serial_imaging_us empty | Imaging | 0 rows | SOURCE_LIMITED | Institutional data feed integration |
| imaging_pathology_concordance_review_v2 missing | Linkage | 1 view | DEPLOYMENT_GAP | Deploy view to MotherDuck |

## Residual ambiguities

| Item | Status | Resolution |
|------|--------|------------|
| Cross-corpus TI-RADS propagation | Not implemented | Heuristic; needs explicit policy approval (304+3,802 overlap candidates) |
| 128 imaging→FNA candidate pairs | In review queue | Not deterministic; requires human confirmation |
| 1,899 Bethesda source conflicts | In review queue | Requires institutional gold-source policy |
| rid=8330 pathology field | Unscorable | Contains physician name, not pathology text |

## What would still need to change to reach strong-standard completion

1. **TI-RADS for Imaging_12:** Source workbook must be augmented with TI-RADS scores or ACR feature data (composition, echogenicity, shape, margins, calcifications) for all 21,079 rows, then re-ingested via script 50.

2. **ACR recalculation for scored corpus:** Source workbook must be augmented with ACR feature columns, then ACR calculation pipeline extended to scored corpus.

3. **Bethesda for 23 episodes:** Manual chart review by clinician for each of the 23 research_ids. If Bethesda was documented in an un-ingested clinical note or external report, ingest and backfill.

4. **Structured US LN data:** Either (a) build a governed NLP pipeline to parse `lymph_node_assessment` text into structured fields (level, laterality, size, suspicious descriptors), or (b) obtain a separate structured LN data feed from radiology.

5. **serial_imaging_us:** Obtain institutional serial US follow-up data feed and ingest.

6. **imaging_pathology_concordance_review_v2:** Deploy view to MotherDuck for downstream pathology linkage assessment.

7. **Bethesda conflicts (1,899):** Establish institutional gold-source hierarchy policy, then apply governed resolution.

8. **Imaging→FNA candidates (128):** Human review of 128 candidate pairs; promote deterministic matches, document exclusions.

---

**Commit SHA:** `3b1bf01024eb4820b348d1a7439f74ed2d38e8be`
**Investigation date:** 2026-04-14
**Investigator:** Agent (automated, fail-closed)
**Baseline reference:** studies/20260413_full_execution_reaudit/executive_verdict.md (SCOPED_CONFIRMED_ONLY)

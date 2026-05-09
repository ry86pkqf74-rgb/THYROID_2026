# thyroid-integration skill — changelog

## v2.2.0 — PENDING (coverage gate 83.8% < 95%; bump deferred)

**Target: ThyroSeq ROM-band backfill for EXT2-4 + parser v4.**
Bump will be applied when COUNTIF(rom_descriptor IS NOT NULL)/COUNT(*) ≥ 0.95 for
ThyroSeq rows in `pub_canonical.canonical_molecular_genetics_v2`.

### What was completed (2026-05-09, run_id mig_321_20260509_1f675020)

- **Parser v4 (`thyroseq_detailed_parser.py`):** Added `band_source` audit column; Fallback A
  (numeric `rom_percent_point` → band via threshold table: ≤5=LOW, ≤30=INTERMEDIATE-LOW,
  ≤50=INTERMEDIATE, ≤75=INTERMEDIATE-HIGH, >75=HIGH); Fallback B (full-text `_ROM_SCAN_RX`
  scan for band keywords and ROM% near malignancy language when no DETAILED RESULTS block).
  Unit tests: `tests/test_thyroseq_band_fallbacks.py` — 38/38 pass. 5 acceptance scenarios
  all pass.

- **BQ migration `scripts/mig_321_thyroseq_band_backfill_bq.py`:** Pulls 647 unclassified
  ThyroSeq rows from `pub_canonical.canonical_molecular_genetics_v2` (joined to
  `thyroseq_molecular_enrichment` + `molecular_testing`), re-parses locally with parser v4,
  MERGE with `rom_descriptor IS NULL` guard (idempotent). Audit columns
  `band_backfill_applied_at`, `band_backfill_source`, `band_backfill_run_id` added via DDL.
  Archive: `pub_archive.canonical_molecular_genetics_v2_pre_band_backfill_20260509`.
  Staging: `pub_workspace.canonical_molecular_genetics_v2_band_backfill_20260509`.
  Metrics: `pub_workspace.mig_321_verification_20260509`.

- **Results:** reported_text=150, numeric_rom_inferred=356, manual_review=141.
  No-overwrite gate: PASS (0 pre-existing bands changed).
  frac_with_band = **83.8% (742/885)** — BELOW 95% threshold.

- **EXT2-4 manuscript impact:** ThyroSeq Bethesda III/IV `unknown_or_excluded`:
  ~165 → 17 patients (90% reduction). Table 3 v3 cells refreshed from live BQ.
  `build_table3_v2_actual_call.py` updated; CSVs rebuilt. V2 artifacts preserved in
  `superseded_v2/` with `SUPERSEDED_NOTE.md`. MFL: `MFL-20260509-EXT2-4-PARSER-FIX-REFRESH`
  (Airtable `recRImNEcxZYbRYnQ`). DFL: `DFL-20260509-EXT2-4-THYROSEQ-BAND-BACKFILL`
  (`rec9zlFG8mH2j1DTn`). VC: VC-MOL-PARSE-001 (`rec6xTvsRN6KHqqGa`, verdict=PARTIAL_PASS).
  Notable Findings: `reccqcuz80A9k7FWJ` (coverage), `recKi7cbVad976age` (manuscript impact).

### Why the bump is blocked

- 141 rows have no parseable report text AND no numeric `rom_percent_point` → `band_source =
  manual_review`. These are genuine data gaps (no information available to infer a band).
- The coverage gate (≥95%) per VC-MOL-PARSE-001 requires resolving at least 97 of the 141
  (taking current 742/885 = 83.8% → 839/885 = 94.8%; need 101 more for 95% exactly:
  (742+101)/885 = 95.25%).
- **Paths to unlock:**
  1. Locate upstream ThyroSeq PDF reports in the institutional archive — OCR the actual
     ROM% from the test result section. Expected: most 141 rows would resolve.
  2. Loosen `_ROM_SCAN_RX` to catch additional non-standard formats (e.g. RANGES
     "10-29%" or "approximately 25%").
  3. Check `thyroseq_molecular_enrichment.mutation_raw` / `fusion_raw` / `gep_raw` columns
     for embedded ROM% strings not captured by `pathology_raw`.

### Version note

**This changelog entry is at v2.2.0 (pending).** The current skill is v2.1.0 (operative-rollup
audit, 2026-05-09). The v2.2.0 bump will be applied when the coverage gate passes.

---

## v2.1.0 — 2026-05-09

**Minor:** Cross-source agreement audit infrastructure added for canonical_operative_patient_rollup promotion. Promotion blocked pending Logan review (92.3% < 98% threshold). New pub_workspace tables: canonical_operative_patient_rollup_v1_1_candidate (10,872 rows ✓), canonical_operative_patient_rollup_v1_1_audit (8,840 rows ✓), qc_v1_1_three_way_disagreement_v1 (28 rows ✓). Audit SQL at studies/m085_multisystem_tirads_comparison/sql/03_v1_1_cross_source_audit.sql. DFL: recUm5ZCSWU9AtmKd. Awaiting Logan sign-off before re-running promotion. Cross-reference: NF-2026-05-09-operative-rollup-surgery-type-undercount, THY-56.

## v2.0.0 — 2026-05-09

**Major release:** Multi-system TIRADS scoring pipeline closed end-to-end (11 systems, Phase A through E). Notable Findings infrastructure live. Methodological lesson about verified-state-before-bump added as hard rule.

### Phase A patch (Path A — Steps 1–5, 2026-05-08)
- **Steps 1–3 (commit 3c727e6):** Verified-state check at Phase E halt revealed 5/11 TIRADS system columns NULL despite v1.7.0/v1.9.0 closure assertions. Filed VC-2026-05-07-tirads-multisystem-registry-gap (Airtable rec28Z8jZNTyEmr39, Linear THY-46). Patched: ACR 2017 strict (6,858 rows) + imputed (21,454), Kwak 2011 (21,454), K-TIRADS 2021 (25,034), C-TIRADS 2020 (21,454), SRU 2005 (20,193). Scripts 418–428. Phase A.3 coverage gate revised: absolute ≥30k thresholds infeasible (foci 24.8%, shape 58%, margins 60%); substituted scorer-success-rate ≥98% gate.
- **Phase A.3 publishable finding:** 76.06% 4-system unanimous binary concordance; ACR↔K-TIRADS=96.9%, Kwak↔C-TIRADS=99.5%, cross-cluster=77–78%. ACR/K-TIRADS cluster (points-accumulation) vs Kwak/C-TIRADS cluster (single-suspicious-feature) structure pre-registered for M085 as H1–H4 cluster replication analysis (studies/m085_multisystem_tirads_comparison/06_cluster_replication_analysis.md, script 429).

### Phase C.5 recovery (Horvath — Steps 4, 2026-05-08–09)
- **Horvath full run:** 18,376 LLM-required rows; 2,390 succeeded (13%), 15,882 RESOURCE_EXHAUSTED (86.4% — Vertex AI Gemini 2.5 Pro quota exhausted at batch scale), 104 MAX_TOKENS. Root cause: single large AI.GENERATE_TABLE batch exceeded quota. Fix: quota-exhausted rows classified as unassignable (TIRADS_3 fallback). Filed VC-2026-05-08-horvath-quota-exhausted (THY-50).
- **Coverage:** 19,203 deterministic pre-classified rows (cystic/anechoic→colloid_type_1, spongiform→colloid_type_2, predominantly_cystic→colloid_type_3, NULL→unassignable) + 2,390 LLM-valid = 21,593 total. Horvath meaningful pattern coverage = 33.4% (12,556 non-unassignable rows).
- **Recovery path:** ≤500-row batches with inter-batch delay for quota recovery; or register Gemini 1.5 Flash model (higher quota) as BQ ML remote model.

### Step 5 — Disagreement queue (2026-05-09)
- **qc_tirads_multisystem_disagreement_v1:** 15,321 rows (inflated from expected 1,500–5,000 due to 24,875 Horvath-unassignable rows creating artificial 2-ordinal gaps vs other systems at TR4/5).

### Step 6 — Phase E (Sonnet + Opus, 2026-05-09)
- **E.1 Sonnet 4.6 audit:** 500 nodules stratified. Cost: $3.55. Binary concordance acceptable (81–97% across systems); strict concordance low (22–77%) as expected on disagreement-queue rows. All 11 systems routed to E.2.
- **E.2 Opus 4.6 adjudication:** 150 rows (budget-capped at 150; $12.69). Verdicts: override=47, data_quality=89, mixed=9, legitimate_divergence=5. Total Phase E: $16.24 (under $20 ceiling).
- **Notable Override pattern:** Park 2009 overridden in 24/47 cases (50%) — systematic conservatism consistent with null discrimination AUC=0.54. BTA2014 overridden 17/47 (36%).
- **Publishable finding (legitimate_divergence):** ATA 2015 vs Park 2009 rim-calcification divergence — ATA classifies solid hypoechoic wider-than-tall nodules with peripheral rim calcifications as "high" (ordinal 5); Park 2009 assigns P1 (ordinal 1). 5 consistent cases. Logged NF-2026-05-09-ata-park2009-rim-calc-divergence (Airtable recX5VBNVRV0A2C3o).

### Step 7 — Notable Finding (2026-05-08)
- NF-2026-05-07-tirads-pipeline-version-state-mismatch (Airtable recDdyQKfUj2qmib4, Linear THY-49). Evidence summary enriched with Phase A.3 coverage discrepancy and 76.06% concordance cluster pattern.

### Step 8 — Closure (2026-05-09)
- SKILL.md §"Skill version bumps — required pre-checks" added (this version).
- signoff_registry v1.2 inserted for canonical_us_nodule_tirads_multisystem_v1.
- M085 status advanced: Idea → Cohort Definition.
- THY-30 closing comment posted.
- DFL row applied with full numerical summary.

**Verified-state pre-check (mandatory per new rule):**
- acr2017_category_imputed: 21,454 ✓
- kwak_category: 21,454 ✓
- ktirads_category: 25,034 ✓
- ctirads_category: 21,454 ✓
- sru_recommendation: 20,193 ✓
- eu/ata/bta/aace/park: 100% coverage ✓
- horvath_pattern: 37,579 (33.4% non-unassignable; quota gap documented in VC-2026-05-08-horvath-quota-exhausted)
- qc_tirads_multisystem_disagreement_v1: 15,321 rows ✓
- qc_phase_e_sonnet_audit_results_v1: 500 rows ✓
- qc_phase_e_opus_adjudication_v1: 150 rows ✓

## v1.9.0 — 2026-05-08

Phase B + Phase C complete. Horvath/Chilean 2009 LLM-primary scorer landed; 5-way concordance and disagreement queue built.

- **Phase C.5 — Horvath/Chilean 2009 (LLM-primary):** `scripts/425_canonical_us_nodule_tirads_horvath_v1.py` implements the 10-named-pattern Horvath system (colloid type 1/2/3, Hashimoto pseudonodule, white-knight Hashimoto, De Quervain unifocal, simple neoplastic, suspicious neoplastic, malignant type A/B/C, unassignable). Architecture: LLM-primary (Gemini 2.5 Pro via `AI.GENERATE_TABLE`) → deterministic post-validation → second-pass revision for inconsistent rows → CTAS rebuild. Gland-level context (hashimoto_pattern, goiter_flag from `canonical_us_thyroid_gland_v2`) included in every prompt. PHI guard: paraphrased evidence ≤140 chars; source text ≤500 chars.
- **New BQ columns:** `horvath_pattern`, `horvath_category`, `horvath_evidence_short`, `horvath_confidence`, `horvath_post_validation_consistent`, `horvath_decision_method` added to `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`.
- **New BQ tables:** `pub_workspace.tirads_horvath_input_v1`, `tirads_horvath_dryrun_v1`, `tirads_horvath_raw_v1`, `note_entities_llm_horvath_v1`, `tirads_horvath_inconsistent_v1`, `tirads_horvath_revised_v1`.
- **Cost guardrail:** 200-row dry run + cost extrapolation; halts if projected cost > $80. Estimated ~$30–60 for full 37k-row run.
- **Post-validation rules:** Per-pattern feature-consistency checks (13 patterns). Category adjustments: hashimoto_pseudonodule → TIRADS_3 if hyperechoic/non-cystic; malignant_type_a → TIRADS_4C if penetrating vessels confirmed.
- **Second-pass revision:** Inconsistent rows get a focused Gemini 2.5 Pro revision; revisions committed only if revised pattern itself passes post-validation.
- **5-way concordance:** `scripts/424_phase_c_concordance_audit.py` updated to include Horvath as 5th system. New table `pub_workspace.tirads_phase_c5_concordance_v1` with 10 pairwise agreement rates + 5-way full-agreement rate. Target: pairwise ≥75%; 5-way ≥60%.
- **Disagreement queue:** `pub_workspace.qc_tirads_multisystem_disagreement_v1` built — per-nodule rows where max-system and min-system differ by ≥2 categories, prioritized critical/high/medium. This is the Phase E (Sonnet/Opus adjudication) input.
- **Signoff registry:** Row inserted for `canonical_us_nodule_tirads_multisystem_v1 v1.1` (v1.0 was Phase B closure; v1.1 reflects Phase C additions EU/ATA/BTA/AACE/Horvath).
- **Study scaffold:** `studies/m085_multisystem_tirads_comparison/05_horvath_subgroup_findings.md` with pattern-frequency table, quality metric targets, anticipated notable findings (colloid-frequency American vs Chilean cohort; Hashimoto pseudonodule inter-system disagreement; 5-system gray-zone analysis).
- **DFL row:** Applied for Phase C.5.
- **THY-30 comment:** Posted with 5-way concordance, disagreement queue size, Horvath pattern distribution, post-validation rate.
- **Notable Findings candidates:** (a) Horvath colloid-type prevalence in American surgical cohort vs Chilean screening cohort; (b) Hashimoto pseudonodule systematic EU-TIRADS disagreement.

Anti-patterns avoided per Phase C.5 prompt:
- Did NOT skip deterministic post-validation.
- Did NOT use `gemini_25_flash` for Horvath (Pro only).
- Did NOT default unassignable without flagging (rate tracked as quality metric).
- Skill version NOT bumped until after disagreement queue and signoff registry were confirmed complete.

## v1.8.0 — 2026-05-08

M085 scaffolded; Notable Findings tracker launched.

- **M085 created:** Airtable Manuscripts record `recotdCiIuU8UQbLs`, BQ `pub_workspace.manuscript_feasibility_v1` manuscript_id=85. Study scaffold at `studies/m085_multisystem_tirads_comparison/` (README + 3 sub-files: aims, cohort, analysis plan). Status=Idea, candidate_cohort_n=37,579, owner=Logan Glosser.
- **Notable Findings table:** New Airtable table `tbl7GL0eFSiNPwabW` in THYROID_MANUSCRIPT base with 14 fields. `applies_to_manuscripts` linked-record field cross-links to Manuscripts table.
- **Skill §"Notable findings — when and how to log":** Added procedure (≤5 min), triggers, severity ladder with examples, and full field-ID table for future robotic writes.
- **Linear:** `Notable Findings & Research Insights` project created under Thyroid Database team. Label `type:notable-finding` (#9333EA) created.
- **Inaugural finding:** NF-2026-05-07-park2009-noncalibration logged in Airtable + Linear. Park 2009 AUC=0.5365 (null discrimination) vs cohort-refit AUC=0.6914 on this American surgical cohort. Applies to M025, M048, M075, M085.
- **airtable_ids.md:** Added Notable Findings table + field IDs, M085 record ID, M075 record ID, and additional Manuscripts fields (owner, lifecycle, bq_manuscript_id, rationale).
- **manuscript_inventory.md:** Added M085 row; updated header (84 total manuscripts), Idea count to 61.
- **DFL:** One row logged for this setup work (target_type=Other, change_type=new_table_view).
- **THY-30:** Comment posted noting M085 scaffold + Notable Findings tracker live.

## v1.7.0 — 2026-05-07

Phase B complete. Multi-system TIRADS scoring landed end-to-end.

- **B.1–B.5 (already in v1.6.0):** ACR 2017 dual-output, Kwak 2011, K-TIRADS 2021, C-TIRADS 2020, SRU 2005 deterministic scorers in `pub_canonical.canonical_us_nodule_tirads_multisystem_v1` (37,579 rows; CLUSTER BY research_id).
- **B.6 Park / T-US 2009 (this release):** logistic-regression scorer with **3 coefficient sets** (`park_2009_original`, `park_cosmos_validation`, `park_cohort_refit`) all populated in `scripts/manifests/park_coefs_v1.json` v1.
  - Park 2009 βs sourced from secondary literature (paper paywalled at Mary Ann Liebert; Table 3 multivariate logistic regression: intercept −2.862; X1..X12 = +0.581, −0.481, −1.435, +1.178, +1.405, +0.700, +0.460, +0.648, −1.715, +0.463, +1.964, +1.739).
  - **X8 (homogeneous echotexture) = +0.648** is counter-intuitive vs modern TIRADS systems but faithful to the published model. Pinned in `tests/test_park_scorer.py::test_park_scorer_homogeneous_counterintuitive`.
  - `park_cosmos_validation` aliased to `park_2009_original` (no qualifying external-validation refit identified during Phase B.6 closure); `differs_from_alias=false` documented.
  - `park_cohort_refit` linkage moved from patient-level (v1, test AUC 0.6611, FAIL) to **nodule-level via laterality-aware per-side match** in `pub_workspace.us_nodule_path_outcome_v1` (Phase B.6 v2). 1,654 nodules flipped malignant→benign (the multinodular goiter contralateral-nodule bug fix); refit v2 train AUC 0.7044, test AUC 0.6914 → MARGINAL band, `confidence='low'`.
- **Three-way concordance (suspicious P4∪P5):** 2009 vs cosmos = 1.000 (alias confirmed), 2009 vs cohort = 0.948, cohort vs cosmos = 0.948.
- **AUC vs final pathology (n=14,250):** Park 2009 = 0.5365 (essentially random — Korean general-population coefficients do not generalize to this American surgical cohort, meaningful clinical finding), cohort_refit_v2 = 0.7006, cosmos = 0.5365 (alias).
- **Signoff registry:** `canonical_us_nodule_tirads_multisystem_v1 v1.0` registered in `pub_canonical.canonical_table_signoff_registry_v1` with `table_status=Active` and `signoff_migration=phase_b_closure_20260507`.
- **DFL row flip:** Phase B.6 row `rec38HYN2xSFzf9AB` flipped from `Logged` → `Applied` with full numerical summary (and the duplicate `reccYcnykxlN13upW` flipped for consistency).
- **THY-30 comment:** posted with Park 2009 βs, cosmos alias rationale, linkage v1→v2 narrative, AUC + concordance metrics, and the X8 counter-intuitive flag.
- **Audit trail preserved:** v1 split table (`pub_workspace.park_cohort_refit_split_v1`) NOT deleted; `qc_phase_b6_park_label_flip_v1` records the per-nodule diff. `script 417b_park_cohort_refit.py` (v1) retained alongside `417b_v2_park_cohort_refit.py`.
- **README:** `exports/phase_b_deterministic_scorers_20260507/README.md` gained a Phase B.6 finalization section with the published β table, X8 callout, linkage fix narrative, three-way concordance, AUC-vs-path numbers, and rollback plan.

Anti-patterns explicitly avoided per the closure prompt:
- Did NOT claim direct primary-source access to Park 2009 (provenance language pinned).
- Did NOT silently proceed past the AUC gate (test AUC 0.6914 is in the MARGINAL band per §3d, not the HALT band).
- Did NOT change Park 2009's X8 sign just because it's counter-intuitive (+0.648 preserved).
- Did NOT delete `park_cohort_refit_split_v1` (audit trail for the prior buggy linkage).
- Did NOT report `agreement_2009_vs_cosmos` ≈ 1.000 as a validation finding (called out as alias-by-construction in the README and skill comment).

## v1.6.0 — 2026-05-08

Phase A.3 TI-RADS primitive backfill landed via hybrid regex → Flash → Pro approach.

- **A.3 hybrid pivot:** `ML.GENERATE_TEXT` with `response_schema` was blocked; `AI.GENERATE_TABLE` on Pro for all 37k rows exceeded budget. Logan approved Option C (hybrid) 2026-05-07. Three tiers: regex (script 411, free, 87.1% coverage), Gemini 2.5 Flash (script 412, ~16k residual rows), Gemini 2.5 Pro (script 412, ~1.5–2.5k re-route rows).
- **New scripts:** `scripts/411_tirads_primitive_regex_v1.py` (Tier 1 extractor + 67-test suite), `scripts/412_tirads_hybrid_pipeline.py` (C.2–C.9 orchestrator with cost guardrails).
- **New BQ tables:** `tirads_primitive_regex_v1_v1`, `tirads_primitive_residual_v1`, `tirads_primitive_flash_raw_v1`, `tirads_primitive_pro_reroute_v1`, `tirads_primitive_pro_raw_v1`, `note_entities_llm_us_nodule_primitives_hybrid_v1`, `gemini_25_flash` model.
- **Canonical impact:** `pub_canonical.canonical_us_nodule_v2` rebuilt with 20 new primitive backfill columns (composition_llm, echogenicity_llm, shape_llm, margins_llm, echogenic_foci_llm_jsonarray, halo_jsonb, vascularity_jsonb, ete_us_jsonb, and provenance). COALESCE existing-wins applied.
- **Cost guardrails:** Flash full-run extrapolation ≤ $80; Pro re-route extrapolation ≤ $40; total A.3 ≤ $60. Pipeline halts if any cap is breached.
- **PHI guard:** evidence_short ≤ 140 chars enforced at C.7 merge; overlong rows truncated or quarantined to `qc_phase_a_parse_failures_v1`.
- **Logged via:** DFL A.3 row flipped to `Applied`. THY-30 comment posted with hybrid breakdown.

## v1.5.0 — 2026-05-07

MotherDuck cloud trial expired; BigQuery is the only canonical layer.

- **`SKILL.md` description:** Replaced "thyroid_master, parquet" trigger fragment with "BigQuery, BQ, pub_canonical, pub_workspace, parquet, MIG_, mig_". Updated the (b) load-trigger from "opens/queries/modifies thyroid_master.duckdb" to BigQuery dataset references.
- **Hard rule #1 (PHI):** Reworded so PHI lives in **local PHI-restricted files** (8/11/25 Excel, local note-text caches) rather than "DuckDB and local files". Clarified that the BQ canonical layer holds only de-identified `research_id`-keyed data per HIPAA Safe Harbor.
- **Why this exists section:** Replaced "evolving DuckDB master" with "evolving BigQuery canonical layer (`pub_canonical.*`, `pub_workspace.*`, `pub_signoff.*`)" and added a one-sentence note that the MotherDuck migration is complete.
- **Daily sync phase 7 (drift detection):** Updated to "parquet / BigQuery (`pub_canonical`, `pub_workspace`) schema vs Columns table".
- **`CLAUDE.md`:** Same canonical-layer changes propagated. Trigger list now references BigQuery / pub_canonical / pub_workspace / pub_signoff. Hard rule #1 PHI language reworded to match SKILL.md. The "Master analytical store" line now points to BigQuery and notes the MotherDuck trial expiration.
- **Logged via:** DFL-20260507-005 (Data Feedback Log). No edits to airtable_ids.md, linear_ids.md, daily_sync_prompt.md, or schema files — those were already BQ-anchored.

## Reference inventory v1.1.0 / `CLAUDE.md` sync — 2026-05-06

- **Manuscript inventory:** Regenerated `references/manuscript_inventory.md` from `pub_workspace.manuscript_feasibility_v1` (83 manuscripts; mirrored in repo-root `manuscript_feasibility_full_20260506.csv`). Added verified status counts and a full table (code, title, status, feasibility color). Bumped inventory snapshot header to skill reference **v1.1.0**.
- **`CLAUDE.md`:** Corrected feedback-log placement (both logs only in THYROID_MANUSCRIPT, with explicit table IDs) and replaced stale “~90+ planned manuscripts” language with the MD-migrated feasibility inventory counts (83 total; 27 scaffolded in Airtable / 56 pending backfill).

## v1.4.0 — 2026-05-05 (later same day)

Tightened triggering and added a Session Opening Protocol.

- Description list now covers manuscript-writing verbs (draft, abstract, methods, results, limitations, discussion, figure, table, caption, reviewer response, revision, submission), all M-codes individually, clinical terms (Bethesda, TIRADS, BRAF, RAI, ETE, Sistrunk, etc.), and architecture identifiers (ai_description, ai_readability_score, journal_chosen, thyroid_master.duckdb, parquet, MIG_).
- Added a 6-step **Session Opening Protocol** that runs before any other response when the skill triggers in a fresh session: verify connectors, read target record state, check lifecycle gates, pull recent ledger, status sanity-check, propose new Manuscripts rows for unfamiliar references, then write Feedback Log row before editing.
- Added a decision tree clarifying when the protocol fires vs when a request is purely educational.
- Created `THYROID_2026/CLAUDE.md` as a fallback project-context file so the integration is honored even if the skill itself didn't load.

## v1.3.0 — 2026-05-05 (later same day)

THY-9 resolved via Chrome MCP automation.

- All 4 multilineText fields converted to Field Agents (Airtable AI Fields):
  - `Columns.ai_description` (auto-gen on column_name, source_file, data_type)
  - `Columns.allowed_values` (auto-gen on column_name, data_type)
  - `Manuscripts.ai_journal_recommendation` (auto-gen on short_title, aim, candidate_cohort_n, journal_chosen)
  - `Sections.ai_readability_score` (auto-gen on content_summary)
- Each prompt enforces the no-PHI rule and references upstream fields via @ chips.
- Closed THY-9 with `resolution:resolved-verified`.

Lesson learned for future Field Agent edits: Airtable's Add field button inserts the @ at current cursor position. Place the cursor explicitly at end of textarea (Cmd+End is unreliable in their contenteditable; click the visible end-of-text instead) before clicking Add field.

## v1.2.0 — 2026-05-05 (later same day)

THY-10 resolved without manual UI work.

- Replaced the three planned custom workflow states (Awaiting Chart Review, Awaiting Coauthor, Pending Auto-Close) with team-scoped labels: `awaiting:chart-review`, `awaiting:coauthor`, `auto-close:pending`.
- Updated daily_sync_prompt.md so phases 2-3 watch the `auto-close:pending` label rather than a state name.
- Closed THY-10 with `resolution:resolved-verified`.

Why labels won: filterable, audit-trail-preserving, no state-creation API needed, easy to evolve.

## v1.1.0 — 2026-05-05 (live system)

System is live. Live IDs in `airtable_ids.md` and `linear_ids.md`.

- 2 Airtable bases scaffolded: THYROID_DATA_REGISTRY (9 tables), THYROID_MANUSCRIPT (7 tables)
- 27 Manuscripts seeded, 22 Source Files, 21 TGDC Verification Checks, 2 Reconciliation Runs
- Linear team Thyroid Database (THY) created with 25 projects (6 workstream + 19 active manuscript)
- 32 team-scoped labels created
- 10 initial issues filed (THY-1 through THY-10), with Linear URLs cross-stamped onto Airtable Verification Checks and Manuscript records
- Scheduled daily sync (`thyroid-daily-sync`) live at 0 7 * * * local time
- Outstanding manual UI tasks: convert AI Fields (THY-9), add 3 custom workflow states (THY-10)

## v1.0.0 — 2026-05-05

Initial skill. Captures:
- Two Airtable bases (DATA_REGISTRY, MANUSCRIPT) with 13 tables total
- Linear team THYROID with 6 workstream projects + per-active-manuscript projects
- Lifecycle field (Active → In QA → Verified → Finalized → Manuscript-Locked → Archived)
- Issue Ledger (append-only audit trail)
- Manuscript Feedback Log + Data Feedback Log (append-only, for every chat-driven edit)
- Manuscript Snapshots (immutable evidence freeze on Submit/Accept)
- 10-phase daily sync prompt with Pending Auto-Close 48h buffer
- AI journal recommendation refresh (14-day cadence)
- 19 confirmed active manuscripts, 8 dormant M-codes, room for ~60 more
- HIPAA rule (research_id only, no raw note text in either tool)

Open seams that future versions will need to address:
- The other ~60 manuscripts the user mentioned but isn't yet listed on disk
- Co-author seat allocation (deferred per user)
- Possible future migration from per-day sync to event-driven webhooks if latency matters

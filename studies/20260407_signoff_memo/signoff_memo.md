# THYROID_2026 Final Sign-Off Memo

> **Supersession (2026-04-07):** This memo captured an early audit before live MotherDuck reconciliation. **Current publication truth** (live DB, MRQ status distribution, lab waves, and latest `119 --release-mode` outcome) lives in [`studies/20260407_publication_signoff_live/`](../20260407_publication_signoff_live/README.md). Verdict: **not** final manuscript sign-off; synthetic MRQ + lab + specimen/FHIR gate — see `final_verdict_memo.md` there. The body below is **preserved as history** (do not delete).

---

**Auditor role:** Claude Coworker -- architecture / sign-off auditor  
**Scope:** Commits cac1351, 2bbde53, 125cf88  
**Artifacts reconciled:** 7 (listed below)  
**Date:** 2026-04-07  
**Verdict:** NOT READY FOR FINAL SIGN-OFF. 8 concrete blockers remain.

### Reconciled Artifacts

1. `studies/v2_domain_promotion_gate_formalization_20260406_v3/report.md`
2. `studies/v2_domain_promotion_gate_formalization_20260406_v3/promotion_recommendation.md`
3. `studies/20260406_domain_inventory_current/domain_inventory.csv`
4. `studies/20260406_domain_inventory_current/inventory_summary.md`
5. `studies/20260407_motherduck_formalization/validation_report.md`
6. `docs/motherduck_database_contract_v1.md`
7. `docs/domain_mapping_rules.md`

---

## 1. Exact Blockers to Final Sign-Off

### HARD BLOCKERS (must resolve before any manuscript stats run)

**B1. MotherDuck v2_stage tables do not exist.**  
The validation report (`studies/20260407_motherduck_formalization/validation_report.md`)
shows all 23 v2 domain tables return `-1` (table not found) in the `v2_stage` column.
The stage loader (`116_md_stage_loader.py`) has either never run successfully or its
output was dropped. Without staged tables, the entire promotion lifecycle
(v2_stage -> main -> release_YYYYMMDD) is inoperable.

**B2. Zero provenance columns in any v2 domain (0/3 across all 23 domains).**  
Gate G3 issued a CONDITIONAL PASS, stating provenance "will be backfilled during
promotion materialization." But the contract (`docs/motherduck_database_contract_v1.md`,
Section 3) lists `extraction_run_id`, `extracted_at`, and `source_file_id` as Required.
No backfill mechanism exists in checked-in code. Every v2 parquet is missing these
columns at the source level.

**B3. Entity type fragmentation in canonical facts: 594 distinct values, many are LLM artifacts.**  
The validation report's canonical fact distribution shows `canonical_extracted_fact_long_v2`
has 594 distinct `entity_type` values across 123,577 rows. Documented garbage entries include:

- `"}]}, but the assistant's response is cut off. Let me complete the JSON structure...` (1 row)
- `"at line 1, column 1, near"` (1 row)
- `"neck_exam, thyroid_palpation, lymph_node_palpation, voice_assessment..." {entity_type}: [findings]...` (1 row)
- `"lymph-25, 2023-07-25. 2023-07-25 is the date..."` (1 row)
- Near-duplicates: `tsh`/`TSH`, `neck`/`Neck`, `thyroid`/`Thyroid`, `pth`/`PTH`, `Ca`/`calcium`

Manuscript queries on entity_type will return fragmented, unreliable results. This
requires a normalization pass with a controlled vocabulary before any analytical
view is safe.

**B4. 23 v2 domain tables missing required core columns in MotherDuck.**  
The validation report flags schema completeness WARN for all 23 v2 domains. Multiple
tables lack `research_id`, `note_row_id`, `entity_type`, `entity_value_raw`, and
`entity_value_norm` -- columns the contract declares Required. This is not a
MotherDuck-only issue; if the parquets themselves lack these columns, the local
DuckDB is equally affected.

**B5. No release_YYYYMMDD schema exists.**  
Validation WARN: "No release_YYYYMMDD schemas found (run 115 to create)." Without a
release snapshot, there is no immutable, auditable point-in-time reference for
manuscript statistics. The contract requires this for reproducibility.

**B6. load_inventory table missing from MotherDuck.**  
Validation WARN: `Catalog Error: Table with name load_inventory does not exist!`
This is the audit trail for what was loaded, when, and from which parquet. Without
it, there is no provenance for the staged data.

**B7. 4 domains exceed the 5% duplicate threshold.**

| Domain | Dup Rate | Rows |
|--------|----------|------|
| labs | 12.20% | 2,160 |
| tg_kinetics | 10.40% | 155 |
| cervical_ln_detail | 9.62% | 94 |
| patient_decision_adherence | 6.55% | 599 |

Gate G4 issued CONDITIONAL PASS, promising dedup "will be applied during promotion."
The promotion SQL (`motherduck_promote.sql`) must implement dedup logic; this needs
verification.

**B8. 5,622 manual review queue rows unresolved (0 reviewed).**  
The review queue contains 2 discordant rows and 5,620 fill candidates. The strict
policy in `promotion_recommendation.md` states: "No row may be auto-promoted. Every
discordant row must have `verification_status` = `confirmed_correct` or
`confirmed_incorrect`." Zero rows have been reviewed. The 2 discordant rows (both
in `medications`) are hard-blocks. The 5,620 fill candidates need a triage policy.

---

## 2. Contradictions That Must Be Resolved

**C1. G8 PASS vs. all v2_stage tables missing.**

- `promotion_recommendation.md` Gate G8: "All v2_stage tables match local parquet
  row counts" -- PASS.
- `validation_report.md` Row Count Parity: every v2_stage column shows -1 (table
  not found). Only 3 tables exist in `main` (imaging, tg_kinetics, pathology); the
  other 20 are absent from MotherDuck entirely.
- **Resolution:** G8 was run without `--motherduck-check` (which defaults G8 to
  PASS per the contract). The PASS is procedurally valid but factually misleading.
  Must re-run with `--motherduck-check` after loading v2_stage.

**C2. G2 PASS ("Schema compliance") vs. validation WARN (23 schema issues).**

- Gate G2 checked "core columns" and passed. But the contract's Required Provenance
  Columns (Section 3) include `entity_type`, `entity_value_raw`, `entity_value_norm`
  -- the same columns the validation flags as missing in tables like `tirads_granular`,
  `us_nodule_dynamics`, `labs`, and `tg_kinetics`.
- **Resolution:** G2's definition of "core columns" is narrower than the contract's
  Required column set. Either the gate criteria must be tightened to match the
  contract, or the contract must explicitly distinguish "core" from "required."

**C3. G3 PASS ("Provenance columns") vs. contract requiring them.**

- Gate G3 says CONDITIONAL PASS with a promise: "provenance will be backfilled
  during promotion materialization."
- The contract says provenance columns are Required for "all tables in main that
  contain extracted entity data."
- No checked-in script implements provenance backfill during promotion. The
  `motherduck_promote.sql` is a generated SQL file that copies from v2_stage to main.
- **Resolution:** Either (a) add provenance injection to the promotion SQL
  generator, or (b) add provenance to the extraction pipeline itself so parquets
  arrive with these columns.

**C4. README claims "extraction pipeline complete" and "626 local DuckDB tables" vs. v2 pipeline incomplete.**

- `README.md` Section "Status": "Extraction pipeline complete."
- Reality: v2 extraction has produced 36 parquets, but the v2 promotion lifecycle
  (stage -> promote -> materialize -> snapshot) has never been executed end-to-end.
  The canonical_extracted_fact_long_v2 table exists in local DuckDB but its
  594-domain entity_type explosion shows it was materialized from raw v2 parquets
  without normalization.
- **Resolution:** Amend README to say "V2 extraction complete; promotion pending."

---

## 3. Disposition Recommendation for the 6 Unclaimed Parquet Stems

The 6 unclaimed parquets (from `domain_inventory.csv`):

| Stem | What it is | Recommended Disposition |
|------|-----------|------------------------|
| `note_entities_llm_complications` | V2 LLM re-extraction of v1 `complications` domain | **Concordance audit artifact.** Run concordance against v1 `note_entities_complications`; log results in `qa.concordance_summary`. Do not promote. Archive after audit. |
| `note_entities_llm_genetics` | V2 LLM re-extraction of v1 `genetics` domain | Same as above. |
| `note_entities_llm_medications` | V2 LLM re-extraction of v1 `medications` domain | Same. The 2 discordant rows in the review queue are from this domain -- resolve those first. |
| `note_entities_llm_problem_list` | V2 LLM re-extraction of v1 `problem_list` domain | Same. |
| `note_entities_llm_procedures` | V2 LLM re-extraction of v1 `procedures` domain | Same. |
| `note_entities_llm_staging` | V2 LLM re-extraction of v1 `staging` domain | Same. Note: staging concordance is already flagged at 21.7% (below the 30% floor; waived as cross-domain). This audit is especially valuable for staging. |

**Implementation:** Add a `classification: concordance-audit` entry for each in
`config/extraction_domain_registry.yaml`. Register them in `docs/domain_mapping_rules.md`
under a new "Concordance Audit" section. Move the 6 parquets to
`processed/output/v2_parquets/audit/` subdirectory.

---

## 4. Final-Release Acceptance Criteria

### 4a. Structurally Clean

| Criterion | Current State | Required Action |
|-----------|--------------|-----------------|
| All 23 v2 domain parquets have required columns (research_id, note_row_id, entity_type, entity_value_raw, entity_value_norm, extraction_run_id, extracted_at) | FAIL: 0/3 provenance columns; multiple domains missing entity_type/value columns | Patch extraction pipeline to emit required columns, or post-process parquets |
| Duplicate rate below 5% for all domains | FAIL: 4 domains above threshold | Run dedup on labs (12.2%), tg_kinetics (10.4%), cervical_ln_detail (9.6%), patient_decision_adherence (6.6%) |
| Entity type vocabulary is controlled (no LLM artifacts, no case variants) | FAIL: 594 distinct values, ~200 are garbage or near-duplicates | Build normalization map; apply to canonical_extracted_fact_long_v2 |
| Gate G1-G8 all PASS with `--motherduck-check` | FAIL: G8 was never run with live MD check | Re-run after loading v2_stage |

### 4b. Clinically Review-Complete

| Criterion | Current State | Required Action |
|-----------|--------------|-----------------|
| All discordant rows resolved | FAIL: 2 discordant rows (medications), 0 reviewed | Manual review of 2 rows |
| Fill-candidate triage policy documented and executed | FAIL: 5,620 fill candidates, no triage policy | Define sampling policy for critical vs. informational domains; execute sampled review; document acceptance threshold |
| Staging concordance above 30% | CONDITIONAL: 21.7%, waived as cross-domain | Document clinical justification for waiver OR run targeted re-extraction |

### 4c. MotherDuck Release-Complete

| Criterion | Current State | Required Action |
|-----------|--------------|-----------------|
| v2_stage schema populated with all 23 domain tables | FAIL: 0/23 tables exist | Run `116_md_stage_loader.py --md` |
| load_inventory table exists | FAIL: table not found | Created by 116 loader; will resolve with B1 |
| Promotion SQL executed (v2_stage -> main) | FAIL: never run | Execute after B1 + gate re-pass |
| release_YYYYMMDD schema created | FAIL: no release schemas | Run `115_release_snapshot.py --md --tag YYYYMMDD` after promotion |
| Validation passes with 0 WARN | FAIL: 5 WARNs | Re-run `119_md_formalization_validate.py --md` after all above |

### 4d. Manuscript-Analysis-Ready

| Criterion | Current State | Required Action |
|-----------|--------------|-----------------|
| Canonical fact table has clean entity_type vocabulary | FAIL: 594 types | Normalize to controlled vocabulary |
| Per-domain patient coverage documented | PARTIAL: per-domain row/patient counts exist | Generate and check into docs/ |
| Manuscript cohort (N=10,871 / 4,136 cancer) joins cleanly with v2 entities | UNTESTED | Verify join completeness; document coverage gaps |
| Tg lab QC complete | PASS: tg_lab_ingestion_qc table exists in qa schema | Verified |

---

## 5. Recommended Final Presentation-Layer Views for Analysts and Manuscript Writers

### Must-have views (create in `main` schema)

1. **`v_patient_entity_summary`** -- Per-patient pivot showing count of extracted
   entities by domain family (pathology, operative, followup, imaging, demographics,
   rai). Enables analysts to see data density per patient at a glance.

2. **`v_domain_completeness_matrix`** -- Crosstab of domain x surgery_year showing
   % of patients with at least one entity. Directly supports manuscript "Data
   Completeness" table/figure.

3. **`v_manuscript_cohort_v2_enriched`** -- Left join of `manuscript_cohort_v1`
   (10,871 patients) with v2 entity pivot tables for the critical domains (pathology,
   vascular_invasion, rai_detailed, recurrence, genetics). Provides a single
   analysis-ready flat table.

4. **`v_entity_type_normalized`** -- Wrapper over `canonical_extracted_fact_long_v2`
   that applies the entity_type normalization map. Analysts should query this instead
   of the raw table.

5. **`v_tg_longitudinal_clean`** -- Join of `thyroglobulin_lab_canonical_v1` +
   `longitudinal_lab_canonical_v1` deduplicated, with Tg kinetics from v2 extraction.
   Provides the Tg trajectory analysis surface.

6. **`v_release_audit_trail`** -- Join of `qa.release_manifest` +
   `qa.promotion_scorecard` + `note_extraction_runs` showing the full lineage from
   extraction run to release tag for any given row.

### Nice-to-have views

7. **`v_source_limited_field_registry`** -- Materialized version of the
   source-limited field documentation, queryable for "why is this field empty?"
   during manuscript review.

8. **`v_concordance_dashboard`** -- V1-vs-V2 concordance rates by domain for the
   6 concordance-audit parquets. Supports reviewer defense documentation.

---

## 6. Ranked Task List

### Tier 1: MUST DO BEFORE FINAL STATS (blocking)

| Rank | Task | Blocker Ref | Effort | Script/File |
|------|------|------------|--------|-------------|
| 1 | Normalize entity_type vocabulary in canonical_extracted_fact_long_v2 -- build a mapping table of 594 raw types to ~80 controlled types; purge LLM garbage rows | B3 | 2-4 hours | New: `scripts/120_entity_type_normalization.py` |
| 2 | Patch v2 parquets to include required provenance columns (extraction_run_id, extracted_at, source_file_id) -- either re-run extraction with patched pipeline or post-process parquets | B2, C3 | 2-3 hours | Modify `llm_extraction/extract_llm.py` or new: `scripts/121_backfill_provenance.py` |
| 3 | Deduplicate the 4 domains above 5% threshold before promotion | B7 | 1 hour | Add dedup logic to `112_v2_domain_promotion_gate.py` promotion SQL generation |
| 4 | Resolve 2 discordant medication rows in manual review queue | B8 | 30 min | Manual: edit `manual_review_queue.csv` |
| 5 | Define and document fill-candidate triage policy for the remaining 5,620 rows | B8 | 1 hour | New section in `docs/domain_mapping_rules.md` |
| 6 | Load v2_stage tables to MotherDuck | B1, B6 | 30 min | `scripts/116_md_stage_loader.py --md` |
| 7 | Re-run promotion gate with `--motherduck-check` | C1 | 15 min | `scripts/112_v2_domain_promotion_gate.py --motherduck-check` |
| 8 | Execute promotion SQL (v2_stage -> main) | B5 | 15 min | Generated `motherduck_promote.sql` |
| 9 | Create release_YYYYMMDD snapshot | B5 | 15 min | `scripts/115_release_snapshot.py --md --tag 20260407` |
| 10 | Re-run formalization validation with 0 WARN target | B4 | 15 min | `scripts/119_md_formalization_validate.py --md` |

### Tier 2: NICE TO CLEAN LATER (non-blocking for manuscript)

| Rank | Task | Rationale | Effort |
|------|------|-----------|--------|
| 11 | Register 6 unclaimed parquets as `concordance-audit` in registry YAML | Structural hygiene; supports reviewer defense | 30 min |
| 12 | Create the 6 recommended presentation-layer views | Analyst productivity; not blocking for stats | 2-3 hours |
| 13 | Staging concordance investigation (21.7%) -- determine if v2 staging extractor is inferior to v1 | Informational; waiver is defensible since v1 staging is the canonical source | 1-2 hours |
| 14 | Amend README to reflect v2 promotion status accurately | Housekeeping; C4 | 15 min |
| 15 | Run concordance audit on 6 unclaimed parquets vs. v1 tables | Supports "LLM vs regex" comparison for supplement | 2 hours |
| 16 | Archive stale study directories (v1 promotion attempts 1-3) | Repo hygiene | 15 min |
| 17 | Tighten G2 gate criteria to match contract Required columns | Prevents future conditional-pass drift | 1 hour |

---

## Summary Verdict

The V2 extraction pipeline produced good raw material (54,346 validated rows across
23 domains, 36 parquets on disk). The gate formalization (8-gate framework) and
MotherDuck contract are well-designed. But the pipeline has not been executed
end-to-end: v2_stage is empty, provenance is absent, entity types are un-normalized,
and the review queue is untouched. The gap between "gates PASS" and "database is
manuscript-safe" is the gap between schema validation and data quality validation.

**What is missing vs. what is source-limited and acceptable:**

- **Missing (actionable):** provenance columns, entity_type normalization, dedup,
  v2_stage loading, release snapshot, manual review resolution, fill-candidate
  triage policy.
- **Source-limited (acceptable):** ~50% note coverage, 0% non-Tg lab dates, 0%
  nuclear medicine notes, 87% vascular invasion ungraded, pre-2019 operative notes
  absent, 88.8% recurrence dates unresolved, 41% RAI dose coverage cap. These are
  institutional data limitations documented in `docs/MANUSCRIPT_CAVEATS_20260313.md`
  and the source-limited field registry.

**Estimated effort to clear all Tier 1 blockers: 8-12 hours of focused work.**  
**After Tier 1 completion, the database will be: structurally clean, clinically
defensible, MotherDuck release-controlled, and ready for final manuscript statistics.**

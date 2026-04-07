# Specimen / FHIR release blocker — root cause and remediation (2026-04-07)

## Branch / environment

- Repo branch: `main`
- MotherDuck catalog: `Thyroid 2026` (prod), read/write via `MD_SA_TOKEN` / service-account resolution
- Query attribution: `MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_specimen_fhir_fix/1.0`, `MOTHERDUCK_SESSION_HINT=specimen_fhir_fix_<UTC>`

## Historical failure (before)

Source: [`studies/20260407_publication_signoff_live/validation_report.md`](../20260407_publication_signoff_live/validation_report.md) (2026-04-07T10:33:51Z, release-mode).

| Diagnostic | Count / detail |
|------------|----------------|
| `broken_fhir_refs` (142 view row count) | **10,139** |
| `high_tier_null_spec` (`qa.v_diag_specimen_provenance_genomic_v1`) | **14** |
| Specimen/FHIR QA check (119 Check 13) | **FAIL** (strict → blocked) |
| Genomic link review burden (open/pending) | **9,952** (WARN only) |

**Root cause — `broken_fhir_refs`:** The row count **10,139** matches **10,139** rows in `main.fhir_encounter_v1` (lineage audit / design memos). The previous `encounter_episode` branch in `qa.v_diag_specimen_fhir_broken_refs_v1` joined `fhir_episode_of_care_v1` through a `(patient_fhir_id, surgery_episode_id)` rollup keyed off `specimen_master_v1`, then compared to the encounter JSON reference. That join did not align with how bundles and encounter JSON actually reference episodes (**full** `EpisodeOfCare/{id}` computed on the encounter row). Result: **every** encounter row looked like a broken episode reference even when the analytic FHIR tables were internally consistent.

**Root cause — `high_tier_null_spec`:** Tier downgrades for `specimen_id IS NULL` were already present in `tier_adj` / `genetic_adj` / `thy_adj`, but edge cases or partial rebuilds could still leave `exact` / `high_confidence` with a null specimen on some union arms. A **second, output-stage guard** on `linkage_confidence_tier` in `140_specimen_genomics_binding_ddl.sql` forces those rows to `plausible_review` so release diagnostics cannot see “high tier + null specimen.”

**Subject / procedure / encounter Patient refs:** Additional `procedure_subject` and `encounter_subject` branches in the 142 view surface mismatches on Procedure and Encounter (not only Specimen), matching the checklist in the remediation task (subject/patient refs).

## After (live verification, 2026-04-07)

Deployed updated `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` via `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md`, then `scripts/119_md_formalization_validate.py --md --release-mode`.

| Metric | Value |
|--------|------:|
| `broken_fhir_refs` | **0** |
| `high_tier_null_spec` | **0** |
| `n_rows` (`v_diag_specimen_provenance_genomic_v1`) | 10,126 |
| 119 Check 13 specimen/FHIR diagnostics | **WARN** (focus-table aggregates unavailable on this catalog only; **not** FAIL) |
| 119 verdict | **25 PASS / 2 WARN / 0 FAIL** |

Breakdown of `broken_fhir_refs` by `issue` after fix: **no rows** (empty view).

Genomic link review open/pending remains high (~9.9k) — informational WARN in 119; out of scope for “broken ref” remediation.

## Code changes (repo)

- `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` — Fix `encounter_episode` join; add `procedure_subject`, `encounter_subject`.
- `scripts/sql/140_specimen_genomics_binding_ddl.sql` — Output-stage `guarded_linkage_tier` for molecular / genetic / thyroseq rows.
- `scripts/138_md_specimen_fhir_layer.py`, `scripts/140_md_specimen_genomics_binding.py`, `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py` — `prefer_service_account=True` on MotherDuck connects (prefer `MD_SA_TOKEN` per contract).

## Operator notes

- Full rebuild on prod was **not** required for this fix: FHIR tables were already consistent; the **diagnostic view** was wrong. Re-run **`138 --md`** after any future identity/FHIR tail change so fact tables and views stay aligned.
- For greenfield catalogs, run **`138 --md`** (applies 139 + 138 tail + 140 + 142) or **`143 --md`** after 138 if only QA views need refresh.

## Full rebuild (prod `Thyroid 2026`, 2026-04-07)

`scripts/138_md_specimen_fhir_layer.py --md` completed successfully after MotherDuck **DuckLake** workarounds in `139_specimen_identity_layer_ddl.sql`:

| Prior pattern | Issue on DuckLake | Replacement |
|---------------|-------------------|-------------|
| Single `con.execute` of entire 139 DDL | Internal IO error | Quote- and `--` comment-aware batched executes in `139_md_specimen_identity_layer.py` |
| `BEGIN`/`COMMIT` around identity + FHIR tail | Internal IO error | Autocommit path when `--md` in `138_md_specimen_fhir_layer.py` |
| `DELETE` selective rows on `specimen_source_xref_v1` | Internal IO error | `CREATE OR REPLACE TABLE … AS SELECT * … WHERE 1=0` then INSERTs refill pathology + molecular xrefs (**entire xref table cleared** each run; only path/mol domains are inserted by 139) |
| `DELETE` from `specimen_tumor_focus_v1` | Internal IO error (including DELETE by PK) | Same empty-table replace; pathology focus rows reinserted from staging |
| `DELETE FROM qa.specimen_merge_review_queue_v1` | Internal IO error | Empty-table replace; rows reinserted by following INSERT |

Post-rebuild release-mode check: [`119_after_full_rebuild/validation_report.md`](119_after_full_rebuild/validation_report.md) — **26 PASS / 1 WARN / 0 FAIL**; specimen/FHIR QA diagnostics **clean**; `broken_fhir_refs=0`, `high_tier_null_spec=0`.

Study artifacts from the rebuild run: [`full_rebuild_prod/`](full_rebuild_prod/) (audit memo, implementation report, etc.).

## Artifacts

- Release-mode report after deploy: [`119_release_validation_final/validation_report.md`](119_release_validation_final/validation_report.md)
- After full **138** rebuild: [`119_after_full_rebuild/validation_report.md`](119_after_full_rebuild/validation_report.md)

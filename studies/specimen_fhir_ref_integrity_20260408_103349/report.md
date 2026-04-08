# Specimen / FHIR reference integrity & genomics validation — 2026-04-08

## Executive summary

- **Root cause (historical ~10k `broken_fhir_refs`):** `qa.v_diag_specimen_fhir_broken_refs_v1` rows in the `encounter_episode` class dominate when **`main.fhir_episode_of_care_v1` is missing or drifts** relative to **`main.fhir_encounter_v1`**: each `Encounter` JSON carries `episodeOfCare[0].reference` = `EpisodeOfCare/{eoc_id_short}`. If EoC is rebuilt only from a **specimen_master–only spine** while encounters already reflect per-specimen hashes, operators can materialize **encounters without matching EoC rows** → one broken-ref row per specimen (order ~bundle row count ≈ 10,139 in the historical incident described in `studies/20260407_publication_signoff_live/final_verdict_memo.md`).

- **Code fix:** `scripts/sql/138_specimen_fhir_tail_ddl.sql` now builds **`fhir_episode_of_care_v1` from an encounter-driven spine** (`fhir_encounter_v1` ∩ `specimen_master_v1`) so every emitted encounter reference has a matching EoC row in the same materialization pass.

- **Bundle consistency:** `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` adds `qa.v_diag_specimen_fhir_bundle_entry_drift_v1` to flag **`entry[].url` ≠ `resourceType + '/' + resource.id`** inside `main.fhir_bundle_specimen_export_v1`.

- **Genomics:** `scripts/140_md_specimen_genomics_binding.py` now persists **`linkage_confidence_tier_enum`**, **`binding_confidence_tier_enum`**, **`A_exact_high_requires_specimen_ids`**, and **`thyroseq_exploded_rows_strict_positive_ord`** into `qa.val_specimen_genomic_binding_v1`, plus matching **`qa.v_diag_specimen_genomics_*`** list views in 142.

- **Validator / UA:** `scripts/119_md_formalization_validate.py` defaults MotherDuck **`custom_user_agent`** to **`specimen_fhir_ref_integrity_v2`** when `MOTHERDUCK_CUSTOM_USER_AGENT` is unset. Check 13 aggregates the new diagnostic counts. `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py` uses the same default UA on `--md`.

## Ref-type breakdown (`qa.v_diag_specimen_fhir_broken_refs_v1`)

| Issue | Meaning |
|-------|---------|
| `specimen_subject` | `Specimen.subject.reference` ≠ canonical `Patient/{patient_fhir_id}` |
| `specimen_collection_procedure` | `collection.procedure.reference` ≠ `Procedure/{proc hash}` |
| `procedure_subject` | Procedure subject drift |
| `procedure_encounter` | `Procedure.encounter.reference` ≠ `Encounter/{enc hash}` |
| `encounter_subject` | Encounter subject drift |
| `encounter_episode` | `episodeOfCare[0].reference` present but **no** `fhir_episode_of_care_v1.fhir_id` match (**primary historical mass failure**) |
| `encounter_episode_patient_mismatch` | Episode resolves but `patient_fhir_id` differs |

Optional references are only evaluated when the JSON path is non-null (see 142 comments).

## Live MotherDuck verification (post-deploy, this session)

After applying `142_specimen_fhir_qa_diagnostics_ddl.sql` via `143 --md` and re-running `140 --md`:

- `SELECT issue, COUNT(*) FROM qa.v_diag_specimen_fhir_broken_refs_v1 GROUP BY issue` → **no rows** (0 total broken refs).
- `SELECT COUNT(*) FROM qa.v_diag_specimen_fhir_bundle_entry_drift_v1` → **0**.
- `SELECT COUNT(*) FROM qa.v_diag_specimen_genomics_dupe_thyroseq_slice_v1` → **0**.
- `scripts/119_md_formalization_validate.py --md --release-mode` → **33 PASS / 6 WARN / 0 FAIL** (WARNs include molecular assay/panel pairing and open genomic link review burden — unchanged governance posture).

## Operator notes

1. **EoC hash fix** lands when **`scripts/138_md_specimen_fhir_layer.py`** (or equivalent) re-executes the **tail DDL** on the target catalog — diagnostics-only `143` does not rebuild `fhir_episode_of_care_v1`.
2. **New QA views** require one **`143_md_specimen_fhir_qa_diagnostics_deploy.py --md`** (or full **138**) so `119` Check 13 does not FAIL on “missing 142 surfaces”.

## Tests

- `tests/test_specimen_fhir_qa_diagnostics.py` — extended happy-path coverage for new `v_diag_*` surfaces; bundle drift + `binding_confidence_tier_enum` regression cases.
- `tests/test_specimen_fhir_scripts_offline.py` — stub DB extended for `fhir_bundle_specimen_export_v1` + genomics columns so `142` applies in CI.

## Files touched (PR scope)

- `scripts/sql/138_specimen_fhir_tail_ddl.sql`
- `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`
- `scripts/119_md_formalization_validate.py`
- `scripts/140_md_specimen_genomics_binding.py`
- `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py`
- `utils/specimen_fhir_release_gate.py`
- `tests/test_specimen_fhir_qa_diagnostics.py`
- `tests/test_specimen_fhir_scripts_offline.py`

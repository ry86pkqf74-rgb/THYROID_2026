# Design memo: canonical specimen identity + analytic FHIR export (v1)

**Audience:** architects, data engineers, analysts.  
**Task class:** audit / design only — **no** new production DDL executed in MotherDuck for this folder; **no** local DuckDB writes.  
**MotherDuck:** read-only evidence + attempted named snapshot (`motherduck_audit_evidence.md`).  
**AGENTS.md alignment:** append-only additive contracts; deterministic keys; ambiguous linkage → review queues; no silent overwrite.

---

## 1. Objectives

1. **Canonical specimen identity** — stable `specimen_id` / `specimen_focus_id` keyed off synoptic pathology + encounter disambiguation + surgery–pathology linkage, suitable for longitudinal joins (genomics, outcomes).  
2. **Analytic FHIR export** — de-identified JSON resources for research tooling, clearly scoped **not** for regulatory clinical interoperability.  
3. **Governance** — QA tables for contract checks and **non-auto-merge** duplicate candidates.

---

## 2. Current implementation pointer

The repo already encodes v1 in:

- [`scripts/sql/138_specimen_fhir_layer_ddl.sql`](../../scripts/sql/138_specimen_fhir_layer_ddl.sql)  
- [`scripts/138_md_specimen_fhir_layer.py`](../../scripts/138_md_specimen_fhir_layer.py)  
- [`utils/specimen_fingerprint.py`](../../utils/specimen_fingerprint.py)  
- Tests: [`tests/test_specimen_fhir_layer.py`](../../tests/test_specimen_fhir_layer.py)  
- Validator: `check_specimen_fhir_layer` in [`scripts/119_md_formalization_validate.py`](../../scripts/119_md_formalization_validate.py)

This memo **documents and tightens design intent**; it does not replace those files in this pass.

---

## 3. Artifact reconciliation (abbrev.)

See [`artifact_reconciliation.md`](artifact_reconciliation.md).

**Headline:** [`studies/20260407_signoff_memo/signoff_memo.md`](../20260407_signoff_memo/signoff_memo.md) is **superseded-without-cleanup** for operational truth by the checked-in [`validation_report.md`](../20260407_formalization_validation_release_mode/validation_report.md) (release-mode PASS) and by **live** MotherDuck counts showing populated specimen + FHIR tables. README status is **current** for navigation but mixes March-13 local freeze history with April formalization — readers should use the “three layers of ready” section as the guide.

---

## 4. Source inventory

See [`source_to_target_inventory.md`](source_to_target_inventory.md).

**Primary seed chain:** `synoptic_tumor_long_v1` + `path_synoptics_encounter_qc_v1` + `surgery_pathology_linkage_v3` + molecular episodes + v3 FNA/preop linkages.

---

## 5. Proposed contracts

See [`table_contracts_proposed.md`](table_contracts_proposed.md).

---

## 6. Fingerprint + matching

See [`fingerprint_and_matching_policy.md`](fingerprint_and_matching_policy.md).

---

## 7. FHIR mapping policy

See [`fhir_mapping_policy.md`](fhir_mapping_policy.md).

---

## 8. Implementation sequence (recommended)

| Step | Action | Script / artifact |
|------|--------|-------------------|
| 1 | Materialize synoptic tumor long | `108_synoptic_tumor_long_v1.py --md` |
| 2 | Encounter QC view (+ val table if used) | `109_synoptic_encounter_qc.py --md` |
| 3 | Episode / linkage / canonical parquet loads | `117_md_contract_views.py --md` (and upstream 103 as needed) |
| 4 | Specimen + FHIR layer | `138_md_specimen_fhir_layer.py --md` |
| 5 | Formalization / release validation | `119_md_formalization_validate.py --md` (`--release-mode` when signing) |

Linkage hardening scripts [`100_episode_linkage_v2_hardening.py`](../../scripts/100_episode_linkage_v2_hardening.py) and [`101_multi_episode_linkage_hardening.py`](../../scripts/101_multi_episode_linkage_hardening.py) should run **before** or as part of freeze regeneration when episode assignment changes materially.

Domain dedup script [`122_dedup_high_dup_domains.py`](../../scripts/122_dedup_high_dup_domains.py) targets **v2 note-entity parquets**, not synoptic specimen tables — indirect benefit only via cleaner downstream facts.

---

## 9. Risk register

| ID | Risk | Likelihood | Impact | Mitigation |
|----|------|------------|--------|------------|
| R1 | **Empty accession** collisions deflate fingerprint specificity | Med | Med | Monitor `qa.specimen_merge_review_queue_v1`; optional require non-null accession for merge or add second key |
| R2 | **`min(specimen_focus_id)`** aggregation for genomics masks multifocal truth | Med | Med | Set `specimen_focus_id` NULL when multiple foci + document; or rank by focus size / linkage |
| R3 | **Encounter mis-pick** when histology tie-break wrong | Low | High | Manual review of `val_path_synoptic_encounter_isolation_v1`; extend queue reason codes |
| R4 | **FHIR id truncation** collision across resource types | Low | Low | Always use `resourceType`; consider longer ids in v2 |
| R5 | **ThyroSeq / genetics** not in v1 xref | Med | Med | Add `specimen_source_xref_v1` domain rows with exact-match policy only |
| R6 | **Entity_type garbage** in canonical facts pollutes cross-domain analytics | Med | Med | Separate normalization program (signoff B3); not blocked by specimen v1 but affects joint studies |
| R7 | **Signoff vs validation cognitive dissonance** for operators | High | Med | Single “release evidence” pointer in README; archive or amend signoff header with supersession note (future editorial) |

---

## 10. Live MotherDuck snapshot (audit session)

Evidence rows: [`motherduck_audit_evidence.md`](motherduck_audit_evidence.md).

---

## 11. Change control

Future schema changes should: bump table suffix (`_v2`) or add nullable columns; update fingerprint helper + tests + Check 13; re-run release-mode validator.

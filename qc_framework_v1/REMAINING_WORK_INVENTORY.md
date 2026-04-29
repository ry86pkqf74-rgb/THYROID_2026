# Remaining Work Inventory — `thyroid_canonical_publication_v1_0`

**Status as of:** 2026-04-28 (post-mig_95 — ETE taxonomy + invasion-family rollups signed off)
**Goal:** Build a clean master canonical database at `thyroid_canonical_publication_v1_0`. Archive deprecated/legacy/intermediate tables to `"Thyroid 2026 UPdated"` (cross-account MotherDuck DB used as the read-only verification reference + cold storage).
**Master plan:** [`MASTER_VERIFICATION_PLAN.md`](MASTER_VERIFICATION_PLAN.md)
**Dashboard:** [`VERIFICATION_PROGRESS.md`](VERIFICATION_PROGRESS.md)
**Closed log:** [`VERIFIED_TABLES.md`](VERIFIED_TABLES.md)

---

## 1. Verified tables (13 / 184 = 7.1 %)

| Table | Rows | Cols | Sign-off | Pattern |
|---|---|---|---|---|
| `canonical_fna_events_v1` | 8,050 | 38 | mig_78 (pilot) | 14-migration arc, manual_source_review |
| `canonical_airway_invasion_events_v1` | 3,155 | 23 | mig_83 | per-finding Logan review (LLM-output canonical) |
| `canonical_path_malignant_events_v1` | 6,689 | 56 | mig_89 | **CTC-equivalence** + Script-rule re-run |
| `canonical_operative_events_v1` | 11,773 | 54 | mig_90 | **CTC-equivalence (single migration)** |
| `canonical_t4b_invasion_events_v1` | 944 | 19 | mig_92 | per-finding Logan review |
| `canonical_esophageal_invasion_events_v1` | 188 | 15 | mig_93 | per-finding Logan review |
| `canonical_vascular_invasion_events_v1` | 3,861 | 22 | mig_94 | per-finding Logan review |
| `canonical_invasion_events_v1` | 51,751 | 20 | mig_91b + mig_95 | UNION CTC-equivalence + ETE taxonomy hardening |
| `canonical_airway_invasion_patient_rollup_v1` | 2,820 | 20 | mig_95 | rollup re-derivation |
| `canonical_esophageal_invasion_patient_rollup_v1` | 60 | 11 | mig_95 | rollup re-derivation |
| `canonical_t4b_invasion_patient_rollup_v1` | 434 | 13 | mig_95 | rollup re-derivation |
| `canonical_vascular_invasion_patient_rollup_v1` | 3,745 | 14 | mig_95 | rollup re-derivation |
| `canonical_invasion_patient_rollup_v1` | 10,871 | 47 | mig_95 | family rollup re-derivation |

**Cumulative cols verified:** 328 / 5,502 = 6.0 %.

---

## 2. Verification patterns (the playbook)

Three distinct verification methods have been validated in production:

| Pattern | When to use | Example |
|---|---|---|
| **CTC-equivalence (mass-equivalence vs pre-script archive)** | Canonical built by `Script-N` SELECT * + filter + UPDATE chain. The archived `_pre<N>_<timestamp>` snapshot in `archive_pub_v1_0` is the value-source-of-truth. One mass-equivalence query verifies dozens of inherited cols at once. | `canonical_path_malignant_events_v1` (mig_87, 36 cols batched), `canonical_operative_events_v1` (mig_90, 38 cols batched) |
| **Script-rule re-run** | Post-build UPDATE-derived cols. Re-execute the original `UPDATE m FROM <secondary> SET ...` logic as a SELECT and compare row-by-row against canonical's stored values. Works for both archived upstreams (TEM v2 pre361) and live ones (`specimen_tumor_focus_v1`, `note_entities_operative_detail`). | path_malignant mig_88 (TEM + STF UPDATEs), operative_events mig_90b (op_detail enrichment cols) |
| **Per-finding Logan review** | LLM-output canonical with no upstream consolidation script (e.g. `note_entities_llm_<X>` directly extracted). Logan reviews the positive subset per-finding, applying clinical adjudication rules (findings-vs-staging, scope filters, template-echo cleanup). | `canonical_airway_invasion_events_v1` (mig_80→83) |

Read-only verification reference: reading from `archive_pub_v1_0.*` for verification is permitted (consistent with `feedback_no_cross_db_canonical_sourcing.md` — that rule forbids _building_ canonical from archive, not reading from it).

---

## 3. Tier 1 events queue — 15 unverified canonicals

### 3a. Quick-close candidates (CTC-equivalence pattern likely applies)

These tables are built by Script-N consolidation chains with archived pre-script snapshots. Expected single-migration close once the archive is identified.

| Table | Cols | Rows | Build script | Archive snapshot | Notes |
|---|---|---|---|---|---|
| `canonical_path_benign_events_v1` | 55 | 11,688 | scripts/361_op_path_consolidation.py (Step 2) | needs probe — possibly `archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_*` (same as malignant) | Sibling of path_malignant. One row per synoptic report. |
| `canonical_path_gland_events_v1` | 20 | 28,724 | scripts/361_op_path_consolidation.py (Step 3) | needs probe | Sibling of path_malignant. One row per anatomical gland (long form). |
| `canonical_frozen_section_events_v1` | 31 | 7,081 | needs investigation (Script 360 close-out memory references it) | needs probe | Memory `project_frozen_section_script_360.md` has context — closed 2026-04-21 at `76b3387`. |
| `canonical_parathyroid_events_v1` | 25 | 8,697 | needs investigation | needs probe |  |
| `canonical_complications_events_v1` | 18 | 10,954 | needs investigation | needs probe |  |
| `canonical_pmh_events_v1` | 19 | 12,444 | needs investigation | needs probe |  |
| `canonical_psh_events_v1` | 19 | 3,919 | needs investigation | needs probe |  |
| `canonical_medications_events_v1` | 19 | 7,501 | needs investigation | needs probe |  |
| `canonical_pathology_clinical_events_v1` | 15 | 13,358 | needs investigation | needs probe |  |
| `canonical_cervical_ln_clinical_events_v1` | 15 | 4,493 | needs investigation | needs probe |  |

### 3b. LLM-output canonicals — per-finding Logan review (airway-style)

These are direct LLM extractions from notes. No upstream consolidation; verification needs Logan's per-finding review of the positive subset (analog to airway invasion mig_80→83). Cowork builds the review CSV; Logan fills it; Claude applies via query_rw.

| Table | Cols | Total rows | Approx positive subset | Verification effort |
|---|---|---|---|---|
| `canonical_esophageal_invasion_events_v1` | 15 | **188** | likely <50 | smallest — fastest to close |
| `canonical_t4b_invasion_events_v1` | 19 | **944** | needs probe | small |
| `canonical_vascular_invasion_events_v1` | 22 | **3,861** | needs probe | medium |
| `canonical_ete_subgrade_events_v1` | 17 | **287** | needs probe | small |

These follow the **findings-vs-staging rule** (`feedback_findings_vs_staging.md`): findings columns are primary; staging implications must be derived from findings, not separately extracted. Apply CAP synoptic template-echo cleanup (memory: airway mig_82).

### 3c. Multi-source UNION canonical (most complex)

| Table | Cols | Rows | Build script | Notes |
|---|---|---|---|---|
| `canonical_invasion_events_v1` | 20 | **51,773** | scripts/363_invasion_canonical.py | UNION of multiple modality CTEs. pre363v3 archive differs by 1,353 hashes from current canonical (post-snapshot rebuild). Verification needs per-modality source mapping — re-derive each modality slice from its source extraction table and compare. |

**This is the table to start with in the next session — see continuation prompt at end.**

---

## 4. Tier 1 anchor + sources (large surface, deprioritized but in-scope)

### 4a. The anchor

| Table | Cols | Notes |
|---|---|---|
| `canonical_patient_master` | **1,592** | The master patient table. Most cols sub-classify as `derived` (auto) once tiered. Real adjudication load probably ~50–100 cols. Per master plan §5: 4–6 sessions. Defer until tier 1 events are mostly closed (most patient_master cols are rollups of events). |

### 4b. Tier 1 sources (12 raw mirrors)

| Table | Cols | Notes |
|---|---|---|
| `path_synoptics` | 311 | Primary source for path_malignant + benign + gland. Mostly `source` category — sample-based verification under Protocol v1, full-row under v2. |
| `manuscript_cohort_v1` | 151 | Per-patient cohort definitions. |
| `nsqip_enrichment` | 94 | NSQIP data load. |
| `nsqip_patient_summary` | 94 | NSQIP per-patient. |
| `canonical_us_nodule_v2` | 57 | Ultrasound nodule extractions. |
| `mri_imaging` | 41 | MRI extraction. |
| `ct_imaging` | 40 | CT extraction. |
| `canonical_us_thyroid_gland_v2` | 32 | US thyroid gland. |
| `rai_treatment_episode_v2` | 32 | RAI treatments. |
| `canonical_us_lymph_node_v2` | 29 | US lymph node. |
| `nuclear_med` | 17 | Nuclear medicine. |
| `clinical_notes_long` | 11 | Long-form notes (the textual source). |

---

## 5. Tier 2 — rollups + derived canonicals (~25 tables)

### 5a. Patient rollups (auto-verify cascade)

19 `canonical_*_patient_rollup_v1` tables. Most cols are `derived` rollups of the corresponding events table — when the events table is verified, the rollup auto-verifies via a re-derivation rule probe. Expected: 15–20 min per rollup once the events table is closed.

Outstanding rollups (the big ones):
- `canonical_frozen_section_patient_rollup_v1` — **188 cols** (largest rollup)
- `canonical_pmh_patient_rollup_v1` — 79 cols
- `canonical_complications_patient_rollup_v1` — 51 cols
- `canonical_invasion_patient_rollup_v1` — **closed by mig_95** (47 cols)
- `canonical_psh_patient_rollup_v1`, `canonical_medications_patient_rollup_v1` — 28 cols each
- `canonical_operative_patient_rollup_v1` — 22 cols (already unblocked since operative events is closed)
- `canonical_fna_patient_rollup_v1` — 20 cols (rollup of the closed FNA events)
- 11 others (9–18 cols each)

### 5b. Tier 2 canonical (16 tables)

Misc canonical tables — labs, molecular, recurrence, ETE adjudication, survival, etc.

| Table | Cols | Notes |
|---|---|---|
| `canonical_molecular_genetics_v2` | 74 | Memory `project_molecular_v2_schema.md` has architecture. |
| `canonical_ete_event_resolved_v1` | 62 | **Closed mig_121 (2026-04-29)** — Protocol v2 Tier-2 multi-source enrichment sign-off; recurrence/survival carry-forwards documented. |
| `canonical_molecular_genetics_from_notes_v2` | 28 | Notes-extracted molecular. |
| `canonical_recurrence_resolved_v1` | 19 | Re-verify pending per master plan §11. |
| `canonical_operative_procedure_codes_v1` | 16 | Built by Script 362 Step 3. |
| `canonical_table_signoff_registry_v1` | 13 | Self-referential — meta. |
| `canonical_column_verification_registry_v1` | 14 | Self-referential — meta. |
| `canonical_survival_followup_v1` | 13 |  |
| `canonical_recurrence_v1` | 12 |  |
| `canonical_ete_inline_adjudication_v1` | 12 | **Closed mig_121 (2026-04-29)** — paired inline adjudication warehouse. |
| `canonical_labs_thyroglobulin_v1` | 12 | Memory `project_lab_consolidation_script_347.md`. |
| `canonical_labs_calcium_v1`, `canonical_labs_pth_v1`, `canonical_labs_tsh_v1`, `canonical_labs_vitamin_d_v1` | 10 each | Same pattern. |
| `manuscript_workspace.canonical_cleanup_audit_v1` | 18 |  |

---

## 6. Tier 3 — extraction + helper tables (~108 tables)

- **17 `note_entities_*` LLM extraction tables** — mostly `na_provenance` (the `parsed_json`/`result_json` payloads aren't row-by-row verified; they're audited via the canonicals they feed). 10 of 17 already auto-verified via the seed pass. 7 remaining are LLM-output canonicals that pair with their tier1 LLM canonicals (e.g. `note_entities_llm_airway_invasion_v2` pairs with `canonical_airway_invasion_events_v1`).
- **91 helper tables** in `manuscript_workspace` and `main` (audit logs, dive maps, validation queues, etc.). Many will be skip-eligible after one look. Master plan budget: 10–20 min per table; many will reclass to `na`.

---

## 7. Open carry-forwards (deferred, non-blocking)

| CF | Source | Description | Status |
|---|---|---|---|
| FNA `days_to_surgery` | mig_78 | Cross-table derivation (FNA fna_date_resolved + operative resolved_surgery_date) | **UNBLOCKED** — operative now verified; can re-open |
| Airway CF-1 (6017) | mig_83 | pT4a anchored on non-airway "extrathyroidal extension into fat"; cohort-build call deferred | Open |
| Airway CF-2 | mig_83 | `t4a_implication` is currently a stored LLM column; per findings-vs-staging rule, future cleanup may convert to deterministic post-derivation | Open |
| Airway CF-3 | mig_83 | 17 `pathologist_call_only` rows have all anatomic findings = `unknown` but `t4a=pT4a`; downstream views may want an `evidence_grade` flag | Open |
| CF-86-1 | mig_86 | 64 path_malignant `tumor_ordinal` rows came via archived TEM v2 text-extraction (not Script 108 SLOT_MAP). Verifiable against `archive_pub_v1_0.tumor_episode_master_v2_pre361_*` if future restore-and-reverify run | Open |
| CF-87-AJCC | mig_87 | path_malignant AJCC7/8 staging cols verified as faithful copies of CTC pre361. Findings-vs-staging derivation correctness (Logan airway-invasion rule extended to ETE/multifocality/nodal) is upstream of canonical (CTC build, scripts 251/266) | Open |
| CF-87-GROSS-ETE | mig_87 | 6 of 6,695 join-duplicate rows show inconsistent `gross_ete` between paired archive rows; canonical row matches at least one archive row in every case | Open (cosmetic) |
| CF-91-GROSS-VS-MICRO-ETE-NAMING | mig_91b | Generic path ETE was defaulted to `gross_ete`; mig_95 introduced `ete_present_not_further_specified` and rebuilt rollups/CPM feeders | **Closed** |
| CF-91-LINKAGE-COL-NAME | mig_91b | `linkage_ambiguous_multi_episode` counted findings, not episodes; mig_95 renamed to `linkage_ambiguous_multi_finding` | **Closed** |
| CF-90-DATE-FORMAT | mig_90 | operative_events `resolved_surgery_date` stored as `MM/DD/YYYY` in canonical vs `YYYY-MM-DD` in pre362 archive; same dates, format reformat by downstream normalization (not Script 362 itself) | Open (cosmetic) |
| FNA CF (open from pilot) | mig_78 | `bethesda_calculated_num` 1,450 rows differ from source `bethesda_raw` (intentional rescore overlay; verified vs `fna_bethesda_rescore_staging_v1` instead) | Open |

---

## 8. Architectural decisions made this session

1. **Drop deprecated cols inline in verification migrations** — FNA pilot precedent (mig_78 dropped `is_index_fna`); path_malignant mig_84 dropped 4 deprecated staging cols + 11 dependent fingerprint views.
2. **Read archived intermediates for verification** — `archive_pub_v1_0` snapshots in `"Thyroid 2026 UPdated"` are valid read-only verification references; canonical never sources from archive at build time, but verification queries CAN read them.
3. **Single-migration close for clean Script-N canonicals** — when the CTC-equivalence pattern hits 100% match across all inherited cols + Script-rule re-run hits 100% on UPDATE-derived cols, no per-column ceremony needed; combine verify + Step D + sign-off in one migration (operative_events mig_90 precedent).
4. **Findings-vs-staging rule extension** — established in airway invasion (mig_81-82), carries forward as the architectural lens for any canonical mixing anatomic findings with staging implications. Memory: `feedback_findings_vs_staging.md`.

---

## 9. Recommended next-session sequence

1. **Finalize `canonical_invasion_events_v1`** (the complicated one) — multi-source UNION; per-modality verification; Cowork builds Logan-review CSV for ambiguous cases. **Continuation prompt below.**
2. **Path benign + path gland (script 361 siblings of path_malignant)** — likely single-migration each via CTC-equivalence pattern with `canonical_tumor_characteristics_v1_pre361` archive (already used for malignant).
3. **3 remaining LLM-output invasion canonicals** (esophageal 188 → t4b 944 → vascular 3,861) — one Logan-review session each, smallest first.
4. **Pivot to event tables built by other scripts** — frozen_section, parathyroid, complications, pmh, psh, medications, pathology_clinical, cervical_ln_clinical (build scripts to be identified).
5. **Patient rollups cascade** — once enough events tables are closed, auto-verify cascade through 19 `canonical_*_patient_rollup_v1` tables.
6. **Tier 2 canonical (labs, molecular, recurrence, etc.)** — medium effort.
7. **Tier 1 anchor `canonical_patient_master`** — 1,592 cols, 4–6 sessions.
8. **Tier 1 sources (raw mirrors)** — sample-based verification per master plan.
9. **Tier 3 helpers** — sweep, many will reclass to `na`.

---

## 10. Continuation prompt for next Cowork chat

A self-contained brief is at the end of this file (section "Continuation prompt — invasion events finalization").

---

## Continuation prompt — invasion events finalization

> ### Context
>
> I'm continuing the Protocol v2 verification of `thyroid_canonical_publication_v1_0` (MotherDuck, account `logan.glosser.eras@gmail.com`). The goal is a clean master canonical at this database; deprecated/legacy/intermediate tables get archived to the cross-account DB `"Thyroid 2026 UPdated"`.
>
> Today's session closed `canonical_path_malignant_events_v1` (mig_84→89) and `canonical_operative_events_v1` (mig_90), bringing total verified to 4 / 184 tables. Two architectural patterns were established:
>
> 1. **CTC-equivalence verification** — for canonicals built by Script-N SELECT*+filter+UPDATE chains, the archived `_pre<N>_<timestamp>` snapshot in `archive_pub_v1_0` is the value-source-of-truth.
> 2. **Script-rule re-run verification** — for post-build UPDATE-derived cols, re-execute the original UPDATE logic as a SELECT and compare.
>
> Memory: `project_ctc_equivalence_verification_pattern.md`.
>
> ### What I want this session to do
>
> Finalize `canonical_invasion_events_v1` — the most complex remaining tier1_events table.
>
> **Shape:** 51,773 rows, 20 cols, multi-source UNION build (`scripts/363_invasion_canonical.py`).
>
> **The complication:** the canonical is a UNION of multiple modality CTEs (one CTE per source kind: synoptic, op_note, ct_imaging, mri_imaging, etc., with linkage to operative + path_malignant via temporal-window logic). It's NOT a simple SELECT * — so the CTC-equivalence pattern doesn't directly apply. The pre363v3 archive at `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_invasion_events_v1_pre363v3_20260422_032942` differs from current canonical by 1,353 row hashes (post-snapshot rebuild).
>
> **Verification approach (you to execute):**
>
> 1. Read `qc_framework_v1/REMAINING_WORK_INVENTORY.md` for full context.
> 2. Read `scripts/363_invasion_canonical.py` end-to-end. Identify each modality CTE in `_build_step_1_sql` and the source table it pulls from.
> 3. For each `source_modality` slice in `canonical_invasion_events_v1`, **re-derive that slice from its source table** (via the CTE logic) and compare row-by-row. This is the same Script-rule re-run methodology that worked for path_malignant mig_88 and operative_events mig_90b — just per-modality instead of single-source.
> 4. Where source tables have been archived (note_entities_llm_*_pre*), verify against the archive (read-only).
> 5. For each source modality, generate a per-row CSV at `verification_csvs/canonical_invasion_events_v1/<modality>__mig_91.csv` with cols: `(invasion_event_id, research_id, source_kind, source_row_id, db_value_<col>, recomputed_value_<col>, match_flag)`. Mismatches sort to top. CSV is gitignored (PHI-adjacent).
> 6. **Surface ambiguous cases for my review.** Specifically: rows where the linkage method is `temporal_90d_ambiguous` (`linkage_ambiguous_multi_finding = TRUE`; renamed by mig_95) — those are cases where canonical picked one of multiple candidate surgery episodes within a 90-day window. I want to see a CSV with `(research_id, finding_date, n_candidate_episodes, picked_episode_id, alternative_episode_ids, evidence_text)` so I can adjudicate which linkage is correct. Save at `verification_csvs/canonical_invasion_events_v1/ambiguous_linkage_review__mig_91.csv`.
> 7. Findings-vs-staging rule applies: invasion findings are primary; any staging implications must follow findings (not the inverse). Flag any rows where `finding_status` disagrees with the source extraction's status.
> 8. Once mismatches and ambiguous cases are resolved (via my review of the CSV(s)), write `qc_framework_v1/migrations/91_invasion_events_verify_and_signoff.sql`, execute via `mcp__motherduck__query_rw`, commit + push.
> 9. Update `qc_framework_v1/VERIFIED_TABLES.md` and `qc_framework_v1/VERIFICATION_PROGRESS.md`.
>
> ### Standing protocol rules (don't violate)
>
> - MotherDuck account: `logan.glosser.eras@gmail.com` for the publication DB.
> - Writes through `mcp__motherduck__query_rw` AND mirrored to `qc_framework_v1/migrations/NN_*.sql`.
> - PHI: never print clinical notes; research_id only; no cloud PHI.
> - Commit hygiene: stage by explicit path, never `-A`. Lint Python (`python3 -m py_compile`) before commit. Author: `Logan Glosser <logan.glosser@gmail.com>`.
> - Read-only verification reference is permitted from `archive_pub_v1_0`; never source canonical from archive at build time.
> - Cite memory files when invoking established rules (`feedback_findings_vs_staging.md`, `project_ctc_equivalence_verification_pattern.md`, `feedback_motherduck_direct_check.md`, `reference_protocol_v2_md_accounts.md`).
>
> ### Sibling tables also queued (after invasion_events closes)
>
> Three smaller LLM-output invasion canonicals follow the airway-invasion review pattern. Smallest first:
>
> - `canonical_esophageal_invasion_events_v1` — 188 rows / 15 cols
> - `canonical_t4b_invasion_events_v1` — 944 rows / 19 cols
> - `canonical_vascular_invasion_events_v1` — 3,861 rows / 22 cols
>
> Each will need a positive-subset CSV for my review. Build the CSVs now (during invasion_events work) so I can review them in parallel.
>
> ### Goal restatement
>
> **Clean master canonical at `thyroid_canonical_publication_v1_0`. Archive deprecated/intermediate tables to `"Thyroid 2026 UPdated"`. Every cell of every column verified against its source-of-truth.** Build CSVs for anything I need to adjudicate; don't make analytical claims without a `query` first; check MotherDuck directly for current state every round.

---

*Generated 2026-04-28 post-mig_90 sign-off.*

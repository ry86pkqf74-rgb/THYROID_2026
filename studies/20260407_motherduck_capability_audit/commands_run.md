# Commands run — MotherDuck capability audit (2026-04-07)

## 1. Token-source detection
```
token_mode() secrets.toml:MOTHERDUCK_TOKEN
read_scaling_token_mode() none
resolve_database_for_env(dev) Thyroid 2026 Molecular Dev 20260407
resolve_database_for_env(qa) Thyroid 2026 Molecular QA 20260407
resolve_database_for_env(prod) Thyroid 2026
EXIT:0
```

## 2. smoke_test_md_connection.py --md

```
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
DuckDB version : v1.4.4
Catalog / DB    : 'Thyroid 2026' / 'Thyroid 2026'
Connection type : MotherDuck (cloud)
PASS
EXIT:0
```

## 3.130 inspect (default token order)

```
-- session
current_database()                current_timestamp
      Thyroid 2026 2026-04-07 09:41:36.240472-04:00

-- MD_INFORMATION_SCHEMA.DATABASES (Thyroid*)
                                                     name     type                created_ts
                                             Thyroid 2026 DUCKLAKE 2026-04-02 04:31:53-04:00
                      Thyroid 2026 Molecular Dev 20260407  DEFAULT 2026-04-07 02:39:51-04:00
Thyroid 2026 Molecular PrePromote agent_20260407_workflow  DEFAULT 2026-04-07 02:39:05-04:00
                       Thyroid 2026 Molecular QA 20260407  DEFAULT 2026-04-07 01:44:43-04:00

-- catalog 'Thyroid 2026' type='DUCKLAKE' ducklake=True

-- recent DATABASE_SNAPSHOTS (prod)
database_name                          snapshot_id snapshot_name              created_ts
 Thyroid 2026 01535255-90b2-4eed-af0d-985c4cbe9ddd          None 2026-04-07 13:35:50.931
 Thyroid 2026 e54d8c84-ca9a-4039-aedb-effdca53ce06          None 2026-04-07 13:34:50.288
 Thyroid 2026 9ed11ecc-29f8-4796-97f9-3ab74ef38f7d          None 2026-04-07 13:33:49.676
 Thyroid 2026 f31fd05b-d5f3-4652-ab1e-b17e9e876a83          None 2026-04-07 13:31:49.011
 Thyroid 2026 cf6101e0-13ee-42d5-96ca-354950e039f7          None 2026-04-07 13:30:48.477
 Thyroid 2026 e2b02a40-729b-46b6-96a2-b6da79aeccc1          None 2026-04-07 13:25:47.853
 Thyroid 2026 c6ad13ac-f61e-4a01-99d5-2c2703d69e69          None 2026-04-07 13:14:54.318
 Thyroid 2026 5367572f-e9fb-49fc-95b7-d8819c5ee9c5          None 2026-04-07 13:13:53.691
 Thyroid 2026 83f951ea-07d4-463f-bc83-a156eb07353c          None 2026-04-07 13:12:53.092
 Thyroid 2026 c89dcde1-dd20-42ea-82fb-c7a68d983607          None 2026-04-07 13:11:52.328
 Thyroid 2026 8f587be3-c5ed-467b-8f63-a96b0a562378          None 2026-04-07 13:10:51.604
 Thyroid 2026 c987bfa5-ff86-4893-9eb4-6ae7d833bd21          None 2026-04-07 12:42:41.773
 Thyroid 2026 a69835a7-99b5-4f49-97ab-881812c6f86d          None 2026-04-07 12:41:41.202
 Thyroid 2026 b2cbad51-8d94-4a5f-b630-e89bcdf44827          None 2026-04-07 12:40:40.582
 Thyroid 2026 ac9a8098-8a5d-4b5e-99cf-0abb1b652b16          None 2026-04-07 12:39:39.881
EXIT:0
```

## 3b. 130 inspect --md-sa (skipped if no MD_SA_TOKEN in env)

```
SKIP: MD_SA_TOKEN not set in environment (.env.motherduck may omit it)
```

## 4. Make targets (md-smoke, md-v2-gate-md-dryrun, md-live-release-dryrun)

### make md-smoke
```
ERROR: Set MOTHERDUCK_TOKEN and/or MD_SA_TOKEN for MotherDuck targets.
  See docs/motherduck_database_contract_v1.md (Connection Reference) and .env.motherduck.example
make: *** [md-smoke] Error 1
EXIT:1
```

`md-v2-gate-md-dryrun` / `md-live-release-dryrun` were not executed via Make for the same token-guard reason; equivalent Python steps are under **§4b** below (including 124 with tag `20991231`).

## 4b. Equivalent Python dry-run path (tokens via motherduck_client / secrets.toml)

### 116_md_stage_loader.py --md --dry-run
```
  [inventory] 30 parquets to load
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
  [dry-run] Would execute:
    CREATE SCHEMA IF NOT EXISTS v2_stage
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_imaging  (11,037 rows from note_entities_llm_imaging.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_tirads_granular  (11,037 rows from note_entities_llm_tirads_granular.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_us_nodule_dynamics  (11,037 rows from note_entities_llm_us_nodule_dynamics.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_labs  (11,037 rows from note_entities_llm_labs.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_tg_kinetics  (11,037 rows from note_entities_llm_tg_kinetics.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_pathology  (11,037 rows from note_entities_llm_pathology.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_synoptic_pathology_enrichment  (11,037 rows from note_entities_llm_synoptic_pathology_enrichment.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_rai_detailed  (11,037 rows from note_entities_llm_rai_detailed.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_rad_treatment  (11,037 rows from note_entities_llm_rad_treatment.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_parathyroid_detail  (11,037 rows from note_entities_llm_parathyroid_detail.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_recurrence  (11,037 rows from note_entities_llm_recurrence.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_survival_followup  (11,037 rows from note_entities_llm_survival_followup.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_cervical_ln_detail  (11,037 rows from note_entities_llm_cervical_ln_detail.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_functional_outcomes  (11,037 rows from note_entities_llm_functional_outcomes.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_past_medical_hx  (11,037 rows from note_entities_llm_past_medical_hx.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_past_surgical_hx  (11,037 rows from note_entities_llm_past_surgical_hx.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_presenting_symptoms  (11,037 rows from note_entities_llm_presenting_symptoms.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_physical_exam  (11,037 rows from note_entities_llm_physical_exam.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_vascular_invasion  (11,037 rows from note_entities_llm_vascular_invasion.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_airway_invasion  (11,037 rows from note_entities_llm_airway_invasion.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_frozen_section_detail  (11,037 rows from note_entities_llm_frozen_section_detail.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_dynamic_risk_response  (11,037 rows from note_entities_llm_dynamic_risk_response.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_patient_decision_adherence  (11,037 rows from note_entities_llm_patient_decision_adherence.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_recurrence_detailed  (11,037 rows from note_entities_llm_recurrence_detailed.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_complications_rln_laryngoscopy  (11,037 rows from note_entities_llm_complications_rln_laryngoscopy.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_medication_management  (11,037 rows from note_entities_llm_medication_management.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_operative_details  (11,037 rows from note_entities_llm_operative_details.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_operative_v2_enrichment  (11,037 rows from note_entities_llm_operative_v2_enrichment.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_parathyroid_per_gland  (11,037 rows from note_entities_llm_parathyroid_per_gland.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_molecular_thyroseq_afirma  (11,037 rows from note_entities_llm_molecular_thyroseq_afirma.parquet)
    30 inventory rows into v2_stage.load_inventory
EXIT:0
```
### 112_v2_domain_promotion_gate.py --motherduck-check
```
09:42:09  INFO      Output directory: /Users/ros/THyroid 2026/THYROID_2026/studies/v2_domain_promotion_gate_make_md_formalization_dryrun_audit
09:42:09  INFO      Phase 1: Domain Inventory
09:42:09  INFO      Registry domains: 31
09:42:09  INFO      On-disk v2 parquets: 37
09:42:09  INFO      Domain inventory: 44 rows (36 with parquets)
09:42:09  INFO      Phase 2: Per-Domain Validation
09:42:09  INFO        Validating imaging                                   (? rows expected)
09:42:09  INFO      Expanded v2 combined input: 11037 note rows -> 8428 entity rows
09:42:09  INFO        Validating tirads_granular                           (? rows expected)
09:42:09  INFO      Expanded v2 combined input: 11037 note rows -> 179 entity rows
09:42:09  INFO        Validating us_nodule_dynamics                        (? rows expected)
09:42:10  INFO      Expanded v2 combined input: 11037 note rows -> 49 entity rows
09:42:10  INFO        Validating labs                                      (? rows expected)
09:42:10  INFO      Expanded v2 combined input: 11037 note rows -> 2460 entity rows
09:42:10  INFO        Validating tg_kinetics                               (? rows expected)
09:42:10  INFO      Expanded v2 combined input: 11037 note rows -> 173 entity rows
09:42:10  INFO        Validating pathology                                 (? rows expected)
09:42:10  INFO      Expanded v2 combined input: 11037 note rows -> 10894 entity rows
09:42:10  INFO        Validating synoptic_pathology_enrichment             (? rows expected)
09:42:10  INFO      Expanded v2 combined input: 11037 note rows -> 38 entity rows
09:42:10  INFO        Validating rai_detailed                              (? rows expected)
09:42:11  INFO      Expanded v2 combined input: 11037 note rows -> 3747 entity rows
09:42:11  INFO        Validating rad_treatment                             (? rows expected)
09:42:11  INFO      Expanded v2 combined input: 11037 note rows -> 519 entity rows
09:42:11  INFO        Validating parathyroid_detail                        (? rows expected)
09:42:11  INFO      Expanded v2 combined input: 11037 note rows -> 255 entity rows
09:42:11  INFO        Validating recurrence                                (? rows expected)
09:42:11  INFO      Expanded v2 combined input: 11037 note rows -> 303 entity rows
09:42:11  INFO        Validating survival_followup                         (? rows expected)
09:42:11  INFO      Expanded v2 combined input: 11037 note rows -> 9809 entity rows
09:42:11  INFO        Validating cervical_ln_detail                        (? rows expected)
09:42:11  INFO      Expanded v2 combined input: 11037 note rows -> 104 entity rows
09:42:11  INFO        Validating functional_outcomes                       (? rows expected)
09:42:12  INFO      Expanded v2 combined input: 11037 note rows -> 3322 entity rows
09:42:12  INFO        Validating past_medical_hx                           (? rows expected)
09:42:12  INFO      Expanded v2 combined input: 11037 note rows -> 865 entity rows
09:42:12  INFO        Validating past_surgical_hx                          (? rows expected)
09:42:12  INFO      Expanded v2 combined input: 11037 note rows -> 3919 entity rows
09:42:12  INFO        Validating presenting_symptoms                       (? rows expected)
09:42:12  INFO      Expanded v2 combined input: 11037 note rows -> 280 entity rows
09:42:12  INFO        Validating physical_exam                             (? rows expected)
09:42:12  INFO      Expanded v2 combined input: 11037 note rows -> 1924 entity rows
09:42:12  INFO        Validating vascular_invasion                         (? rows expected)
09:42:12  INFO      Expanded v2 combined input: 11037 note rows -> 4241 entity rows
09:42:12  INFO        Validating airway_invasion                           (? rows expected)
09:42:12  INFO      Expanded v2 combined input: 11037 note rows -> 3116 entity rows
09:42:12  INFO        Validating frozen_section_detail                     (? rows expected)
09:42:13  INFO      Expanded v2 combined input: 11037 note rows -> 380 entity rows
09:42:13  INFO        Validating dynamic_risk_response                     (? rows expected)
09:42:13  INFO      Expanded v2 combined input: 11037 note rows -> 53 entity rows
09:42:13  INFO        Validating patient_decision_adherence                (? rows expected)
09:42:13  INFO      Expanded v2 combined input: 11037 note rows -> 641 entity rows
09:42:13  INFO      Phase 3: Cross-Domain Concordance
09:42:13  INFO      Expanded v2 combined input: 11037 note rows -> 8428 entity rows
09:42:13  INFO      Expanded v2 combined input: 11037 note rows -> 179 entity rows
09:42:13  INFO      Expanded v2 combined input: 11037 note rows -> 49 entity rows
09:42:13  INFO      Expanded v2 combined input: 11037 note rows -> 2460 entity rows
09:42:14  INFO      Expanded v2 combined input: 11037 note rows -> 173 entity rows
09:42:14  INFO      Expanded v2 combined input: 11037 note rows -> 10894 entity rows
09:42:14  INFO      Expanded v2 combined input: 11037 note rows -> 38 entity rows
09:42:14  INFO      Expanded v2 combined input: 11037 note rows -> 3747 entity rows
09:42:14  INFO      Expanded v2 combined input: 11037 note rows -> 519 entity rows
09:42:14  INFO      Expanded v2 combined input: 11037 note rows -> 255 entity rows
09:42:15  INFO      Expanded v2 combined input: 11037 note rows -> 303 entity rows
09:42:15  INFO      Expanded v2 combined input: 11037 note rows -> 9809 entity rows
09:42:15  INFO      Expanded v2 combined input: 11037 note rows -> 104 entity rows
09:42:15  INFO      Expanded v2 combined input: 11037 note rows -> 3322 entity rows
09:42:15  INFO      Expanded v2 combined input: 11037 note rows -> 865 entity rows
09:42:15  INFO      Expanded v2 combined input: 11037 note rows -> 3919 entity rows
09:42:15  INFO      Expanded v2 combined input: 11037 note rows -> 280 entity rows
09:42:16  INFO      Expanded v2 combined input: 11037 note rows -> 1924 entity rows
09:42:16  INFO      Expanded v2 combined input: 11037 note rows -> 4241 entity rows
09:42:16  INFO      Expanded v2 combined input: 11037 note rows -> 3116 entity rows
09:42:16  INFO      Expanded v2 combined input: 11037 note rows -> 380 entity rows
09:42:16  INFO      Expanded v2 combined input: 11037 note rows -> 53 entity rows
09:42:16  INFO      Expanded v2 combined input: 11037 note rows -> 641 entity rows
09:42:16  INFO      Combined entity DataFrame: 55699 rows from 23 domain files
09:42:16  INFO      Classifying 55699 entity rows into comparison domains
09:42:19  INFO      Comparison domains: complications, genetics, medications, operative_detail, problem_list, procedures, staging
09:42:19  INFO      Building baseline (note_entities_*) comparators
09:42:20  INFO      Building structured (canonical tables) comparators
09:42:21  INFO      Building side-by-side concordance
09:42:23  INFO        => llm_side_by_side.parquet    55,699 rows x  78 cols  (4.55 MB)
09:42:24  INFO      Phase 4: Manual Review Queue (strict)
09:42:24  INFO      Manual review queue: 5622 rows
09:42:24  INFO      Phase 5: Promotion Gate
09:42:24  INFO        Running G8 MotherDuck parity check
09:42:29  INFO      Phase 6: Reports & MotherDuck Artifacts

======================================================================
  V2 DOMAIN PROMOTION GATE — PASS
======================================================================
  [PASS]  G1: Domain completeness (v2 only)
  [PASS]  G2: Schema compliance (core columns)
  [PASS]  G3: Provenance columns
  [PASS]  G4: Duplicate rate
  [PASS]  G5: Date coverage (critical domains)
  [PASS]  G6: Concordance floor (critical domains)
  [PASS]  G7: Unresolved discordance
  [PASS]  G8: MotherDuck v2_stage parity
======================================================================
  Artifacts: /Users/ros/THyroid 2026/THYROID_2026/studies/v2_domain_promotion_gate_make_md_formalization_dryrun_audit
  Verdict:   PASS
======================================================================

EXIT:0
```
### 119_md_formalization_validate.py --md (non-release-mode per Makefile)
```
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
=== MotherDuck Formalization Validation (STRUCTURAL) ===

--- Check 1: MD Attachment ---
  [PASS] MD attachment: 9 databases attached

--- Check 2: Row Count Parity ---
  [PASS] Row count parity: 23 domains checked, all match
  [PASS] Canonical canonical_extracted_fact_long_v2: local=123,577  md=123,577
  [PASS] Canonical canonical_fact_quarantine_v2: local=199  md=199
  [FAIL] Canonical note_extraction_runs: local=5  md=3

--- Check 3: Schema Completeness ---
  [PASS] Schema completeness: Wide note-level v2 contract on 23 promoted table(s); entity_type/entity_value_* in main.canonical_extracted_fact_long_v2 (see docs/domain_mapping_rules.md). Example stems: note_entities_llm_imaging, note_entities_llm_tirads_granular, note_entities_llm_us_nodule_dynamics…

--- Check 4: Canonical Distribution ---
  [PASS] Canonical dist (canonical_extracted_fact_long_v2): 594 domains, 123,577 total rows
  [PASS] Canonical dist (canonical_fact_quarantine_v2): 69 domains, 199 total rows

--- Check 5: Review Queue ---
  [PASS] Review queue: 5,622 total, 5,622 reviewed, 0 pending

--- Check 6: QA Views ---
  [PASS] QA view promotion_scorecard_summary_v: 6 rows
  [PASS] QA view domain_validation_summary_v: 6 rows
  [PASS] QA view date_provenance_completeness_v: 23 rows
  [PASS] QA view manual_review_queue_summary_v: 21 rows

--- Check 7: Load Inventory ---
  [PASS] Load inventory: 180 entries, all match

--- Check 8: Release Schemas ---
  [PASS] Release schemas: 6 found: release_20260406, release_20260407, release_20260407_final, release_20260407_final2, release_20260408, release_20260409

--- Check 11: Analyst presentation layer (master_*_verified_v1) ---
  [PASS] Presentation master_fact_long_verified_v1: 123,577 rows; core traceability non-null (reviewer_status may be NULL)
  [PASS] Presentation master_source_lineage_v1: 123,577 rows; core traceability non-null (reviewer_status may be NULL)
  [PASS] Presentation master_patient_rollup_verified_v1: 5,574 patient rows; research_id + release_tag + review metrics present

--- Check 12: Molecular normalized contract views ---
  [PASS] Molecular row counts: main.molecular_results is empty — contract view checks skipped

--- Check 13: Specimen + analytic FHIR layer ---
  [PASS] Specimen/FHIR tables present: 10 objects found
  [PASS] Specimen master fingerprint uniqueness: distinct fingerprints
  [PASS] qa.val_specimen_contract_v1: no FAIL rows recorded
  [PASS] qa.val_specimen_genomic_binding_v1: no FAIL rows recorded
  [WARN] Specimen/FHIR QA diagnostics (142 views + focus checks): dup_master_fp=0, dup_focus_fp=None, orphan_focus=None, orphan_genomic(master/focus)=0/n/a, broken_fhir_refs=0, prov_gaps(master/focus/high_tier_null_spec)=0/n/a/0 | NOTE: some focus-table scans unavailable on this catalog
  [WARN] Specimen-adjacent review burden (open/pending): genomic_link_review open/pending=9966; merge queue: direct COUNT unavailable (MotherDuck/catalog — audit manually)

=== Summary: 22 PASS / 2 WARN / 1 FAIL ===

  [report] /Users/ros/THyroid 2026/THYROID_2026/studies/20260407_motherduck_formalization/validation_report.md
EXIT:0
```
### 124_md_live_release_audit.py --md --dry-run --tag 20260407
```
======================================================================
  124 — MotherDuck Live Release Audit
  Tag  : 20260407
  Dir  : /Users/ros/THyroid 2026/THYROID_2026/studies/20260407_motherduck_live_release_audit
  MD   : YES (fail-closed)
  Mode : dry-run
======================================================================
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)

======================================================================
  STEP: Preflight (MD attachment + database inventory)
======================================================================
  [PASS] MD attachment: 9 database(s) attached, md_confirmed=True
         1027  md_information_schema           None
         1090  thyroid_research_ro_v2          _share/thyroid_research_ro_v2/2558f066-1c5d-46a5-afbc-800fd5f7568d
         1092  sample_data                     _share/sample_data/23b0d623-1361-421d-ae77-62d701d471e6
         1094  Thyroid 2026 Molecular Dev 20260407  Thyroid 2026 Molecular Dev 20260407
         1096  Thyroid 2026 Molecular PrePromote agent_20260407_workflow  Thyroid 2026 Molecular PrePromote agent_20260407_workflow
         1098  Thyroid 2026 Molecular QA 20260407  Thyroid 2026 Molecular QA 20260407
         1100  Thyroid 2026                    Thyroid 2026
         1102  rosflow                         rosflow
         1104  my_db                           my_db
  [PASS] md_information_schema.databases: 6 row(s)
         {'name': 'my_db', 'uuid': UUID('e0db7fdc-5cb6-4f57-9944-6baadccd82c1'), 'created_ts': Timestamp('2026-03-07 02:53:36-0500', tz='America/New_York'), 'transient': False, 'historical_snapshot_retention': Timedelta('7 days 00:00:00'), 'type': 'DEFAULT'}
         {'name': 'rosflow', 'uuid': UUID('c8f55a3a-ab80-4f93-8bb1-74b55770ad39'), 'created_ts': Timestamp('2026-03-15 13:16:55-0400', tz='America/New_York'), 'transient': False, 'historical_snapshot_retention': Timedelta('7 days 00:00:00'), 'type': 'DEFAULT'}
         {'name': 'Thyroid 2026', 'uuid': UUID('b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc'), 'created_ts': Timestamp('2026-04-02 04:31:53-0400', tz='America/New_York'), 'transient': False, 'historical_snapshot_retention': Timedelta('7 days 00:00:00'), 'type': 'DUCKLAKE'}
         {'name': 'Thyroid 2026 Molecular Dev 20260407', 'uuid': UUID('02fdc556-f9e6-4fdd-9f8d-84377f055e96'), 'created_ts': Timestamp('2026-04-07 02:39:51-0400', tz='America/New_York'), 'transient': False, 'historical_snapshot_retention': Timedelta('7 days 00:00:00'), 'type': 'DEFAULT'}
         {'name': 'Thyroid 2026 Molecular PrePromote agent_20260407_workflow', 'uuid': UUID('9b6d3949-4698-4be7-95b1-3b4e7c30d9e4'), 'created_ts': Timestamp('2026-04-07 02:39:05-0400', tz='America/New_York'), 'transient': False, 'historical_snapshot_retention': Timedelta('7 days 00:00:00'), 'type': 'DEFAULT'}
         {'name': 'Thyroid 2026 Molecular QA 20260407', 'uuid': UUID('f05500b0-2eea-41bf-b5d4-35e677f6ebc5'), 'created_ts': Timestamp('2026-04-07 01:44:43-0400', tz='America/New_York'), 'transient': False, 'historical_snapshot_retention': Timedelta('7 days 00:00:00'), 'type': 'DEFAULT'}
  [INFO] Schemas present: hn, kaggle, main, mm_contract_dev, nyc, qa, release_20260406, release_20260407, release_20260407_final, release_20260407_final2, release_20260408, release_20260409, stackoverflow_survey, v2_stage, who
  [INFO] Snapshot retention query: Catalog Error: Table with name snapshots does not exist!
Did you mean "storage_info_history"?

LINE 1: SELECT * FROM md_information_schema.snapshots
                      ^ (may be unavailable on this plan tier)
  [write] preflight_db_list.json

======================================================================
  STEP: Stage refresh (116)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/116_md_stage_loader.py --md --dry-run
  LOG : stage_refresh_output.log
======================================================================
  [inventory] 30 parquets to load
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
  [dry-run] Would execute:
    CREATE SCHEMA IF NOT EXISTS v2_stage
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_imaging  (11,037 rows from note_entities_llm_imaging.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_tirads_granular  (11,037 rows from note_entities_llm_tirads_granular.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_us_nodule_dynamics  (11,037 rows from note_entities_llm_us_nodule_dynamics.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_labs  (11,037 rows from note_entities_llm_labs.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_tg_kinetics  (11,037 rows from note_entities_llm_tg_kinetics.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_pathology  (11,037 rows from note_entities_llm_pathology.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_synoptic_pathology_enrichment  (11,037 rows from note_entities_llm_synoptic_pathology_enrichment.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_rai_detailed  (11,037 rows from note_entities_llm_rai_detailed.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_rad_treatment  (11,037 rows from note_entities_llm_rad_treatment.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_parathyroid_detail  (11,037 rows from note_entities_llm_parathyroid_detail.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_recurrence  (11,037 rows from note_entities_llm_recurrence.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_survival_followup  (11,037 rows from note_entities_llm_survival_followup.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_cervical_ln_detail  (11,037 rows from note_entities_llm_cervical_ln_detail.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_functional_outcomes  (11,037 rows from note_entities_llm_functional_outcomes.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_past_medical_hx  (11,037 rows from note_entities_llm_past_medical_hx.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_past_surgical_hx  (11,037 rows from note_entities_llm_past_surgical_hx.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_presenting_symptoms  (11,037 rows from note_entities_llm_presenting_symptoms.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_physical_exam  (11,037 rows from note_entities_llm_physical_exam.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_vascular_invasion  (11,037 rows from note_entities_llm_vascular_invasion.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_airway_invasion  (11,037 rows from note_entities_llm_airway_invasion.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_frozen_section_detail  (11,037 rows from note_entities_llm_frozen_section_detail.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_dynamic_risk_response  (11,037 rows from note_entities_llm_dynamic_risk_response.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_patient_decision_adherence  (11,037 rows from note_entities_llm_patient_decision_adherence.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_recurrence_detailed  (11,037 rows from note_entities_llm_recurrence_detailed.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_complications_rln_laryngoscopy  (11,037 rows from note_entities_llm_complications_rln_laryngoscopy.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_medication_management  (11,037 rows from note_entities_llm_medication_management.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_operative_details  (11,037 rows from note_entities_llm_operative_details.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_operative_v2_enrichment  (11,037 rows from note_entities_llm_operative_v2_enrichment.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_parathyroid_per_gland  (11,037 rows from note_entities_llm_parathyroid_per_gland.parquet)
    CREATE OR REPLACE TABLE v2_stage.note_entities_llm_molecular_thyroseq_afirma  (11,037 rows from note_entities_llm_molecular_thyroseq_afirma.parquet)
    30 inventory rows into v2_stage.load_inventory
  [Stage refresh (116)] OK (exit 0)

======================================================================
  STEP: Promotion gate (112)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/112_v2_domain_promotion_gate.py --run-label promote_20260407_0942 --output-dir /Users/ros/THyroid 2026/THYROID_2026/studies/20260407_motherduck_live_release_audit/promotion_gate --motherduck-check
  LOG : promotion_gate_output.log
======================================================================
09:42:55  INFO      Output directory: /Users/ros/THyroid 2026/THYROID_2026/studies/20260407_motherduck_live_release_audit/promotion_gate
09:42:55  INFO      Phase 1: Domain Inventory
09:42:55  INFO      Registry domains: 31
09:42:55  INFO      On-disk v2 parquets: 37
09:42:55  INFO      Domain inventory: 44 rows (36 with parquets)
09:42:55  INFO      Phase 2: Per-Domain Validation
09:42:55  INFO        Validating imaging                                   (? rows expected)
09:42:56  INFO      Expanded v2 combined input: 11037 note rows -> 8428 entity rows
09:42:56  INFO        Validating tirads_granular                           (? rows expected)
09:42:56  INFO      Expanded v2 combined input: 11037 note rows -> 179 entity rows
09:42:56  INFO        Validating us_nodule_dynamics                        (? rows expected)
09:42:56  INFO      Expanded v2 combined input: 11037 note rows -> 49 entity rows
09:42:56  INFO        Validating labs                                      (? rows expected)
09:42:56  INFO      Expanded v2 combined input: 11037 note rows -> 2460 entity rows
09:42:56  INFO        Validating tg_kinetics                               (? rows expected)
09:42:56  INFO      Expanded v2 combined input: 11037 note rows -> 173 entity rows
09:42:56  INFO        Validating pathology                                 (? rows expected)
09:42:56  INFO      Expanded v2 combined input: 11037 note rows -> 10894 entity rows
09:42:56  INFO        Validating synoptic_pathology_enrichment             (? rows expected)
09:42:57  INFO      Expanded v2 combined input: 11037 note rows -> 38 entity rows
09:42:57  INFO        Validating rai_detailed                              (? rows expected)
09:42:57  INFO      Expanded v2 combined input: 11037 note rows -> 3747 entity rows
09:42:57  INFO        Validating rad_treatment                             (? rows expected)
09:42:57  INFO      Expanded v2 combined input: 11037 note rows -> 519 entity rows
09:42:57  INFO        Validating parathyroid_detail                        (? rows expected)
09:42:57  INFO      Expanded v2 combined input: 11037 note rows -> 255 entity rows
09:42:57  INFO        Validating recurrence                                (? rows expected)
09:42:57  INFO      Expanded v2 combined input: 11037 note rows -> 303 entity rows
09:42:57  INFO        Validating survival_followup                         (? rows expected)
09:42:58  INFO      Expanded v2 combined input: 11037 note rows -> 9809 entity rows
09:42:58  INFO        Validating cervical_ln_detail                        (? rows expected)
09:42:58  INFO      Expanded v2 combined input: 11037 note rows -> 104 entity rows
09:42:58  INFO        Validating functional_outcomes                       (? rows expected)
09:42:58  INFO      Expanded v2 combined input: 11037 note rows -> 3322 entity rows
09:42:58  INFO        Validating past_medical_hx                           (? rows expected)
09:42:58  INFO      Expanded v2 combined input: 11037 note rows -> 865 entity rows
09:42:58  INFO        Validating past_surgical_hx                          (? rows expected)
09:42:58  INFO      Expanded v2 combined input: 11037 note rows -> 3919 entity rows
09:42:58  INFO        Validating presenting_symptoms                       (? rows expected)
09:42:59  INFO      Expanded v2 combined input: 11037 note rows -> 280 entity rows
09:42:59  INFO        Validating physical_exam                             (? rows expected)
09:42:59  INFO      Expanded v2 combined input: 11037 note rows -> 1924 entity rows
09:42:59  INFO        Validating vascular_invasion                         (? rows expected)
09:42:59  INFO      Expanded v2 combined input: 11037 note rows -> 4241 entity rows
09:42:59  INFO        Validating airway_invasion                           (? rows expected)
09:42:59  INFO      Expanded v2 combined input: 11037 note rows -> 3116 entity rows
09:42:59  INFO        Validating frozen_section_detail                     (? rows expected)
09:42:59  INFO      Expanded v2 combined input: 11037 note rows -> 380 entity rows
09:42:59  INFO        Validating dynamic_risk_response                     (? rows expected)
09:43:00  INFO      Expanded v2 combined input: 11037 note rows -> 53 entity rows
09:43:00  INFO        Validating patient_decision_adherence                (? rows expected)
09:43:00  INFO      Expanded v2 combined input: 11037 note rows -> 641 entity rows
09:43:00  INFO      Phase 3: Cross-Domain Concordance
09:43:00  INFO      Expanded v2 combined input: 11037 note rows -> 8428 entity rows
09:43:00  INFO      Expanded v2 combined input: 11037 note rows -> 179 entity rows
09:43:00  INFO      Expanded v2 combined input: 11037 note rows -> 49 entity rows
09:43:00  INFO      Expanded v2 combined input: 11037 note rows -> 2460 entity rows
09:43:01  INFO      Expanded v2 combined input: 11037 note rows -> 173 entity rows
09:43:01  INFO      Expanded v2 combined input: 11037 note rows -> 10894 entity rows
09:43:01  INFO      Expanded v2 combined input: 11037 note rows -> 38 entity rows
09:43:01  INFO      Expanded v2 combined input: 11037 note rows -> 3747 entity rows
09:43:01  INFO      Expanded v2 combined input: 11037 note rows -> 519 entity rows
09:43:01  INFO      Expanded v2 combined input: 11037 note rows -> 255 entity rows
09:43:02  INFO      Expanded v2 combined input: 11037 note rows -> 303 entity rows
09:43:02  INFO      Expanded v2 combined input: 11037 note rows -> 9809 entity rows
09:43:02  INFO      Expanded v2 combined input: 11037 note rows -> 104 entity rows
09:43:02  INFO      Expanded v2 combined input: 11037 note rows -> 3322 entity rows
09:43:02  INFO      Expanded v2 combined input: 11037 note rows -> 865 entity rows
09:43:03  INFO      Expanded v2 combined input: 11037 note rows -> 3919 entity rows
09:43:03  INFO      Expanded v2 combined input: 11037 note rows -> 280 entity rows
09:43:03  INFO      Expanded v2 combined input: 11037 note rows -> 1924 entity rows
09:43:03  INFO      Expanded v2 combined input: 11037 note rows -> 4241 entity rows
09:43:03  INFO      Expanded v2 combined input: 11037 note rows -> 3116 entity rows
09:43:03  INFO      Expanded v2 combined input: 11037 note rows -> 380 entity rows
09:43:03  INFO      Expanded v2 combined input: 11037 note rows -> 53 entity rows
09:43:04  INFO      Expanded v2 combined input: 11037 note rows -> 641 entity rows
09:43:04  INFO      Combined entity DataFrame: 55699 rows from 23 domain files
09:43:04  INFO      Classifying 55699 entity rows into comparison domains
09:43:06  INFO      Comparison domains: complications, genetics, medications, operative_detail, problem_list, procedures, staging
09:43:06  INFO      Building baseline (note_entities_*) comparators
09:43:08  INFO      Building structured (canonical tables) comparators
09:43:08  INFO      Building side-by-side concordance
09:43:11  INFO        => llm_side_by_side.parquet    55,699 rows x  78 cols  (4.55 MB)
09:43:11  INFO      Phase 4: Manual Review Queue (strict)
09:43:12  INFO      Manual review queue: 5622 rows
09:43:12  INFO      Phase 5: Promotion Gate
09:43:12  INFO        Running G8 MotherDuck parity check
09:43:16  INFO      Phase 6: Reports & MotherDuck Artifacts

======================================================================
  V2 DOMAIN PROMOTION GATE — PASS
======================================================================
  [PASS]  G1: Domain completeness (v2 only)
  [PASS]  G2: Schema compliance (core columns)
  [PASS]  G3: Provenance columns
  [PASS]  G4: Duplicate rate
  [PASS]  G5: Date coverage (critical domains)
  [PASS]  G6: Concordance floor (critical domains)
  [PASS]  G7: Unresolved discordance
  [PASS]  G8: MotherDuck v2_stage parity
======================================================================
  Artifacts: /Users/ros/THyroid 2026/THYROID_2026/studies/20260407_motherduck_live_release_audit/promotion_gate
  Verdict:   PASS
======================================================================

  [Promotion gate (112)] OK (exit 0)

======================================================================
  STEP: Canonical materialization (103)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/103_fact_lineage_materialize.py --md --dry-run
  LOG : canonical_output.log
======================================================================
/Users/ros/THyroid 2026/THYROID_2026/scripts/103_fact_lineage_materialize.py:725: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  uni = pd.concat(aligned, ignore_index=True)
======================================================================
  103 — canonical_extracted_fact_long (v1 + v2) + quarantine
  Registry: YAML-driven
  Domains: 30
======================================================================
  loaded note_entities_staging: 3,807 rows  [family=pathology]
  loaded note_entities_genetics: 1,738 rows  [family=molecular]
  loaded note_entities_procedures: 21,942 rows  [family=operative]
  loaded note_entities_operative_detail: 12,151 rows  [family=operative]
  loaded note_entities_complications: 9,359 rows  [family=operative]
  loaded note_entities_medications: 7,501 rows  [family=followup]
  loaded note_entities_problem_list: 11,579 rows  [family=demographics]
    expanded fleet format: 11,037 note rows → 8,428 entity rows
  loaded note_entities_llm_imaging: 8,428 rows  [family=imaging]
    expanded fleet format: 11,037 note rows → 179 entity rows
  loaded note_entities_llm_tirads_granular: 179 rows  [family=imaging]
    expanded fleet format: 11,037 note rows → 49 entity rows
  loaded note_entities_llm_us_nodule_dynamics: 49 rows  [family=imaging]
    expanded fleet format: 11,037 note rows → 2,460 entity rows
  loaded note_entities_llm_labs: 2,460 rows  [family=followup]
    expanded fleet format: 11,037 note rows → 173 entity rows
  loaded note_entities_llm_tg_kinetics: 173 rows  [family=followup]
    expanded fleet format: 11,037 note rows → 10,894 entity rows
  loaded note_entities_llm_pathology: 10,894 rows  [family=pathology]
    expanded fleet format: 11,037 note rows → 38 entity rows
  loaded note_entities_llm_synoptic_pathology_enrichment: 38 rows  [family=pathology]
    expanded fleet format: 11,037 note rows → 3,747 entity rows
  loaded note_entities_llm_rai_detailed: 3,747 rows  [family=rai]
    expanded fleet format: 11,037 note rows → 519 entity rows
  loaded note_entities_llm_rad_treatment: 519 rows  [family=rai]
    expanded fleet format: 11,037 note rows → 255 entity rows
  loaded note_entities_llm_parathyroid_detail: 255 rows  [family=operative]
    expanded fleet format: 11,037 note rows → 303 entity rows
  loaded note_entities_llm_recurrence: 303 rows  [family=followup]
    expanded fleet format: 11,037 note rows → 9,809 entity rows
  loaded note_entities_llm_survival_followup: 9,809 rows  [family=followup]
    expanded fleet format: 11,037 note rows → 104 entity rows
  loaded note_entities_llm_cervical_ln_detail: 104 rows  [family=pathology]
    expanded fleet format: 11,037 note rows → 3,322 entity rows
  loaded note_entities_llm_functional_outcomes: 3,322 rows  [family=followup]
    expanded fleet format: 11,037 note rows → 865 entity rows
  loaded note_entities_llm_past_medical_hx: 865 rows  [family=demographics]
    expanded fleet format: 11,037 note rows → 3,919 entity rows
  loaded note_entities_llm_past_surgical_hx: 3,919 rows  [family=demographics]
    expanded fleet format: 11,037 note rows → 280 entity rows
  loaded note_entities_llm_presenting_symptoms: 280 rows  [family=demographics]
    expanded fleet format: 11,037 note rows → 1,924 entity rows
  loaded note_entities_llm_physical_exam: 1,924 rows  [family=demographics]
    expanded fleet format: 11,037 note rows → 4,241 entity rows
  loaded note_entities_llm_vascular_invasion: 4,241 rows  [family=pathology]
    expanded fleet format: 11,037 note rows → 3,116 entity rows
  loaded note_entities_llm_airway_invasion: 3,116 rows  [family=operative]
    expanded fleet format: 11,037 note rows → 380 entity rows
  loaded note_entities_llm_frozen_section_detail: 380 rows  [family=operative]
    expanded fleet format: 11,037 note rows → 53 entity rows
  loaded note_entities_llm_dynamic_risk_response: 53 rows  [family=followup]
    expanded fleet format: 11,037 note rows → 641 entity rows
  loaded note_entities_llm_patient_decision_adherence: 641 rows  [family=followup]
  backfilled extraction_run_id from note_extraction_runs (19,495 rows were blank before timeline resolution)
  note: clinical_notes_long has no note_row_id — synthesizing (same rule as scripts/build_clinical_notes_long.py)
  merged clinical_notes_long
  multi-surgery patients: 0
  inferred episode linkage (family-specific)
  v1 split: clean=68,077  quarantined=0
  v2 split: clean=123,577  quarantined=199
  dry-run: v1 clean=68,077 → canonical_extracted_fact_long_v1.parquet
  dry-run: v1 quarantine=0 → canonical_fact_quarantine_v1.parquet
  dry-run: v2 clean=123,577 → canonical_extracted_fact_long_v2.parquet
  dry-run: v2 quarantine=199 → canonical_fact_quarantine_v2.parquet

# Fact Lineage QC Report

Generated: 2026-04-07 13:43 UTC

## Row Counts

| Output | Rows |
|--------|------|
| canonical_extracted_fact_long_v1 | 68,077 |
| canonical_fact_quarantine_v1 | 0 |
| canonical_extracted_fact_long_v2 | 123,577 |
| canonical_fact_quarantine_v2 | 199 |

## V2 Clean Facts by Domain

| Domain | Rows | Linked % |
|--------|------|----------|
| airway_invasion | 3,076 | 0.0% |
| cervical_ln_detail | 104 | 0.0% |
| complications | 9,359 | 0.0% |
| dynamic_risk_response | 52 | 0.0% |
| frozen_section_detail | 375 | 0.0% |
| functional_outcomes | 3,289 | 0.0% |
| genetics | 1,738 | 0.0% |
| imaging | 8,403 | 0.0% |
| labs | 2,447 | 0.0% |
| medications | 7,501 | 0.0% |
| operative_detail | 12,151 | 0.0% |
| parathyroid_detail | 248 | 0.0% |
| past_medical_hx | 865 | 0.0% |
| past_surgical_hx | 3,918 | 0.0% |
| pathology | 10,867 | 0.0% |
| patient_decision_adherence | 640 | 0.0% |
| physical_exam | 1,919 | 0.0% |
| presenting_symptoms | 279 | 0.0% |
| problem_list | 11,579 | 0.0% |
| procedures | 21,942 | 0.0% |
| rad_treatment | 518 | 0.0% |
| rai_detailed | 3,744 | 0.0% |
| recurrence | 281 | 0.0% |
| staging | 3,807 | 0.0% |
| survival_followup | 9,806 | 0.0% |
| synoptic_pathology_enrichment | 38 | 0.0% |
| tg_kinetics | 167 | 0.0% |
| tirads_granular | 176 | 0.0% |
| us_nodule_dynamics | 47 | 0.0% |
| vascular_invasion | 4,241 | 0.0% |

## V2 Quarantine Reasons

| Reason | Count |
|--------|-------|
| low_confidence_llm_date | 199 |

## V2 Unresolved Episode Linkage

Unlinked rows in clean v2: 123,577

## Duplicate Facts

Duplicate rows (on dedup key): 14,778

## Linkage Family Distribution (V2 clean)

| Family | Domains | Rows |
|--------|---------|------|
| demographics | 5 | 18,560 |
| followup | 8 | 24,183 |
| imaging | 3 | 8,626 |
| molecular | 1 | 1,738 |
| operative | 6 | 47,151 |
| pathology | 5 | 19,057 |
| rai | 2 | 4,262 |
  [Canonical materialization (103)] OK (exit 0)

======================================================================
  STEP: QA schema setup (114)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/114_qa_schema_setup.py --md
  LOG : qa_setup_output.log
======================================================================
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
  [ddl] qa schema and tables created/verified
  [verify] schemas with tables: ['hn', 'kaggle', 'main', 'mm_contract_dev', 'nyc', 'qa', 'release_20260406', 'release_20260407', 'release_20260407_final', 'release_20260407_final2', 'release_20260408', 'release_20260409', 'stackoverflow_survey', 'v2_stage', 'who']
  [verify] qa tables: ['concordance_summary', 'concordance_summary', 'concordance_summary', 'concordance_summary', 'concordance_summary', 'date_provenance_completeness_v', 'date_provenance_completeness_v', 'date_provenance_completeness_v', 'date_provenance_completeness_v', 'date_provenance_completeness_v', 'domain_validation', 'domain_validation', 'domain_validation', 'domain_validation', 'domain_validation', 'domain_validation_summary_v', 'domain_validation_summary_v', 'domain_validation_summary_v', 'domain_validation_summary_v', 'domain_validation_summary_v', 'manual_review_queue', 'manual_review_queue', 'manual_review_queue', 'manual_review_queue', 'manual_review_queue', 'manual_review_queue_summary_v', 'manual_review_queue_summary_v', 'manual_review_queue_summary_v', 'manual_review_queue_summary_v', 'manual_review_queue_summary_v', 'molecular_pipeline_run_audit', 'molecular_pipeline_run_audit', 'molecular_pipeline_run_audit', 'molecular_pipeline_run_audit', 'promotion_review_decisions', 'promotion_review_decisions', 'promotion_review_decisions', 'promotion_review_decisions', 'promotion_review_decisions', 'promotion_scorecard', 'promotion_scorecard', 'promotion_scorecard', 'promotion_scorecard', 'promotion_scorecard', 'promotion_scorecard_summary_v', 'promotion_scorecard_summary_v', 'promotion_scorecard_summary_v', 'promotion_scorecard_summary_v', 'promotion_scorecard_summary_v', 'release_manifest', 'release_manifest', 'release_manifest', 'release_manifest', 'release_manifest', 'specimen_genomic_link_review_v1', 'specimen_genomic_link_review_v1', 'specimen_merge_review_queue_v1', 'specimen_merge_review_queue_v1', 'tg_lab_ingestion_qc', 'tg_lab_ingestion_qc', 'tg_lab_ingestion_qc', 'tg_lab_ingestion_qc', 'tg_lab_ingestion_qc', 'v_diag_specimen_duplicate_focus_fp_v1', 'v_diag_specimen_duplicate_focus_fp_v1', 'v_diag_specimen_duplicate_master_fp_v1', 'v_diag_specimen_duplicate_master_fp_v1', 'v_diag_specimen_fhir_broken_refs_v1', 'v_diag_specimen_fhir_broken_refs_v1', 'v_diag_specimen_orphan_focus_v1', 'v_diag_specimen_orphan_focus_v1', 'v_diag_specimen_orphan_genomic_master_v1', 'v_diag_specimen_orphan_genomic_master_v1', 'v_diag_specimen_orphan_genomic_v1', 'v_diag_specimen_orphan_genomic_v1', 'v_diag_specimen_provenance_focus_v1', 'v_diag_specimen_provenance_focus_v1', 'v_diag_specimen_provenance_genomic_v1', 'v_diag_specimen_provenance_genomic_v1', 'v_diag_specimen_provenance_master_v1', 'v_diag_specimen_provenance_master_v1', 'v_diag_specimen_provenance_summary_v1', 'v_diag_specimen_provenance_summary_v1', 'v_diag_specimen_review_burden_v1', 'v_diag_specimen_review_burden_v1', 'val_specimen_contract_v1', 'val_specimen_contract_v1', 'val_specimen_genomic_binding_v1', 'val_specimen_genomic_binding_v1']
  [done] qa schema setup complete
  [QA schema setup (114)] OK (exit 0)

======================================================================
  STEP: Contract views (117)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/117_md_contract_views.py --md --dry-run
  LOG : contract_views_output.log
======================================================================
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
=== Phase 2: Canonical table materialization ===
  [dry-run] main.canonical_extracted_fact_long_v2: 123,577 rows from canonical_extracted_fact_long_v2.parquet
  [dry-run] main.canonical_fact_quarantine_v2: 199 rows from canonical_fact_quarantine_v2.parquet
  [dry-run] main.note_extraction_runs: 5 rows from note_extraction_runs.parquet
  [dry-run] main.longitudinal_lab_canonical_v1: 76,971 rows from longitudinal_lab_canonical_v1.parquet
  [dry-run] main.thyroglobulin_lab_canonical_v1: 76,971 rows from thyroglobulin_lab_canonical_v1.parquet

=== Phase 3: Episode/linkage contract tables ===
  [dry-run] main.tumor_episode_master_v2: 11,691 rows from tumor_episode_master_v2.parquet
  [dry-run] main.molecular_test_episode_v2: 10,126 rows from molecular_test_episode_v2.parquet
  [dry-run] main.rai_treatment_episode_v2: 1,857 rows from rai_treatment_episode_v2.parquet
  [dry-run] main.operative_episode_detail_v2: 9,371 rows from operative_episode_detail_v2.parquet

=== Contract views (DDL) ===
  [dry-run] [117_contract] CREATE OR REPLACE VIEW main.longitudinal_lab_deduped_v AS WITH ranked AS (     S...
  [dry-run] [117_contract] CREATE OR REPLACE VIEW main.linkage_summary_v AS SELECT     t.research_id,     C...
  [dry-run] [117_contract] CREATE OR REPLACE VIEW main.episode_completeness_summary_v AS SELECT     'tumor_...
  [dry-run] [133_molecular_contract] CREATE OR REPLACE VIEW main.molecular_results_contract_v AS SELECT     molecular...
  [dry-run] [133_molecular_contract] CREATE OR REPLACE VIEW main.molecular_variant_contract_v AS SELECT     v.molecul...
  [dry-run] [133_molecular_contract] CREATE OR REPLACE VIEW main.molecular_qc_summary_v AS SELECT     COALESCE(source...
  [dry-run] [133_molecular_contract] CREATE OR REPLACE VIEW main.molecular_patient_rollup_v AS SELECT     r.research_...

  [done] contract views setup complete
  [Contract views (117)] OK (exit 0)

======================================================================
  STEP: Molecular lineage views (132)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/132_molecular_fact_lineage_views.py --validate-only --md
  LOG : molecular_lineage_views_output.log
======================================================================
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)

── Molecular lineage view counts ──
  main.molecular_fact_long_base_v: ERROR Catalog Error: Table with name molecular_fact_long_base_v does not exist!
Did you mean "molecular_variant_long"?

LINE 1: SELECT COUNT(*) FROM main.molecular_fact_long_base_v
                             ^
  main.molecular_fact_long_v: ERROR Catalog Error: Table with name molecular_fact_long_v does not exist!
Did you mean "molecular_variant_long"?

LINE 1: SELECT COUNT(*) FROM main.molecular_fact_long_v
                             ^
  main.molecular_results_unified_v: ERROR Catalog Error: Table with name molecular_results_unified_v does not exist!
Did you mean "molecular_results_enriched_v1"?

LINE 1: SELECT COUNT(*) FROM main.molecular_results_unified_v
                             ^
  main.molecular_fact_lineage_qa_duplicate_candidates_v: ERROR Catalog Error: Table with name molecular_fact_lineage_qa_duplicate_candidates_v does not exist!
Did you mean "molecular_variant_long_contract_v1"?

LINE 1: SELECT COUNT(*) FROM main.molecular_fact_lineage_qa_duplicate_candidates_v
                             ^

── Primary vs supporting (precedence) ──
  ERROR Catalog Error: Table with name molecular_fact_long_v does not exist!
Did you mean "molecular_variant_long"?

LINE 1: SELECT COUNT(*) FROM main.molecular_fact_long_v
                             ^

── Duplicate assay-event candidates (note vs structured, ±21d) ──
  ERROR Catalog Error: Table with name molecular_fact_lineage_qa_duplicate_candidates_v does not exist!
Did you mean "molecular_variant_long_contract_v1"?

LINE 1: SELECT COUNT(*) FROM main.molecular_fact_lineage_qa_duplicate_candidates_v
                             ^

── Sample rows: primary assay with note support ──
  ERROR Catalog Error: Table with name molecular_fact_long_v does not exist!
Did you mean "molecular_variant_long"?

LINE 5:             FROM main.molecular_fact_long_v
                         ^

── Sample rows: supporting note (suppressed from primary analytics) ──
  ERROR Catalog Error: Table with name molecular_fact_long_v does not exist!
Did you mean "molecular_variant_long"?

LINE 5:             FROM main.molecular_fact_long_v
                         ^
  [Molecular lineage views (132)] OK (exit 0)

======================================================================
  STEP: Presentation views (125)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/125_master_verified_views.py --md --dry-run
  LOG : presentation_views_output.log
======================================================================
======================================================================
  125 — master verified views (analyst presentation layer)
======================================================================
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)

  [dry-run] master_fact_long_verified_v1
            One row per extracted entity fact; joins canonical_extracted_fact_long_v2 with reviewer status and latest release tag.
  DDL preview (first 4 lines):
    CREATE OR REPLACE VIEW main.master_fact_long_verified_v1 AS
    WITH latest_release AS (
        SELECT release_tag
        FROM   qa.release_manifest
    ...

  [dry-run] master_patient_rollup_verified_v1
            Per-patient summary: fact counts by linkage family, review coverage, release tag.
  DDL preview (first 4 lines):
    CREATE OR REPLACE VIEW main.master_patient_rollup_verified_v1 AS
    SELECT
        f.research_id,
        COUNT(*)                                                    AS total_facts,
    ...

  [dry-run] master_source_lineage_v1
            Full provenance chain from extraction run to reviewer decision to release tag.
  DDL preview (first 4 lines):
    CREATE OR REPLACE VIEW main.master_source_lineage_v1 AS
    SELECT
        f.research_id,
        -- source domain and object identity
    ...
======================================================================
  DONE (dry-run — no views created)
======================================================================
  [Presentation views (125)] OK (exit 0)

======================================================================
  STEP: Release snapshot (115)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/115_release_snapshot.py --tag 20260407 --md --dry-run
  LOG : release_snapshot_output.log
======================================================================
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
  [error] Schema release_20260407 already exists. Use a different tag.
  [Release snapshot (115)] FAILED (exit 1)

  ABORT: Release snapshot failed.
### 124_md_live_release_audit.py --md --dry-run --tag 20991231 (unique tag to avoid schema collision)
```
  [dry-run] main.note_entities_llm_dynamic_risk_response: 11,037 rows -> note_entities_llm_dynamic_risk_response.parquet
  [dry-run] main.note_entities_llm_patient_decision_adherence: 11,037 rows -> note_entities_llm_patient_decision_adherence.parquet

--- qa schema ---
  [dry-run] qa.promotion_scorecard: 48 rows -> promotion_scorecard.parquet
  [dry-run] qa.domain_validation: 138 rows -> domain_validation.parquet
  [dry-run] qa.manual_review_queue: 5,622 rows -> manual_review_queue.parquet

  [summary] 33 files, 416,483 total rows
  [done] Release bundle (dry-run) created at /Users/ros/THyroid 2026/THYROID_2026/exports/parquet_release_20991231
  [Parquet release bundle (118)] OK (exit 0)

======================================================================
  STEP: Formalization validation (119)
  CMD : /Users/ros/THyroid 2026/THYROID_2026/.venv/bin/python /Users/ros/THyroid 2026/THYROID_2026/scripts/119_md_formalization_validate.py --output-dir /Users/ros/THyroid 2026/THYROID_2026/studies/20991231_motherduck_live_release_audit/validation_run --md
  LOG : validation_output.log
======================================================================
  Connected to MotherDuck (md:Thyroid 2026)
  MotherDuck connection verified (fail-closed gate passed)
=== MotherDuck Formalization Validation (STRUCTURAL) ===

--- Check 1: MD Attachment ---
  [PASS] MD attachment: 9 databases attached

--- Check 2: Row Count Parity ---
  [PASS] Row count parity: 23 domains checked, all match
  [PASS] Canonical canonical_extracted_fact_long_v2: local=123,577  md=123,577
  [PASS] Canonical canonical_fact_quarantine_v2: local=199  md=199
  [FAIL] Canonical note_extraction_runs: local=5  md=3

--- Check 3: Schema Completeness ---
  [PASS] Schema completeness: Wide note-level v2 contract on 23 promoted table(s); entity_type/entity_value_* in main.canonical_extracted_fact_long_v2 (see docs/domain_mapping_rules.md). Example stems: note_entities_llm_imaging, note_entities_llm_tirads_granular, note_entities_llm_us_nodule_dynamics…

--- Check 4: Canonical Distribution ---
  [PASS] Canonical dist (canonical_extracted_fact_long_v2): 594 domains, 123,577 total rows
  [PASS] Canonical dist (canonical_fact_quarantine_v2): 69 domains, 199 total rows

--- Check 5: Review Queue ---
  [PASS] Review queue: 5,622 total, 5,622 reviewed, 0 pending

--- Check 6: QA Views ---
  [PASS] QA view promotion_scorecard_summary_v: 6 rows
  [PASS] QA view domain_validation_summary_v: 6 rows
  [PASS] QA view date_provenance_completeness_v: 23 rows
  [PASS] QA view manual_review_queue_summary_v: 21 rows

--- Check 7: Load Inventory ---
  [PASS] Load inventory: 180 entries, all match

--- Check 8: Release Schemas ---
  [PASS] Release schemas: 6 found: release_20260406, release_20260407, release_20260407_final, release_20260407_final2, release_20260408, release_20260409

--- Check 11: Analyst presentation layer (master_*_verified_v1) ---
  [PASS] Presentation master_fact_long_verified_v1: 123,577 rows; core traceability non-null (reviewer_status may be NULL)
  [PASS] Presentation master_source_lineage_v1: 123,577 rows; core traceability non-null (reviewer_status may be NULL)
  [PASS] Presentation master_patient_rollup_verified_v1: 5,574 patient rows; research_id + release_tag + review metrics present

--- Check 12: Molecular normalized contract views ---
  [PASS] Molecular row counts: main.molecular_results is empty — contract view checks skipped

--- Check 13: Specimen + analytic FHIR layer ---
  [PASS] Specimen/FHIR tables present: 10 objects found
  [PASS] Specimen master fingerprint uniqueness: distinct fingerprints
  [PASS] qa.val_specimen_contract_v1: no FAIL rows recorded
  [PASS] qa.val_specimen_genomic_binding_v1: no FAIL rows recorded
  [WARN] Specimen/FHIR QA diagnostics (142 views + focus checks): dup_master_fp=0, dup_focus_fp=None, orphan_focus=None, orphan_genomic(master/focus)=0/n/a, broken_fhir_refs=0, prov_gaps(master/focus/high_tier_null_spec)=0/n/a/0 | NOTE: some focus-table scans unavailable on this catalog
  [WARN] Specimen-adjacent review burden (open/pending): genomic_link_review open/pending=9966; merge queue: direct COUNT unavailable (MotherDuck/catalog — audit manually)

=== Summary: 22 PASS / 2 WARN / 1 FAIL ===

  [report] /Users/ros/THyroid 2026/THYROID_2026/studies/20991231_motherduck_live_release_audit/validation_run/validation_report.md
  [Formalization validation (119)] OK (exit 0)
  [copy] validation_report.md
  [write] audit_summary.md

======================================================================
  124 — DONE
  Verdict : PASS
  Audit dir: /Users/ros/THyroid 2026/THYROID_2026/studies/20991231_motherduck_live_release_audit
======================================================================
EXIT:
```

## 5. Read-scaling dry-run (136) + connect attempt

### 136 reader --md-env prod --dry-run
```
REFRESH DATABASE "Thyroid 2026"
EXIT:0
```
### 136 writer --md-env prod --dry-run
```
CREATE SNAPSHOT OF "Thyroid 2026"
EXIT:0
```
### connect_read_scaling() live attempt
```
read_scaling_token_mode: none
RuntimeError: No read-scaling MotherDuck token. Set MD_READ_SCALING_TOKEN (or MOTHERDUCK_READ_SCALING_TOKEN), optionally with MD_READ_SCALING_SESSION_HINT.
EXIT:0
```

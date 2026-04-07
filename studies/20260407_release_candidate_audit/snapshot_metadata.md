## Session

```sql
SELECT current_database() AS current_database, current_timestamp AS ts
```
| current_database   | ts                               |
|:-------------------|:---------------------------------|
| Thyroid 2026       | 2026-04-06 22:45:44.294886-04:00 |
- **custom_user_agent (connection):** `THYROID_2026_rc_audit/1.0`
- **motherduck_session_hint:** `rc_release_candidate_audit_20260407`
- **current_database:** `Thyroid 2026`

## MD_INFORMATION_SCHEMA.DATABASES

```sql
SELECT * FROM MD_INFORMATION_SCHEMA.DATABASES
```
| name         | uuid                                 | created_ts                | transient   | historical_snapshot_retention   | type     |
|:-------------|:-------------------------------------|:--------------------------|:------------|:--------------------------------|:---------|
| my_db        | e0db7fdc-5cb6-4f57-9944-6baadccd82c1 | 2026-03-07 02:53:36-05:00 | False       | 7 days 00:00:00                 | DEFAULT  |
| rosflow      | c8f55a3a-ab80-4f93-8bb1-74b55770ad39 | 2026-03-15 13:16:55-04:00 | False       | 7 days 00:00:00                 | DEFAULT  |
| Thyroid 2026 | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 2026-04-02 04:31:53-04:00 | False       | 7 days 00:00:00                 | DUCKLAKE |
## DATABASE_SNAPSHOTS (thyroid filter)

```sql
SELECT * FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS WHERE database_name ILIKE '%thyroid%' ORDER BY created_ts DESC LIMIT 50
```
| database_name         | database_id                          | snapshot_id                          | snapshot_name   | created_ts                 |   active_bytes |   bytes_written |   bytes_deleted | user_name     | user_id                              |
|:----------------------|:-------------------------------------|:-------------------------------------|:----------------|:---------------------------|---------------:|----------------:|----------------:|:--------------|:-------------------------------------|
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | d1840496-8218-40e1-ad6f-715c023e8b27 |                 | 2026-04-07 02:37:39.118000 |       12595200 |         3411968 |         3149824 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 99cd7b4d-1773-443d-810f-23fff040bdb1 |                 | 2026-04-07 02:05:08.269000 |       12333056 |         6819840 |         7344128 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | d9f90b9d-246b-413a-95d6-c1fcf77f7d10 |                 | 2026-04-07 02:04:07.531000 |       12857344 |         6033408 |         6033408 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | becd69e5-5db0-45be-a8a6-41e764a59010 |                 | 2026-04-07 02:03:06.856000 |       12857344 |         6033408 |         5246976 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 36d7d73a-3bef-46fa-b898-e9383f51a70a |                 | 2026-04-07 01:58:06.165000 |       12070912 |         5509120 |         3936256 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 7ea8f730-7d54-4a23-ac2c-67cf3cdcb2e3 |                 | 2026-04-07 01:57:05.525000 |       10498048 |         6033408 |         4984832 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | eb041131-e868-41cf-8949-9ed23225a2bc |                 | 2026-04-07 01:10:23.608000 |        9449472 |         6033408 |         5771264 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 7e87ce93-3503-4ea4-a149-255d53b3ba65 |                 | 2026-04-07 01:09:22.965000 |        9187328 |         4722688 |         4198400 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 732258f1-12b8-42c6-a651-3e1f513e4b82 |                 | 2026-04-07 01:08:22.400000 |        8663040 |         5509120 |         4198400 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | cad0483a-d034-4b11-8227-c56a548a5159 |                 | 2026-04-06 23:37:59.458000 |        7352320 |         4722688 |         5509120 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | e6c69e17-e16a-4a28-9f38-163a13189bc6 |                 | 2026-04-06 23:36:58.685000 |        8138752 |         4460544 |         4198400 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 80c39f17-f76c-442c-a4a3-e64da4390ece |                 | 2026-04-06 23:34:58.128000 |        7876608 |         3936256 |         3411968 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 2509fe1d-4b5a-4bfc-b88a-fe2d69a3b6b2 |                 | 2026-04-06 23:33:57.410000 |        7352320 |         5246976 |         5246976 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | f04f4981-3a19-4c1e-9eac-d342f41eac07 |                 | 2026-04-06 18:21:21.448000 |        7352320 |         2363392 |         2363392 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 2c552628-e2b1-4705-b112-52e446e778cd |                 | 2026-04-06 06:39:56.773000 |        7352320 |         2625536 |         2625536 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | bf7c847a-f536-4809-af39-e886f0bdc5af |                 | 2026-04-05 18:12:14.610000 |        7352320 |         2363392 |         2363392 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 300eab6a-644c-4a7e-8d01-2e2f94f2ef1c |                 | 2026-04-05 06:29:17.081000 |        7352320 |         2625536 |         2625536 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | bfb2f747-a6b9-41a3-90a3-b11d0f2dc03d |                 | 2026-04-04 18:11:15.240000 |        7352320 |         2363392 |         2363392 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 8736dad5-95e3-4e20-8d2d-504a4eb2040f |                 | 2026-04-04 06:24:37.386000 |        7352320 |         2625536 |         2625536 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | d0857b45-0092-4bd7-86d3-5d949c448eff |                 | 2026-04-03 18:14:56.193000 |        7352320 |         2363392 |         2101248 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 677377da-a4dc-4e7b-9eee-a9ce09371a54 |                 | 2026-04-03 14:42:38.076000 |        7090176 |         5509120 |         4984832 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | c715a639-9224-4f67-a630-10ed0caf875e |                 | 2026-04-03 09:59:05.830000 |        6565888 |         4722688 |         3674112 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | e453c452-059b-441f-bcf6-04987d61717f |                 | 2026-04-03 06:27:56.562000 |        5517312 |         1839104 |         1314816 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | e430ed89-17c9-4fe2-9c44-30ae37b7729b |                 | 2026-04-02 21:00:46.216000 |        4993024 |         4198400 |         3936256 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| thyroid_research_2026 | bd53436b-9e36-473c-8b93-c4d186875f7e | 3e937061-001d-42f5-8266-750b96069ce5 |                 | 2026-04-02 20:56:20.533000 |          12288 |           12288 |               0 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 4188a1d8-ad79-467d-a80c-29f6c938055b |                 | 2026-04-02 10:09:13.021000 |        4730880 |         3936256 |         3936256 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 5d16b64b-ee36-44a7-86cc-4951ce8e1b73 |                 | 2026-04-02 10:05:12.387000 |        4730880 |         3936256 |         3674112 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | d40d61cd-4eae-4d89-8adb-a8a1998d6be7 |                 | 2026-04-02 09:06:41.534000 |        4468736 |         4198400 |         2887680 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | dfe2c0a9-05e8-4793-89e5-ae65095df091 |                 | 2026-04-02 09:05:41.008000 |        3158016 |         2363392 |         1052672 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | e72b4c61-87a7-425d-8950-1ee81de5809b |                 | 2026-04-02 08:31:59.375000 |        1847296 |         1839104 |            4096 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | eae053a4-8ac8-487d-b95b-cbe2a62e7405 |                 | 2026-04-02 08:31:53.948000 |          12288 |           12288 |               0 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
## Table counts by schema

```sql
SELECT table_schema, COUNT(*) AS n_tables FROM information_schema.tables WHERE table_catalog = current_database() GROUP BY 1 ORDER BY 1
```
| table_schema     |   n_tables |
|:-----------------|-----------:|
| main             |         74 |
| qa               |         11 |
| release_20260407 |          5 |
| release_20260408 |         10 |
| release_20260409 |         10 |
| v2_stage         |         38 |
## Per-domain row counts (local vs v2_stage vs main)

| domain                        | stem                                            | qa_tier       |   local_parquet |   v2_stage |   main | stage_eq_local   | main_eq_local   |
|:------------------------------|:------------------------------------------------|:--------------|----------------:|-----------:|-------:|:-----------------|:----------------|
| imaging                       | note_entities_llm_imaging                       | standard      |           11037 |      11037 |  11037 | True             | True            |
| tirads_granular               | note_entities_llm_tirads_granular               | standard      |           11037 |      11037 |  11037 | True             | True            |
| us_nodule_dynamics            | note_entities_llm_us_nodule_dynamics            | standard      |           11037 |      11037 |  11037 | True             | True            |
| labs                          | note_entities_llm_labs                          | standard      |           11037 |      11037 |  11037 | True             | True            |
| tg_kinetics                   | note_entities_llm_tg_kinetics                   | standard      |           11037 |      11037 |  11037 | True             | True            |
| pathology                     | note_entities_llm_pathology                     | critical      |           11037 |      11037 |  11037 | True             | True            |
| synoptic_pathology_enrichment | note_entities_llm_synoptic_pathology_enrichment | critical      |           11037 |      11037 |  11037 | True             | True            |
| rai_detailed                  | note_entities_llm_rai_detailed                  | critical      |           11037 |      11037 |  11037 | True             | True            |
| rad_treatment                 | note_entities_llm_rad_treatment                 | standard      |           11037 |      11037 |  11037 | True             | True            |
| parathyroid_detail            | note_entities_llm_parathyroid_detail            | standard      |           11037 |      11037 |  11037 | True             | True            |
| recurrence                    | note_entities_llm_recurrence                    | critical      |           11037 |      11037 |  11037 | True             | True            |
| survival_followup             | note_entities_llm_survival_followup             | standard      |           11037 |      11037 |  11037 | True             | True            |
| cervical_ln_detail            | note_entities_llm_cervical_ln_detail            | standard      |           11037 |      11037 |  11037 | True             | True            |
| functional_outcomes           | note_entities_llm_functional_outcomes           | informational |           11037 |      11037 |  11037 | True             | True            |
| past_medical_hx               | note_entities_llm_past_medical_hx               | informational |           11037 |      11037 |  11037 | True             | True            |
| past_surgical_hx              | note_entities_llm_past_surgical_hx              | informational |           11037 |      11037 |  11037 | True             | True            |
| presenting_symptoms           | note_entities_llm_presenting_symptoms           | informational |           11037 |      11037 |  11037 | True             | True            |
| physical_exam                 | note_entities_llm_physical_exam                 | informational |           11037 |      11037 |  11037 | True             | True            |
| vascular_invasion             | note_entities_llm_vascular_invasion             | critical      |           11037 |      11037 |  11037 | True             | True            |
| airway_invasion               | note_entities_llm_airway_invasion               | standard      |           11037 |      11037 |  11037 | True             | True            |
| frozen_section_detail         | note_entities_llm_frozen_section_detail         | standard      |           11037 |      11037 |  11037 | True             | True            |
| dynamic_risk_response         | note_entities_llm_dynamic_risk_response         | standard      |           11037 |      11037 |  11037 | True             | True            |
| patient_decision_adherence    | note_entities_llm_patient_decision_adherence    | informational |           11037 |      11037 |  11037 | True             | True            |
## manual_review_queue by run_label and domain

```sql
SELECT run_label, domain, COUNT(*) AS n, COUNT(*) FILTER (WHERE verification_status IS NOT NULL) AS reviewed, COUNT(*) FILTER (WHERE verification_status IS NULL) AS pending FROM qa.manual_review_queue GROUP BY 1, 2 ORDER BY 1, 2
```
| run_label        | domain           |    n |   reviewed |   pending |
|:-----------------|:-----------------|-----:|-----------:|----------:|
| mrq_hydrate_gate | complications    |  203 |        203 |         0 |
| mrq_hydrate_gate | genetics         |  510 |        510 |         0 |
| mrq_hydrate_gate | medications      |  368 |        368 |         0 |
| mrq_hydrate_gate | operative_detail | 1980 |       1980 |         0 |
| mrq_hydrate_gate | procedures       | 1062 |       1062 |         0 |
| mrq_hydrate_gate | staging          | 1499 |       1499 |         0 |
## manual_review_queue by algorithm_status

```sql
SELECT algorithm_status, COUNT(*) AS n FROM qa.manual_review_queue GROUP BY 1 ORDER BY 2 DESC
```
| algorithm_status                |    n |
|:--------------------------------|-----:|
| existing_missing_fill_candidate | 5620 |
| discordant_existing             |    2 |
## release_manifest (latest)

```sql
SELECT release_tag, created_at, created_by FROM qa.release_manifest ORDER BY created_at DESC LIMIT 10
```
|   release_tag | created_at                 | created_by                          |
|--------------:|:---------------------------|:------------------------------------|
|      20260409 | 2026-04-07 02:05:07.189573 | scripts/126_final_master_release.py |
|      20260408 | 2026-04-07 02:03:20.732093 | scripts/126_final_master_release.py |
|      20260407 | 2026-04-07 01:09:57.717289 | scripts/115_release_snapshot.py     |
## View master_fact_long_verified_v1

```sql
SELECT COUNT(*) AS n FROM main.master_fact_long_verified_v1
```
|      n |
|-------:|
| 123577 |
## View master_patient_rollup_verified_v1

```sql
SELECT COUNT(*) AS n FROM main.master_patient_rollup_verified_v1
```
|    n |
|-----:|
| 5574 |
## View master_source_lineage_v1

```sql
SELECT COUNT(*) AS n FROM main.master_source_lineage_v1
```
|      n |
|-------:|
| 123577 |

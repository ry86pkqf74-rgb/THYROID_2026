## Session

```sql
SELECT current_database() AS current_database, current_timestamp AS ts
```
| current_database   | ts                               |
|:-------------------|:---------------------------------|
| Thyroid 2026       | 2026-04-07 00:42:29.949850-04:00 |
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
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 9a1f531a-5b22-4b91-afd8-d929589c9d15 |                 | 2026-04-07 04:16:12.187000 |       19935232 |         3936256 |         3149824 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | b5ac5b5c-8040-4130-b0ee-1f6b6e4c6f3f |                 | 2026-04-07 04:10:11.587000 |       19148800 |         6557696 |         5509120 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | b1918f0e-4a4a-419b-b089-72e6f839bd43 |                 | 2026-04-07 04:08:10.971000 |       18100224 |         9441280 |         9965568 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 60ba729d-b41d-4e20-b88d-f077ef071263 |                 | 2026-04-07 04:07:10.110000 |       18624512 |         6557696 |         6295552 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 3b788041-e2c4-4968-bcfc-239796836ea8 |                 | 2026-04-07 04:06:09.395000 |       18362368 |         8392704 |         7606272 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 6d7e8ebb-31a9-4396-98dc-bd6ed5aec782 |                 | 2026-04-07 04:03:08.629000 |       17575936 |         6557696 |         6033408 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 225512ab-979f-4b3f-bdfd-81db72f23f6e |                 | 2026-04-07 04:02:07.913000 |       17051648 |         7081984 |         7344128 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 6f466fc1-09c8-4a08-844f-eb4dfdf15de2 |                 | 2026-04-07 03:59:07.244000 |       17313792 |         5509120 |         4722688 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | b2544dce-064c-4833-95b1-7ac7c9ae3715 |                 | 2026-04-07 03:58:06.656000 |       16527360 |         6819840 |         6819840 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 053d5a5a-ce88-4ac0-81e4-d7dcade0cad4 |                 | 2026-04-07 03:57:05.943000 |       16527360 |         6819840 |         6819840 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | eb1f3737-bc00-482f-adf1-4a2c94fd06d8 |                 | 2026-04-07 03:33:05.254000 |       16527360 |         4722688 |         4722688 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 99b0399f-87d2-493d-a532-d7f9ec2dabcf |                 | 2026-04-07 03:32:04.600000 |       16527360 |         4460544 |         3411968 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 88c961bf-2bb8-4913-a363-ab15793f117e |                 | 2026-04-07 03:31:03.881000 |       15478784 |         3936256 |         4460544 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 46b63117-bad5-46f9-8cb5-252818404583 |                 | 2026-04-07 03:30:03.184000 |       16003072 |         3411968 |         2887680 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 4eed1e73-a4e8-408c-a672-3f9b5e8a2355 |                 | 2026-04-07 03:23:02.496000 |       15478784 |         6819840 |         6819840 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | a184e1e9-bbf9-4abe-9ff8-044f882e50a8 |                 | 2026-04-07 03:19:01.910000 |       15478784 |         6033408 |         5771264 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | fda23cc2-6344-4cba-9323-ff99a12c78df |                 | 2026-04-07 03:18:01.324000 |       15216640 |         4984832 |         4984832 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | d1608ebb-8ffc-4df5-bac2-3f9afce879ba |                 | 2026-04-07 03:17:00.714000 |       15216640 |         5509120 |         4722688 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 5b7f1ba0-8fcb-4ad2-a4d1-2fe0a2540f88 |                 | 2026-04-07 03:12:00.136000 |       14430208 |         4984832 |         3674112 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 5f150ab6-6b27-40a7-9a9c-2c1e4766a2a0 |                 | 2026-04-07 03:01:59.562000 |       13119488 |         2101248 |         1839104 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026          | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 75305b2b-5024-4ad5-8093-1940f8634a5c |                 | 2026-04-07 03:00:58.876000 |       12857344 |         7606272 |         7344128 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
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
## Table counts by schema

```sql
SELECT table_schema, COUNT(*) AS n_tables FROM information_schema.tables WHERE table_catalog = current_database() GROUP BY 1 ORDER BY 1
```
| table_schema     |   n_tables |
|:-----------------|-----------:|
| main             |         85 |
| mm_contract_dev  |         26 |
| qa               |         11 |
| release_20260406 |          6 |
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
| run_label        | domain                        |    n |   reviewed |   pending |
|:-----------------|:------------------------------|-----:|-----------:|----------:|
| gate             | airway_invasion               | 2045 |       2045 |         0 |
| gate             | cervical_ln_detail            |   37 |         37 |         0 |
| gate             | dynamic_risk_response         |    1 |          1 |         0 |
| gate             | frozen_section_detail         |    2 |          2 |         0 |
| gate             | functional_outcomes           |  346 |        346 |         0 |
| gate             | imaging                       |  601 |        601 |         0 |
| gate             | labs                          |   99 |         99 |         0 |
| gate             | parathyroid_detail            |    2 |          2 |         0 |
| gate             | past_medical_hx               |   11 |         11 |         0 |
| gate             | past_surgical_hx              |  231 |        231 |         0 |
| gate             | pathology                     |  683 |        683 |         0 |
| gate             | patient_decision_adherence    |   52 |         52 |         0 |
| gate             | physical_exam                 |  247 |        247 |         0 |
| gate             | presenting_symptoms           |    6 |          6 |         0 |
| gate             | rad_treatment                 |   16 |         16 |         0 |
| gate             | rai_detailed                  |  564 |        564 |         0 |
| gate             | recurrence                    |   22 |         22 |         0 |
| gate             | survival_followup             |  279 |        279 |         0 |
| gate             | synoptic_pathology_enrichment |    7 |          7 |         0 |
| gate             | tirads_granular               |    1 |          1 |         0 |
| gate             | vascular_invasion             |  370 |        370 |         0 |
| mrq_hydrate_gate | complications                 |  203 |        203 |         0 |
| mrq_hydrate_gate | genetics                      |  510 |        510 |         0 |
| mrq_hydrate_gate | medications                   |  368 |        368 |         0 |
| mrq_hydrate_gate | operative_detail              | 1980 |       1980 |         0 |
| mrq_hydrate_gate | procedures                    | 1062 |       1062 |         0 |
| mrq_hydrate_gate | staging                       | 1499 |       1499 |         0 |
| promotion_gate   | airway_invasion               | 2045 |       2045 |         0 |
| promotion_gate   | cervical_ln_detail            |   37 |         37 |         0 |
| promotion_gate   | dynamic_risk_response         |    1 |          1 |         0 |
| promotion_gate   | frozen_section_detail         |    2 |          2 |         0 |
| promotion_gate   | functional_outcomes           |  346 |        346 |         0 |
| promotion_gate   | imaging                       |  601 |        601 |         0 |
| promotion_gate   | labs                          |   99 |         99 |         0 |
| promotion_gate   | parathyroid_detail            |    2 |          2 |         0 |
| promotion_gate   | past_medical_hx               |   11 |         11 |         0 |
| promotion_gate   | past_surgical_hx              |  231 |        231 |         0 |
| promotion_gate   | pathology                     |  683 |        683 |         0 |
| promotion_gate   | patient_decision_adherence    |   52 |         52 |         0 |
| promotion_gate   | physical_exam                 |  247 |        247 |         0 |
| promotion_gate   | presenting_symptoms           |    6 |          6 |         0 |
| promotion_gate   | rad_treatment                 |   16 |         16 |         0 |
| promotion_gate   | rai_detailed                  |  564 |        564 |         0 |
| promotion_gate   | recurrence                    |   22 |         22 |         0 |
| promotion_gate   | survival_followup             |  279 |        279 |         0 |
| promotion_gate   | synoptic_pathology_enrichment |    7 |          7 |         0 |
| promotion_gate   | tirads_granular               |    1 |          1 |         0 |
| promotion_gate   | vascular_invasion             |  370 |        370 |         0 |
## manual_review_queue by algorithm_status

```sql
SELECT algorithm_status, COUNT(*) AS n FROM qa.manual_review_queue GROUP BY 1 ORDER BY 2 DESC
```
| algorithm_status                |     n |
|:--------------------------------|------:|
| existing_missing_fill_candidate | 16860 |
| discordant_existing             |     6 |
## release_manifest (latest)

```sql
SELECT release_tag, created_at, created_by FROM qa.release_manifest ORDER BY created_at DESC LIMIT 10
```
|   release_tag | created_at                 | created_by                          |
|--------------:|:---------------------------|:------------------------------------|
|      20260406 | 2026-04-07 04:07:52.519215 | scripts/115_release_snapshot.py     |
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

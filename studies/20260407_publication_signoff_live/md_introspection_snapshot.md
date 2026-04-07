# Live MotherDuck introspection (automated)


## PRAGMA database_list
```
 seq                                                      name                                                               file
1027                                     md_information_schema                                                               None
1090                                    thyroid_research_ro_v2 _share/thyroid_research_ro_v2/2558f066-1c5d-46a5-afbc-800fd5f7568d
1092                                               sample_data            _share/sample_data/23b0d623-1361-421d-ae77-62d701d471e6
1094                       Thyroid 2026 Molecular Dev 20260407                                Thyroid 2026 Molecular Dev 20260407
1096 Thyroid 2026 Molecular PrePromote agent_20260407_workflow          Thyroid 2026 Molecular PrePromote agent_20260407_workflow
1098                        Thyroid 2026 Molecular QA 20260407                                 Thyroid 2026 Molecular QA 20260407
1100                                              Thyroid 2026                                                       Thyroid 2026
1102                                                   rosflow                                                            rosflow
1104                                                     my_db                                                              my_db
```

## duckdb_databases()
```
                                            database_name  database_oid                                                               path  internal
                                             Thyroid 2026          1100                                                       Thyroid 2026     False
                      Thyroid 2026 Molecular Dev 20260407          1094                                Thyroid 2026 Molecular Dev 20260407     False
Thyroid 2026 Molecular PrePromote agent_20260407_workflow          1096          Thyroid 2026 Molecular PrePromote agent_20260407_workflow     False
                       Thyroid 2026 Molecular QA 20260407          1098                                 Thyroid 2026 Molecular QA 20260407     False
                                    md_information_schema          1027                                                               None     False
                                                    my_db          1104                                                              my_db     False
                                                  rosflow          1102                                                            rosflow     False
                                              sample_data          1092            _share/sample_data/23b0d623-1361-421d-ae77-62d701d471e6     False
                                                   system             0                                                               None      True
                                                     temp          2555                                                               None      True
                                   thyroid_research_ro_v2          1090 _share/thyroid_research_ro_v2/2558f066-1c5d-46a5-afbc-800fd5f7568d     False
```

## md_information_schema.databases sample
```
        name                                 uuid                created_ts  transient historical_snapshot_retention     type
       my_db e0db7fdc-5cb6-4f57-9944-6baadccd82c1 2026-03-07 02:53:36-05:00      False                        7 days  DEFAULT
     rosflow c8f55a3a-ab80-4f93-8bb1-74b55770ad39 2026-03-15 13:16:55-04:00      False                        7 days  DEFAULT
Thyroid 2026 b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc 2026-04-02 04:31:53-04:00      False                        7 days DUCKLAKE
```

## md_information_schema.database_snapshots sample
```
database_name                          database_id                          snapshot_id snapshot_name              created_ts  active_bytes  bytes_written  bytes_deleted     user_name                              user_id
        my_db e0db7fdc-5cb6-4f57-9944-6baadccd82c1 d9095350-bcc0-46f9-b9ee-268ed3e2ff93          None 2026-03-07 07:53:36.742         12288          12288              0 logan_glosser 1a5fc41a-c574-4b4f-920b-a6687fe9fc84
        my_db e0db7fdc-5cb6-4f57-9944-6baadccd82c1 d5d431ef-9e65-4e61-9619-5e7262f10ae0          None 2026-04-02 09:43:11.740        536576         528384           4096 logan_glosser 1a5fc41a-c574-4b4f-920b-a6687fe9fc84
      rosflow c8f55a3a-ab80-4f93-8bb1-74b55770ad39 ffe8af62-be12-4ce5-aeee-d640a1d5a2a2          None 2026-03-15 17:18:07.255       1847296              0              0 logan_glosser 1a5fc41a-c574-4b4f-920b-a6687fe9fc84
```

## md_information_schema.query_history sample
```
                            query_id                                                                                                                                                                                         query_text                       start_time                         end_time         execution_time              wait_time     total_elapsed_time error_message error_type                                       user_agent     user_name  query_nr  transaction_nr                        connection_id                            duckdb_id duckdb_version instance_type query_type  bytes_uploaded  bytes_downloaded  bytes_spilled_to_disk      duckling_id session_name
819cc74a-72f3-4e6e-8038-542ba4746039                                                                                                                                                    from md_live_duckling_size() -- MD UI keepalive 2026-03-07 02:54:31.027848-05:00 2026-03-07 02:54:31.031261-05:00 0 days 00:00:00.001961 0 days 00:00:00.001451 0 days 00:00:00.003412          None       None duckdb/v1.4.4(wasm_eh) motherduck-wasm hatchling logan_glosser         1               1 86f421c0-2b7d-41d9-bd5c-8b5fa448cfbb 819cc749-9c5b-4bca-a6ba-314d1c91ca92         v1.4.4      Standard      QUERY               0                50                      0 logan_glosser.rw         None
819cc74b-49ef-47c8-af63-86b99559b0e4                                                                                                                                                    from md_live_duckling_size() -- MD UI keepalive 2026-03-07 02:55:26.063381-05:00 2026-03-07 02:55:26.066361-05:00 0 days 00:00:00.001584 0 days 00:00:00.001396 0 days 00:00:00.002980          None       None duckdb/v1.4.4(wasm_eh) motherduck-wasm hatchling logan_glosser         1               2 86f421c0-2b7d-41d9-bd5c-8b5fa448cfbb 819cc749-9c5b-4bca-a6ba-314d1c91ca92         v1.4.4      Standard      QUERY               0                50                      0 logan_glosser.rw         None
819cc74b-a239-4399-a371-27ed8542c23c \nSELECT\n  name AS database_name,\n  type\nFROM md_databases()\nWHERE database_name NOT IN (\n  SELECT database_name\n  FROM duckdb_databases()\n  WHERE type = 'motherduck'\n)\nORDER BY 1 ASC\n 2026-03-07 02:55:48.665244-05:00 2026-03-07 02:55:48.718109-05:00 0 days 00:00:00.009521 0 days 00:00:00.043344 0 days 00:00:00.052865          None       None duckdb/v1.4.4(wasm_eh) motherduck-wasm hatchling logan_glosser         1               5 81ebd9c7-1517-4c03-915c-6af402e32eed 819cc749-9c5b-4bca-a6ba-314d1c91ca92         v1.4.4      Standard      QUERY              59                 0                      0 logan_glosser.rw         None
```

## md_information_schema.recent_queries sample
```
                            query_id                                                      query_text                       start_time                         end_time         execution_time              wait_time     total_elapsed_time error_message error_type                           user_agent     user_name  query_nr  transaction_nr                        connection_id                            duckdb_id duckdb_version instance_type query_type  bytes_uploaded  bytes_downloaded  bytes_spilled_to_disk      duckling_id                      session_name
819d6781-e218-4030-8336-615ba71e8c89     SELECT * FROM md_information_schema.recent_queries LIMIT 3; 2026-04-07 06:34:18.520000-04:00                              NaT 0 days 00:00:00.003198 0 days 00:00:00.000419 0 days 00:00:00.003617          None       None duckdb/v1.4.4(osx_arm64) python/3.14 logan_glosser         1              11 853a70ec-4788-4274-85e8-569d141642a7 819d6781-ce8b-4966-bb80-e3cdb1a775d1         v1.4.4         Jumbo      QUERY               0                 0                      0 logan_glosser.rw publication_signoff_20260407_1034
819d6781-dfe2-4ff6-8d5a-44aafafd903b      SELECT * FROM md_information_schema.query_history LIMIT 3; 2026-04-07 06:34:17.954000-04:00 2026-04-07 06:34:18.485000-04:00 0 days 00:00:00.527395 0 days 00:00:00.003525 0 days 00:00:00.530921          None       None duckdb/v1.4.4(osx_arm64) python/3.14 logan_glosser         1              10 853a70ec-4788-4274-85e8-569d141642a7 819d6781-ce8b-4966-bb80-e3cdb1a775d1         v1.4.4         Jumbo      QUERY               0               714                      0 logan_glosser.rw publication_signoff_20260407_1034
819d6781-d7b3-49d9-86e9-df6d285e8776 SELECT * FROM md_information_schema.database_snapshots LIMIT 3; 2026-04-07 06:34:15.860000-04:00 2026-04-07 06:34:17.891000-04:00 0 days 00:00:02.028929 0 days 00:00:00.002362 0 days 00:00:02.031291          None       None duckdb/v1.4.4(osx_arm64) python/3.14 logan_glosser         1               9 853a70ec-4788-4274-85e8-569d141642a7 819d6781-ce8b-4966-bb80-e3cdb1a775d1         v1.4.4         Jumbo      QUERY               0               312                      0 logan_glosser.rw publication_signoff_20260407_1034
```

## schemas
```
            schema_name
                     hn
                 kaggle
                   main
                   main
                   main
                   main
                   main
                   main
                   main
                   main
                   main
                   main
                   main
        mm_contract_dev
        mm_contract_dev
        mm_contract_dev
        mm_contract_dev
        mm_contract_dev
                    nyc
                     qa
                     qa
                     qa
                     qa
                     qa
       release_20260406
       release_20260406
       release_20260406
       release_20260406
       release_20260406
       release_20260407
       release_20260407
       release_20260407
       release_20260407
       release_20260407
 release_20260407_final
 release_20260407_final
 release_20260407_final
 release_20260407_final
 release_20260407_final
release_20260407_final2
release_20260407_final2
release_20260407_final2
release_20260407_final2
release_20260407_final2
       release_20260408
       release_20260408
       release_20260408
       release_20260408
       release_20260408
       release_20260409
       release_20260409
       release_20260409
       release_20260409
       release_20260409
   stackoverflow_survey
               v2_stage
               v2_stage
               v2_stage
               v2_stage
               v2_stage
                    who
```

## release_* schemas
```
            schema_name
       release_20260406
       release_20260406
       release_20260406
       release_20260406
       release_20260406
       release_20260407
       release_20260407
       release_20260407
       release_20260407
       release_20260407
 release_20260407_final
 release_20260407_final
 release_20260407_final
 release_20260407_final
 release_20260407_final
release_20260407_final2
release_20260407_final2
release_20260407_final2
release_20260407_final2
release_20260407_final2
       release_20260408
       release_20260408
       release_20260408
       release_20260408
       release_20260408
       release_20260409
       release_20260409
       release_20260409
       release_20260409
       release_20260409
```
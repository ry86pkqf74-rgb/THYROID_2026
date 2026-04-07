## Session

```sql
SELECT current_database() AS current_database, current_timestamp AS ts
```
| current_database   | ts                               |
|:-------------------|:---------------------------------|
| Thyroid 2026       | 2026-04-07 18:00:23.000783-04:00 |
- **custom_user_agent (connection):** `THYROID_2026_rc_audit/20260407`
- **motherduck_session_hint:** `cursor_mrq_next_steps_rc`
- **current_database:** `Thyroid 2026`

## MD_INFORMATION_SCHEMA.DATABASES

```sql
SELECT * FROM MD_INFORMATION_SCHEMA.DATABASES
```
| name                                                               | uuid                                 | created_ts                | transient   | historical_snapshot_retention   | type     |
|:-------------------------------------------------------------------|:-------------------------------------|:--------------------------|:------------|:--------------------------------|:---------|
| my_db                                                              | e0db7fdc-5cb6-4f57-9944-6baadccd82c1 | 2026-03-07 02:53:36-05:00 | False       | 7 days 00:00:00                 | DEFAULT  |
| rosflow                                                            | c8f55a3a-ab80-4f93-8bb1-74b55770ad39 | 2026-03-15 13:16:55-04:00 | False       | 7 days 00:00:00                 | DEFAULT  |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 2026-04-02 04:31:53-04:00 | False       | 7 days 00:00:00                 | DUCKLAKE |
| Thyroid 2026 Molecular Dev 20260407                                | 92bc15f0-1dcd-42bb-b548-74e5cc24aedd | 2026-04-07 12:10:18-04:00 | False       | 7 days 00:00:00                 | DEFAULT  |
| Thyroid 2026 Molecular PrePromote 20260407_cursor_prod_release     | 6011a8ad-92a8-497e-ad99-43fdefa5d23e | 2026-04-07 12:14:06-04:00 | False       | 7 days 00:00:00                 | DEFAULT  |
| Thyroid 2026 Molecular PrePromote 20260410_20260407_161831_promote | fd1af03b-a3d3-4170-910c-c61a173dfedd | 2026-04-07 12:18:34-04:00 | False       | 7 days 00:00:00                 | DEFAULT  |
| Thyroid 2026 Molecular PrePromote agent_20260407_workflow          | 9b6d3949-4698-4be7-95b1-3b4e7c30d9e4 | 2026-04-07 02:39:05-04:00 | False       | 7 days 00:00:00                 | DEFAULT  |
| Thyroid 2026 Molecular QA 20260407                                 | f05500b0-2eea-41bf-b5d4-35e677f6ebc5 | 2026-04-07 01:44:43-04:00 | False       | 7 days 00:00:00                 | DEFAULT  |
## DATABASE_SNAPSHOTS (thyroid filter)

```sql
SELECT * FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS WHERE database_name ILIKE '%thyroid%' ORDER BY created_ts DESC LIMIT 50
```
| database_name                                                      | database_id                          | snapshot_id                          | snapshot_name   | created_ts                 |   active_bytes |   bytes_written |   bytes_deleted | user_name     | user_id                              |
|:-------------------------------------------------------------------|:-------------------------------------|:-------------------------------------|:----------------|:---------------------------|---------------:|----------------:|----------------:|:--------------|:-------------------------------------|
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | eb4b8db9-d8d3-4aa4-8c64-ba1d117c3289 |                 | 2026-04-07 20:05:11.506000 |       37236736 |         7606272 |         6819840 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 0980cff3-dd7d-4977-b44a-3c5ab97d5640 |                 | 2026-04-07 20:04:10.656000 |       36450304 |        14946304 |        15208448 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 45f52055-f5e3-4ab3-a6ce-261e0ec9ed8c |                 | 2026-04-07 19:53:52.555000 |       36712448 |         6819840 |         6557696 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 51d86838-4076-4d16-8ee7-ff212ff01291 |                 | 2026-04-07 19:52:51.726000 |       36450304 |         8392704 |         7606272 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 48055060-f78f-459b-929f-3d4478b14b48 |                 | 2026-04-07 19:51:50.996000 |       35663872 |         6557696 |         7344128 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 93b6f161-11f3-43d0-8337-69a52c5178cd |                 | 2026-04-07 19:50:50.274000 |       36450304 |        14422016 |        13897728 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 844ccba0-8adf-4567-a5b1-424f46af795a |                 | 2026-04-07 19:22:05.713000 |       35926016 |         7344128 |         5771264 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 671c644a-b49b-4237-aed7-f9b7155a2de8 |                 | 2026-04-07 19:16:04.760000 |       34353152 |         7344128 |         7606272 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 046c5643-7006-4fe1-8f6c-ad859da79ab4 |                 | 2026-04-07 19:15:03.948000 |       34615296 |         5771264 |         5771264 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 723137a2-aad5-42ae-8768-bd5ffb7f9462 |                 | 2026-04-07 19:14:02.976000 |       34615296 |         4722688 |         4460544 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 4919b9d9-b992-4a5c-b0c7-8b944e4e63cd |                 | 2026-04-07 19:13:02.256000 |       34353152 |        13111296 |        12062720 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 0f7ec436-dc42-4f11-815b-8c05f3b28bd0 |                 | 2026-04-07 18:21:57.309000 |       33304576 |        10752000 |        11276288 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular Dev 20260407                                | 92bc15f0-1dcd-42bb-b548-74e5cc24aedd | ec2aeee2-79b4-45ba-9dfb-7197fc8f1596 |                 | 2026-04-07 16:37:43.237000 |      530853888 |               0 |        40370176 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 65dd6927-cab5-4266-bf5b-5e2010b229ca |                 | 2026-04-07 16:32:17.111000 |       33828864 |         7081984 |         6819840 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 351bf783-7cc7-476d-819f-442b7eb40732 |                 | 2026-04-07 16:23:15.733000 |       33566720 |         7606272 |         7344128 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | a74ff975-8182-4b2b-90e9-e30905bcbc49 |                 | 2026-04-07 16:22:14.842000 |       33304576 |         9179136 |         7868416 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | aec87b14-0b52-4f24-a19c-646272bcf44e |                 | 2026-04-07 16:21:14.130000 |       31993856 |         7344128 |         8654848 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular PrePromote 20260410_20260407_161831_promote | fd1af03b-a3d3-4170-910c-c61a173dfedd | 268f858f-2523-4049-be96-9d5787566fa1 |                 | 2026-04-07 16:19:02.488000 |      529018880 |       529010688 |            4096 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular PrePromote 20260410_20260407_161831_promote | fd1af03b-a3d3-4170-910c-c61a173dfedd | ae5a518c-b0eb-485c-bf20-71e4ee16c42f |                 | 2026-04-07 16:18:34.641000 |          12288 |           12288 |               0 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | caa8ac71-581e-4f8b-a062-a62da2482e38 |                 | 2026-04-07 16:18:13.330000 |       33304576 |         5771264 |         6033408 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 0de6bb49-ace9-4807-833b-092cd2e01c46 |                 | 2026-04-07 16:17:12.597000 |       33566720 |         3936256 |         3674112 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 788444dd-46bb-47e6-af4b-32a6df033544 |                 | 2026-04-07 16:16:11.866000 |       33304576 |         5771264 |         6033408 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular PrePromote 20260407_cursor_prod_release     | 6011a8ad-92a8-497e-ad99-43fdefa5d23e | 71ccd562-6832-42e0-800b-d94d8233ffdc |                 | 2026-04-07 16:14:33.500000 |      529018880 |       529010688 |            4096 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular PrePromote 20260407_cursor_prod_release     | 6011a8ad-92a8-497e-ad99-43fdefa5d23e | f2c60a3f-da40-440e-a76d-632dcfb5f69e |                 | 2026-04-07 16:14:06.453000 |          12288 |           12288 |               0 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular Dev 20260407                                | 92bc15f0-1dcd-42bb-b548-74e5cc24aedd | f1013300-fe4f-47ee-abe0-10a79a3cff33 |                 | 2026-04-07 16:11:16.125000 |      571224064 |            4096 |            4096 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular Dev 20260407                                | 92bc15f0-1dcd-42bb-b548-74e5cc24aedd | e3ede0d0-2237-4032-9d0a-b8de2748b01d |                 | 2026-04-07 16:11:15.984000 |      571224064 |        42205184 |               0 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular Dev 20260407                                | 92bc15f0-1dcd-42bb-b548-74e5cc24aedd | 5207fa80-b7c9-457f-bfef-95d300b04110 |                 | 2026-04-07 16:10:59.541000 |      529018880 |       529010688 |            4096 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular Dev 20260407                                | 92bc15f0-1dcd-42bb-b548-74e5cc24aedd | 118e9182-f46b-4d25-be3a-a5ccfb1756a7 |                 | 2026-04-07 16:10:18.362000 |          12288 |           12288 |               0 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular Dev 20260407                                | 02fdc556-f9e6-4fdd-9f8d-84377f055e96 | a5debd7a-2bb1-4b3d-bf9e-6128bac251b0 |                 | 2026-04-07 16:08:11.155000 |      391131136 |         1052672 |         2101248 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 49dc7ed0-5c63-4d99-9dc9-1c7336711d1e |                 | 2026-04-07 16:07:10.589000 |       33566720 |         3936256 |         3674112 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 9e8d45f2-6d95-4101-91c3-4dfbb3e12cf1 |                 | 2026-04-07 16:06:09.947000 |       33304576 |         5771264 |         6033408 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | f8a2e398-99b4-4f39-ad34-3f3e46a5be05 |                 | 2026-04-07 16:03:08.766000 |       33566720 |         3936256 |         3674112 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 4854dd22-afd4-48a6-ae74-f85f97360a5e |                 | 2026-04-07 16:02:07.972000 |       33304576 |        10489856 |         9965568 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026 Molecular Dev 20260407                                | 02fdc556-f9e6-4fdd-9f8d-84377f055e96 | 99aff958-e9b4-4960-acc0-65b72ef01d98 |                 | 2026-04-07 15:56:07.097000 |      392179712 |         4984832 |          790528 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | cf8c375a-231a-42e4-a1ec-ca60a22ed3d0 |                 | 2026-04-07 15:35:29.624000 |       32780288 |         6033408 |         5509120 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 3cbe74a3-be6a-4c8b-bcc1-656dbb8869ab |                 | 2026-04-07 15:34:28.692000 |       32256000 |         5771264 |         5771264 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 4e4b53d8-d182-4040-bc04-1bc04cf6792c |                 | 2026-04-07 15:27:27.868000 |       32256000 |         5771264 |         4984832 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | b1079bf7-44f2-4c49-b3f4-9a154de17152 |                 | 2026-04-07 15:25:27.242000 |       31469568 |         8130560 |         7868416 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 4653dd95-d15a-4eb8-8900-1c6e62375377 |                 | 2026-04-07 15:24:24.383000 |       31207424 |         7606272 |         7081984 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 1954c8f0-0744-4c26-8da8-1e0942a3a4c2 |                 | 2026-04-07 15:23:19.093000 |       30683136 |         6557696 |         5509120 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | f098e4f9-8fa2-4243-b03f-e2a1ca8fee54 |                 | 2026-04-07 15:22:18.409000 |       29634560 |         7344128 |         7344128 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | eee2c999-1925-4f05-822e-03d869b36733 |                 | 2026-04-07 15:15:17.639000 |       29634560 |         3149824 |         2363392 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | c513f0eb-e33f-4986-b09f-c7aba3ef5328 |                 | 2026-04-07 15:14:16.913000 |       28848128 |         7344128 |         7606272 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 22b30e71-6c10-43ae-a0a1-99b96d1495ee |                 | 2026-04-07 15:13:32.254000 |       29110272 |         3936256 |         3411968 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | df56720b-3696-4416-8560-ca56f6d05ac1 |                 | 2026-04-07 15:13:16.040000 |       28585984 |         6557696 |         7081984 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 444a03b5-43fd-4cf9-a405-2068c8a26b52 |                 | 2026-04-07 15:12:45.580000 |       29110272 |         4198400 |         4984832 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | da853af6-74f1-46d3-9480-d01f398a1a51 |                 | 2026-04-07 15:12:15.269000 |       29896704 |         4722688 |         3674112 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 8b2b46ca-13ad-4e72-8ccc-221bd6f29387 |                 | 2026-04-07 15:11:14.580000 |       28848128 |         4460544 |         5771264 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | 965f5d6e-3689-4124-a265-73b812f5a29d |                 | 2026-04-07 15:10:13.919000 |       30158848 |         2887680 |         1576960 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
| Thyroid 2026                                                       | b64d4ec9-aafb-49cc-bf39-d1dfd85e68dc | c957444f-e7a9-42b5-b370-5e5508d8aaec |                 | 2026-04-07 15:09:30.035000 |       28848128 |         3674112 |         4984832 | logan_glosser | 1a5fc41a-c574-4b4f-920b-a6687fe9fc84 |
## Table counts by schema

```sql
SELECT table_schema, COUNT(*) AS n_tables FROM information_schema.tables WHERE table_catalog = current_database() GROUP BY 1 ORDER BY 1
```
| table_schema            |   n_tables |
|:------------------------|-----------:|
| main                    |        146 |
| mm_contract_dev         |         32 |
| qa                      |         27 |
| release_20260406        |          6 |
| release_20260407        |          5 |
| release_20260407_final  |         10 |
| release_20260407_final2 |         10 |
| release_20260407_tier   |         10 |
| release_20260408        |         10 |
| release_20260409        |         10 |
| release_20260410        |         10 |
| release_20260411        |         10 |
| v2_stage                |         38 |
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
| run_label                        | domain                        |    n |   reviewed |   pending |
|:---------------------------------|:------------------------------|-----:|-----------:|----------:|
| 20260407_tier_policy_review_gate | airway_invasion               | 2045 |       2045 |         0 |
| 20260407_tier_policy_review_gate | cervical_ln_detail            |   37 |         37 |         0 |
| 20260407_tier_policy_review_gate | dynamic_risk_response         |    1 |          1 |         0 |
| 20260407_tier_policy_review_gate | frozen_section_detail         |    2 |          2 |         0 |
| 20260407_tier_policy_review_gate | functional_outcomes           |  346 |        346 |         0 |
| 20260407_tier_policy_review_gate | imaging                       |  601 |        601 |         0 |
| 20260407_tier_policy_review_gate | labs                          |   99 |         99 |         0 |
| 20260407_tier_policy_review_gate | parathyroid_detail            |    2 |          2 |         0 |
| 20260407_tier_policy_review_gate | past_medical_hx               |   11 |         11 |         0 |
| 20260407_tier_policy_review_gate | past_surgical_hx              |  231 |        231 |         0 |
| 20260407_tier_policy_review_gate | pathology                     |  683 |        683 |         0 |
| 20260407_tier_policy_review_gate | patient_decision_adherence    |   52 |         52 |         0 |
| 20260407_tier_policy_review_gate | physical_exam                 |  247 |        247 |         0 |
| 20260407_tier_policy_review_gate | presenting_symptoms           |    6 |          6 |         0 |
| 20260407_tier_policy_review_gate | rad_treatment                 |   16 |         16 |         0 |
| 20260407_tier_policy_review_gate | rai_detailed                  |  564 |        564 |         0 |
| 20260407_tier_policy_review_gate | recurrence                    |   22 |         22 |         0 |
| 20260407_tier_policy_review_gate | survival_followup             |  279 |        279 |         0 |
| 20260407_tier_policy_review_gate | synoptic_pathology_enrichment |    7 |          7 |         0 |
| 20260407_tier_policy_review_gate | tirads_granular               |    1 |          1 |         0 |
| 20260407_tier_policy_review_gate | vascular_invasion             |  370 |        370 |         0 |
| promotion_gate                   | airway_invasion               | 2045 |       2045 |         0 |
| promotion_gate                   | cervical_ln_detail            |   37 |         37 |         0 |
| promotion_gate                   | dynamic_risk_response         |    1 |          1 |         0 |
| promotion_gate                   | frozen_section_detail         |    2 |          2 |         0 |
| promotion_gate                   | functional_outcomes           |  346 |        346 |         0 |
| promotion_gate                   | imaging                       |  601 |        601 |         0 |
| promotion_gate                   | labs                          |   99 |         99 |         0 |
| promotion_gate                   | parathyroid_detail            |    2 |          2 |         0 |
| promotion_gate                   | past_medical_hx               |   11 |         11 |         0 |
| promotion_gate                   | past_surgical_hx              |  231 |        231 |         0 |
| promotion_gate                   | pathology                     |  683 |        683 |         0 |
| promotion_gate                   | patient_decision_adherence    |   52 |         52 |         0 |
| promotion_gate                   | physical_exam                 |  247 |        247 |         0 |
| promotion_gate                   | presenting_symptoms           |    6 |          6 |         0 |
| promotion_gate                   | rad_treatment                 |   16 |         16 |         0 |
| promotion_gate                   | rai_detailed                  |  564 |        564 |         0 |
| promotion_gate                   | recurrence                    |   22 |         22 |         0 |
| promotion_gate                   | survival_followup             |  279 |        279 |         0 |
| promotion_gate                   | synoptic_pathology_enrichment |    7 |          7 |         0 |
| promotion_gate                   | tirads_granular               |    1 |          1 |         0 |
| promotion_gate                   | vascular_invasion             |  370 |        370 |         0 |
## manual_review_queue by algorithm_status

```sql
SELECT algorithm_status, COUNT(*) AS n FROM qa.manual_review_queue GROUP BY 1 ORDER BY 2 DESC
```
| algorithm_status                |     n |
|:--------------------------------|------:|
| existing_missing_fill_candidate | 11240 |
| discordant_existing             |     4 |
## release_manifest (latest)

```sql
SELECT release_tag, created_at, created_by FROM qa.release_manifest ORDER BY created_at DESC LIMIT 10
```
| release_tag     | created_at                 | created_by                          |
|:----------------|:---------------------------|:------------------------------------|
| 20260411        | 2026-04-07 19:15:39.106720 | scripts/126_final_master_release.py |
| 20260410        | 2026-04-07 16:22:53.465299 | scripts/115_release_snapshot.py     |
| 20260407_tier   | 2026-04-07 15:25:17.363482 | scripts/126_final_master_release.py |
| 20260407_final2 | 2026-04-07 05:11:41.171561 | scripts/115_release_snapshot.py     |
| 20260407_final  | 2026-04-07 05:08:12.328508 | scripts/115_release_snapshot.py     |
| 20260406        | 2026-04-07 04:07:52.519215 | scripts/115_release_snapshot.py     |
| 20260409        | 2026-04-07 02:05:07.189573 | scripts/126_final_master_release.py |
| 20260408        | 2026-04-07 02:03:20.732093 | scripts/126_final_master_release.py |
| 20260407        | 2026-04-07 01:09:57.717289 | scripts/115_release_snapshot.py     |
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

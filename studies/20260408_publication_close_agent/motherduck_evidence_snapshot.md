## PRAGMA database_list

- name=md_information_schema file_present=False
- name=thyroid_research_ro_v2 file_present=True
- name=sample_data file_present=True
- name=Thyroid 2026 Molecular PrePromote 20260407_cursor_prod_release file_present=True
- name=Thyroid 2026 Molecular PrePromote 20260410_20260407_161831_promote file_present=True
- name=Thyroid 2026 Molecular Dev 20260407 file_present=True
- name=Thyroid 2026 Molecular PrePromote agent_20260407_workflow file_present=True
- name=Thyroid 2026 Molecular QA 20260407 file_present=True
- name=Thyroid 2026 file_present=True
- name=Thyroid 2026 Molecular PrePromote 20260408_full_081638_exec file_present=True
- name=rosflow file_present=True
- name=my_db file_present=True

## qa.release_manifest (latest 5 by created_at)

| release_tag | git_sha | registry_version | created_at | created_by |
|---|---|---|---|---|
| 20260408r4 | d9b9dc9 | entity_schema_v3_2026-04-03 | 2026-04-08 08:56:49.086697 | scripts/126_manual_tail_after_collision.py |
| 20260408r3 | a593544 | entity_schema_v3_2026-04-03 | 2026-04-08 05:20:40.752314 | scripts/post_backfill_resnapshot |
| 20260408r2 | a593544 | entity_schema_v3_2026-04-03 | 2026-04-08 05:18:04.189662 | scripts/126_completion_after_collision |

**Note:** `row_counts` JSON on these rows still records **canonical_extracted_fact_long_v2 = 20,188** (point-in-time at snapshot). **Live `main`** after this session was **55,500** — manifest vs live drift; cut a new `release_*` / manifest row after intentional promotion.

## MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS

row_count: 227

## Key counts

- main.canonical_extracted_fact_long_v2: 55500
- main.longitudinal_lab_canonical_v1: 77960
- qa.manual_review_queue: 5622
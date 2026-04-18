# Script 272 — Dictionary reconciliation
Run UTC: 2026-04-18T08:19:59+00:00

- main.data_dictionary_v279 confirmed sole dictionary in main: ✓
- data_dictionary_v240 archive presence: False
- data_dictionary_v266a archive presence: False
- archive_pub_v1_0 dictionary matches: ['data_dictionary_v240_pre243_backup_20260416', 'data_dictionary_v240_pre251_20260417T012311Z', 'data_dictionary_v240_pre266a_20260417T081936Z', 'data_dictionary_v240_pre266c_20260418T003012Z', 'data_dictionary_v266a_pre271_20260418T010726Z', 'data_dictionary_v266a_pre271b_20260418T021518Z', 'data_dictionary_v266a_pre271b_20260418T021547Z', 'data_dictionary_v266a_pre271b_20260418T021618Z', 'data_dictionary_v266a_pre279_20260418T070418Z', 'data_dictionary_v266a_pre279_20260418T070553Z']
- CPM columns in v279: 1526 / 1526
- Coverage-gap rows written: 0
- Auto-seeded placeholder rows: 0

Notes: predecessors (v240, v266a) were dropped earlier; this run does not
attempt restoration. If absent from archive_pub_v1_0, that fact is logged here.

# Script 399 — Phase 0 probe (malignant NULL stage_group close-out)

## Halt gates (H1–H10)

| all_pass | True |

- **H1 (malignant NULL stage_group):** 10 (expected 10)
- **H2a (A-1 WHERE rows):** 1
- **H2b (A-2 WHERE rows):** 1
- **H3 queue — 6 targets absent from queue (counts):** {'4015': 0, '9600': 0, '423': 0, '924': 0, '6275': 0, '6768': 0}
- **H3 395 sanity (1404, 12198 in queue):** {'1404': 1, '12198': 1}
- **H4 CPM total:** 10871
- **H6 reason uniqueness / non-empty:** True
- **H7 snapshot prefix tables:** 0
- **H8 CPM SET audit (stage_group only):** True
- **H9 `ajcc8_t_stage` column (queue):** absent (want absent or VARCHAR)
- **H10 repo dependents:** ok=True (ok)

## H5 — Apply-row signals

- **111:** (28, 'DTC_NOS', 'M0', 'I', 'I')
- **106:** ('MTC', 'T1b', 'N0', 'M0', None, 'I')

## Target cohort (8 rows) — CPM current

| rid | dx | age | T | N | M | stage | corrected | path_raw |
|---:|---|---:|---|---|---|---:|---|---|
| 106 | MTC | 60 | T1b | N0 | M0 | NULL | NULL | I |
| 111 | DTC_NOS | 28 | T1b | N1a | M0 | NULL | I | I |
| 4015 | MTC | 72 | T2 | N1a | M0 | NULL | NULL | NULL |
| 423 | MTC | 47 | NULL | N1a | M0 | NULL | I | NULL |
| 6275 | other_malignant | 38 | NULL | N0 | M0 | NULL | I | NULL |
| 6768 | other_malignant | 62 | T1a | N1a | M0 | NULL | NULL | II |
| 924 | MTC | 33 | T3b | N1a | M0 | NULL | I | I |
| 9600 | MTC | 63 | T1b | N0 | M1 | NULL | IVB | IVB |

## Planned writes (revised)

- **S-1:** `ALTER TABLE ... ADD COLUMN` `ajcc8_t_stage` VARCHAR (idempotent) if absent.
- **S-2, S-3:** Backfill 1404, 12198 queue rows from CPM `ajcc8_t_stage` (source_script=395).
- **A-1, A-2 (CPM):** 111 → I; 106 → I (stage_group only).
- **B-1..6 (queue):** 4015..6768 with structured `ajcc8_t_stage` (source_script=399).
- **Snapshot:** thyroid_canonical_publication_v1_0.archive_pub_v1_0.cpm_pre_malignant_null_stage_group_closeout_<ts> (8 CPM rows).

---HASH-BOUNDARY---

## Generation footer (excluded from PROBE_REPORT_SHA256)

Written UTC: 2026-04-23T03:44:18.662958+00:00

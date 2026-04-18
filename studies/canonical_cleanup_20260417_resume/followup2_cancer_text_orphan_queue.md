# Follow-up 2 — Cancer-text orphan investigation queue

_Generated 2026-04-18T06:04:42.362168+00:00_  
_Queue table: `manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1` (read-only; no canonical data modified)._  

## Counts

- Tg-lab orphan rids scanned: **403**
- Cancer-suggestive procedure_raw match: **83**
- Benign-only mention skipped (e.g. 'follicular' without 'carcinoma'): **27**
- Queue rows inserted into manuscript_workspace: **83** (status=`awaiting_upstream_triage`)

## Top cancer-suggestive terms hit

| term | n rids |
|:---|---:|
| `carcinoma` | 78 |
| `papillary` | 54 |
| `follicular` | 17 |
| `hurthle` | 11 |
| `oncocytic carcinoma` | 7 |
| `ptc` | 3 |
| `medullary` | 2 |
| `cancer` | 1 |

## Sample rows (first 15)

| rid | n_oed | match terms | procedure_raw (truncated) | n_tg | first_tg | last_tg |
|---:|---:|:---|:---|---:|:---|:---|
| 151 | 1 | `carcinoma,papillary` | Papillary Thyroid Carcinoma \| unknown | 23 | 2001-04-26 13:00:00 | 2007-12-21 11:35:00 |
| 184 | 1 | `carcinoma,hurthle,oncocytic carcinoma` | Oncocytic Carcinoma (Hurthle Cell Carcinoma); \| unknown | 41 | 2001-03-02 06:35:00 | 2008-04-28 12:34:00 |
| 224 | 1 | `carcinoma,follicular` | Follicular Thyroid Carcinoma; \| unknown | 12 | 2001-03-08 10:42:00 | 2002-08-28 09:27:00 |
| 240 | 1 | `carcinoma,hurthle,oncocytic carcinoma` | Oncocytic Carcinoma (Hurthle Cell Carcinoma); \| unknown | 2 | 2001-04-03 08:32:00 | 2001-04-03 08:32:00 |
| 253 | 1 | `carcinoma,follicular` | Follicular Thyroid Carcinoma; \| unknown | 28 | 2002-01-22 13:21:00 | 2017-11-16 12:27:23 |
| 264 | 1 | `carcinoma,papillary` | Papillary Thyroid Carcinoma; \| unknown | 15 | 2001-09-18 15:44:00 | 2008-04-29 12:29:00 |
| 457 | 1 | `carcinoma,papillary` | Papillary Thyroid Carcinoma; \| unknown | 31 | 2003-02-07 14:55:00 | 2016-12-02 11:16:00 |
| 514 | 1 | `hurthle` | Hurthle Cell Adenoma; \| unknown | 2 | 2004-05-28 08:15:00 | 2004-05-28 08:15:00 |
| 547 | 1 | `carcinoma,papillary` | Papillary Thyroid Carcinoma; \| unknown | 49 | 2005-09-07 14:25:00 | 2020-01-16 07:41:00 |
| 551 | 1 | `carcinoma,papillary` | Papillary Thyroid Carcinoma; \| unknown | 14 | 2005-06-10 03:49:00 | 2008-05-02 12:55:00 |
| 555 | 1 | `carcinoma,hurthle,oncocytic carcinoma,papillary` | Papillary Thyroid Carcinoma;Oncocytic Carcinoma (Hurthle Cell Carcinom | 24 | 2003-10-30 11:40:00 | 2012-01-12 09:30:00 |
| 560 | 1 | `carcinoma,papillary` | Papillary Thyroid Carcinoma; \| unknown | 2 | 2004-02-02 11:09:00 | 2004-02-02 11:09:00 |
| 581 | 1 | `carcinoma,follicular` | Follicular Thyroid Carcinoma; \| unknown | 57 | 2004-09-07 12:40:00 | 2025-06-23 12:38:00 |
| 599 | 1 | `carcinoma,papillary` | Papillary Thyroid Carcinoma \| unknown | 1 | 2003-08-04 11:15:00 | 2003-08-04 11:15:00 |
| 652 | 1 | `carcinoma,papillary` | Papillary Thyroid Carcinoma; \| unknown | 68 | 2014-03-05 15:20:00 | 2025-02-26 14:34:00 |

## What the queue is asking

For every rid here: the operative_episode_detail_v2.procedure_raw explicitly mentions a cancer histology, yet none of the 5 cancer-evidence tables (FNA, tumor_episode, synoptic_tumor, path_synoptic, imaging_nodule) carry evidence for this patient, and the patient is not in CPM. Triage decision per rid:

1. **Upstream extraction gap** → fix the feeder that should have captured the cancer evidence; admit to CPM.
2. **Intentional exclusion** → document the rule (e.g. benign on final path despite operative-note suspicion); update `is_in_canonical_cancer_cohort` rationale.
3. **OED procedure_raw is itself wrong** → the operative note text was extracted incorrectly; correct upstream and re-run.

_CSV with full procedure_raw / Tg dates per rid_: [`followup2_cancer_text_orphan_queue.csv`](./followup2_cancer_text_orphan_queue.csv)

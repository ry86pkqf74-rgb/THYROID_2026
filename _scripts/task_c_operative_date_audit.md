# Task C — Stratified Spot-Check: Pre-1999 Surgery Dates
**Audit date:** 2026-05-06  
**Source table:** `thyroid_canonical_publication_v1_0.main.canonical_operative_events_v1`  
**Analyst note:** All research_ids were used only in transient `/tmp/` working files and are not present in this deliverable.

---

## 1. Bucket Counts (full population)

| Bucket | N rows |
|--------|-------:|
| 1985–89 | 0 |
| 1990–94 | 46 |
| 1995–98 | 20 |
| 1999–2009 | 2,268 |
| 2010+ | 9,439 |
| **Total with non-NULL resolved_surgery_date** | **11,773** |

> **Note:** Task 1 flagged 66 pre-1999 rows. Confirmed: 46 + 20 = 66. No 1985–89 rows exist; the Task 1 floor widening to 1985 was conservative and correct.

---

## 2. Sentinel Value Check

Checked for: `0001-01-01`, `1900-01-01`, `9999-12-31`, `1970-01-01`

**Result: CLEAN — zero sentinel values found.**  
No new migration required.

---

## 3. Stratified Spot-Check Sample (5 per bucket, PHI-free)

### Bucket: 1990–94 (5 earliest of 46)

| resolved_date | date_status | note_date_source | note_date_conf |
|---------------|-------------|------------------|---------------|
| 1990-01-17 | opnote_clustered | opnote | 0.30 |
| 1990-01-18 | opnote_clustered | opnote | 0.30 |
| 1990-02-20 | opnote_clustered | opnote | 0.30 |
| 1990-05-18 | opnote_clustered | opnote | 0.30 |
| 1990-06-25 | opnote_clustered | opnote | 0.30 |

### Bucket: 1995–98 (5 earliest of 20)

| resolved_date | date_status | note_date_source | note_date_conf |
|---------------|-------------|------------------|---------------|
| 1995-01-21 | opnote_clustered | opnote | 0.30 |
| 1995-01-30 | opnote_clustered | opnote | 0.30 |
| 1995-04-04 | opnote_clustered | opnote | 0.30 |
| 1995-06-01 | opnote_clustered | opnote | 0.30 |
| 1995-06-14 | opnote_clustered | opnote | 0.30 |

### Bucket: 1999–2009 (5 earliest of 2,268)

| resolved_date | date_status | note_date_source | note_date_conf |
|---------------|-------------|------------------|---------------|
| 1999-01-18 | opnote_clustered | opnote | 0.30 |
| 1999-01-20 | exact_source_date | surgery_date_fallback | 0.60 |
| 1999-01-20 | exact_source_date | surgery_date_fallback | 0.60 |
| 1999-01-26 | exact_source_date | surgery_date_fallback | 0.60 |
| 1999-01-27 | exact_source_date | surgery_date_fallback | 0.60 |

### Bucket: 2010+ (5 earliest of 9,439)

| resolved_date | date_status | note_date_source | note_date_conf |
|---------------|-------------|------------------|---------------|
| 2010-01-05 | exact_source_date | surgery_date_fallback | 0.60 |
| 2010-01-05 | cpm_v2_anchor | cpm_v2 | 0.85 |
| 2010-01-06 | exact_source_date | surgery_date_fallback | 0.60 |
| 2010-01-06 | exact_source_date | surgery_date_fallback | 0.60 |
| 2010-01-06 | exact_source_date | surgery_date_fallback | 0.60 |

---

## 4. date_status × note_date_source Classification Consistency

### Pre-1999 only (all 66 rows)

| bucket | date_status | note_date_source | n |
|--------|-------------|------------------|--:|
| 1990–94 | cpm_v2_anchor | cpm_v2 | 1 |
| 1990–94 | opnote_clustered | opnote | 45 |
| 1995–98 | cpm_v2_anchor | cpm_v2 | 2 |
| 1995–98 | opnote_clustered | opnote | 18 |

**Internal consistency: PASS.**  
Every `opnote_clustered` row uses `note_date_source = opnote`. Every `cpm_v2_anchor` row uses `note_date_source = cpm_v2`. No mixed or anomalous pairings.

### Confidence distribution (pre-1999)

| note_date_confidence | meaning | n |
|---------------------|---------|--:|
| 0.30 | opnote_clustered (legacy op-note text) | 63 |
| 0.85 | cpm_v2_anchor (CPM patient master anchor) | 3 |

`note_date_confidence` range: **0.30 – 0.85**. Zero rows with confidence < 0.30.

### cpm_v2_anchor rows (3 rows, all pre-1999)

| resolved_date | bucket | note_date_source | note_date_conf |
|---------------|--------|------------------|---------------|
| 1994-05-25 | 1990–94 | cpm_v2 | 0.85 |
| 1996-03-13 | 1995–98 | cpm_v2 | 0.85 |
| 1997-08-20 | 1995–98 | cpm_v2 | 0.85 |

These 3 patients had their surgery dates anchored via CPM v2 (canonical patient master) rather than operative notes — appropriate and expected for patients where op-note clustering did not resolve cleanly.

---

## 5. Verdict

**SPOT-CHECK PASSED.**

- All 66 pre-1999 dates fall in 1990–1998 (plausible historical range; no 1985–89 dates exist).
- Sentinel values: **none found**.
- date_status ↔ note_date_source pairing is **internally consistent** across all 66 rows and both post-1999 sample buckets.
- Confidence values are at expected tiers (0.30 opnote, 0.60 surgery_fallback, 0.85 cpm_anchor).
- Task 1's widening of the floor to 1985 was **conservative and justified** — the actual earliest date is 1990-01-17.
- **No new mig required.**

---

## 6. Recommendations

1. The `opnote_clustered` confidence of 0.30 for all 63 legacy op-note dates is the documented floor for this source. No action needed.
2. If future analysis requires higher-confidence surgery dates for the pre-1999 cohort, the 3 `cpm_v2_anchor` rows (conf 0.85) are the only high-confidence pre-1999 dates in the table.
3. The 1985 floor in `mig_010` remains appropriate; no back-fill to earlier dates is warranted by current data.

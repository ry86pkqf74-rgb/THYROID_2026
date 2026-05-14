# Prompt 5 — Legacy vs canonical row-drop audit (2026-05-14)

BigQuery project: `thyroid-canonical-pub-2026`  
Legacy snapshot: `pub_legacy_source_20260416`  
Canonical: `pub_canonical`

Evidence collected with `.venv/bin/python3` + `google.cloud.bigquery` (live queries).

---

## ITEM B — `canonical_survival_followup_v1`

### Question

Legacy: **11,504** patients (`COUNT(DISTINCT research_id)`). Canonical: **10,871** (= `canonical_patient_master` count). Are the **633** dropped IDs **only** patients outside the publication master cohort?

### Validation SQL

```sql
-- Expect 0 accidental drops (still in master but missing from canonical survival)
SELECT COUNT(*) AS dropped_still_in_master
FROM (
  SELECT DISTINCT CAST(research_id AS STRING) AS research_id
  FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.canonical_survival_followup_v1`
  EXCEPT DISTINCT
  SELECT DISTINCT CAST(research_id AS STRING)
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_survival_followup_v1`
) x
WHERE research_id IN (
  SELECT CAST(research_id AS STRING)
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
);
```

### Live results

| Metric | Value |
|--------|------:|
| `dropped_still_in_master` | **0** |
| Distinct IDs in legacy − canonical | **633** |
| Of those 633, **not** in `canonical_patient_master` | **633** |
| Of those 633, **in** `canonical_patient_master` | **0** |
| `canonical_survival_followup_v1` rows / distinct patients | **10,871 / 10,871** |
| `canonical_patient_master` rows / distinct patients | **10,871 / 10,871** |

### Verdict — **INTENTIONAL**

**Exclusion rule (documentation):** Publication/canonical survival follow-up is defined **only for the `canonical_patient_master` cohort** (10,871 de-identified surgical/registry patients). The legacy freeze retained **633 additional `research_id` values** that do **not** appear in `canonical_patient_master`; canonical correctly omits them. There is **no join-loss** among master-cohort patients.

---

## ITEM A — `note_entities_llm_cervical_ln_detail`

### Counts

| Source | Rows | Distinct patients |
|--------|-----:|-------------------:|
| Legacy | 11,037 | 5,641 |
| Canonical | 10,084 | 5,106 |
| Patients in legacy **not** in canonical | — | **535** |

Legacy rows tied to those **535** patients (still in legacy only): **730**.

### Hypothesis check

Not all drops are “empty LLM `entities`.” Breakdown on legacy rows for patients who have **no** canonical rows:

| Pattern | Row count |
|---------|----------:|
| Parseable JSON, `entities` is `[]` | 519 |
| Parseable JSON, `entities` key missing / null | 204 |
| Parseable JSON, **`entities` non-empty** | **7** |
| `SAFE.PARSE_JSON` null | 0 |

### Patient-level split (535 “dropped-only” patients)

Using  
`MAX(COALESCE(ARRAY_LENGTH(JSON_QUERY_ARRAY(SAFE.PARSE_JSON(result_json), '$.entities')), 0))`  
per patient across their legacy rows:

| Category | Patients |
|----------|----------:|
| All legacy notes have **empty/missing** entities arrays | **528** |
| At least one legacy note has **non-empty** `entities` | **7** |
| **Total** | **535** |

### The seven non-empty legacy-only notes (content loss vs canonical)

All are **`history_summary`** notes; **`research_id` is present on `canonical_patient_master`** but **`note_entities_llm_cervical_ln_detail` has zero rows** for that patient in canonical.

| research_id | note_row_id (prefix) | Entities (count) |
|-------------|----------------------|------------------|
| 2978 | `358b30e0655adf52…` | 1 (`ln_level`, central neck III) |
| 3229 | `d6e344eb7c5bb413…` | 4 |
| 3268 | `c40da2bc8d731b07…` | 4 |
| 3694 | `1be1ebebf448d78a…` | 1 (`present_or_negated`: negated LN) |
| 3732 | `fa948a5c025efa26…` | 4 |
| 3748 | `6ea9941d0ef6171c…` | 3 |
| 3911 | `240a2102c79a6c42…` | 4 |

These `note_row_id` values **do not appear** in `pub_canonical.note_entities_llm_cervical_ln_detail`.

### Root cause (engineering lineage)

Canonical row count (**10,084**) matches **MotherDuck after Script 382** (`scripts/382_cervical_ln_clinical_merge_load_rollup.py`): the live table was rebuilt from the **round-2 checkpoint parquet** (`runs/round2_20260421/cervical_ln_detail/…`), which **replaced** the wider pre-round-2 note set (**11,037** rows in legacy freeze).

So:

- **528 / 535 patients**: legacy rows carry **no usable cervical-LN entity payload** under the JSON schema above → consistent with intentional omission from the promoted subset **or** empty extraction.
- **7 / 535 patients**: legacy JSON **does** carry cervical-LN entities, but those notes **were not carried forward** in the round-2 promotion artifact → **not** explained by “empty extraction”; this is a **small promotion-scope / checkpoint coverage gap** vs the legacy snapshot.

### Verdict — **MOSTLY intentional; 7-patient exception**

- **528 patients**: consistent with the hypothesis that canonical intentionally aligns to the **round-2 promoted note spine** and drops notes with **no populated `entities` array**.
- **7 patients (7 notes)**: **not intentional** from a clinical-NLP-content perspective; remediation requires **restoring those rows** from legacy (or re-running extraction for those `note_row_id`s) in **MotherDuck `main.note_entities_llm_cervical_ln_detail`** and **re-exporting** BigQuery so MD and BQ stay aligned.

### Optional BigQuery backfill (BQ-only — **warn**: next MD→BQ bulk publish may overwrite unless MD is patched)

```sql
INSERT INTO `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_cervical_ln_detail`
SELECT src.*
FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.note_entities_llm_cervical_ln_detail` AS src
WHERE src.note_row_id IN (
  '358b30e0655adf52423f4bcb7d64623379541e51',
  'd6e344eb7c5bb413c0bcad94dbc21c3296f4decc',
  'c40da2bc8d731b073821caafd50740c2fe892d09',
  '1be1ebebf448d78ab2ee961c745a64f22b5b715d',
  'fa948a5c025efa265987bcf1ba5b7d4757d70601',
  '6ea9941d0ef6171c2593b44d71b1fd0b9f21f92b',
  '240a2102c79a6c4221a26aeffddc7d1ad32a2ef8'
);
```

Run only after confirming column compatibility between datasets and coordinating MotherDuck.

---

## Acceptance summary

| Item | Intentional? | Notes |
|------|--------------|-------|
| **B** — survival | **Yes** | All 633 dropped IDs are **outside** `canonical_patient_master`; canonical survival patients **=** master (**10,871**). |
| **A** — cervical LN NLP | **Mixed** | **528 / 535** dropped-only patients match empty/missing entities in legacy; **7 / 535** have **real entities** in legacy → **fix via row restoration / checkpoint coverage**, not by tweaking an “empty JSON” filter alone. |

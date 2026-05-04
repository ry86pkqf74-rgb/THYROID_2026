# Cursor Composer Dispatch — mig_269: `canonical_recurrence_events_v1` SSOT (event-grain consolidation)

**Generated:** 2026-05-03 by Cowork at HEAD `be75bee`.
**Lane:** mig_269 — Build `canonical_recurrence_events_v1` as the event-grain SSOT for thyroid cancer recurrence. Today recurrence is split across `canonical_recurrence_v1` (patient-rollup, 10,871 rows), `canonical_recurrence_resolved_v1` (patient-rollup, 10,871 rows), `recurrence_event_clean_v1` (1,946 events but non-canonical name), and 7+ scattered Tier-1/Tier-3 NLP sources. mig_269 consolidates to a clean events table + repoints CPM rollup cols to derive from it.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs rule disambiguation on event de-dup keys + biochemical-vs-structural precedence before mechanical apply.
**Estimated runtime:** 3-5 hours (largest mig in this round).
**Triggered by:** Logan greenlit 2026-05-03 ("269 - do"); also gates clean inputs for M044 Cox PH multi-event sensitivity if scope expands.
**Severity:** MED. Doesn't break any current manuscript (M044 Cox uses one-event recurrence_confirmed flag), but fixes a known canonical-naming-convention violation + makes future multi-event analysis tractable.
**Closes carry-forward:** CF-RECURRENCE-NAMING-CONVENTION + CF-RECURRENCE-EVENT-GRAIN-MISSING.

---

## §0 — First message to paste into Cursor Chat

> mig_269 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_269_RECURRENCE_EVENTS_SSOT_20260503.md` end-to-end. This is a Tier-2 canonical build per Logan's `reference_canonical_naming_convention.md`. Use Chat first to walk through §3 design decisions (de-dup key, biochem-vs-struct precedence, prior-thy attribution). Surface a 4-5 row sample diff to me before authorizing Composer-direct apply.
>
> MotherDuck DB is `thyroid_canonical_publication_v1_0`. Sources at `main.recurrence_event_clean_v1` (1,946 events), `main.note_entities_llm_recurrence`, `main.canonical_recurrence_v1` (patient-grain), `main.canonical_recurrence_resolved_v1`. Pre-snapshot any rebuilt object to `"Thyroid 2026 UPdated".archive_pub_v1_0`.

---

## §1 — Why this lane exists

### Current state (from MD probe 2026-05-03)

| Object | Grain | n_rows | n_pts | Issue |
|---|---|---:|---:|---|
| `main.canonical_recurrence_v1` | patient-rollup | 10,871 | 10,871 | Name suggests events but is rollup; date col is `DATE` (good) |
| `main.canonical_recurrence_resolved_v1` | patient-rollup | 10,871 | 10,871 | Resolved/cleaned variant of above; near-duplicate |
| `main.recurrence_event_clean_v1` | event | 1,946 | ~1,400-ish | Correct grain BUT non-canonical naming (no `canonical_*_events_v1` prefix per `reference_canonical_naming_convention.md`) |
| `main.note_entities_llm_recurrence` | mention | ? | ? | Tier-3 NLP feed; raw extractor output |

### Naming-convention violation

Per `reference_canonical_naming_convention.md` (Logan-ratified): Tier-2 masters MUST be `canonical_<domain>_events_v1` + `canonical_<domain>_patient_rollup_v1`. Recurrence has neither matching name.

### Manuscript impact

- M044 Cox PH currently uses `any_recurrence_flag` from CPM (single-event indicator). Works today but blocks any "≥2 recurrence events" analysis.
- Multi-event sensitivity for M044 + future M046 disease-free survival paper need event-grain SSOT.

---

## §2 — Pre-task probes

```sql
-- 2.1 Inspect recurrence_event_clean_v1 schema + sample
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='main' AND table_name='recurrence_event_clean_v1'
ORDER BY ordinal_position;

SELECT * FROM main.recurrence_event_clean_v1 LIMIT 5;

-- 2.2 Cardinality probe: events per patient distribution
SELECT n_events, COUNT(*) AS n_pts
FROM (SELECT research_id, COUNT(*) AS n_events FROM main.recurrence_event_clean_v1 GROUP BY research_id)
GROUP BY n_events ORDER BY n_events;

-- 2.3 Mention-grain de-dup probe (per feedback_mention_grain_partition_probe.md)
SELECT
  COUNT(*) AS n_mentions,
  COUNT(DISTINCT (research_id, recurrence_date, recurrence_site)) AS n_distinct_naive,
  COUNT(DISTINCT (research_id, recurrence_date, recurrence_site, evidence_quote)) AS n_distinct_richer
FROM main.note_entities_llm_recurrence;

-- 2.4 Check biochemical vs structural split
SELECT
  COALESCE(recurrence_type, '(null)') AS rec_type,
  COUNT(*) AS n_events,
  COUNT(DISTINCT research_id) AS n_pts
FROM main.recurrence_event_clean_v1
GROUP BY 1 ORDER BY 2 DESC;

-- 2.5 Resolved-vs-raw drift (mig_255 closed biochem TTR drift but worth re-checking)
SELECT
  v1.research_id, v1.recurrence_confirmed AS v1_conf, vr.recurrence_confirmed AS vr_conf,
  v1.first_recurrence_date AS v1_dt, vr.first_recurrence_date AS vr_dt
FROM main.canonical_recurrence_v1 v1
JOIN main.canonical_recurrence_resolved_v1 vr USING (research_id)
WHERE v1.recurrence_confirmed IS DISTINCT FROM vr.recurrence_confirmed
   OR v1.first_recurrence_date IS DISTINCT FROM vr.first_recurrence_date
LIMIT 20;
-- If non-empty: surface to Logan; resolved is SOT.
```

---

## §3 — Design decisions (Chat-first; surface to Logan)

### §3.1 — Event de-dup key

Per `feedback_mention_grain_partition_probe.md` (mig_362 collapse trap), naive `(research_id, recurrence_date)` may silently collapse multi-site same-day events. Default richer key:

`(research_id, recurrence_date, recurrence_site, recurrence_type)`

with tiebreaker on `evidence_strength` desc → `note_row_id` asc. Walk through with Chat; confirm against §2.3 probe.

### §3.2 — Biochemical vs structural precedence

When the same patient has both biochem (Tg ↑) and structural (imaging-detected mass) on overlapping dates, the events table records both as separate rows but `canonical_recurrence_patient_rollup_v1.recurrence_definition` follows ATA hierarchy: structural > biochemical_indeterminate > biochemical.

### §3.3 — Prior-thy attribution

For r1c residuals (LN-only at this surgery): if prior thyroidectomy is documented (`pshx_nlp_prior_thyroidectomy=TRUE`), classify the LN finding as recurrence not de-novo per `feedback_ln_only_pt0_prior_thy_upstage.md`. Carry over the rule into `canonical_recurrence_events_v1.attribution_basis = 'prior_thy_lnonly_upstage'`.

### §3.4 — Schema (proposed)

```sql
CREATE OR REPLACE TABLE main.canonical_recurrence_events_v1 (
  research_id              VARCHAR     NOT NULL,
  recurrence_event_seq     INTEGER     NOT NULL,    -- 1, 2, 3 ... per pt, ordered by date
  recurrence_date          DATE,
  recurrence_date_source   VARCHAR,                 -- 'note', 'resolved', 'inferred', etc.
  recurrence_type          VARCHAR,                 -- 'structural','biochemical','biochemical_indeterminate'
  recurrence_site          VARCHAR,                 -- 'central_neck','lateral_neck','distant_lung', etc.
  recurrence_site_text     VARCHAR,                 -- raw text
  recurrence_laterality    VARCHAR,                 -- 'left','right','bilateral','midline','unspecified'
  recurrence_histology     VARCHAR,                 -- when path-confirmed
  evidence_strength        VARCHAR,                 -- 'definitive','probable','possible'
  evidence_source          VARCHAR,                 -- 'pathology','imaging','tg_lab','clinic_note'
  evidence_quote           VARCHAR,
  attribution_basis        VARCHAR,                 -- 'denovo','recurrence_per_resolved','prior_thy_lnonly_upstage'
  note_row_id              VARCHAR,                 -- provenance back to note source
  llm_model                VARCHAR,                 -- per feedback_llm_model_tag
  build_ts                 TIMESTAMP,
  PRIMARY KEY (research_id, recurrence_event_seq)
);
```

And rollup:

```sql
CREATE OR REPLACE TABLE main.canonical_recurrence_patient_rollup_v1 AS
WITH events AS (SELECT * FROM main.canonical_recurrence_events_v1),
ordered AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY recurrence_date NULLS LAST, recurrence_event_seq) AS rn,
    COUNT(*) OVER (PARTITION BY research_id) AS n_events
  FROM events
)
SELECT
  research_id,
  COUNT(*) AS n_recurrence_events,
  MAX(CASE WHEN evidence_strength IN ('definitive','probable') THEN TRUE ELSE FALSE END) AS recurrence_confirmed,
  MIN(recurrence_date) AS first_recurrence_date,
  MAX(recurrence_date) AS last_recurrence_date,
  -- ATA-hierarchy primary type (structural > biochem_indet > biochem):
  COALESCE(
    MAX(CASE WHEN recurrence_type='structural' THEN recurrence_type END),
    MAX(CASE WHEN recurrence_type='biochemical_indeterminate' THEN recurrence_type END),
    MAX(CASE WHEN recurrence_type='biochemical' THEN recurrence_type END)
  ) AS recurrence_type_primary,
  MAX(recurrence_site) FILTER (WHERE rn=1) AS recurrence_site_primary,
  MAX(attribution_basis) FILTER (WHERE rn=1) AS attribution_basis_primary
FROM ordered
GROUP BY research_id;
```

### §3.5 — CPM repoint cols

Repoint these CPM cols to derive from `canonical_recurrence_patient_rollup_v1`:
- `any_recurrence_flag` ← `recurrence_confirmed`
- `first_recurrence_date` ← `first_recurrence_date`
- `recurrence_type_primary` ← `recurrence_type_primary`
- `recurrence_site_primary` ← `recurrence_site_primary`
- `time_to_recurrence_days` ← `DATE_DIFF('day', first_surgery_date, first_recurrence_date)`

Clamp `time_to_recurrence_days` per mig_257 rule (no negatives, no > overall_survival_years).

---

## §4 — Apply (after Chat sign-off)

### §4a — Pre-snapshot

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig269_20260503 AS
  SELECT * FROM main.canonical_recurrence_v1;
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_mig269_20260503 AS
  SELECT * FROM main.canonical_recurrence_resolved_v1;
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.recurrence_event_clean_v1_pre_mig269_20260503 AS
  SELECT * FROM main.recurrence_event_clean_v1;
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_recurrence_cols_pre_mig269_20260503 AS
  SELECT research_id,
         any_recurrence_flag, first_recurrence_date, recurrence_type_primary,
         recurrence_site_primary, time_to_recurrence_days, recurrence_confirmed
  FROM main.canonical_patient_master;
```

### §4b — Build events SSOT

```sql
-- Build canonical_recurrence_events_v1 from recurrence_event_clean_v1 (de-dup per §3.1)
CREATE OR REPLACE TABLE main.canonical_recurrence_events_v1 AS
WITH dedup AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY research_id, recurrence_date, recurrence_site, recurrence_type
      ORDER BY
        CASE evidence_strength
          WHEN 'definitive' THEN 1 WHEN 'probable' THEN 2 WHEN 'possible' THEN 3 ELSE 4
        END,
        note_row_id
    ) AS dedup_rank
  FROM main.recurrence_event_clean_v1
),
seq AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY recurrence_date NULLS LAST, dedup_rank) AS recurrence_event_seq
  FROM dedup WHERE dedup_rank = 1
)
SELECT
  research_id, recurrence_event_seq, recurrence_date, recurrence_date_source,
  recurrence_type, recurrence_site, recurrence_site_text, recurrence_laterality,
  recurrence_histology, evidence_strength, evidence_source, evidence_quote,
  attribution_basis, note_row_id, llm_model,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM seq;
```

### §4c — Build rollup

```sql
-- Per §3.4 spec
CREATE OR REPLACE TABLE main.canonical_recurrence_patient_rollup_v1 AS ...;
```

### §4d — Repoint CPM cols (UPDATE in place per Protocol v2)

```sql
UPDATE main.canonical_patient_master pm
SET
  any_recurrence_flag = r.recurrence_confirmed,
  first_recurrence_date = r.first_recurrence_date,
  recurrence_type_primary = r.recurrence_type_primary,
  recurrence_site_primary = r.recurrence_site_primary,
  time_to_recurrence_days = CASE
    WHEN r.first_recurrence_date IS NULL OR pm.first_surgery_date IS NULL THEN NULL
    WHEN DATE_DIFF('day', pm.first_surgery_date, r.first_recurrence_date) < 0 THEN NULL
    ELSE LEAST(
      DATE_DIFF('day', pm.first_surgery_date, r.first_recurrence_date),
      COALESCE(pm.overall_survival_days, 99999)
    )
  END,
  recurrence_confirmed = r.recurrence_confirmed
FROM main.canonical_recurrence_patient_rollup_v1 r
WHERE pm.research_id = r.research_id;
-- Set any_recurrence_flag = FALSE for pts not in rollup (no events):
UPDATE main.canonical_patient_master pm
SET any_recurrence_flag = FALSE, recurrence_confirmed = FALSE
WHERE NOT EXISTS (SELECT 1 FROM main.canonical_recurrence_patient_rollup_v1 r WHERE r.research_id = pm.research_id);
```

### §4e — Verify

```sql
-- Count parity
SELECT
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_recurrence_events_v1) AS pts_with_events,
  (SELECT COUNT(*) FROM main.canonical_recurrence_events_v1) AS n_events,
  (SELECT COUNT(*) FROM main.canonical_recurrence_patient_rollup_v1) AS rollup_pts,
  (SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) FROM main.canonical_patient_master) AS cpm_pts_w_recur;
-- Expected: events_pts ≈ 1,400-1,500 / events ≈ 1,946 / rollup = events_pts / cpm = events_pts.

-- Drift check vs canonical_recurrence_resolved_v1 (resolved is current SOT)
SELECT COUNT(*) AS n_drift
FROM main.canonical_patient_master pm
JOIN main.canonical_recurrence_resolved_v1 r ON pm.research_id = r.research_id
WHERE pm.any_recurrence_flag IS DISTINCT FROM r.recurrence_confirmed
   OR pm.first_recurrence_date IS DISTINCT FROM r.first_recurrence_date;
-- Expected: small (< 50). If large, surface to Logan.
```

### §4f — Deprecate legacy objects (defer)

DO NOT drop `canonical_recurrence_v1`, `canonical_recurrence_resolved_v1`, or `recurrence_event_clean_v1` in this mig. Schedule mig_269b after Cowork validates downstream queries don't hit them. Log them in CF.

### §4g — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_269', CURRENT_TIMESTAMP, 'cursor_composer_mig269',
 'mig_269: Built canonical_recurrence_events_v1 (event-grain SSOT, ~1946 events / ~1400 pts) + canonical_recurrence_patient_rollup_v1 per Logan canonical_naming_convention. Repointed CPM any_recurrence_flag/first_recurrence_date/recurrence_type_primary/recurrence_site_primary/time_to_recurrence_days/recurrence_confirmed to derive from rollup. Legacy canonical_recurrence_v1 / _resolved_v1 / recurrence_event_clean_v1 LEFT IN PLACE pending mig_269b deprecation. Closes CF-RECURRENCE-NAMING-CONVENTION + CF-RECURRENCE-EVENT-GRAIN-MISSING.');
```

---

## §5 — Snowflake re-verify

After commit + push, Cowork will re-export CPM + new canonicals → SF, rebuild flat views, re-run M044 Cox PH (now with cleaner recurrence input) + report any rate change.

---

## §6 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-RECURRENCE-NAMING-CONVENTION | **CLOSED on apply** | Now matches `canonical_*_events_v1` + `_patient_rollup_v1` |
| CF-RECURRENCE-EVENT-GRAIN-MISSING | **CLOSED on apply** | Event grain available |
| CF-mig269b-LEGACY-DEPRECATE | **OPEN** | Drop canonical_recurrence_v1 / _resolved_v1 / recurrence_event_clean_v1 in mig_269b after downstream consumer audit |
| CF-mig269-MULTI-EVENT-M046 | **OPEN** | Future M046 disease-free survival paper can use multi-event grain |

---

## §7 — Surgical git add

```
qc_framework_v1/migrations/269_recurrence_events_ssot_20260503.sql
scripts/output/mig_269_apply_log.txt
scripts/output/mig_269_dryrun_diff.csv
cursor_prompts/CURSOR_PROMPT_MIG_269_RECURRENCE_EVENTS_SSOT_20260503.md
```

Commit message:
```
feat(md): mig_269 canonical_recurrence_events_v1 SSOT

- Build canonical_recurrence_events_v1 (~1946 events, ~1400 pts) as event-grain SSOT
- Build canonical_recurrence_patient_rollup_v1 with ATA-hierarchy primary type
- Repoint CPM recurrence cols to derive from rollup
- Legacy canonical_recurrence_v1 / _resolved_v1 / recurrence_event_clean_v1 retained
  pending mig_269b deprecation after downstream consumer audit
- Closes CF-RECURRENCE-NAMING-CONVENTION + CF-RECURRENCE-EVENT-GRAIN-MISSING
```

---

**End of mig_269 dispatch.**

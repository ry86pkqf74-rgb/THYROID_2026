# Cursor Prompt — mig_175 mig_136 Days-Semantic Adjudication (58 cols)

**Lane:** 64 / mig_175
**Batch_id:** `mig_175_mig136_days_semantic_adjudication_20260429`
**Generated:** 2026-04-29
**Type:** Read-only profile + 3-option Logan-decision package. **No data writes.**

---

## §0 Why this lane exists

`CF-mig136-DAYS-SEMANTIC` is currently the **largest single CF cluster** in the registry (Cowork live 2026-04-29: **58 cols** carry this tag). The CF flags that for `*_first_days_from_surg` / `*_n_mentions` / `*_first_date` / `*_first_days` cols (mostly in the `pmhx_nlp_*` family — Past Medical History NLP extractions — but also reaching into other NLP clusters), the **anchor for the day-count** has not been decided in a way that's recorded per col, and different cols may be silently using different anchors.

Three candidate anchors:
1. **`event_start`** — days from the event itself (e.g., the first mention date).
2. **`first_surgery`** — days from `first_surgery_date` (current PM standard for survival calcs).
3. **`LKA`** — days from Last Known Alive (used by survival/recurrence canonicals).

Different manuscript analytics need different anchors, so this is genuinely a clinical-research decision — not a code refactor. Logan must ratify the canonical anchor (or a per-col override list) before any apply.

This lane is the **adjudication package**: profile the 58 cols, surface a 3-option decision package with live counts of impact under each option, and return the ratification request to Logan.

Sample of the 58 cols (Cowork live first 25, alphabetical):
`pmhx_nlp_afib`, `pmhx_nlp_afib_n_mentions`, `pmhx_nlp_asthma`, `pmhx_nlp_asthma_n_mentions`, `pmhx_nlp_autoimmune_thyroid_hx`, `pmhx_nlp_autoimmune_thyroid_hx_n_mentions`, `pmhx_nlp_breast_cancer`, `pmhx_nlp_breast_cancer_n_mentions`, `pmhx_nlp_cad`, `pmhx_nlp_cad_n_mentions`, `pmhx_nlp_ckd`, `pmhx_nlp_ckd_n_mentions`, `pmhx_nlp_coagulopathy`, `pmhx_nlp_comorbidity_list`, `pmhx_nlp_copd`, `pmhx_nlp_copd_n_mentions`, `pmhx_nlp_depression`, `pmhx_nlp_depression_n_mentions`, `pmhx_nlp_diabetes`, `pmhx_nlp_diabetes_first_date`, `pmhx_nlp_diabetes_first_days_from_surg`, `pmhx_nlp_diabetes_n_mentions`, `pmhx_nlp_extraction_method`, `pmhx_nlp_family_hx_cancer`, `pmhx_nlp_family_hx_thyroid_n_mentions`.

## §1 Governance posture

- Read-only profile. No `query_rw`.
- Output: profile report + 3-option decision package + commented probe SQL.
- Decision package surfaces live counts per option so Logan can evaluate clinical impact before ratifying.
- AGENTS-governance binding: agent ships profile only.

## §2 Required pre-flight probes

```sql
-- §2a Full list of 58 cols carrying CF-mig136-DAYS-SEMANTIC
SELECT column_name, data_type, COALESCE(verification_status,'unknown') AS status
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-mig136-DAYS-SEMANTIC%'
ORDER BY column_name;
-- Expect: 58 rows. Paste full list in design doc.

-- §2b Categorize cols by sub-type (*_first_date, *_first_days, *_first_days_from_surg, *_n_mentions, etc.)
SELECT
  COUNT(*) FILTER (WHERE column_name LIKE '%_first_days_from_surg') AS n_days_from_surg,
  COUNT(*) FILTER (WHERE column_name LIKE '%_first_date')           AS n_first_date,
  COUNT(*) FILTER (WHERE column_name LIKE '%_first_days')           AS n_first_days,
  COUNT(*) FILTER (WHERE column_name LIKE '%_n_mentions')           AS n_n_mentions,
  COUNT(*)                                                          AS n_total
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-mig136-DAYS-SEMANTIC%';

-- §2c For *_first_days_from_surg cols: live distribution sample
SELECT pmhx_nlp_diabetes_first_days_from_surg, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE pmhx_nlp_diabetes_first_days_from_surg IS NOT NULL
GROUP BY 1 ORDER BY 1 LIMIT 25;
-- Repeat for 3-5 representative cols.

-- §2d Per-anchor live counts: how many patients have *_first_date < first_surgery_date,
-- < last_contact_date, etc.? Probe one representative col (e.g., diabetes).
SELECT
  SUM(CASE WHEN pmhx_nlp_diabetes_first_date < first_surgery_date THEN 1 ELSE 0 END) AS n_diab_pre_surg,
  SUM(CASE WHEN pmhx_nlp_diabetes_first_date >= first_surgery_date THEN 1 ELSE 0 END) AS n_diab_post_surg,
  SUM(CASE WHEN pmhx_nlp_diabetes_first_date IS NOT NULL AND first_surgery_date IS NOT NULL THEN 1 ELSE 0 END) AS n_both_present
FROM main.canonical_patient_master;
```

## §3 The 3-option decision package

Agent must populate this table for the design doc using live MD probes:

| Option | Anchor formula | n_cols affected | n_patients impacted | Pros | Cons |
|---|---|---:|---:|---|---|
| A | `days = (date - event_start)` | 58 | live-count | Per-event semantics; matches NLP extraction author intent | Different anchor per row → cross-col comparisons hard |
| B | `days = (date - first_surgery_date)` | 58 | live-count | Matches PM/manuscript convention (negative = pre-op, positive = post-op) | Pre-surgery NLP findings → all negative; sign convention awkward for PMH |
| C | `days = (date - last_contact_date)` | 58 | live-count | LKA anchor matches survival canonicals | Censoring confusion: anchor moves over time |

**Hybrid options** to surface as well:
- **A+B per sub-type**: PMH cols use `event_start` (Option A); intra-care cols use `first_surgery` (Option B). Decision: which sub-types fall in each bucket?
- **B with sign-flipped convention**: PMH days are reported as positive integers since they all precede surgery (i.e., `days_pre_surg` instead of `days_from_surg`).

Agent provides recommendation with rationale; Logan ratifies one option.

## §4 Sub-type categorization (must be in design doc)

Group the 58 cols into sub-types and propose per-sub-type anchor:

| Sub-type | Example col | Proposed anchor | Rationale |
|---|---|---|---|
| PMHX first-date NLP | `pmhx_nlp_diabetes_first_date` | event_start (Option A) or `days_pre_surg` (Option B-flipped) | These are pre-op chronic conditions; anchor on event makes more sense |
| PMHX n-mentions | `pmhx_nlp_diabetes_n_mentions` | n/a (count, not date) | Pure count, no anchor decision needed |
| PMHX first-days | `pmhx_nlp_diabetes_first_days_from_surg` | first_surgery_date (Option B) | Implied by name |
| Family hx | `pmhx_nlp_family_hx_thyroid_n_mentions` | n/a (count) | |
| Extraction method | `pmhx_nlp_extraction_method` | n/a (categorical) | |

Agent profiles all 58 cols and bins them. Some cols don't actually need an anchor decision (counts, categorical) — those should be reclassified as `na` or have the CF dropped.

## §5 Required CFs

- `CF-mig136-DAYS-SEMANTIC` → STAYS OPEN until Logan ratifies; this lane provides the decision package
- `CF-mig175-DAYS-ANCHOR-PROPOSAL-RECOMMEND-OPTION-<X>` (informational; agent's recommendation)
- `CF-mig175-NA-RECLASS-CANDIDATE-COLS` (informational; cols in the 58 that don't need an anchor decision and could be reclassified or have CF dropped)

## §6 Files + Git workflow

- `qc_framework_v1/reports/mig_175_mig136_days_semantic_adjudication_20260429.md` — full profile + 3-option package + per-sub-type proposal + recommendation
- `qc_framework_v1/migrations/175_days_semantic_probes_20260429.sql` — commented probe SQL
- Commit: `qc: mig_175 mig_136 days-semantic adjudication (read-only profile + decision package)`
- Push.

## §7 Out of scope

- Do NOT apply any UPDATE.
- Do NOT modify cols. mig_175b (later, after Logan ratifies) will apply.
- Do NOT touch survival_followup days cols (those have their own anchor — `surv_*` cols).
- Do NOT propose changes to `first_surgery_date` or `last_contact_date` themselves.

## §8 Apply governance

Read-only lane. Agent ships profile + decision package. Logan ratifies. mig_175b applies later.

Per AGENTS governance: agent ships profile only. **No `query_rw` from agent.**

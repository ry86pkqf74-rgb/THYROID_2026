# ETE event resolved mig_121 close-out — 2026-04-29

**MotherDuck:** `thyroid_canonical_publication_v1_0` — Protocol v2 column registry sign-off executed.

## Scope

| Object | Rows / grain | Columns closed |
|---|---|---|
| `main.canonical_ete_event_resolved_v1` | 6,689 event rows, 4,137 patients | 57 verified + 5 na = 62 |
| `main.canonical_ete_inline_adjudication_v1` | 3,021 rows | 9 verified + 3 na = 12 |

**Migration:** `qc_framework_v1/migrations/121_ete_event_resolved_inline_family_signoff.sql` (paired Option A).

## Why this verification is notable

First **full Tier‑2 enrichment** sign-off spanning multiple upstream verified families (`canonical_path_malignant_events_v1` mig_89 backbone, mig_61 ete_manuscript lineage, mig_61c inline adjudication, mig_62 recurrence_resolved linkage, survival follow-up layering) in one coherent registry pass. `ete_manuscript_analytic_v6`/`v7` views currently fail to compile in-catalog (missing fingerprint helper table); probes used **materialized** `canonical_ete_event_resolved_v1` + base tables instead.

## Probes summarized (MotherDuck, 2026-04-29)

- Cardinality aligns `canonical_path_malignant_events_v1`; distinct 5-tuple sets identical (EXCEPT both ways zero).
- Raw specimen join subset (n=5,261): `extrathyroidal_extension`/`path_event_ete_raw`, `size_greatest_dimension_cm`, `reported_t_stage_ajcc8`/`t_stage_ajcc8` — zero drift pairwise.
- `canonical_recurrence_resolved_v1` join — zero drift recurrence flags/date summary columns on resolved table.
- Survival vs `canonical_survival_followup_v1` — bounded drift flagged with CF (TIMESTAMP/date bridge + naming); column-level closure uses pending/bounded methodology.
- `t_stage_discordance_flag` prevalence ≈3% (below 30% escalation threshold).

## Carry-forwards filed in registry notes

`CF-mig121-ETE-EVENT-RESOLVED-RECURRENCE-PENDING`, `CF-mig121-ETE-EVENT-RESOLVED-SURVIVAL-PENDING`, `CF-mig121-ETE-EVENT-LAST-ALIVE-RETYPE` (→ `CF-100-DATE-RETYPE` family).

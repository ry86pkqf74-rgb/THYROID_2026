# Phase 6 — deterministic QC assertions

Each `.sql` file in this directory writes offending rows to `pub_eval.qc_assertions_v1`. No LLM required. Designed to expose silent data bugs that would otherwise reach manuscripts.

Run all of them:

```bash
bash scripts/phase6.sh   # iterates every .sql here
```

Run one:

```bash
bq query --use_legacy_sql=false < qc_framework_v1/assertions/qc_t_stage_discordance.sql
```

## What each assertion checks

| File | Catches | Expected count |
|---|---|---|
| `qc_t_stage_discordance.sql` | Reported vs derived AJCC8 T-stage disagreement | ~207 known |
| `qc_n0_with_positive_nodes.sql` | N0 stage but positive LN documented | unknown |
| `qc_m0_with_distant_mets.sql` | M0 stage but distant mets in CT/MRI/NM | unknown |
| `qc_recurrence_before_surgery.sql` | recurrence_date before surgery date — impossible | unknown |
| `qc_lab_sentinels.sql` | Tg/Ca/PTH out-of-physiological-range values | unknown |
| `qc_focality_contradictions.sql` | unifocal flag + tumor count >1 | unknown |
| `qc_date_monotonicity.sql` | FNA/surgery/follow-up ordering violations | unknown |
| `qc_unparseable_tumor_sizes.sql` | Non-numeric tumor_size_cm strings | 49 known |

Output schema for `pub_eval.qc_assertions_v1`:

```
assertion_id    STRING  -- e.g. 'qc_t_stage_discordance'
research_id     STRING
event_date      DATE
detail          STRING  -- human-readable explanation of the violation
detected_at     TIMESTAMP
```

Each file deletes prior rows for its `assertion_id` before re-inserting, so the table always reflects current state.

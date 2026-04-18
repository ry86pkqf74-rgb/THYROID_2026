# Phase 1 — Hypoparathyroidism adjudication outcomes

_Generated 2026-04-18T02:59:10.211367+00:00_  
_Strict bar: (B) requires PTH<15 pg/mL > day 180 AND active replacement med > day 180 AND no resolution evidence — any one missing => (C). PROMPT 18 holds (rids 7487, 9765) default to (C) regardless._

| rid | action | basis (one-line) |
|---:|:---|:---|
| 6447 | **C** `indeterminate_requires_chart_review` | Insufficient or mixed structured evidence to flip CPM under strict criteria; deferring to chart review. |
| 7487 | **C** `indeterminate_requires_chart_review` | Insufficient or mixed structured evidence to flip CPM under strict criteria; deferring to chart review. |
| 9765 | **C** `indeterminate_requires_chart_review` | Insufficient or mixed structured evidence to flip CPM under strict criteria; deferring to chart review. |
| 10743 | **C** `indeterminate_requires_chart_review` | Insufficient or mixed structured evidence to flip CPM under strict criteria; deferring to chart review. |

## Per-patient evidence summary (structured)

### rid 6447 — action **C**

```json
{
  "phenotype_rows": 1,
  "latest_phenotype_status": "confirmed_transient",
  "latest_phenotype_date_days_postop": 2232,
  "pth_values_post_180d": [],
  "calcium_values_post_180d": [],
  "active_replacement_med_at_day_180": true,
  "decision_basis": "Insufficient or mixed structured evidence to flip CPM under strict criteria; deferring to chart review."
}
```

### rid 7487 — action **C**

```json
{
  "phenotype_rows": 1,
  "latest_phenotype_status": "confirmed_transient",
  "latest_phenotype_date_days_postop": 2051,
  "pth_values_post_180d": [],
  "calcium_values_post_180d": [],
  "active_replacement_med_at_day_180": false,
  "decision_basis": "Insufficient or mixed structured evidence to flip CPM under strict criteria; deferring to chart review."
}
```

### rid 9765 — action **C**

```json
{
  "phenotype_rows": 1,
  "latest_phenotype_status": "confirmed_transient",
  "latest_phenotype_date_days_postop": 713,
  "pth_values_post_180d": [
    {
      "days_postop": 713,
      "value": 79.0,
      "source": "CPM lab_pth_most_recent"
    }
  ],
  "calcium_values_post_180d": [],
  "active_replacement_med_at_day_180": "unknown",
  "decision_basis": "Insufficient or mixed structured evidence to flip CPM under strict criteria; deferring to chart review."
}
```

### rid 10743 — action **C**

```json
{
  "phenotype_rows": 1,
  "latest_phenotype_status": "confirmed_transient",
  "latest_phenotype_date_days_postop": 27,
  "pth_values_post_180d": [],
  "calcium_values_post_180d": [],
  "active_replacement_med_at_day_180": "unknown",
  "decision_basis": "Insufficient or mixed structured evidence to flip CPM under strict criteria; deferring to chart review."
}
```


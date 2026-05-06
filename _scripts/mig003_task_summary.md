# MIG-003 task summary (2026-05-06) — **complete**

- **Decision:** Repopulate (Step 2B), not deprecate.
- **n_distinct_paretic (AI_CLASSIFY clinical label):** **1** (`research_id` **8616**).
- **BigQuery:** `comp_vc_paresis_confirmed=TRUE`, `comp_vc_paresis_evidence_tier=2` for `8616`; cohort count **1**.
- **Dry-run:** ~46.1 MB bytes processed (UPDATE bound).
- **DFL:** DFL-20260506-082.
- **Migration log:** `mig_081_mig003_vc_paresis_ai_classify_bq_20260506` (prior blocker: `mig_080`).
- **SQL:** `sql/mig_081_mig003_vc_paresis_bq_update_20260506.sql`.
- **Docs:** `memory/feedback_complications_transient_vs_permanent.md`, H2 v2 Limitations item 8.
- **Linear THY-15:** In Review + `auto-close:pending` after migration closeout.

**Status:** MIG-003 done. **mig_081.** Key result: **`n_distinct_paretic = 1`** (8616); **not** deprecated.

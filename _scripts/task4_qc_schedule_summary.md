# Task 4 — QC schedule & BQ-native runner (summary)

**Date:** 2026-05-06  
**Project:** `thyroid-canonical-pub-2026` · **Dataset:** `pub_signoff`

---

## 1. Schema verification (Step 1)

`bq show` on live tables showed **different columns** than the original Cowork assumptions in `qc_scheduled_query.sql`.

| Table | Expected (Cowork draft) | **Live (`mig_007` + extensions)** |
|-------|-------------------------|-----------------------------------|
| `qc_assertions_v1` | `rule_name`, `rule_sql`, optional `active` | `assertion_id`, **`check_sql`**, `severity`, `active`, `category`, … |
| `qc_violations_v1` | `run_timestamp`, `rule_name`, `severity`, `raw_sql`, … | **`ran_at`**, **`assertion_id`**, **`passed`**, no `severity`/`raw_sql` on the fact table |

**Actions taken**

- **`qc_scheduled_query.sql`** (repo root): rewrote `run_qc_assertions` to mirror `_scripts/qc_runner.py`:
  - `violation_count = COUNT(*)` over the **`check_sql` result set** (0 rows = pass).
  - Inserts **`run_id`, `assertion_id`, `ran_at`, `passed`, `violation_count`,** null bytes/duration/sample, optional `error_message`.
  - Dynamic SQL built with `CONCAT` + `FORMAT('%T', …)` so `check_sql` is not parsed as `FORMAT` placeholders.
- **`qc_daily_summary_v1_view.sql`**: joins **`qc_assertions_v1`** for severity; uses **`ran_at`**; `latest_run` via `QUALIFY ROW_NUMBER`; **`warn_severity_failures`** counts **`severity IN ('warning','warn')`** (BQML rules use `warn` in live data).
- **No `ALTER TABLE` on `qc_violations_v1`** — all required insert columns already exist.

---

## 2. Migration files (Step 2)

Canonical copies (with headers) live on the migration machine:

| File | `migration_id` |
|------|----------------|
| `.../bq_migrations/mig_075_task4_qc_scheduled_procedure.sql` | **mig_075_task4_qc_scheduled_procedure** |
| `.../bq_migrations/mig_076_task4_qc_daily_summary_views.sql` | **mig_076_task4_qc_daily_summary_views** |

**Note:** Prompt suggested `mig_010_qc_*.sql`; **`mig_010` is already used** (`applied/mig_010_surgery_date_assertion_fix.sql`). Mirrors were added as `mig_010_qc_scheduled_query_TASK4_mirror.sql` and `mig_010_qc_daily_summary_view_TASK4_mirror.sql` (same body as mig_075/076).

Both SQL files were applied successfully with:

`bq query --use_legacy_sql=false < <file>`

---

## 3. Smoke test (Step 3)

```text
CALL `thyroid-canonical-pub-2026.pub_signoff.run_qc_assertions`()
```

| Metric | Value |
|--------|--------|
| Latest **`run_id`** | `38933cd9-493a-41aa-bfe1-0314f1dc0044` |
| **`run_timestamp` (summary view)** | `2026-05-06 06:56:19` UTC |
| **`rules_run`** | **17** (active assertions; not 15 — rules were added after `mig_007` seed) |
| **`rules_with_violations`** | **2** (BQML baseline AUC checks) |
| **`rules_errored`** | 0 |

**`qc_daily_summary_v1`:** queried with `--format=prettyjson` after view redeploy — `warn_severity_failures` correctly counts `warn`-tier rules.

---

## 4. Scheduled transfer (Steps 4–5) — **blocked**

Creating the DTS scheduled query failed:

```text
DTS service agent needs iam.serviceAccounts.getAccessToken permission …
Running the following command may resolve this error:
gcloud iam service-accounts add-iam-policy-binding \
  thyroid-pub-loader@thyroid-canonical-pub-2026.iam.gserviceaccount.com \
  --member='serviceAccount:service-915373663815@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com' \
  --role='roles/iam.serviceAccountTokenCreator'
```

**Transfer config resource name:** *not created* until the binding (or Console equivalent) is applied by a project owner.

**After IAM fix**, rerun (adjust `--location` if your dataset region differs):

```bash
cd "/Users/loganglosser/Desktop/Thyroid Motherduck To GC migration"
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/_creds/thyroid-pub-loader-key.json"
SA_EMAIL=$(python3 -c "import json; print(json.load(open('_creds/thyroid-pub-loader-key.json'))['client_email'])")

bq mk --transfer_config \
  --project_id=thyroid-canonical-pub-2026 \
  --location=us-central1 \
  --target_dataset=pub_signoff \
  --display_name='qc_daily_runner' \
  --data_source=scheduled_query \
  --schedule='every 24 hours' \
  --params='{"query":"CALL `thyroid-canonical-pub-2026.pub_signoff.run_qc_assertions`();"}' \
  --service_account_name="$SA_EMAIL"
```

**Violation count caveat:**  
`SELECT COUNT(*) FROM qc_violations_v1 WHERE DATE(ran_at) = CURRENT_DATE()` counts **all rows from all runs that day**. For **one scheduled batch**, use `WHERE run_id = '<that run_uuid>'` (expect **17** rows matching active rules).

---

## 5. Governance (Step 6)

### `bq_migration_log_v1`

Two rows inserted (semantically separate artifacts):

| migration_id |
|--------------|
| **mig_075_task4_qc_scheduled_procedure** |
| **mig_076_task4_qc_daily_summary_views** |

### Airtable — Data Feedback Log (`tblsiYKJtKcktkzze`)

| feedback_id | Airtable record id | Notes |
|-------------|--------------------|-------|
| **DFL-20260506-T4A** | `recQ0C5bgKz6mx5wA` | Procedure deploy |
| **DFL-20260506-T4B** | `recSyMWPZwBOxRViq` | Views deploy |

The table schema has **no `migration` choice** under `change_type`; used **`other`** (procedure) and **`view_repair`** (views). Both use **`target_type` = `BQ infrastructure`**.

---

## 6. Source of truth in git (`THYROID_2026`)

Updated files:

- `qc_scheduled_query.sql`
- `qc_daily_summary_v1_view.sql`
- `_scripts/task4_qc_schedule_summary.md` (this file)




Shell
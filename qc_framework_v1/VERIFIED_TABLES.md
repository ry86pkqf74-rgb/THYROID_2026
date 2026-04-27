# Verified Tables — Append-Only Log

This file is appended to whenever a table reaches `table_status = 'verified'` in
`main.canonical_table_signoff_registry_v1`. Each entry records the date, the
sign-off migration, and a one-line summary.

Order: chronological (newest at the bottom).

---

## Format

```
### YYYY-MM-DD — schema.table_name

- Columns: N_total (N_verified verified / N_na auto-skipped)
- Sign-off migration: qc_framework_v1/migrations/NN_table_signoff_<table>.sql
- Notes: ...
```

---

(no tables verified yet — verification starts with the pilot table
`main.canonical_fna_events_v1`)

# Read-scaling token configured (live check)

UTC: `2026-04-08T06:06:25.035560+00:00`

- `read_scaling_token_mode()`: `secrets.toml:MD_READ_SCALING_TOKEN`
- Read-scaling credential length (opaque): **445**

## `connect_read_scaling()`

- `current_database()`: `Thyroid 2026`
- `information_schema.tables` (main): `146`
- `master_patient_rollup_verified_v1` rows: `2702`
- `master_fact_long_verified_v1` rows: `20188`

## Script 136

- `reader --md-env prod`: **OK** (`REFRESH DATABASE "Thyroid 2026"`) in this session.

> **Security:** token was pasted in chat; rotate in MotherDuck and update `.streamlit/secrets.toml` if this thread is not private.

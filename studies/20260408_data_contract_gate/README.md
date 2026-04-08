# Study: Data contract gate (2026-04-08)

Artifacts from `scripts/145_data_contract_gate.py` should be written here per run, e.g.:

```bash
.venv/bin/python scripts/145_data_contract_gate.py \
  --contract-name longitudinal_lab_canonical_v1 \
  --input-path exports/manuscript_freeze_v1/data/longitudinal_lab_canonical_v1.parquet \
  --output-dir studies/20260408_data_contract_gate/run_<timestamp> \
  --strict
```

See `docs/data_contract_gate.md` for how this layer complements scripts **112**, **29**, and **119**.

Default path is **offline** (no MotherDuck token required). Optional `--md` reads a table from MotherDuck using a token from gitignored `motherduck.local.toml` (copy from `motherduck.local.toml.example`).

Do not commit PHI or full clinical note text; inputs should be structured tables only.

"""Insert the Phase B.6 closure signoff row into canonical_table_signoff_registry_v1."""
from __future__ import annotations
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
client = bigquery.Client(project=PROJECT)

# Verified schema (from INFORMATION_SCHEMA earlier):
# schema_name STRING, table_name STRING, n_columns_total INT64, n_verified INT64,
# n_not_started INT64, n_failed INT64, n_na INT64, table_status STRING,
# signed_off_ts TIMESTAMP, signoff_migration STRING, priority_tier STRING,
# notes STRING, registered_ts TIMESTAMP

# Get the column count of canonical_us_nodule_tirads_multisystem_v1
n_cols = list(client.query(
    """SELECT COUNT(*) AS n FROM `pub_canonical.INFORMATION_SCHEMA.COLUMNS`
       WHERE table_name = 'canonical_us_nodule_tirads_multisystem_v1'"""
).result())[0].n
print(f"canonical_us_nodule_tirads_multisystem_v1 has {n_cols} columns")

# Idempotency: don't double-insert if a Phase B closure row already exists.
existing = list(client.query(
    """SELECT COUNT(*) AS n FROM `pub_canonical.canonical_table_signoff_registry_v1`
       WHERE table_name = 'canonical_us_nodule_tirads_multisystem_v1'
         AND signoff_migration = 'phase_b_closure_20260507'"""
).result())[0].n
if existing:
    print(f"Phase B closure row already exists ({existing} row(s)); skipping insert.")
else:
    insert_sql = f"""
    INSERT INTO `pub_canonical.canonical_table_signoff_registry_v1`
    (schema_name, table_name, n_columns_total, n_verified, n_not_started,
     n_failed, n_na, table_status, signed_off_ts, signoff_migration,
     priority_tier, notes, registered_ts)
    VALUES (
      'pub_canonical',
      'canonical_us_nodule_tirads_multisystem_v1',
      {n_cols},
      {n_cols},
      0,
      0,
      0,
      'Active',
      CURRENT_TIMESTAMP(),
      'phase_b_closure_20260507',
      'P1',
      CONCAT(
        'Phase B closure 2026-05-07 (Cowork+Cursor session): 5 deterministic TIRADS scorers ',
        '(ACR 2017 dual-output, Kwak 2011, K-TIRADS 2021, C-TIRADS 2020, SRU 2005) plus Park / T-US 2009 ',
        'with 3 coefficient sets (park_2009_original = published, park_cosmos_validation = alias, ',
        'park_cohort_refit = nodule-level v2). 37,579 rows; CLUSTER BY research_id. ',
        'Park 2009 \u03b2s sourced from secondary literature (paper paywalled); cohort refit nodule-level via ',
        'pub_workspace.us_nodule_path_outcome_v1 (laterality-aware, Phase B.6 v2). ',
        'AUC vs path (n=14,250): Park 2009 = 0.5365 (Korean general-pop coefs do not generalize to American surgical cohort), ',
        'cohort_refit_v2 = 0.7006 (MARGINAL, confidence=low), cosmos = 0.5365 (alias). ',
        '3-way concordance on suspicious (P4\u222aP5): 2009 vs cohort = 0.948, 2009 vs cosmos = 1.000 (alias). ',
        'See exports/phase_b_deterministic_scorers_20260507/README.md and DFL row rec38HYN2xSFzf9AB.'
      ),
      CURRENT_TIMESTAMP()
    )
    """
    client.query(insert_sql).result()
    print("Inserted signoff registry row.")

# Read back
print("\n=== Latest signoff row ===")
for r in client.query(
    """SELECT * FROM `pub_canonical.canonical_table_signoff_registry_v1`
       WHERE table_name = 'canonical_us_nodule_tirads_multisystem_v1'
       ORDER BY registered_ts DESC LIMIT 1"""
).result():
    for k, v in dict(r).items():
        print(f"  {k:25s} {v}")

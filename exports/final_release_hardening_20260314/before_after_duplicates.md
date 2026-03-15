
MATERIALIZATION_MAP Duplicate Audit  —  2026-03-14
=====================================================

BEFORE (script-85 false-positive run, 2026-03-14 20:22 UTC)
─────────────────────────────────────────────────────────────
Script 85's check_map_duplicates() used re.findall(r'"(md_[^"]+)"', entire_file).
This scanned ALL occurrences of quoted md_* strings in the WHOLE of script 26,
including SQL substitution blocks and print statements outside the MAP definition.

  Falsely flagged as duplicates:
  ┌─────────────────────────────────────┬────────────────────────────────────┐
  │  Name                               │  Why it appeared twice              │
  ├─────────────────────────────────────┼────────────────────────────────────┤
  │ md_pathology_recon_review_v2        │ MAP line 66 + SQL .replace() L598  │
  │ md_molecular_linkage_review_v2      │ MAP line 67 + SQL .replace() L600  │
  │ md_rai_adjudication_review_v2       │ MAP line 68 + SQL .replace() L602  │
  │ md_imaging_path_concordance_v2      │ MAP line 69 + SQL .replace() L604  │
  │ md_op_path_recon_review_v2          │ MAP line 70 + SQL .replace() L606  │
  │ md_lineage_audit_v1                 │ MAP line 186 + SURVIVE SQL L629   │
  └─────────────────────────────────────┴────────────────────────────────────┘
  All 6 are LEGITIMATE — they're referenced in the cross-DB substitution
  block that rewrites source table names when materializing to a second DB.

AFTER (script-94 fix applied)
───────────────────────────────
  Resolution: MAP-scoped parser that exits at the closing `]`.
  True duplicate count: 0
  True MATERIALIZATION_MAP entry count: 220

  Classification of all 6 cases:
  ┌─────────────────────────────────────┬────────────────────────────────────┐
  │  Name                               │  Classification                    │
  ├─────────────────────────────────────┼────────────────────────────────────┤
  │ md_pathology_recon_review_v2        │ LEGITIMATE — SQL template variable │
  │ md_molecular_linkage_review_v2      │ LEGITIMATE — SQL template variable │
  │ md_rai_adjudication_review_v2       │ LEGITIMATE — SQL template variable │
  │ md_imaging_path_concordance_v2      │ LEGITIMATE — SQL template variable │
  │ md_op_path_recon_review_v2          │ LEGITIMATE — SQL template variable │
  │ md_lineage_audit_v1                 │ LEGITIMATE — SQL template variable │
  └─────────────────────────────────────┴────────────────────────────────────┘

Action taken:
  • Fixed check_map_duplicates() in 85_materialization_performance_audit.py
  • Created 94_map_dedup_validator.py as the authoritative CI check
  • CI job added: runs script 94 in lint-and-syntax job (no MotherDuck needed)


# Existing linkage-related repo assets

Reuse these before inventing new join logic:

| path | role |
|------|------|
| `scripts/49_enhanced_linkage_v3.py` | v3 scored linkage tables + linkage_ambiguity_review_v1 |
| `scripts/129_imaging_fna_linkage_mm_v1.py` | imaging_fna_linkage_mm_v1 + QA/review |
| `utils/imaging_fna_linkage_mm_v1.py` | specimen key normalization |
| `utils/canonical_nodule_linkage.py` | canonical nodule chain SQL (this study) |
| `scripts/sql/139_specimen_identity_layer_ddl.sql` | specimen spine uses v3 linkages |
| `scripts/sql/140_specimen_genomics_binding_ddl.sql` | genomics binding over v3 |
| `scripts/117_md_contract_views.py` | MotherDuck contract / episode surfaces |

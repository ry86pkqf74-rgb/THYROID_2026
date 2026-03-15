# Episode Linkage Repair Report — 20260315

## Overview

This report documents the episode-aware linkage repair pass executed on 20260315.
The repair converted patient-level-only linkage into episode-aware linkage for
multi-surgery patients across 5 domains.

## Multi-Surgery Patient Population

| Surgeries | Patients |
|-----------|----------|
| 1 | 10108 |
| 2 | 721 |
| 3 | 33 |
| 4 | 7 |
| 5 | 1 |
| 6 | 1 |

## Repair Summary

| Domain | Total Linked | Multi-Surg | Exact | Anchored | Ambiguous | No Date |
|--------|-------------|------------|-------|----------|-----------|---------|
| chains | 5088 | 543 | 220 | 2613 | 624 | 0 |
| labs | 12057 | 2773 | 121 | 10737 | 12 | 1187 |
| notes | 8323 | 891 | 3596 | 206 | 11 | 4510 |
| pathology_rai | 2465 | 1628 | 619 | 231 | 1692 | 585 |

## Ambiguity Registry

Total ambiguous items requiring manual review: **2339**

Ambiguous items are exported to `exports/episode_linkage_manual_review_packets/`.

## Domain-Specific Notes

### Notes (Phase A)
- Operative notes linked by same-day or +1 day match to surgery date
- H&P notes linked by -1 to +0 day match
- Discharge summaries linked by 0 to +3 day match
- Multi-surgery patients use nearest-surgery disambiguation

### Labs (Phase B)
- Pre-op window: -30 to -1 days before surgery
- Post-op window: 0 to +365 days (or midpoint to next surgery for multi-surg)
- Overlapping windows flagged for manual review

### Imaging/FNA/Molecular Chains (Phase C)
- FNA→Surgery linkage uses date + laterality matching
- Molecular→Surgery inherits FNA episode where available
- Closer-to-other-surgery flag identifies ambiguous chain linkages

### Pathology & RAI (Phases D/E)
- Pathology verified against surgery date (same-day accession)
- RAI linked to nearest cancer surgery within 14-365 day protocol window
- Non-cancer surgery RAI links flagged as weak

## Source-Limited Domains

- **Imaging→FNA linkage**: imaging_nodule_long_v2 size data not populated
- **Nuclear medicine notes**: absent from clinical_notes_long corpus
- **RAI dose**: 59% missing, capped by structured data availability
- **Operative NLP fields**: V2 extractor outputs not fully materialized

## MotherDuck Objects Created

| Table | Purpose |
|-------|---------|
| episode_note_linkage_repair_v1 | Note-to-surgery episode linkage |
| episode_lab_linkage_repair_v1 | Lab-to-surgery episode windowing |
| episode_chain_linkage_repair_v1 | FNA/molecular chain repair |
| episode_pathrai_linkage_repair_v1 | Pathology/RAI anchoring |
| episode_ambiguity_registry_v1 | All ambiguous linkages |
| episode_linkage_repair_summary_v1 | Per-domain summary metrics |
| md_episode_*_v1 | MotherDuck mirrors of above |

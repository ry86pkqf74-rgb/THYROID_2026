# Clinician Review Packet — 2026-04-14

## Overview

This packet summarizes items requiring clinician review. All deterministic algorithmic fixes have been exhausted; the remaining gaps are source-limited or require human judgment.

## 1. FNA Bethesda — 23 Unscorable Episodes

**Status:** All 23 episodes have been triaged. No numeric Bethesda can be assigned algorithmically.

| Category | Count | Description |
|----------|-------|-------------|
| no_episode_or_cytology_bethesda | 22 | No Bethesda score in fna_episode_master_v2, fna_cytology, or fna_history |
| pathology_present_bethesda_unparsed | 1 | pathology_diagnosis field contains physician name, not pathology text |

**Action required:** Manual chart review for each of the 23 research_ids listed in `human_review_packet.csv` to determine if Bethesda was documented elsewhere in the clinical record.

## 2. Imaging_12 TI-RADS — 8,794 Nodules Without Scores

**Status:** Source-limited. The Imaging_12_1_25.xlsx workbook does not contain TI-RADS scores or ACR feature data (`n_criteria_available = 0` for all rows).

**Cross-corpus overlap (informational):**
- 304 Imaging_12 canonical rows have a COMPLETE-corpus match within ±30d (COMPLETE row already has TI-RADS)
- 3,802 have a scored-corpus match within ±30d

**Action required:** No clinical action needed. If TI-RADS is desired for these exams, a re-score from original imaging reports is required.

## 3. US Lymph Node Structured Detail

**Status:** Source-limited. The `ultrasound_reports.lymph_node_assessment` field contains narrative text only. No structured per-level, per-laterality, or per-size LN data exists in the source.

**Narrative breakdown:**
- 6,453 exams: negative/normal LN
- 340 exams: other narrative (some with level mentions in text)

**Action required:** If structured LN detail is needed, a governed NLP extraction pipeline or radiologist re-review of source reports would be required.

## 4. Serial Imaging US

**Status:** Empty placeholder table. No data source provides serial US follow-up data in structured format.

**Action required:** Institutional data feed integration needed.

## 5. Bethesda Conflicts (from prior remediation pack)

**Status:** 1,899 cross-source Bethesda conflicts were documented in the 20260413 remediation pack. These require an institutional gold-source policy (which source has priority when sources disagree). Not auto-resolved.

**Action required:** Institutional gold-source policy decision, then apply via governed hierarchy.

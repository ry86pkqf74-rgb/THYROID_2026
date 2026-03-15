# Episode Linkage v2 Hardening Report — 20260315

## Overview

Phase 2 hardening of episode-aware multi-surgery linkage.
Target environment: **prod**

## Baseline (pre-v2)

| Metric | Value |
|--------|-------|
| 1-surgery patients | 10109 (10109 episodes) |
| 2-surgery patients | 721 (1444 episodes) |
| 3-surgery patients | 32 (96 episodes) |
| 4-surgery patients | 7 (28 episodes) |
| 5-surgery patients | 1 (5 episodes) |
| 6-surgery patients | 1 (6 episodes) |

## Phase A: Surgery-Pathology Re-scoring

- Total rows processed: 9354
- Multi-surgery rows: 1241
- EP-ID changed: 3
- Ambiguity flagged: 1237
- High-confidence applied: 0

### Multi-surgery tier distribution
| Tier | Count |
|------|-------|
| exact_day | 1237 |
| weak_temporal | 2 |
| plausible_extended | 2 |

## Phase B: Preop-Surgery Re-anchoring

- Total rows: 3549
- EP-ID changed: 0

## Phase C: Pathology-RAI Re-anchoring

- Total rows: 22
- EP-ID changed: 0

## Phase D: Downstream Sync

- OED corrections: 0
- EARD corrections: 0
- OED no-match: 647

## Phase E: Validation

| Metric | Value | Status |
|--------|-------|--------|
| ambiguity_rate_multi | 99.7%% | WARN |
| multi_surgery_patients | 10871 | INFO |
| oed_epid_correctness | 100.0%% | PASS |
| sp_high_conf_rate_multi | 99.7%% | PASS |
| sp_linkage_ep_gt1_coverage | 12.6%% | WARN |

Review queue: 1324 items

## Phase F: Non-Regression

All pass: **True**
- episode_analysis_resolved_v1_dedup: total=8110, non-ep1=0 [PASS]
- operative_episode_detail_v2: total=8111, non-ep1=0 [PASS]
- preop_surgery_linkage_v3: total=3263, non-ep1=0 [PASS]
- surgery_pathology_linkage_v3: total=8113, non-ep1=0 [PASS]

## Phase G: Before/After Delta

| Domain | Total | Multi | Changed | Ambiguous | % Changed |
|--------|-------|-------|---------|-----------|-----------|
| pathology_rai | 22 | 4 | 0 | 0 | 0.0% |
| preop_surgery | 3549 | 286 | 0 | 87 | 0.0% |
| surgery_pathology | 9354 | 1241 | 3 | 1237 | 0.2% |
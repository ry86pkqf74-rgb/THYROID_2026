# Intrinsic Evaluation Report: rln_injury

**Date:** 2026-03-12 17:30
**Entity family:** rln_injury, vocal_cord_paralysis, vocal_cord_paresis
**Total mentions evaluated:** 200

## Estimated Precision (Current Tier 3)

| Metric | Value |
|--------|-------|
| True injuries (heuristic) | 7 |
| False positives (heuristic) | 193 |
| **Estimated precision** | **3.5%** |

## Classification Breakdown

| Classification | Count | % |
|---------------|-------|---|
| rln_injury_risk_mentioned | 184 | 92.0% |
| rln_injury_confirmed | 7 | 3.5% |
| rln_injury_suspected | 6 | 3.0% |
| rln_injury_historical | 2 | 1.0% |
| rln_injury_identified_preserved | 1 | 0.5% |

## Rule Triggers

| Rule | Count | % |
|------|-------|---|
| risk_discussion | 182 | 91.0% |
| true_injury_language | 7 | 3.5% |
| same_day_unclassified | 6 | 3.0% |
| historical_reference | 2 | 1.0% |
| same_day_hp_generic_rln | 2 | 1.0% |
| preservation_language | 1 | 0.5% |

## Tier Recommendations

| Tier | Label | Count | % |
|------|-------|-------|---|
| 0 | Exclude | 193 | 96.5% |
| 1 | Confirmed | 3 | 1.5% |
| 2 | Probable | 4 | 2.0% |

## Evidence Strength Distribution

| Strength | Count |
|----------|-------|
| none | 185 |
| weak | 8 |
| medium | 4 |
| strong | 3 |

## Suggested New Entity Values

| Current | Suggested Replacement |
|---------|----------------------|
| rln_injury (in risk discussion) | rln_injury_risk_mentioned |
| rln_injury (nerve preserved) | rln_injury_identified_preserved |
| rln_injury (historical) | rln_injury_historical |
| rln_injury (confirmed post-op) | rln_injury_confirmed |
| rln_injury (suspected, weak) | rln_injury_suspected |
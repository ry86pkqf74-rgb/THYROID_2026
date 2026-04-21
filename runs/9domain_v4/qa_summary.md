# 9domain_v4 QA Acceptance Gates

Model: Qwen2.5-32B-Instruct-AWQ via vLLM on H200 NVL

Gate thresholds: parse_fail <1%, api_error <1%, has_entities >=25% (domain-dependent)

| domain | expected | rows_extracted | coverage_pct | parse_ok | parse_ok_pct | parse_fail | parse_fail_pct | api_error | api_error_pct | has_entities | has_entities_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| frozen_section_detail | 32417 | 32408 | 99.972 | 32408 | 100.0 | 0 | 0.0 | 0 | 0.0 | 4652 | 14.354 |
| airway_invasion | 48262 | 48262 | 100.0 | 48262 | 100.0 | 0 | 0.0 | 0 | 0.0 | 5625 | 11.655 |
| vascular_invasion | 39210 | 39210 | 100.0 | 39210 | 100.0 | 0 | 0.0 | 0 | 0.0 | 7545 | 19.243 |
| parathyroid_detail | 17368 | 17368 | 100.0 | 17367 | 99.994 | 1 | 0.006 | 0 | 0.0 | 4523 | 26.042 |

## Gate verdicts
- **frozen_section_detail**: parse_fail<1%=PASS / api_error<1%=PASS / has_entities>=25%=FAIL
- **airway_invasion**: parse_fail<1%=PASS / api_error<1%=PASS / has_entities>=25%=FAIL
- **vascular_invasion**: parse_fail<1%=PASS / api_error<1%=PASS / has_entities>=25%=FAIL
- **parathyroid_detail**: parse_fail<1%=PASS / api_error<1%=PASS / has_entities>=25%=PASS

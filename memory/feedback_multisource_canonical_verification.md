# Feedback — multi-source canonical verification pattern

Use this pattern for canonicals built from heterogeneous source families.

## Pattern

- Start with `source_table` distribution and treat each source family separately.
- Re-derive deterministic builder sources from the original build SQL/script and compare at row grain with `IS DISTINCT FROM`.
- If an upstream source was dropped after canonicalization, verify against the archived source-of-truth snapshot, not a reconstructed substitute.
- Verify curated synthetic rows as injected: preserve rows, check provenance/invariants (`is_preexisting`, traceable `anchor_source`, valid hash, expected vocab), and do not mass-modify.
- Run cross-source carry-forward checks after per-source equivalence, not before.
- Flip common column registry entries once at table level with a `verification_method` describing all source-family methods.

## Example

`canonical_pmh_events_v1` mig_107:

- Legacy `note_entities_problem_list`: 11,579/11,579 rows matched archived source re-derivation.
- LLM `note_entities_llm_past_medical_hx`: 865/865 rows matched JSON re-derivation.
- Synthetic `mig_98*` / `mig_103`: verified-as-injected by invariants.
- Carry-forwards: 4 cross-source status disagreements; 0 complication→PMH misses.

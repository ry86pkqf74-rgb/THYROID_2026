# mig_183 apply close-out — PM `vessel_count` last not_started col

**Date:** 2026-04-30
**Lane:** mig_183 / pm_vessel_count_last_not_started
**Cursor authored:** `932a1d4`
**Cowork applied:** 2026-04-30 via Path C
**Outcome:** **PM is now fully verified (table_status='verified', 1,591 v / 24 na / 0 not_started / 1,615 total)** — last gate before mig_162 PM finalization.

---

## §1 Executive summary

`vessel_count` was identified as the last PM not_started col (DOUBLE, 46 nonnull / 10,825 null, values 1–6). Lineage trace: created in `scripts/frozen/204_canonical_master_assembly.py` and `scripts/frozen/205_canonical_consolidation.py` as `g.vascular_vessel_count AS vessel_count` — exact alias of `vascular_vessel_count`. Live MD probe confirmed exact equality on all 46 populated rows against `vascular_vessel_count`, `vasc_vessel_count_v13`, and `vi_vessels_max`.

Verification disposition: **verified** with `verification_method = 'derivation_vs_vascular_vessel_count'`.

---

## §2 Path-C apply trace

| Step | Action | Result |
|---|---|---|
| §A | Pre-snapshot 1 registry row to archive | 1 row ✓ |
| §B + §C | Path-C stamp + status flip on `vessel_count` row (combined) | 1 row updated ✓ |
| §E | PM signoff resync (1,590 / 24 / 1 → 1,591 / 24 / 0; table_status `in_progress` → `verified`) | 1 row updated ✓ |
| §F | cpm_reconciliation_provenance_v1 row inserted | 1 row ✓ |

---

## §3 Post-state

| Metric | Pre-mig_183 | Post-mig_183 |
|---|---:|---:|
| PM n_verified | 1,590 | **1,591** |
| PM n_not_started | 1 | **0** |
| PM table_status | `in_progress` | **`verified`** |
| gate1 (verified canonicals) | 171 | **172** |
| gate2/3/4 | 0/0/0 | 0/0/0 |
| gate5 (date retype) | 21 | **46** (delta = 25 PM cols newly counted now that PM is verified — closes via mig_160) |
| Cohort parity | 10,871/10,871 | 10,871/10,871 |

---

## §4 CFs

**Closed:**
- `CF-mig183-PM-VESSEL-COUNT-LAST-NOT-STARTED`

**Side-effect (informational, not new CFs):**
- gate5 jumped 21 → 46 because 25 VARCHAR-with-date-pattern cols on PM are now in scope (PM was previously `in_progress` and excluded). All 25 are addressed by mig_160's structural date retype.

---

## §5 Next prerequisite for mig_162 PM finalization

PM is now table_status='verified' with 0 not_started — the structural prerequisite for `mig_162` (PM finalization + lakehouse coverage report) is met. mig_162 can run as soon as Logan is ready.

---

End of close-out.

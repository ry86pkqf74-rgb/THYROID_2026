# Cowork Verifications Log — 2026-04-29 (chat-2)

End-of-chat artifact capturing all Path-C verifications, investigations, and new findings from the second 2026-04-29 Cowork session. Companion to `COWORK_SESSION_SUMMARY_2026-04-29.md` (chat-1) and `COWORK_HANDOFF_PROMPT_2026-04-29_v6.md`.

This chat re-scoped per Logan to **verification + investigation + documentation only**. Cursor / VSC agents handle execution. Apply queue Steps 1–9 remain pre-flighted but **not applied** in this chat.

---

## §1 Path-C verifications completed

### §1.1 mig_169 PM dtype/units audit (`49f6b61`) — CLEAN

| Claim | Cursor | Live MD |
|---|---:|---:|
| Verified PM cols audited | 1,302 | 1,302 ✓ |
| Total findings | 55 | 55 ✓ |
| Bucket breakdown | 19/11/8/7/7/3 | matched ✓ |

Spot-checked 7 col dtypes (`first_surgery_date` TIMESTAMP, `last_contact_date` TIMESTAMP, `ops_surg_date` VARCHAR, `nsqip_admission_date` VARCHAR, `syn_left_lobe_size_cm` VARCHAR, `nsqip_smoker` VARCHAR, `bethesda_num` DOUBLE). All exact. Zero writes in SQL probe file. **No apply needed (read-only audit).**

### §1.2 mig_163b ANY-RECURRENCE HYBRID SQL (`1476a46`) — CLEAN, ready to apply

Every preflight number exact against live MD:

| metric | Cursor claim | live MD |
|---|---:|---:|
| strict_n / path_proven_n / hybrid_union_n | 514 / 145 / 514 | matched ✓ |
| path_proven_added_by_hybrid | 0 | 0 ✓ |
| 2x2 (both / pm_only_dropped / hybrid_only_added / neither) | 165 / 219 / 349 / 10138 | matched ✓ |
| PM rows / distinct rids | 10871 / 10871 | matched ✓ |
| pre-state TRUE / FALSE / NULL | 384 / 10487 / 0 | matched ✓ |

SQL governance-compliant: pre-snapshots PM slice + registry row to `archive_pub_v1_0`, single-transaction UPDATE PM + UPDATE registry note, post-verify Section C.

### §1.3 mig_170 cross-canonical dtype drift audit (`e31f20c`) — CLEAN

| Claim | Cursor | Live MD |
|---|---|---|
| research_id 3 distinct dtypes / 55 tables | BIGINT:15 / INTEGER:5 / VARCHAR:35 | matched ✓ |
| bethesda_category PM-vs-genetics_v2 | VARCHAR vs INTEGER | matched ✓ |
| first_surgery_date 3 tables | PM TIMESTAMP / recurrence_v1 TIMESTAMP / survival_followup_v1 DATE | matched ✓ |
| vessel_count PM-vs-vascular_invasion | DOUBLE vs INTEGER | matched ✓ |

SQL probe file: 0 INSERT/UPDATE/DELETE/ALTER/CREATE/DROP. **No apply needed.**

### §1.4 mig_171 canonical_us_lymph_node_v2 design + skeleton (`f6d0313`) — CLEAN, design ratification pending

| Claim | Cursor | Live MD |
|---|---:|---:|
| canonical_us_lymph_node_v2 rows / patients | 6,801 / 4,077 | matched ✓ |
| nlp_backfill_pending=TRUE | 6,793 | 6,793 ✓ |
| suspicious_flag=TRUE | 8 | 8 ✓ |
| exam_master rows / patients | 11,759 / 4,360 | matched ✓ |
| Direct us_exam_id portability | 19 / 6,801 | matched ✓ |
| (rid, exam_date) portability | 6,801 / 6,801 | matched ✓ |
| Cohort coverage | 37.50% | 37.50% ✓ |
| tp_central_examined / tp_ln_examined / tp_ln_positive non-null | 3,986 / 3,946 / 3,764 | matched ✓ |

Skeleton SQL: 2 `CREATE TABLE IF NOT EXISTS` only; zero data writes. Probes SQL: 0 writes.

**One Cursor judgment-call needs Logan's input before mig_171b**: the rollup skeleton carries 9 `tp_*` PM cols as bridge placeholders, but `tp_*` semantics is pathology-grain (`canonical_path_malignant_events_v1`), not US-imaging-grain. Decision needed: does tp_* live in `canonical_us_lymph_node_patient_rollup_v2` (current skeleton), in a broader cervical/pathology LN rollup, or stay on PM with re-derivation from path_malignant?

---

## §2 New findings opened this chat (NOT yet in registry — Logan ratification needed)

### §2.1 PM `histology_final` vs `canonical_path_malignant_patient_rollup_v1.dominant_histology` — 378-pt drift

Surfaced via cross-canonical reconcile probe (Cowork chat-2). 2x2 analysis:

| metric | n |
|---|---:|
| Both populated, exact match | 3,759 |
| Both populated, **mismatch** | **378** |
| PM only | 0 |
| Canonical only | 0 |
| Both null | 6,734 |
| **Total** | **10,871** |

Mismatch breakdown by category:

**Vocab drift (~150 pts — closes with mig_172):**
- `'PTC'` vs `'PTC '` (49)
- `'follicular carcinoma'` vs `'Follicular carcinoma'` (42)
- `'metastatic PTC'` vs `'Metastatic PTC'` (7)
- `'pooly differentiated thyroid carcinoma'` vs `'poorly differentiated thyroid carcinoma'` (1)
- ... etc.

**Real semantic disagreements (~228 pts — manuscript-blocking):**
- `'follicular carcinoma'` vs `'PTC'`: 72
- `'metastatic PTC'` vs `'PTC'`: 53
- `'PTC'` vs `'NIFTP'`: 37
- `'MTC'` vs `'PTC'`: 8 (and `'PTC'` vs `'MTC'`: 2)
- `'follicular carcinoma'` vs `'NIFTP'`: 9
- `'PTC'` vs `'FTUMP'`: 7
- `'PTC'` vs `'Follicular carcinoma'`: 6
- `'poorly differentiated thyroid carcinoma'` vs `'PTC'`: 5
- `'NIFTP'` vs `'PTC'`: 2
- `'metastatic PTC classical'` vs `'PTC'`: 2
- `'anaplastic carcinoma'` vs `'PTC'`: 3
- `'PTC'` vs `'PTC microcarcinoma'`: 2

These are different cancer histologies, not vocab variants. Hypotheses:
- PM picks "max grade" or aggressive variant; rollup picks "primary tumor"
- Multi-tumor patients aggregated differently
- Different upstream sources (PM may use synoptic_path; rollup uses path_malignant events)

**Recommended action**: open `CF-mig178-PM-HISTOLOGY-FINAL-VS-PATH-ROLLUP-DOMINANT-378PT` after Logan confirms framing. Vocab portion auto-closes when mig_172 lands; semantic ~228 pts requires clinical review (likely sits alongside mig_172 / mig_177 invasion-family work). The CF should reference both `histology_final` (PM) and `dominant_histology` (rollup).

### §2.2 canonical_invasion_events_v1.linkage_ambiguous_multi_finding — 76% TRUE

Cohort sweep on event-grain table: 39,574 TRUE / 12,177 FALSE / 0 NULL across 51,751 events. 76% ambiguous linkage may be expected (events grain has natural multi-finding noise) but worth surfacing if not already documented in mig_154 close-out. **Action**: cross-check existing CF-mig154-* notes; if uncovered, propose `CF-mig178-INVASION-EVENT-LINKAGE-AMBIGUOUS-76PCT` (informational).

### §2.3 canonical_complications_events_v1.detection_date_inferred — 95% TRUE

4,776 TRUE / 274 FALSE / 0 NULL across 5,050 events. Type-A near-uniform-TRUE pattern. Likely expected (most complications dates are inferred from note timing rather than explicit date statements). **Action**: spot-check whether mig_99 close-out notes already document this; if not, informational CF.

---

## §3 Apply queue full pre-flight verdict — ALL GREEN

Per Logan's chat-2 re-scoping, Cowork did not apply the queue. All 9 SQL files pre-flighted against live MD:

| Step | File | Status |
|---|---|---|
| 1 | `161_mig155_independent_reverification_20260429.sql` | GREEN: 31 mig_155 batch rows confirmed; named SSOTs all exist |
| 2 | `161b_mig155_ata_initial_dup_cf_20260429.sql` (Cowork-authored, this chat, `a75f510`) | GREEN: ata_initial = ata_category on 10871/10871 verified live |
| 3 | `159_patient_master_final_residual_cluster_signoff_20260429.sql` | GREEN: 27 PM cols, all `not_started`; max_stimulated_tg_date is DATE per CF-mig159 |
| 3.5 | `168b_pm_empty_varchar_reclass_and_bool_cf_20260429.sql` (Cowork-authored, this chat, `a75f510`) | GREEN: all 9 cols match v6 §9.2 spec exactly (4 BOOL 0T/10871F/0N + 5 VARCHAR 0 non-null) |
| 4 | `160_global_clinical_date_retype_20260429.sql` | GREEN: 21 cols × 5 tables match expected pre-state dtypes; needs separate "go" before structural ALTERs |
| 5 | `166_canonical_cleanup_audit_v1_signoff_20260429.sql` | GREEN: 18 cols on `manuscript_workspace.canonical_cleanup_audit_v1` |
| 6 | `167_mig165_retroactive_verification_20260429.sql` | GREEN: 11 snapshot tables, 1306 mig_165 rows |
| 7 | `164_view_layer_registration_signoff_20260429.sql` | GREEN: 4 VIEWs exist as VIEW type; 0 signoff rows yet (confirms inserts will land cleanly) |
| 8 | `163b_any_recurrence_hybrid_apply_20260429.sql` (`1476a46`) | GREEN: every HYBRID preflight number exact |

**Apply order ratified by Logan** in chat opening message: chain Steps 1–3, pause for "go" before Step 4 (structural ALTERs), then chain 5–9. mig_168b inserts after Step 3 per v6 §9.4. Total ~107 query_rw calls.

---

## §4 New artifacts shipped this chat

### §4.1 7 Cursor (GPT-5.5) prompts (Lanes 60–66, mig_171–177) — committed at `aa00552`

| Lane | mig | Type | Notes |
|---|---|---|---|
| 60 | mig_171 | Tier-2 BUILD design + skeleton | **Cursor returned design — Path-C verified clean (`f6d0313`)** |
| 61 | mig_172 | Apply lane (high clinical-review) | histology vocab normalization (8 cols) |
| 62 | mig_173 | Schema reform + apply | syn_*_size_cm 3-axis dtype reform |
| 63 | mig_174 | Design + decision package | cnln/lateral/ene multi-label parser |
| 64 | mig_175 | Profile + 3-option decision | mig_136 days-semantic adjudication (58 cols) |
| 65 | mig_176 | Profile + resolution rule | dominant_nodule v1/v2 reconcile (1,065 mismatches) |
| 66 | mig_177 | Profile + per-family decision | mig_154 invasion family reconcile (23 cols) |

### §4.2 Cowork-authored apply queue add-ons — committed at `a75f510`

- `qc_framework_v1/migrations/161b_mig155_ata_initial_dup_cf_20260429.sql` (Step 2 of apply queue)
- `qc_framework_v1/migrations/168b_pm_empty_varchar_reclass_and_bool_cf_20260429.sql` (Step 3.5 of apply queue)

### §4.3 This log — `qc_framework_v1/COWORK_VERIFICATIONS_LOG_2026-04-29.md`

---

## §5 CF backlog density inventory

Per-table CF density (from `canonical_column_verification_registry_v1.notes` regexp):

| Table | Distinct CFs | Cols with any CF |
|---|---:|---:|
| **canonical_patient_master** | **82** | **450** |
| rai_treatment_episode_v2 | 5 | 16 |
| canonical_recurrence_v1 | 3 | 11 |
| canonical_ete_event_resolved_v1 | 2 | 15 |
| canonical_path_malignant_patient_rollup_v1 | 1 | 14 |
| canonical_molecular_genetics_from_notes_v2 | 1 | 2 |
| canonical_parathyroid_patient_rollup_v1 | 1 | 2 |
| canonical_survival_followup_v1 | 1 | 1 |

Insight: PM is the entire CF mountain. Non-PM canonicals are largely clean. Confirms apply queue + 7 Cursor prompts are correctly prioritized.

---

## §6 Cross-canonical reconcile spot-probes

| Pair | Match | Mismatch | Verdict |
|---|---:|---:|---|
| PM `ete_grade_adjudicated` ↔ canonical_ete_event_resolved_v1.patient_master_ete_grade_adjudicated | 45 | 0 | clean (perfect mirror) |
| PM `recurrence_type` ↔ canonical_recurrence_v1.recurrence_type | 10,871 | 0 | clean (perfect mirror) |
| **PM `histology_final` ↔ canonical_path_malignant_patient_rollup_v1.dominant_histology** | 3,759 | **378** | **new drift — §2.1** |

---

## §7 Items added to backlog (need more work)

1. **`CF-mig178-PM-HISTOLOGY-FINAL-VS-PATH-ROLLUP-DOMINANT-378PT`** (§2.1) — Logan ratifies framing; ~150 vocab closes with mig_172, ~228 semantic needs review
2. **mig_171b/c authoring** pending Logan's tp_* placement decision (US LN rollup vs broader cervical/pathology LN rollup vs PM re-derivation)
3. **mig_171b** can proceed once design is ratified — Cursor next-step
4. **Apply queue Steps 1–9** unchanged from v6 handoff; pre-flight all green; Cursor/VSC executes when Logan greenlights
5. **`CF-mig178-INVASION-EVENT-LINKAGE-AMBIGUOUS-76PCT`** (§2.2) — informational; cross-check vs existing mig_154 close-out before opening
6. **`CF-mig178-COMPLICATIONS-DETECTION-DATE-INFERRED-95PCT`** (§2.3) — informational; cross-check vs mig_99 close-out before opening
7. **Full 439-col BOOLEAN sweep** on 38 non-PM verified canonicals — fill-time work, available if Cursor's queue empties

---

## §8 Chat-2 standing context

- 5-gate audit: 165 / 0 / 0 / 0 / 21 (unchanged from chat handoff; Cursor will close gate-5 when Step 4 mig_160 fires)
- Local working dir has stale `exports/mig170_..._181843/` artifacts (different timestamp from committed `181945`) — duplicates from a parallel local run; can be deleted/gitignored
- `memory/MEMORY.md` has uncommitted dedup change (6 close-out entries removed); not touched
- Local `.git/HEAD.lock` cleared via Desktop Commander after sandbox-bash failed pull-rebase

---

End of log. Apply queue stands ready; mig_171b ready to author after Logan's tp_* decision.

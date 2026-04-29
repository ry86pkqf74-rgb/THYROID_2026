# Cursor Agent Task — `canonical_molecular_genetics_v2` Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 90-120 minutes (single big table, complex domain)
**Run order:** Lane 9 (run after labs lane 8 finishes)

---

## 1. Goal

Verify `canonical_molecular_genetics_v2` (1,384 rows / 1,151 patients / **69 not_started cols, 5 na cols**). The largest single Tier 2 canonical remaining unverified.

---

## 2. Domain context

Per `project_molecular_v2_schema.md` memory:
- Single master `canonical_molecular_genetics_v2` (1,384 rows / 1,151 pts) consolidated 2026-04-21
- Plus `canonical_molecular_genetics_from_notes_v2` (separate; covers an LLM-extracted-from-notes path)
- Plus 2 UNNEST views (variants + fusions)
- 13 legacy molecular tables archived to `"Thyroid 2026 UPdated".molecular_legacy_20260421`

This canonical is the result of a heavy consolidation effort. Find the build script(s):
```
grep -rn "canonical_molecular_genetics_v2" scripts qc_framework_v1 | head
```

Schema preview (first 30 cols):
- IDs / linkage: research_id, molecular_episode_id, linked_fna_episode_id, linked_nodule_id, linked_surgery_episode_id
- Dates: test_date_native (TIMESTAMP), resolved_test_date (VARCHAR) — **likely date-type violations** (see §3)
- Platform: platform, platform_raw, platform_version
- Source: bethesda_category, specimen_site_normalized, parser, parse_status, n_fields_parsed
- Result rollup: test_result_summary, rom_descriptor, rom_percent_raw/low/high/point, rom_description
- Specimen: specimen_adequacy_raw, specimen_adequacy_norm
- Variants/fusions/CNAs: gene_mutations_raw/status, gene_fusions_raw/status, cna_raw/status
- (39 more cols — probe full schema yourself)

---

## 3. Date-type CF candidates — flag during verification

**`test_date_native` is TIMESTAMP** and **`resolved_test_date` is VARCHAR**. Both look like clinical-event date cols (when was the molecular test done). Per Logan's `feedback_clinical_dates_calendar_only.md`, clinical event dates MUST be DATE.

- Verify the cols faithfully against build (not blocked by date type)
- Document as CF-mig<N>-MOL-DATE-RETYPE for a future date-cleanup pass (joining the existing CF list inherited from clinical_date_retype Cursor 1 pattern)
- Do NOT block sign-off on type repair

If `linked_*_episode_id` cols are heavily NULL or violate `feedback_no_crossdomain_linkage_ids.md` (which says canonical tables key on research_id only, no cross-domain linkage IDs), flag as a separate CF.

---

## 4. Methodology

This is a complex consolidated canonical. Likely a UNION/MERGE of multiple legacy sources. Verification approach:

### 4a. Probe build path
Find the build script. If it's a deterministic SELECT from a single source: extraction-faithfulness pattern (mig_102/110 sibling). If it's a UNION/MERGE of multiple legacy sources: per-source verification (mig_107 PMH 3-source pattern).

### 4b. Cohort + parity sanity
- 1,384 rows / 1,151 patients (some patients have multiple molecular tests — expected)
- Verify parity vs `archive_pub_v1_0.molecular_legacy_20260421` consolidated counts (per memory, 13 legacy tables → 1 master)

### 4c. Per-col verification — group by col category
- IDs/linkage: na_provenance pattern (most likely already na or auto-skip)
- Dates: extraction-faithfulness vs upstream (flag retype CF separately)
- Platform vocab: confirm against expected enum (ThyroSeq, Afirma, etc.)
- ROM percentages: check rom_percent_low ≤ rom_percent_point ≤ rom_percent_high invariant
- Result rollup: TBD
- Variants/fusions/CNAs: probably JSON or comma-separated lists; verify parsing is consistent

### 4d. Sign-off SQL
`qc_framework_v1/migrations/<N>_molecular_genetics_v2_signoff.sql`. Multi-block (one per col category) for clarity.

---

## 5. Acceptance gates

- 69 not_started cols flipped to verified or na (some may be na for cols never populated)
- table_status='verified'
- ROM invariant probe: `low ≤ point ≤ high` for all rows (or documented exceptions)
- Platform vocab clean (no unexpected raw strings escaping)
- CF list documented (date retype + any others)

---

## 6. Don't touch (active parallel lanes)

- `canonical_ete_subgrade_events_v1` / `canonical_ete_subgrade_patient_rollup_v1` — Cowork's lane
- `canonical_molecular_genetics_from_notes_v2` — separate canonical (LLM-from-notes path); leave for a future round
- Any table touched by sibling Cursor lanes 8 + 10 (labs, US v2)

---

## 7. Reference reading

Required:
- `project_molecular_v2_schema.md` — schema + consolidation context
- `feedback_clinical_dates_calendar_only.md` — date-type rule
- `feedback_no_crossdomain_linkage_ids.md` — cross-domain linkage rule
- `feedback_motherduck_direct_check.md`
- `feedback_surgical_git_add.md`

Repo:
- `qc_framework_v1/migrations/102_parathyroid_events_table_signoff.sql` — extraction-faithfulness template
- `qc_framework_v1/migrations/107_pmh_events_table_signoff.sql` — multi-source template (Cursor's lane 4)

---

## 8. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Migration filename: `<N>_molecular_genetics_v2_signoff.sql`
- Surgical `git add`
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP

---

## 9. If something unexpected surfaces

- ROM invariant violations > 5 rows → STOP, ask Logan
- linked_*_episode_id cols populated (not null/auto-na) → potential cross-domain linkage rule violation; flag as CF + ask Logan
- Multiple distinct build_script values (suggests repeated re-runs without snapshot) → document; not a blocker
- Massive cohort drift vs memory's 1,384/1,151 → re-pull and re-probe; something has changed since the consolidation snapshot

---

End of prompt. Lane 9. Largest single Tier 2 canonical close-out today.

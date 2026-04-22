from scripts._md_connect import connect_locked
con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'

print("=" * 70)
print("CLAIM VERIFICATION — other Claude report dated 2026-04-20")
print("=" * 70)

# 1. CPM dimensions (claim: 10,871 x 1,499)
r = con.execute(f"SELECT COUNT(*) FROM {DB}.main.canonical_patient_master").fetchone()
cols = con.execute(f"""
    SELECT COUNT(*) FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='canonical_patient_master'
""").fetchone()
print(f"\n[1] CPM rows: {r[0]} (claim: 10,871), cols: {cols[0]} (claim: 1,499)")

# 2. LLM entity tables — row counts + distinct RIDs + extraction_timestamp max
print("\n[2] note_entities_llm_* tables (claim: 5 integrated, 12 stuck at Apr-3 qwen3:32b with 11,037 rows/5,641 RIDs):")
tabs = con.execute(f"""
    SELECT table_name FROM duckdb_tables()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name LIKE 'note_entities_llm_%'
     ORDER BY table_name
""").fetchall()
for (t,) in tabs:
    try:
        row = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id), MAX(extraction_timestamp) FROM {DB}.main.\"{t}\"").fetchone()
        print(f"  {t:60s}  rows={row[0]:>7}  rids={row[1]:>6}  max_ts={row[2]}")
    except Exception as e:
        print(f"  {t}: ERROR {e}")

# 3. TIRADS v2 coverage claims
print("\n[3] TIRADS v2 — claim: 3,021 nodule-raw RIDs, 2,465 in CPM tirads_v2_worst_category, 4,073 report-rollup RIDs")
try:
    a = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {DB}.main.tirads_v2_nodules_raw").fetchone()
    b = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {DB}.main.tirads_v2_reports_raw").fetchone()
    c = con.execute(f"SELECT COUNT(*) FROM {DB}.main.canonical_patient_master WHERE tirads_v2_worst_category IS NOT NULL").fetchone()
    d = con.execute(f"SELECT COUNT(*) FROM {DB}.main.canonical_patient_master WHERE tirads_v2_n_nodules_scored IS NOT NULL").fetchone()
    print(f"  tirads_v2_nodules_raw.distinct_rid = {a[0]}")
    print(f"  tirads_v2_reports_raw.distinct_rid = {b[0]}")
    print(f"  cpm.tirads_v2_worst_category nonnull = {c[0]}")
    print(f"  cpm.tirads_v2_n_nodules_scored nonnull = {d[0]}")
    # Report-level exposure in CPM
    for col in ['tirads_v2_any_fna_recommended','tirads_v2_any_suspicious_ln_on_us','tirads_v2_any_ete_on_us','tirads_v2_any_interval_growth']:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {DB}.main.canonical_patient_master WHERE {col} IS NOT NULL").fetchone()[0]
            print(f"  cpm.{col} nonnull = {cnt}")
        except Exception as e:
            print(f"  cpm.{col}: ERR {e}")
    # Re-extraction queue
    try:
        q = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {DB}.main.tirads_reextraction_queue_v1").fetchone()
        print(f"  tirads_reextraction_queue_v1: rows={q[0]} rids={q[1]}")
    except Exception as e:
        print(f"  tirads_reextraction_queue_v1: MISSING or ERROR {e}")
except Exception as e:
    print(f"  ERROR: {e}")

# 4. Allegedly-NULL columns — verify they're actually NULL
print("\n[4] Claimed 100%-NULL CPM columns (post Scripts 288-291):")
for col in ['biochemical_concern_first_date','recurrence_histology','recurrence_site','recurrence_site_primary','op_esophageal_inv_any','path_stage_raw','gm_path_stage_raw','rai_scan_findings_v9','comp_vc_paralysis_evidence_tier','comp_vc_paresis_evidence_tier']:
    try:
        nn = con.execute(f"SELECT COUNT(*) FROM {DB}.main.canonical_patient_master WHERE {col} IS NOT NULL").fetchone()[0]
        print(f"  {col:50s} nonnull = {nn}")
    except Exception as e:
        print(f"  {col}: COLUMN MISSING or ERR: {e}")

# 5. Operative episode detail v2 — claim: 9,371 rows / 9,368 patients, 735 re-ops silently missing
print("\n[5] Operative episode detail v2 — claim: 9,371 rows / 9,368 RIDs; n_surgeries_v2 says 738 have >=2")
try:
    a = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {DB}.main.operative_episode_detail_v2").fetchone()
    b = con.execute(f"SELECT n_surgeries_v2, COUNT(*) FROM {DB}.main.canonical_patient_master GROUP BY n_surgeries_v2 ORDER BY n_surgeries_v2").fetchall()
    c = con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT research_id, COUNT(*) AS n_rows
            FROM {DB}.main.operative_episode_detail_v2
           GROUP BY research_id
        ) WHERE n_rows >= 2
    """).fetchone()
    print(f"  operative_episode_detail_v2: rows={a[0]} rids={a[1]}")
    print(f"  n_surgeries_v2 distribution: {b}")
    print(f"  patients with >=2 rows in detail table: {c[0]}")
    # n_surgeries v1 vs v2 conflicts
    d = con.execute(f"""
        SELECT COUNT(*) FROM {DB}.main.canonical_patient_master
         WHERE n_surgeries IS NOT NULL AND n_surgeries_v2 IS NOT NULL
           AND n_surgeries != n_surgeries_v2
    """).fetchone()
    print(f"  n_surgeries v1 != v2 conflict count: {d[0]} (claim: 598)")
except Exception as e:
    print(f"  ERROR: {e}")

# 6. Complication rates
print("\n[6] Complication rates (claim: hypocalcemia 98/10871, hypopara 34/10871)")
for col in ['comp_hypocalcemia_confirmed','comp_hypoparathyroidism_confirmed','comp_hypopara_permanent','comp_rln_injury_confirmed','comp_rln_paresis_confirmed','comp_rln_paralysis_confirmed','postop_calcium_min_value']:
    try:
        if col == 'postop_calcium_min_value':
            cnt = con.execute(f"SELECT COUNT(*) FROM {DB}.main.canonical_patient_master WHERE {col} IS NOT NULL").fetchone()[0]
            print(f"  {col:45s} nonnull = {cnt}")
        else:
            cnt = con.execute(f"SELECT COUNT(*) FROM {DB}.main.canonical_patient_master WHERE {col} = TRUE OR {col}='Y' OR {col}=1").fetchone()[0]
            print(f"  {col:45s} TRUE/Y/1 = {cnt}")
    except Exception as e:
        print(f"  {col}: ERR {e}")

# 7. Calcium lab coverage after Script 291
print("\n[7] Calcium lab coverage — Script 291 allegedly added 44 TSH rows from LLM; check calcium too")
try:
    a = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {DB}.main.longitudinal_lab_canonical_v1").fetchone()
    b = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {DB}.main.longitudinal_lab_canonical_v1 WHERE lab_name ILIKE '%calcium%' OR lab_name ILIKE '%ca++%'").fetchone()
    c = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {DB}.main.longitudinal_lab_canonical_v1 WHERE lab_name ILIKE '%tsh%'").fetchone()
    print(f"  longitudinal_lab_canonical_v1 distinct RIDs total: {a[0]}")
    print(f"  distinct RIDs with calcium labs: {b[0]}")
    print(f"  distinct RIDs with TSH labs: {c[0]}")
except Exception as e:
    print(f"  ERROR: {e}")

# 8. Adjudication queues
print("\n[8] Open adjudication queues (claim: 5 queues + 403 lab_orphan)")
for q in ['path_size_adjudication_v241','path_tumor_size_correction_queue_v1','ete_adjudication_v1','cpm_hypopara_adjudication_queue_v1','cpm_is_malignant_flag_review_v1','cpm_ete_self_contradiction_queue_v1','cohort_view_duplicate_review_v1','lab_orphan_cohort_review_v1','tg_orphan_cancer_text_investigation_queue_v1','ln_extract_noncohort_orphan_v279']:
    try:
        r = con.execute(f"SELECT COUNT(*) FROM {DB}.main.{q}").fetchone()
        print(f"  {q:55s} rows={r[0]}")
    except Exception as e:
        try:
            # try manuscript_workspace
            r = con.execute(f"SELECT COUNT(*) FROM {DB}.manuscript_workspace.{q}").fetchone()
            print(f"  (mw.){q:50s} rows={r[0]}")
        except Exception as e2:
            print(f"  {q}: NOT FOUND")

# 9. Current git HEAD
print("\n[9] See separate git log")
print("\nDONE.")

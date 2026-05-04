"""Quick probe of M044 cohort cols for sensitivity script debug."""
import os
import duckdb
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")
df = md.execute("""
SELECT c.research_id, c.ete_grade_final, c.histology_final,
       pm.tumor_size_cm_max AS tumor_size_cm,
       c.age_at_surgery, c.sex, c.ajcc8_n_stage,
       COALESCE(c.any_recurrence_flag, FALSE) AS recurrence_any,
       pm.pmhx_nlp_smoking_status, pm.nsqip_smoker
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
LEFT JOIN main.canonical_patient_master pm USING (research_id)
""").fetch_df()
strict_dtc = df['histology_final'].str.contains('PTC|FTC|Hurthle|High-grade|Metastatic|Poorly', case=False, na=False)
df = df[strict_dtc & df['ete_grade_final'].isin(['none','absent','false','microscopic','gross'])].copy()
df['y_pp'] = df['recurrence_any'].astype(bool).astype(int)
print('n:', len(df), 'events:', df['y_pp'].sum())
print('ete_grade_final:'); print(df['ete_grade_final'].value_counts())
print('sex unique:', df['sex'].unique())
print('ajcc8_n_stage unique:', df['ajcc8_n_stage'].unique())
print('tumor_size NaN:', df['tumor_size_cm'].isna().sum())
print('age NaN:', df['age_at_surgery'].isna().sum())
print('sex NaN:', df['sex'].isna().sum())
md.close()

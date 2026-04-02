# LLM Extraction Validation Report

- Input parquet: `processed/output/v2_parquets/note_entities_llm_combined.parquet`
- Generated at: `2026-04-02T09:42:31.888648+00:00`
- Total LLM rows: `8,428`
- Unique patients: `1,759`
- Gold rows (`verification_status` concordant | existing_missing_fill_candidate, per policy): `178`

## Domain / algorithm status

| Domain | Algorithm status | Rows | Patients | Structured matches | Baseline matches | Fill candidates | Review conflicts |
|---|---:|---:|---:|---:|---:|---:|---:|
| complications | existing_missing_fill_candidate | 1 | 1 | 0 | 0 | 1 | 0 |
| genetics | concordant_existing | 1 | 1 | 1 | 1 | 0 | 0 |
| genetics | concordant_existing_extraction_only | 5 | 5 | 0 | 5 | 0 | 0 |
| genetics | discordant_existing | 4 | 4 | 0 | 0 | 0 | 4 |
| genetics | existing_missing_fill_candidate | 85 | 78 | 0 | 0 | 85 | 0 |
| medications | concordant_existing | 4 | 3 | 4 | 4 | 0 | 0 |
| medications | concordant_existing_extraction_only | 56 | 48 | 0 | 56 | 0 | 0 |
| medications | existing_missing_fill_candidate | 7 | 6 | 0 | 0 | 7 | 0 |
| operative_detail | concordant_existing | 4 | 3 | 4 | 0 | 0 | 0 |
| operative_detail | discordant_existing | 135 | 110 | 0 | 0 | 0 | 135 |
| operative_detail | existing_missing_fill_candidate | 144 | 125 | 0 | 0 | 144 | 0 |
| problem_list | concordant_existing_extraction_only | 4 | 4 | 0 | 4 | 0 | 0 |
| problem_list | source_limited | 15 | 14 | 0 | 0 | 0 | 0 |
| procedures | concordant_existing | 109 | 71 | 109 | 93 | 0 | 0 |
| procedures | discordant_existing | 211 | 166 | 0 | 19 | 0 | 211 |
| procedures | existing_missing_fill_candidate | 4 | 4 | 0 | 0 | 4 | 0 |
| staging | concordant_existing | 10 | 9 | 10 | 0 | 0 | 0 |
| staging | discordant_existing | 15 | 14 | 0 | 8 | 0 | 15 |
| unmapped | source_limited | 7,614 | 1,680 | 0 | 0 | 0 | 0 |

## Sample Review Conflicts

| research_id | domain | llm_value | existing_values | source_sheet | source_column |
|---:|---|---|---|---|---|
| 3097 | procedures | Neck ultrasound in 8/21 showing stable partially calcified heterogeneous area in level IV left cervical area | ["Total Thyroidectomy", "total_thyroidectomy"] |  |  |
| 3732 | procedures | enlarged level IV left cervical lymph node | ["Total Thyroidectomy", "total_thyroidectomy"] |  |  |
| 3732 | procedures | enlarged level IV left cervical lymph node | ["Total Thyroidectomy", "total_thyroidectomy"] |  |  |
| 3732 | procedures | enlarged level IV left cervical lymph node | ["Total Thyroidectomy", "total_thyroidectomy"] |  |  |
| 5086 | procedures | total thyroidectomy | ["Left\u00a0Lobectomy (LL)", "Right Lobectomy (RL)", "completion", "hemithyroidectomy"] |  |  |
| 5209 | procedures | Benign-appearing level II, level III and level IV cervical lymph nodes | ["Total Thyroidectomy", "total_thyroidectomy"] |  |  |
| 5209 | procedures | Benign-appearing level II, level III, and level IV cervical lymph nodes | ["Total Thyroidectomy", "total_thyroidectomy"] |  |  |
| 5804 | procedures | benign right level II LN, benign right level IV LN, benign left level II, III, and IV LN | ["Right Lobectomy (RL)", "hemithyroidectomy"] |  |  |
| 5878 | procedures | stable bilateral LN (R level IV 1.1cm LN w/ peripheral doppler flow and punctate echogenic foci) | ["Total Thyroidectomy", "central compartment dissection", "total_thyroidectomy"] |  |  |
| 5944 | procedures | left neck level IV LN measuring 1.4 x .9 x 1.4cm without a fatty hilum, suspicious | ["Left\u00a0Lobectomy (LL)", "hemithyroidectomy"] |  |  |
| 6558 | procedures | visible node within left level IV | ["Right Lobectomy (RL)", "hemithyroidectomy"] |  |  |
| 6907 | operative_detail | hyperechoic area adjacent to the right side of the trachea in the thyroidectomy bed measuring 7 x 5 mm | ["gross_ete", "local_invasion"] |  |  |
| 6907 | procedures | changes of thyroidectomy, no recurrent mass, scattered bilateral jugular chain lymph nodes without dominant adenopathy | ["Total Thyroidectomy", "central compartment dissection", "total_thyroidectomy"] |  |  |
| 7206 | procedures | benign right level II node measuring 1.8 x 2.4 x 0.9 cm with reniform shape and normal fatty hilum | ["Left\u00a0Lobectomy (LL)", "hemithyroidectomy"] |  |  |
| 7206 | procedures | 2 left cervical nodes with benign morphology, level II measuring 1.5 x 0.8 x 1.2 cm | ["Left\u00a0Lobectomy (LL)", "hemithyroidectomy"] |  |  |
| 7615 | operative_detail | 2.5 x 1.5 x 0.9 cm homogenously avidly enhancing soft tissue nodule along the right strap muscles extending to the level of the hyoid bone | ["rln_monitoring"] |  |  |
| 7671 | operative_detail | 9.7cm substernal L thyroid with evidence of tracheal compression and normal R thyroid | ["rln_monitoring"] |  |  |
| 7748 | operative_detail | 1.5cm hypodense thyroid nodule and questionable narrowing of subglottic trachea with focal right lateral and posterior wall thickening | ["rln_monitoring"] |  |  |
| 7792 | operative_detail | Left lobe of thyroid is heterogeneous with MN with tracheal, glottic and supraglottic displacement to the tright, there is a mild substernal component. Left lobe measures 13.5 x 6.1 x 7.5 | ["rln_monitoring"] |  |  |
| 7833 | operative_detail | There is tracheal deviation to the right | ["drain_placed", "rln_monitoring"] |  |  |

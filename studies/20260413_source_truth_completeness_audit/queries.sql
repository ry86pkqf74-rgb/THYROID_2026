-- db_imaging_nodule
SELECT * FROM imaging_nodule_master_v1;

-- db_fna_episode
SELECT * FROM fna_episode_master_v2;

-- db_fna_cytology
SELECT * FROM fna_cytology;

-- db_link_mm
SELECT * FROM imaging_fna_linkage_mm_v1;

-- db_ultrasound_reports
SELECT research_id, us_report_number, ultrasound_date, lymph_node_assessment FROM ultrasound_reports;

-- db_extracted_tirads
SELECT * FROM extracted_tirads_validated_v1;

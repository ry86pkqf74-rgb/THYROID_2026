-- MotherDuck Promotion SQL: v2_stage -> main
-- REVIEW BEFORE EXECUTING. Do not auto-run.
-- Prerequisite: all 8 promotion gate criteria must be PASS.

-- ATTACH 'md:YOUR_DATABASE' AS md (TYPE DUCKDB);

-- Domain: imaging  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_imaging AS
SELECT * FROM v2_stage.note_entities_llm_imaging;

-- Domain: tirads_granular  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_tirads_granular AS
SELECT * FROM v2_stage.note_entities_llm_tirads_granular;

-- Domain: labs  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_labs AS
SELECT * FROM v2_stage.note_entities_llm_labs;

-- Domain: tg_kinetics  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_tg_kinetics AS
SELECT * FROM v2_stage.note_entities_llm_tg_kinetics;

-- Domain: pathology  qa_tier=critical
CREATE OR REPLACE TABLE main.note_entities_llm_pathology AS
SELECT * FROM v2_stage.note_entities_llm_pathology;

-- Domain: synoptic_pathology_enrichment  qa_tier=critical
CREATE OR REPLACE TABLE main.note_entities_llm_synoptic_pathology_enrichment AS
SELECT * FROM v2_stage.note_entities_llm_synoptic_pathology_enrichment;

-- Domain: rai_detailed  qa_tier=critical
CREATE OR REPLACE TABLE main.note_entities_llm_rai_detailed AS
SELECT * FROM v2_stage.note_entities_llm_rai_detailed;

-- Domain: rad_treatment  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_rad_treatment AS
SELECT * FROM v2_stage.note_entities_llm_rad_treatment;

-- Domain: parathyroid_detail  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_parathyroid_detail AS
SELECT * FROM v2_stage.note_entities_llm_parathyroid_detail;

-- Domain: recurrence  qa_tier=critical
CREATE OR REPLACE TABLE main.note_entities_llm_recurrence AS
SELECT * FROM v2_stage.note_entities_llm_recurrence;

-- Domain: survival_followup  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_survival_followup AS
SELECT * FROM v2_stage.note_entities_llm_survival_followup;

-- Domain: cervical_ln_detail  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_cervical_ln_detail AS
SELECT * FROM v2_stage.note_entities_llm_cervical_ln_detail;

-- Domain: functional_outcomes  qa_tier=informational
CREATE OR REPLACE TABLE main.note_entities_llm_functional_outcomes AS
SELECT * FROM v2_stage.note_entities_llm_functional_outcomes;

-- Domain: past_medical_hx  qa_tier=informational
CREATE OR REPLACE TABLE main.note_entities_llm_past_medical_hx AS
SELECT * FROM v2_stage.note_entities_llm_past_medical_hx;

-- Domain: past_surgical_hx  qa_tier=informational
CREATE OR REPLACE TABLE main.note_entities_llm_past_surgical_hx AS
SELECT * FROM v2_stage.note_entities_llm_past_surgical_hx;

-- Domain: presenting_symptoms  qa_tier=informational
CREATE OR REPLACE TABLE main.note_entities_llm_presenting_symptoms AS
SELECT * FROM v2_stage.note_entities_llm_presenting_symptoms;

-- Domain: physical_exam  qa_tier=informational
CREATE OR REPLACE TABLE main.note_entities_llm_physical_exam AS
SELECT * FROM v2_stage.note_entities_llm_physical_exam;

-- Domain: vascular_invasion  qa_tier=critical
CREATE OR REPLACE TABLE main.note_entities_llm_vascular_invasion AS
SELECT * FROM v2_stage.note_entities_llm_vascular_invasion;

-- Domain: airway_invasion  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_airway_invasion AS
SELECT * FROM v2_stage.note_entities_llm_airway_invasion;

-- Domain: dynamic_risk_response  qa_tier=standard
CREATE OR REPLACE TABLE main.note_entities_llm_dynamic_risk_response AS
SELECT * FROM v2_stage.note_entities_llm_dynamic_risk_response;

-- Domain: patient_decision_adherence  qa_tier=informational
CREATE OR REPLACE TABLE main.note_entities_llm_patient_decision_adherence AS
SELECT * FROM v2_stage.note_entities_llm_patient_decision_adherence;


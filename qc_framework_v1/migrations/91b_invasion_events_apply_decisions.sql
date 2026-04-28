-- =============================================================================
-- Migration 91b -- canonical_invasion_events_v1 APPLY decisions + sign-off
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser <logan.glosser@gmail.com> (Cowork session)
-- Plan:   Apply Logan-confirmed dispositions for the 47 cancer orphan
--         finding_status downgrades discovered in mig_91 CTC-equivalence
--         verification of canonical_invasion_events_v1 against pre-Script-363
--         archive snapshot.
-- =============================================================================
--
-- Decisions JSON: verification_csvs/canonical_invasion_events_v1/mig_91_decisions.json
-- Decisions MD:   verification_csvs/canonical_invasion_events_v1/mig_91_decisions.md
-- Review .xlsx:   verification_csvs/canonical_invasion_events_v1/orphan_review__mig_91.xlsx
-- Memory:         feedback_invasion_orphan_clinical_rules.md
--
-- Pre-state:
--   51,773 rows / 10,871 pts / 20 cols / 6 modality x kind slices
--   Registry: 9 na (auto-skip) + 11 not_started + 0 verified
--   table_status='not_started'
--
-- Logan-confirmed dispositions (47 cancer orphans):
--   16 FLIP_TO_PRESENT          (13 HIGH_POS Logan-reviewed
--                                + 3 KEYWORD source-text-confirmed: 2073 FVPTC,
--                                  9636 microcarcinoma capsule+muscle, 10872 TGDC PTC hyoid bone)
--    4 RECLASS to capsular+FLIP (encapsulated tumor capsule invasion:
--                                2641, 5048-syn, 8825 PTC encapsulated; 11201 FTUMP pseudopod)
--   22 DELETE                   (17 LN_ENE -- separate domain;
--                                 1 incidental laryngocele (12129);
--                                 2 vocal cord/mass effect -> belongs in
--                                   canonical_complications_events_v1
--                                   per Logan (5048-airway, 11862);
--                                 1 post-thyroidectomy CT recurrence (4107);
--                                 1 metastatic LUNG adenocarcinoma to thyroid CT (5048-ct))
--    5 ACCEPT (no UPDATE)       (5186 arytenoid sclerosis age-related;
--                                9174 'focus suspicious' correctly weakened to suspected;
--                                9209 N/A absent->indet correct;
--                                9224 PTC CAP template echo;
--                                9829 PTC microcarcinoma no ETE evidence)
--
-- Post-state:
--   51,751 rows (-22)
--   capsular: 4,266 + 4 = 4,270
--   present:  +20 net (16 FLIP + 4 RECLASS+FLIP)
--
-- Carry-forwards:
--   CF-91-VOCAL-CORD -- 5048-airway + 11862 should re-emerge in
--      canonical_complications_events_v1 when that canonical is rebuilt
--   CF-91-NON-PRIMARY-THYROID -- 4107 + 5048-ct rejected from invasion_events
--      because patient is post-thyroidectomy or metastatic-from-non-thyroid;
--      may need a non-primary-thyroid invasion canonical eventually
--   CF-91-LN-ENE-DOMAIN -- 17 LN ENE rows deleted; should re-emerge in
--      canonical_lymph_node_events_v1 / canonical_path_malignant_events_v1.lymph_node_ene
--      when that domain is built
--   CF-91-LINKAGE-COL-NAME -- linkage_ambiguous_multi_episode counts findings
--      not episodes; rename to linkage_ambiguous_multi_finding (cosmetic)
--   CF-91-GROSS-VS-MICRO-ETE-NAMING -- gross_ete is acting as default bucket
--      for any synoptic ETE without explicit 'microscopic' label; numbers are
--      reversed from clinical expectation (gross 1,084 > micro 279 pts).
--      Domain re-derivation may be needed for clean gross/micro split.
-- =============================================================================

-- Step 1: 16 FLIP_TO_PRESENT
UPDATE main.canonical_invasion_events_v1
SET finding_status='present'
WHERE invasion_event_id IN (
  -- HIGH_POS (13)
  'ea6c7df7daf2444c39bd44a281dd5925',
  'e84c9dbd73bf900aaea006315a6c07f8',
  '7e4b09fa1581a741aafc0d783c60e655',
  '6d8dc84209b12bb97b56e3142ebe35b8',
  '95cd284f93b6da420580c8d9a3152cf1',
  '2c5863dd58b9d909395f8689169f0f97',
  '35734857ee2b5b7bb6bc9e1509262038',
  'd2246a98915f09e0b5536d3933a09c27',
  'b74f901c58192f10fa6d18453e353f0b',
  'efb66d32c3647c00526e6a96601f9de3',
  '52c35754bd8be40f2e45b57f569a28b5',
  '85b016ad24d27a2c2399ffd19fc1e840',
  'ad0547fbd3b6f949ccc2f4787dfb0b92',
  -- KEYWORD source-text confirmed (3)
  '201e3dfff7796079ef09c18218b88dd3',
  '87cf1865288de7de5f81ee8714440624',
  'c980d475f6c6185caf795e99bacebabd'
);

-- Step 2: 4 RECLASS to capsular + FLIP_TO_PRESENT
UPDATE main.canonical_invasion_events_v1
SET invasion_type='capsular', finding_status='present'
WHERE invasion_event_id IN (
  'd26bc195cbe9981898a2e987c44a62fb',
  '2830660de1d0e1a60bc51c7a2bee64cb',
  '7fe6cac6291d3d9df863faccfc3fee77',
  '8a5da4b0be2bfec3dd78b5da481485ab'
);

-- Step 3: 22 DELETE
DELETE FROM main.canonical_invasion_events_v1
WHERE invasion_event_id IN (
  -- LN_ENE original (8)
  'ceed28d4034bc37fda751bafb52e1964',
  '36b0ab96bbd774569195fa618a03b25c',
  '013a89caf0a03e6a73382972393ed0e9',
  '1381cc455e412f1d2446d031dc930f8c',
  'ae4cb7938ace8eccb799587811e6fe97',
  '038546936ff56090f417a8a05c3b4a0d',
  '25617526ed57c587507af4522eb7c7f6',
  '1bdea24e353f8419683684de36f89074',
  -- AMBIG_EC reclassified to LN_ENE (4)
  '028c6a07147d12a62bb09c9c9d4bd7c3',
  'd7a4c792e32e7a5ddc43c7096605bc9a',
  '235e3f753663962aa0e7ea901a65017b',
  '4aaa0dd52fe8ffcca0f7bd943d44bbfa',
  -- KEYWORD reclassified to LN_ENE (4)
  'ed70f795d14ba34319b93a45cf0bcbc4',
  '6255d6bc32292893faa60ede78fdfe03',
  '2c08595f052addea46fe3659701c0fba',
  'b4ea0565c1809e4181af646629a22fc8',
  -- KEYWORD source-text confirmed LN_ENE (1)
  'bbd9090a44922c3d77ef39e112ca3c45',
  -- Vocal cord / mass effect -> complications domain
  'b79ddab49f77d446c6d232e4819ee2b0',
  '8684d0f01e0ca1a16a90275f5314858b',
  -- Incidental
  '824d626fe694ff1dbf8dbc4a7f52c971',
  -- CT context: not primary-thyroid invasion
  'd5750ee1b0a51b125d9f433936572cbd',
  'cb07722463eb6eeeca8a280ce58dcc04'
);

-- Step 4: flag all 11 not_started cols verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='verified',
    verified_by='Logan Glosser (Cowork extrapolation 2026-04-28)',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method=CASE
      WHEN column_name IN ('finding_status','evidence_qualifier','evidence_span_hash','confidence')
        THEN 'mechanical_derivation_compare + Logan-reviewed orphan dispositions'
      ELSE 'ctc_equivalence_pre363'
    END,
    batch_id='mig_91b_2026-04-28',
    notes='CTC-equivalence vs pre363v3_20260422_032942 archive. 7 cols (invasion_type, finding_date, source_modality, source_kind, linkage_method, n_candidate_episodes, linkage_ambiguous_multi_episode) had 0 diffs. 4 cols had localized diffs adjudicated per Logan rules.'
WHERE schema_name='main'
  AND table_name='canonical_invasion_events_v1'
  AND verification_status='not_started';

-- Step 5: refresh table_signoff_registry -> 'verified'
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/91b_invasion_events_apply_decisions.sql',
    notes = '8th table verified under Protocol v2. 47 cancer orphans dispositioned (16 FLIP + 4 RECLASS+FLIP + 22 DELETE + 5 ACCEPT). 54 BENIGN orphans Rule#1-defensible (no change). Final state: 51,751 rows / 10,871 pts / 9,945 present.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_invasion_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- Verification (post-state):
--   SELECT COUNT(*), COUNT(DISTINCT research_id),
--          SUM(CASE WHEN finding_status='present' THEN 1 ELSE 0 END),
--          SUM(CASE WHEN invasion_type='capsular' THEN 1 ELSE 0 END)
--   FROM main.canonical_invasion_events_v1;
--   -> 51,751 / 10,871 / 9,945 / 4,270  CONFIRMED 2026-04-28 16:23 UTC
--
--   SELECT * FROM main.canonical_table_signoff_registry_v1
--   WHERE table_name='canonical_invasion_events_v1';
--   -> table_status='verified', n_verified=11, n_na=9, n_not_started=0
-- =============================================================================
-- end of mig_91b -- canonical_invasion_events_v1 verified (8th Protocol v2 table)
-- =============================================================================

# Airtable IDs (live, scaffolded 2026-05-05)

These IDs are needed by the daily sync prompt and any direct API calls. Keep this file updated whenever schema changes.

## Workspace

- ID: `wspDGHtW2HNuT20GQ`
- Name: My First Workspace

## Base A: THYROID_DATA_REGISTRY

- Base ID: `appTGeB1jIizZbjnw`

| Table | Table ID |
|---|---|
| Source Files | `tblB6i7A9Q9XKCiLt` |
| Co-Authors | `tbluSa4JH7pXcFkWK` |
| Columns | `tbl8zPiMQtDf6iB0T` |
| Cohort Patients | `tbl9KwwXzvV8HNlOM` |
| Reconciliation Runs | `tblriTwHnZ4DJRg5M` |
| Issue Ledger | `tblkWAwe2aoPiUzms` |
| Manuscript Snapshots | `tbliQJ1UDy6Me1VfE` |
| Verification Checks | `tbl65mYqMWIGEQIBZ` |
| Override Decisions | `tblSIQTdzVjkQfyFE` |

### Source Files key fields

| Field | Field ID |
|---|---|
| filename (primary) | `fld2nuFAPv7OAZggX` |
| domain | `fldZb1TAi6zjt4ldh` |
| status | `flddEQTe0JvbLEuc2` |
| filepath | `fld62vEJpwFxhbGVV` |
| ingest_notes | `fld2pNcj77NDDTynV` |

### Verification Checks key fields

| Field | Field ID |
|---|---|
| check_id (primary) | `fldpUwzKJOHWSaLoc` |
| metric_name | `fldwUNystSPrszeoW` |
| linked_manuscript_code | `fldk41zAOfxnd3QR9` |
| manuscript_value | `fldwlLrQksYNZ0MgT` |
| db_value | `fldfmkdSKpOlCS9xB` |
| verdict | `fldK82maEAwDWrfRd` |
| severity | `fldo9mWaGQb8CJLFo` |
| status | `fldEG5oZZhcCFMXWU` |
| linked_columns | `fldbwmUakJ336zDmt` |
| linked_linear_issue_id | `fldMjF9TupksvkNed` |
| linked_linear_issue_url | `fld0c9hJXaZfFuUXr` |
| lifecycle | `fldnmwPc2ymkR0H2h` |
| last_run_date | `fldJh1nA2SeaVBTkF` |
| fix_action | `fldEOLMHrLMGe0jSU` |

## Base B: THYROID_MANUSCRIPT

- Base ID: `appJYOnUb7KrHKwpV`

| Table | Table ID |
|---|---|
| Manuscripts | `tblLsp8ls3rU1eEc9` |
| References | `tblUSflxduy4xxNwa` |
| Sections | `tblU9JLinirdcXUb8` |
| Tables and Figures | `tblR10rBaDTeTcABv` |
| Submission Targets | `tblwEDG6PA8aOYh0j` |
| Data Feedback Log | `tblsiYKJtKcktkzze` |
| Manuscript Feedback Log | `tblYSCBzRFC4RGPMq` |
| Notable Findings | `tbl7GL0eFSiNPwabW` |

### Manuscripts key fields

| Field | Field ID |
|---|---|
| code (primary) | `fld2HiaF0VRRcnys5` |
| short_title | `fldfuOm8M4TilKhhJ` |
| full_title | `fld7lX2LRKMgqSduG` |
| status | `fldY4gLEOWz1spm6v` |
| aim | `fldlF0bOm5BHc6Asj` |
| candidate_cohort_n | `fldSje6HzMYjGxIRb` |
| study_dir | `fldnoRRDynfpi2DME` |
| ai_journal_recommendation | `fldPLmUa8Ee7Dvm8W` |
| ai_journal_rec_last_refreshed | `fldAtau6czQkot1yT` |
| journal_chosen | `fldryVwMsLCa7UG3G` |
| linked_linear_project | `fldcWJqDnardZJfOL` |
| owner | `fldPthOVGEKD1k8E2` |
| lifecycle | `fldsokU6dgR5xYsru` |
| bq_manuscript_id | `fldSO5KJ9XHdCIcxI` |
| rationale | `fldoyaywsATZH5fcd` |

### Notable Findings key fields

| Field | Field ID |
|---|---|
| finding_id (primary) | `fldt70RW16S8Mqqya` |
| title | `fldn0rtUzEEH6WvIS` |
| date_found | `fldg9Y24IYhLq4BgN` |
| domain | `fldTHzzbIFnw15wNe` |
| severity | `fldZShM8LcMblw2oA` |
| finding_summary | `fldIgknZbDHJFxaPl` |
| evidence_summary | `fld3Ehs4XEaoixhzW` |
| supporting_artifacts | `fldwOlWchkHky5lzG` |
| applies_to_manuscripts | `fldBz1hv5Xxexp7iF` |
| linked_linear_issue_id | `fld9jJXIFANzY2IiR` |
| discovery_session | `fldvtR7mZinElKjCJ` |
| next_action | `fldxnEB2BuNB5g0BH` |
| lifecycle | `fldHA2e1rjylgW80c` |
| created_by | `fldUvw5I7sP0StM0d` |

## Manuscript record IDs (for cross-base joins)

| Code | Record ID |
|---|---|
| M025 | `rectho37S5qziyHte` |
| M032 | `recbWMsd6mQ1oGKTc` |
| M036 | `recaJi9YRWkjOfTG4` |
| M037 | `rec2bIXPJBl0w8tcU` |
| M038 | `recNCLtTae6b51oVj` |
| M044 | `recayZDT7J9fRIFrG` |
| M048 | `recgaLY2sajz1WmFw` |
| M083 | `recaQXm3eavnOc9s4` |
| Mo36 | `recp7f3k3sQmy0H39` |
| H1 | `recj3NRxzqNQsRF3J` |
| H2 | `recQEwf0Hd6SDSZV8` |
| TGDC | `recOATVhQAR1PRcZL` |
| MULTIMODAL | `recIlyCrFQEzcwLp1` |
| MOLIMG | `recf28zn3AE0idzGX` |
| SURGEON | `rec6zmI8Ka9ScZoQI` |
| ETE | `reciotqXhaNTBA9EB` |
| EXT2-4 | `rec1GJyrmKdKxjlaY` |
| M084 (parathyroid + thyroid) | `recx6Jr6WFtF2hZxb` |
| NSQIP-PTH | `recHyloOjBHKH7Jk3` |
| LOBMOL | `recsFDy2NFfs452hD` |
| M004 | `recpSCzbr4KNhXHIK` |
| M085 | `recotdCiIuU8UQbLs` |
| M075 | `recsPTyiV6awoS217` |
| M019 | `recMJ8rYjYxEbyytk` |
| M027 | `recgRzYTPtam1BNUl` |
| M028 | `recJfXLiO9etKUEcz` |
| M029 | `recrgyucp2GExAWcQ` |
| M033 | `recTm8PqDvHn3g0JU` |
| M043 | `rec8T7TtSRtMzBB0Y` |
| M047 | `recxI8uzH4S4gluQ6` |
| M053 | `recjUILCjp6EPqcMk` |

## Linear

- Existing team: ROS — `ce8175e5-4c09-4e95-9bea-038eb4f783a1`
- THYROID team: `c4afb51b-8bca-413a-a53e-15eb825cffbd` (Thyroid Database)

### Notable Findings & Research Insights project
- Linear project for tracking notable findings (cross-linked to Airtable Notable Findings table `tbl7GL0eFSiNPwabW`)
- Project ID: `db85100d-5480-4ab6-b972-2f0db8cf5a4a`
- URL: https://linear.app/rostemp/project/notable-findings-and-research-insights-7f4de86bcdf4
- Label: `type:notable-finding` — ID `c7bc51b7-403a-4dad-baf9-267e43d8b315`, color `#9333EA`, team-scoped to THY
- Inaugural issue: THY-45 (NF-2026-05-07-park2009-noncalibration)
  - URL: https://linear.app/rostemp/issue/THY-45/nf-2026-05-07-park2009-noncalibration-park-2009-korean-coefficients

To create projects, use `save_project` with `addTeams: ["c4afb51b-8bca-413a-a53e-15eb825cffbd"]`.

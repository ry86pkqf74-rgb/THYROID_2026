# Methods note — multi-nodule TIRADS attribution limitation (mig_222)

For ultrasound reports describing multiple thyroid nodules, nodule-level TIRADS attributes were used only when the extraction could be assigned deterministically to a canonical nodule row. A dedicated Lane F triage identified 448 US exams and 825 deferred LLM-absorption patients where reported TIRADS features could not be safely attributed to a single canonical nodule. These records were not bulk-absorbed into per-nodule phenotype fields; instead, affected canonical nodule rows were flagged with `multi_nodule_attribution_unresolved=TRUE` and preserved for sensitivity analyses.

Primary nodule-level TIRADS analyses should exclude rows with unresolved multi-nodule attribution, or include them only in predefined sensitivity analyses with the limitation explicitly noted.

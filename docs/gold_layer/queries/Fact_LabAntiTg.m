// Table: Fact_LabAntiTg
// Source: 01_SILVER_DEID_PARQUET/lab_anti_thyroglobulin_facts.parquet
// Load via: Home → Get Data → Blank Query → Advanced Editor → paste this
let
    SilverPath = SilverLayerPath,
    Source = Parquet.Contents(SilverPath & "lab_anti_thyroglobulin_facts.parquet"),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    // no transforms needed — Silver layer pre-cleaned
    Output = #"Promoted Headers"
in
    Output

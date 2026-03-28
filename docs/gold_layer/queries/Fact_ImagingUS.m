// Table: Fact_ImagingUS
// Source: 01_SILVER_DEID_PARQUET/imaging_ultrasound_facts.parquet
// Load via: Home → Get Data → Blank Query → Advanced Editor → paste this
let
    SilverPath = SilverLayerPath,
    Source = Parquet.Contents(SilverPath & "imaging_ultrasound_facts.parquet"),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    // no transforms needed — Silver layer pre-cleaned
    Output = #"Promoted Headers"
in
    Output

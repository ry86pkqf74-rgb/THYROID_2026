// Table: Fact_ImagingNuclear
// Source: 01_SILVER_DEID_PARQUET/imaging_nuclear_facts.parquet
// Load via: Home → Get Data → Blank Query → Advanced Editor → paste this
let
    SilverPath = SilverLayerPath,
    Source = Parquet.Contents(SilverPath & "imaging_nuclear_facts.parquet"),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    // no transforms needed — Silver layer pre-cleaned
    Output = #"Promoted Headers"
in
    Output

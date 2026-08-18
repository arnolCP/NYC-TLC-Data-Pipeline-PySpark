let
    Source = Parquet.Document(File.Contents(pAuditViewsFolder & "\latest_quality_results.parquet")),
    AddMetadata = Table.AddColumn(Source, "metadata", each try Json.Document([metadata_json]) otherwise null, type record),
    AddTaxiType = Table.AddColumn(AddMetadata, "taxi_type", each if [metadata] = null then null else Record.FieldOrDefault([metadata], "taxi_type"), type text),
    AddYear = Table.AddColumn(AddTaxiType, "year", each if [metadata] = null then null else Record.FieldOrDefault([metadata], "year"), Int64.Type),
    AddDimension = Table.AddColumn(AddYear, "dimension", each if [metadata] = null then null else Record.FieldOrDefault([metadata], "dimension"), type text),
    AddScore = Table.AddColumn(AddDimension, "score", each if [metadata] = null then null else Record.FieldOrDefault([metadata], "score"), type number),
    AddThreshold = Table.AddColumn(AddScore, "threshold", each if [metadata] = null then null else Record.FieldOrDefault([metadata], "threshold"), type number),
    Types = Table.TransformColumnTypes(AddThreshold, {{"event_id", type text}, {"occurred_at", type datetimezone}, {"pipeline_id", type text}, {"stage_run_id", type text}, {"stage_name", type text}, {"status", type text}, {"dataset_name", type text}, {"rule_id", type text}, {"rule_name", type text}, {"records_evaluated", Int64.Type}, {"records_failed", Int64.Type}, {"failure_rate", type number}, {"allowed_failure_rate", type number}}),
    Result = Table.RemoveColumns(Types, {"metadata"})
in
    Result

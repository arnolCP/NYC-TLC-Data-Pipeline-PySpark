let
    Source = Parquet.Document(File.Contents(pAuditViewsFolder & "\latest_file_runs.parquet")),
    AddMetadata = Table.AddColumn(Source, "metadata", each try Json.Document([metadata_json]) otherwise null, type record),
    AddChangeType = Table.AddColumn(AddMetadata, "change_type", each if [metadata] = null then null else Record.FieldOrDefault([metadata], "change_type"), type text),
    AddSizeBytes = Table.AddColumn(AddChangeType, "size_bytes", each if [metadata] = null then null else Record.FieldOrDefault([metadata], "size_bytes"), Int64.Type),
    AddExtension = Table.AddColumn(AddSizeBytes, "extension", each if [metadata] = null then null else Record.FieldOrDefault([metadata], "extension"), type text),
    Types = Table.TransformColumnTypes(AddExtension, {{"event_id", type text}, {"occurred_at", type datetimezone}, {"pipeline_id", type text}, {"stage_run_id", type text}, {"stage_name", type text}, {"status", type text}, {"file_name", type text}, {"target_path", type text}, {"source_hash", type text}}),
    Result = Table.RemoveColumns(Types, {"metadata"})
in
    Result

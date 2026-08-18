let
    Source = Parquet.Document(File.Contents(pAuditViewsFolder & "\latest_pipeline_run.parquet")),
    Types = Table.TransformColumnTypes(Source, {{"pipeline_id", type text}, {"pipeline_name", type text}, {"pipeline_version", type text}, {"environment", type text}, {"started_at", type datetimezone}, {"finished_at", type datetimezone}, {"status", type text}, {"message", type text}})
in
    Types

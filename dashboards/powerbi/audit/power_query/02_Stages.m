let
    Source = Parquet.Document(File.Contents(pAuditViewsFolder & "\latest_stage_runs.parquet")),
    Types = Table.TransformColumnTypes(Source, {{"pipeline_id", type text}, {"stage_run_id", type text}, {"stage_name", type text}, {"started_at", type datetimezone}, {"finished_at", type datetimezone}, {"status", type text}, {"duration_seconds", type number}, {"return_code", Int64.Type}, {"message", type text}, {"stdout_log", type text}, {"stderr_log", type text}})
in
    Types

# Modelo de datos

Las cuatro consultas leen las vistas Parquet reales de la última ejecución:

- latest_pipeline_run.parquet: 1 fila de pipeline.
- latest_stage_runs.parquet: 7 filas de etapas en la ejecución actual.
- latest_quality_results.parquet: reglas de calidad de la última ejecución exitosa.
- latest_file_runs.parquet: artefactos de la última ejecución exitosa.

Relaciones requeridas, con filtro simple desde Pipeline:

```text
Pipeline[pipeline_id]  1 ─── *  Stages[pipeline_id]
Pipeline[pipeline_id]  1 ─── *  Quality[pipeline_id]
Pipeline[pipeline_id]  1 ─── *  Files[pipeline_id]
```

No active relaciones bidireccionales.

## Campos derivados en Power Query

Quality extrae desde metadata_json: taxi_type, year, dimension, score y threshold.

Files extrae desde metadata_json: change_type, size_bytes y extension.

## Formato recomendado

- started_at, finished_at y occurred_at: fecha/hora.
- duration_seconds: número decimal.
- failure_rate, allowed_failure_rate y medidas de tasa: porcentaje con 2 decimales.
- size_bytes: entero; use la medida Tamaño Artefactos (MB) en tarjetas.

# Layout del Dashboard de Auditoría

## Página 1 — Resumen

Tarjetas: Estado Pipeline, Duración Pipeline (seg), Total Etapas, Etapas Fallidas, Reglas Fallidas, Total Artefactos y Última Ejecución.

Visuales:

- Barras: Stages[stage_name] por Stages[duration_seconds].
- Dona: Stages[status] por cantidad de etapas.
- Tabla: stage_name, status, duration_seconds, return_code y message.

## Página 2 — Rendimiento

- Barras horizontales de duración por etapa.
- Tarjeta: Etapa Más Lenta.
- Tabla: stage_name, started_at, finished_at, duration_seconds y return_code.
- Segmentador: Stages[status].

## Página 3 — Calidad

- Tarjetas: Reglas Aprobadas, Reglas Fallidas, Tasa Aprobación Calidad y Tasa Fallos Calidad Ponderada.
- Barras: Quality[dataset_name] por reglas fallidas.
- Comparativo: Quality[failure_rate] y Quality[allowed_failure_rate] por Quality[rule_name].
- Tabla: taxi_type, year, dimension, dataset_name, rule_name, status, records_evaluated, records_failed, failure_rate y allowed_failure_rate.
- Segmentadores: taxi_type, year, dimension y status.

## Página 4 — Errores

- Tabla filtrada a Stages[status] = FAILED.
- Campos: stage_name, return_code, message, duration_seconds, stdout_log y stderr_log.
- Barras: cantidad de fallos por etapa.

## Página 5 — Trazabilidad

- Tarjetas: Total Artefactos, Artefactos Creados, Artefactos Modificados y Tamaño Artefactos (MB).
- Barras: archivos por Files[stage_name].
- Dona: Files[change_type].
- Tabla: stage_name, file_name, target_path, extension, size_bytes, source_hash y change_type.

# Pipeline Silver

Ejecuta los comandos desde la raíz del proyecto. Para la ejecución integrada
con auditoría, usa `python -m src.pipeline --taxi all --years 2026`.

Para ejecutarlo manualmente, este es el orden vigente:

```powershell
python src/silver/01_profiling_inicial/profiling_inicial.py --taxi all --years 2026
python src/silver/02_detect_schema_changes/detect_schema_changes.py --taxi all --years 2023 2024 2025 2026
python src/silver/03_generate_unified_schema/generate_unified_schema.py --taxi all --force
python src/silver/04_limpieza/bronze_to_silver.py --taxi all --years 2026 --force
python src/silver/05_normalizacion_b1/normalizacion_b1.py --taxi all --years 2026 --force
python src/silver/05_analisis_de_calidad/quality_analysis.py --taxi all --years 2026 --force
python src/silver/06_profiling_final/profiling_final.py --taxi all --years 2026 --force
```

La normalización B1 toma `data/silver/cleaned/` y genera la capa canónica
`data/silver/trip_data_normalized/`. También registra cada resultado en
`data/logs/silver_b_normalization_audit.jsonl`.

Para comprobar qué entradas procesaría B1 sin lanzar Spark:

```powershell
python src/silver/05_normalizacion_b1/normalizacion_b1.py --taxi all --years 2026 --dry-run
```

# Actualización

Ejecute:

```powershell
python -m src.pipeline --years 2026 --taxi all --force-quality
python -m src.audit.dashboard_views
```

Luego, en Power BI Desktop seleccione Inicio > Actualizar.

No reimporte los Parquet: las consultas ya apuntan a audit/runtime/views.

Si Quality o Files quedan vacías, confirme que existe una ejecución exitosa previa; esas vistas se filtran a la última ejecución SUCCESS.

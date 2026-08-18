# Importación en Power BI Desktop

1. Abra Power BI Desktop y cree un archivo nuevo.
2. En Transformar datos, cree una consulta en blanco por cada archivo de power_query.
3. Pegue primero 00_Parametros.m y nómbrelo pAuditViewsFolder.
4. Pegue 01_Pipeline.m, 02_Stages.m, 03_Quality.m y 04_Files.m; nómbrelos Pipeline, Stages, Quality y Files.
5. Cierre y aplique los cambios.
6. Cree las tres relaciones descritas en data_model.md.
7. Importe theme/audit_theme.json desde Vista > Temas > Buscar temas.
8. Cree la tabla y las medidas del archivo dax/measures.dax.
9. Arme las cinco páginas siguiendo dashboard_layout.md.
10. Guarde como dashboards/powerbi/Dashboard_Auditoria.pbix.

La ruta de pAuditViewsFolder ya está configurada para este proyecto. Si el repositorio cambia de ubicación, solo edite esa consulta.

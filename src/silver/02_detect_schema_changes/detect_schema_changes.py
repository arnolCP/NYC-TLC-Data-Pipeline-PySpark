"""
DETECCIÓN DE CAMBIOS DE ESQUEMA - VERSIÓN PRODUCCIÓN
- Compara esquemas entre años Y entre meses del mismo año
- Detecta columnas nuevas, eliminadas o con cambios de tipo
- Genera reporte detallado para unified schema
- Logs en data/logs/schema_detection_manifest.jsonl
- Optimizado para producción
"""
import os
import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession

# ============================================================
# CONFIGURACIÓN
# ============================================================
ROOT_DIR = Path(__file__).parent.parent.parent.parent

# Parche de Hadoop para Windows
if os.name == 'nt':
    pass  # Hadoop lo configura command_runner.py
    pass  # Hadoop lo configura command_runner.py

# Configuración de Spark
SPARK_CONFIG = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}

class SchemaChangeDetector:
    def __init__(self, spark):
        self.spark = spark
        self.pipeline_id = f"schema_detection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.pipeline_name = "schema_change_detection"
        self.pipeline_version = "1.0.0"
        self.start_time = datetime.now()
        
        # Rutas
        self.base_dir = ROOT_DIR / "data" / "silver" / "schema_detection"
        self.reports_dir = self.base_dir / "_reports"
        self.metadata_dir = self.base_dir / "_metadata"
        self.log_dir = ROOT_DIR / "data" / "logs"
        self.log_path = self.log_dir / "schema_detection_manifest.jsonl"
        self.metadata_path = self.metadata_dir / "metadata.jsonl"
        
        # Crear directorios
        for path in [self.reports_dir, self.metadata_dir, self.log_dir]:
            path.mkdir(parents=True, exist_ok=True)
        
        self.trip_data_base = ROOT_DIR / "data" / "bronze" / "files" / "trip_data"
        self.taxi_types = ["fhv", "fhvhv", "green", "yellow"]
        
        self.execution_stats = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "root_dir": str(ROOT_DIR),
            "processed": [],
            "errors": []
        }
        
        self._log_info(f"Pipeline ID: {self.pipeline_id}")
    
    def _log_info(self, message):
        print(f"[INFO] {message}")
    
    def _log_error(self, message):
        print(f"[ERROR] {message}", file=sys.stderr)
    
    def get_schema_for_month(self, taxi_type, year, month):
        """Obtiene el esquema de un mes específico"""
        month_dir = self.trip_data_base / f"tipo_dataset={taxi_type}" / f"anio={year}" / f"mes={month:02d}"
        
        if not month_dir.exists():
            return None
        
        parquet_files = list(month_dir.glob("*.parquet"))
        if not parquet_files:
            parquet_files = list(month_dir.glob("*.snappy.parquet"))
        if not parquet_files:
            return None
        
        try:
            df = self.spark.read.parquet(str(parquet_files[0]))
            return {col: str(dtype) for col, dtype in df.dtypes}
        except Exception as e:
            self._log_error(f"Error en {taxi_type} {year}/{month:02d}: {str(e)[:80]}")
            return None
    
    def get_available_months(self, taxi_type, year):
        """Obtiene meses disponibles para un año"""
        year_path = self.trip_data_base / f"tipo_dataset={taxi_type}" / f"anio={year}"
        if not year_path.exists():
            return []
        
        months = []
        for month_dir in year_path.glob("mes=*"):
            if month_dir.is_dir():
                try:
                    month = int(month_dir.name.split("=")[1])
                    months.append(month)
                except:
                    continue
        return sorted(months)
    
    def compare_schemas(self, schema_prev, schema_curr, label_prev, label_curr):
        """Compara dos esquemas y detecta cambios"""
        changes = {
            "from": label_prev,
            "to": label_curr,
            "new_columns": [],
            "removed_columns": [],
            "type_changes": [],
            "has_changes": False
        }
        
        new_cols = set(schema_curr.keys()) - set(schema_prev.keys())
        if new_cols:
            changes["new_columns"] = sorted(list(new_cols))
            changes["has_changes"] = True
        
        removed_cols = set(schema_prev.keys()) - set(schema_curr.keys())
        if removed_cols:
            changes["removed_columns"] = sorted(list(removed_cols))
            changes["has_changes"] = True
        
        for col in set(schema_prev.keys()) & set(schema_curr.keys()):
            if schema_prev[col] != schema_curr[col]:
                changes["type_changes"].append({
                    "column": col,
                    "previous_type": schema_prev[col],
                    "current_type": schema_curr[col]
                })
                changes["has_changes"] = True
        
        return changes
    
    def detect_changes_by_year(self, taxi_type, years):
        """Detecta cambios entre años"""
        self._log_info(f"Analizando {taxi_type.upper()} - Anios: {years}")
        
        schemas = {}
        for year in years:
            months = self.get_available_months(taxi_type, year)
            if months:
                schema = self.get_schema_for_month(taxi_type, year, months[0])
                if schema:
                    schemas[year] = schema
        
        if len(schemas) < 2:
            self._log_info(f"   Advertencia: No hay suficientes anios para comparar")
            return None
        
        year_changes = []
        sorted_years = sorted(schemas.keys())
        
        for i in range(len(sorted_years) - 1):
            year_prev = sorted_years[i]
            year_curr = sorted_years[i + 1]
            
            change = self.compare_schemas(
                schemas[year_prev],
                schemas[year_curr],
                str(year_prev),
                str(year_curr)
            )
            year_changes.append(change)
        
        return {
            "taxi_type": taxi_type,
            "analysis_type": "year_over_year",
            "years": sorted_years,
            "changes": year_changes,
            "total_changes": sum(1 for c in year_changes if c["has_changes"])
        }
    
    def detect_changes_by_month(self, taxi_type, year):
        """Detecta cambios entre meses del mismo año"""
        months = self.get_available_months(taxi_type, year)
        if len(months) < 2:
            return None
        
        schemas = {}
        for month in months:
            schema = self.get_schema_for_month(taxi_type, year, month)
            if schema:
                schemas[month] = schema
        
        month_changes = []
        
        for i in range(len(months) - 1):
            month_prev = months[i]
            month_curr = months[i + 1]
            
            if month_prev in schemas and month_curr in schemas:
                change = self.compare_schemas(
                    schemas[month_prev],
                    schemas[month_curr],
                    f"{year}-{month_prev:02d}",
                    f"{year}-{month_curr:02d}"
                )
                month_changes.append(change)
        
        return {
            "taxi_type": taxi_type,
            "analysis_type": "month_over_month",
            "year": year,
            "months": months,
            "changes": month_changes,
            "total_changes": sum(1 for c in month_changes if c["has_changes"])
        }
    
    def process(self, taxi_types, years):
        """Procesa detección de cambios para todos los tipos de taxi"""
        self._log_info(f"Procesando {len(taxi_types)} tipos de taxi")
        
        for taxi_type in taxi_types:
            self._log_info(f"\n{'='*50}")
            self._log_info(f"Categoria: {taxi_type.upper()}")
            self._log_info(f"{'='*50}")
            
            result = {
                "taxi_type": taxi_type,
                "year_over_year": None,
                "month_over_month": {}
            }
            
            # 1. Analisis año a año
            yearly_result = self.detect_changes_by_year(taxi_type, years)
            if yearly_result:
                result["year_over_year"] = yearly_result
            
            # 2. Analisis mes a mes
            for year in years:
                monthly_result = self.detect_changes_by_month(taxi_type, year)
                if monthly_result:
                    result["month_over_month"][str(year)] = monthly_result
            
            # 3. Determinar si hay cambios
            has_changes = False
            if yearly_result and yearly_result["total_changes"] > 0:
                has_changes = True
            
            for year, monthly in result["month_over_month"].items():
                if monthly["total_changes"] > 0:
                    has_changes = True
                    for change in monthly["changes"]:
                        if change["has_changes"]:
                            if change["new_columns"]:
                                self._log_info(f"   + {change['from']}->{change['to']}: Nuevas: {change['new_columns']}")
                            if change["removed_columns"]:
                                self._log_info(f"   - {change['from']}->{change['to']}: Eliminadas: {change['removed_columns']}")
                            if change["type_changes"]:
                                for tc in change["type_changes"]:
                                    self._log_info(f"   * {change['from']}->{change['to']}: {tc['column']}: {tc['previous_type']} -> {tc['current_type']}")
            
            if not has_changes:
                self._log_info(f"   Sin cambios detectados")
            
            # 4. Guardar reporte
            report_path = self.reports_dir / f"{taxi_type}_schema_changes.json"
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, default=str)
            
            self._log_info(f"   Reporte guardado: {report_path}")
            
            self.execution_stats["processed"].append({
                "taxi_type": taxi_type,
                "years": years,
                "has_changes": has_changes,
                "report_path": str(report_path)
            })
            
            self._log_audit(
                taxi_type=taxi_type,
                status="completed",
                message=f"Reporte generado: {report_path}",
                file_path=str(report_path),
                metrics={
                    "years_analyzed": len(years),
                    "has_changes": has_changes
                }
            )
        
        self.execution_stats["finished_at"] = datetime.now().isoformat()
        self.execution_stats["total_processed"] = len(self.execution_stats["processed"])
        self.execution_stats["total_errors"] = len(self.execution_stats["errors"])
        
        self._save_metadata()
        self._show_summary()
        
        return self.execution_stats
    
    def _log_audit(self, taxi_type, status, message, file_path=None, error=None, metrics=None):
        """Registra auditoria en JSONL"""
        log_entry = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "trip_type": taxi_type,
            "file_name": file_path.split("\\")[-1] if file_path else None,
            "local_path": file_path,
            "status": status,
            "error": error,
            "metrics": metrics or {}
        }
        
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    
    def _save_metadata(self):
        """Guarda metadata de ejecucion en JSONL"""
        metadata_entry = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "root_dir": str(ROOT_DIR),
            "total_processed": len(self.execution_stats["processed"]),
            "total_errors": len(self.execution_stats["errors"]),
            "profiles": self.execution_stats["processed"],
            "errors": self.execution_stats["errors"]
        }
        
        with open(self.metadata_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(metadata_entry, ensure_ascii=False) + "\n")
        
        self._log_info(f"   Metadata: {self.metadata_path}")
    
    def _show_summary(self):
        """Muestra resumen final"""
        print(f"\n{'='*70}")
        print(f"RESUMEN DE DETECCION DE CAMBIOS")
        print(f"{'='*70}")
        print(f"Pipeline ID: {self.pipeline_id}")
        print(f"Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'-'*70}")
        
        if self.execution_stats["processed"]:
            print(f"Procesados: {len(self.execution_stats['processed'])}")
            for item in self.execution_stats["processed"]:
                status = "Cambios detectados" if item['has_changes'] else "Sin cambios"
                print(f"   - {item['taxi_type']}: {status}")
        
        if self.execution_stats["errors"]:
            print(f"Errores: {len(self.execution_stats['errors'])}")
            for item in self.execution_stats["errors"]:
                print(f"   - {item['taxi_type']}: {item['error']}")
        
        print(f"\nUbicaciones:")
        print(f"   Reportes: {self.reports_dir}")
        print(f"   Metadata: {self.metadata_dir}")
        print(f"   Logs: {self.log_path}")
        print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(
        description="Deteccion de cambios de esquema - Produccion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS:
  # Detectar cambios en yellow (2023-2025)
  python detect_schema_changes.py --taxi yellow --years 2023 2024 2025
  
  # Detectar cambios en todos los tipos de taxi
  python detect_schema_changes.py --taxi all --years 2023 2024 2025
        """
    )
    
    parser.add_argument("--taxi", nargs="+",
                       choices=["fhv", "fhvhv", "green", "yellow", "all"],
                       default=["all"],
                       help="Tipos de taxi a procesar (default: all)")
    
    parser.add_argument("--years", nargs="+", type=int,
                       default=[2023, 2024, 2025],
                       help="Anios a comparar (default: 2023 2024 2025)")
    
    args = parser.parse_args()
    
    if "all" in args.taxi:
        taxi_types = ["fhv", "fhvhv", "green", "yellow"]
    else:
        taxi_types = args.taxi
    
    # Crear sesion Spark
    spark = SparkSession.builder \
        .appName("SchemaChangeDetector") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print(f"\nIniciando deteccion de cambios de esquema...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    detector = SchemaChangeDetector(spark)
    detector.process(taxi_types, args.years)
    
    print(f"\nDETECCION DE CAMBIOS COMPLETADA")
    print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()

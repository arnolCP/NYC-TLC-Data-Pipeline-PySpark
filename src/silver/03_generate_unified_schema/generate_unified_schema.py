"""
GENERACIÓN DE ESQUEMA UNIFICADO - VERSIÓN PRODUCCIÓN MEJORADA
- Lee el reporte de cambios de esquema generado por detect_schema_changes.py
- COMBINA default_schema (todas las columnas) + reporte (solo cambios)
- Genera un esquema maestro unificado para cada tipo de taxi
- Elige el tipo de dato más "amplio" para cada columna
- Guarda el esquema en JSON para usarlo en bronze_to_silver.py
- Logs en data/logs/unified_schema_manifest.jsonl
"""
import os
import json
import argparse
import sys
from pathlib import Path
from datetime import datetime

# ============================================================
# CONFIGURACIÓN
# ============================================================
ROOT_DIR = Path(__file__).parent.parent.parent.parent

class UnifiedSchemaGenerator:
    def __init__(self):
        self.pipeline_id = f"unified_schema_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.pipeline_name = "unified_schema_generation"
        self.pipeline_version = "1.0.0"
        self.start_time = datetime.now()
        
        # Rutas
        self.base_dir = ROOT_DIR / "data" / "silver" / "unified_schema"
        self.schemas_dir = self.base_dir / "_schemas"
        self.metadata_dir = self.base_dir / "_metadata"
        self.log_dir = ROOT_DIR / "data" / "logs"
        self.log_path = self.log_dir / "unified_schema_manifest.jsonl"
        self.metadata_path = self.metadata_dir / "metadata.jsonl"
        
        # Directorio donde están los reportes de detect_schema_changes
        self.reports_dir = ROOT_DIR / "data" / "silver" / "schema_detection" / "_reports"
        
        # Crear directorios
        for path in [self.schemas_dir, self.metadata_dir, self.log_dir]:
            path.mkdir(parents=True, exist_ok=True)
        
        self.taxi_types = ["fhv", "fhvhv", "green", "yellow"]
        
        # Jerarquía de tipos: el más "amplio" tiene prioridad (menor número)
        self.type_hierarchy = {
            "string": 0,
            "timestamp": 1,
            "double": 2,
            "float": 3,
            "decimal": 4,
            "bigint": 5,
            "int": 6,
            "smallint": 7,
            "tinyint": 8,
            "boolean": 9
        }
        
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
    
    def load_report(self, taxi_type):
        """Carga el reporte de cambios de esquema"""
        report_path = self.reports_dir / f"{taxi_type}_schema_changes.json"
        
        if not report_path.exists():
            self._log_error(f"Reporte no encontrado: {report_path}")
            self._log_info(f"   Ejecuta primero detect_schema_changes.py para {taxi_type}")
            return None
        
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            self._log_error(f"Error leyendo reporte: {str(e)}")
            return None
    
    def get_widest_type(self, types_list):
        """
        Dada una lista de tipos, retorna el tipo más "amplio"
        Ejemplo: ['double', 'decimal(10,2)'] → 'double'
        """
        if not types_list:
            return None
        
        # Limpiar tipos (quitar precisiones como decimal(10,2) → decimal)
        cleaned_types = []
        for t in types_list:
            t_lower = t.lower()
            if t_lower.startswith("decimal"):
                cleaned_types.append("decimal")
            elif t_lower.startswith("timestamp"):
                cleaned_types.append("timestamp")
            elif t_lower.startswith("bigint"):
                cleaned_types.append("bigint")
            elif t_lower.startswith("smallint"):
                cleaned_types.append("smallint")
            elif t_lower.startswith("tinyint"):
                cleaned_types.append("tinyint")
            elif t_lower.startswith("int"):
                cleaned_types.append("int")
            else:
                cleaned_types.append(t_lower)
        
        # Ordenar por jerarquía (menor número = más amplio)
        sorted_types = sorted(
            cleaned_types,
            key=lambda x: self.type_hierarchy.get(x, 99)
        )
        
        # Retornar el tipo más amplio (el de menor jerarquía)
        widest = sorted_types[0]
        
        # Si el tipo original era decimal, mantener la notación decimal(10,2)
        if widest == "decimal":
            decimal_types = [t for t in types_list if t.lower().startswith("decimal")]
            if decimal_types:
                def get_precision(d):
                    try:
                        return int(d.split("(")[1].split(",")[0])
                    except:
                        return 0
                return max(decimal_types, key=get_precision)
            return "double"
        
        # Si el tipo original era int y hay bigint, elegir bigint
        if widest == "int" and any(t == "bigint" for t in cleaned_types):
            return "bigint"
        
        return widest
    
    def extract_all_column_types_from_report(self, report):
        """
        Extrae TODAS las columnas y sus tipos del reporte
        Incluye columnas de new_columns, removed_columns y type_changes
        """
        all_columns = {}
        
        # 1. Recorrer month_over_month
        for year_str, monthly_data in report.get("month_over_month", {}).items():
            if not monthly_data or not monthly_data.get("changes"):
                continue
            
            for change in monthly_data.get("changes", []):
                # 1a. Nuevas columnas
                for col in change.get("new_columns", []):
                    if col not in all_columns:
                        all_columns[col] = set()
                    # Intentar obtener el tipo de la columna de type_changes
                    for tc in change.get("type_changes", []):
                        if tc["column"] == col:
                            all_columns[col].add(tc["current_type"])
                    # Si no tiene tipo, asumir string
                    if not all_columns[col]:
                        all_columns[col].add("string")
                
                # 1b. Columnas removidas
                for col in change.get("removed_columns", []):
                    if col not in all_columns:
                        all_columns[col] = set()
                    all_columns[col].add("string")  # Marcamos como removida
        
        # 2. Recorrer year_over_year
        if report.get("year_over_year"):
            for change in report["year_over_year"].get("changes", []):
                # 2a. Nuevas columnas
                for col in change.get("new_columns", []):
                    if col not in all_columns:
                        all_columns[col] = set()
                    all_columns[col].add("string")
                
                # 2b. Columnas removidas
                for col in change.get("removed_columns", []):
                    if col not in all_columns:
                        all_columns[col] = set()
                    all_columns[col].add("string")
        
        return all_columns
    
    def generate_unified_schema(self, taxi_type):
        """Genera el esquema unificado para un tipo de taxi"""
        self._log_info(f"Generando esquema unificado para: {taxi_type.upper()}")
        
        # 1. Obtener default_schema (TODAS las columnas conocidas)
        default_schema = self._get_default_schema(taxi_type)
        self._log_info(f"   Default schema: {len(default_schema)} columnas")
        
        # 2. Cargar reporte de cambios
        report = self.load_report(taxi_type)
        
        # 3. Si no hay reporte, usar solo default_schema
        if not report:
            self._log_info("   No hay reporte. Usando esquema por defecto.")
            unified_schema = default_schema.copy()
        else:
            # 4. COMBINAR default_schema + reporte
            unified_schema = default_schema.copy()
            
            # 4a. Extraer todas las columnas del reporte
            report_columns = self.extract_all_column_types_from_report(report)
            self._log_info(f"   Reporte: {len(report_columns)} columnas con cambios")
            
            # 4b. Añadir columnas nuevas del reporte
            for col, types_set in report_columns.items():
                # Si la columna ya existe en default_schema, comparar tipos
                if col in unified_schema:
                    existing_type = unified_schema[col]
                    # Añadir el tipo existente a la lista de tipos
                    types_set.add(existing_type)
                    # Elegir el tipo más amplio
                    unified_schema[col] = self.get_widest_type(list(types_set))
                else:
                    # Columna nueva: añadir con el tipo del reporte
                    unified_schema[col] = self.get_widest_type(list(types_set))
            
            # 4c. Eliminar columnas que fueron removidas (si existen en el default)
            for year_str, monthly_data in report.get("month_over_month", {}).items():
                if not monthly_data or not monthly_data.get("changes"):
                    continue
                for change in monthly_data.get("changes", []):
                    for col in change.get("removed_columns", []):
                        if col in unified_schema:
                            # Verificar si la columna existe en todos los meses
                            # Si fue removida en un mes pero existe en otros, la mantenemos
                            # Por ahora, si fue removida en algún mes, la marcamos como opcional
                            # Pero la mantenemos en el esquema (se pondrá NULL en los meses que no la tengan)
                            pass
        
        # 5. Ordenar columnas alfabéticamente para consistencia
        unified_schema = dict(sorted(unified_schema.items()))
        
        # 6. Guardar esquema unificado
        schema_path = self.schemas_dir / f"{taxi_type}_unified_schema.json"
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump(unified_schema, f, indent=2, default=str)
        
        self._log_info(f"   Esquema guardado: {schema_path}")
        self._log_info(f"   Columnas: {len(unified_schema)}")
        
        # Registrar en estadísticas
        self.execution_stats["processed"].append({
            "taxi_type": taxi_type,
            "columns": len(unified_schema),
            "schema_path": str(schema_path)
        })
        
        self._log_audit(
            taxi_type=taxi_type,
            status="completed",
            message=f"Esquema unificado generado: {schema_path}",
            file_path=str(schema_path),
            metrics={
                "columns": len(unified_schema),
                "has_report": report is not None
            }
        )
        
        return unified_schema
    
    def _get_default_schema(self, taxi_type):
        """Esquema por defecto si no se detectan cambios"""
        default_schemas = {
            "yellow": {
                "VendorID": "int",
                "tpep_pickup_datetime": "timestamp",
                "tpep_dropoff_datetime": "timestamp",
                "passenger_count": "int",
                "trip_distance": "double",
                "RatecodeID": "int",
                "store_and_fwd_flag": "string",
                "PULocationID": "int",
                "DOLocationID": "int",
                "payment_type": "int",
                "fare_amount": "double",
                "extra": "double",
                "mta_tax": "double",
                "tip_amount": "double",
                "tolls_amount": "double",
                "improvement_surcharge": "double",
                "total_amount": "double",
                "congestion_surcharge": "double",
                "airport_fee": "double"
            },
            "green": {
                "VendorID": "int",
                "lpep_pickup_datetime": "timestamp",
                "lpep_dropoff_datetime": "timestamp",
                "store_and_fwd_flag": "string",
                "RatecodeID": "int",
                "PULocationID": "int",
                "DOLocationID": "int",
                "passenger_count": "int",
                "trip_distance": "double",
                "fare_amount": "double",
                "extra": "double",
                "mta_tax": "double",
                "tip_amount": "double",
                "tolls_amount": "double",
                "ehail_fee": "double",
                "improvement_surcharge": "double",
                "total_amount": "double",
                "payment_type": "int",
                "trip_type": "int",
                "congestion_surcharge": "double"
            },
            "fhv": {
                "pickup_datetime": "timestamp",
                "dropoff_datetime": "timestamp",
                "PULocationID": "int",
                "DOLocationID": "int",
                "SR_Flag": "int",
                "Affiliated_base_number": "string"
            },
            "fhvhv": {
                "hvfhs_license_num": "string",
                "dispatching_base_num": "string",
                "originating_base_num": "string",
                "request_datetime": "timestamp",
                "on_scene_datetime": "timestamp",
                "pickup_datetime": "timestamp",
                "dropoff_datetime": "timestamp",
                "PULocationID": "int",
                "DOLocationID": "int",
                "trip_miles": "double",
                "trip_time": "int",
                "base_passenger_fare": "double",
                "tolls": "double",
                "bcf": "double",
                "sales_tax": "double",
                "congestion_surcharge": "double",
                "airport_fee": "double",
                "tips": "double",
                "driver_pay": "double",
                "shared_request_flag": "string",
                "shared_match_flag": "string",
                "access_a_ride_flag": "string",
                "wav_request_flag": "string",
                "wav_match_flag": "string",
                "cbd_congestion_fee": "double"
            }
        }
        
        schema = default_schemas.get(taxi_type, {})
        result = {}
        for col, dtype in schema.items():
            result[col] = dtype
        
        return result
    
    def process(self, taxi_types, force=False):
        """Procesa generación de esquemas unificados para todos los tipos de taxi"""
        self._log_info(f"Procesando {len(taxi_types)} tipos de taxi")
        
        for taxi_type in taxi_types:
            self._log_info(f"\n{'='*50}")
            self._log_info(f"Categoria: {taxi_type.upper()}")
            self._log_info(f"{'='*50}")
            
            # Verificar si ya existe el esquema
            schema_path = self.schemas_dir / f"{taxi_type}_unified_schema.json"
            if schema_path.exists() and not force:
                self._log_info(f"   Esquema ya existe: {schema_path}")
                self._log_info(f"   Usa --force para regenerar")
                continue
            
            try:
                self.generate_unified_schema(taxi_type)
            except Exception as e:
                self._log_error(f"   Error generando esquema: {str(e)}")
                self.execution_stats["errors"].append({
                    "taxi_type": taxi_type,
                    "error": str(e)
                })
                self._log_audit(
                    taxi_type=taxi_type,
                    status="failed",
                    message="Error generando esquema unificado",
                    error=str(e)
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
        print(f"RESUMEN DE GENERACION DE ESQUEMAS UNIFICADOS")
        print(f"{'='*70}")
        print(f"Pipeline ID: {self.pipeline_id}")
        print(f"Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'-'*70}")
        
        if self.execution_stats["processed"]:
            print(f"Procesados: {len(self.execution_stats['processed'])}")
            for item in self.execution_stats["processed"]:
                print(f"   - {item['taxi_type']}: {item['columns']} columnas")
        
        if self.execution_stats["errors"]:
            print(f"Errores: {len(self.execution_stats['errors'])}")
            for item in self.execution_stats["errors"]:
                print(f"   - {item['taxi_type']}: {item['error']}")
        
        print(f"\nUbicaciones:")
        print(f"   Esquemas: {self.schemas_dir}")
        print(f"   Metadata: {self.metadata_dir}")
        print(f"   Logs: {self.log_path}")
        print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(
        description="Generacion de esquema unificado - Produccion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS:
  # Generar esquema para yellow
  python generate_unified_schema.py --taxi yellow
  
  # Generar esquema para todos los tipos de taxi
  python generate_unified_schema.py --taxi all
  
  # Forzar regeneracion
  python generate_unified_schema.py --taxi all --force
        """
    )
    
    parser.add_argument("--taxi", nargs="+",
                       choices=["fhv", "fhvhv", "green", "yellow", "all"],
                       default=["all"],
                       help="Tipos de taxi a procesar (default: all)")
    
    parser.add_argument("--force", action="store_true",
                       help="Forzar regeneracion de esquemas existentes")
    
    args = parser.parse_args()
    
    if "all" in args.taxi:
        taxi_types = ["fhv", "fhvhv", "green", "yellow"]
    else:
        taxi_types = args.taxi
    
    print(f"\nIniciando generacion de esquemas unificados...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    generator = UnifiedSchemaGenerator()
    generator.process(taxi_types, args.force)
    
    print(f"\nGENERACION DE ESQUEMAS COMPLETADA")
    print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
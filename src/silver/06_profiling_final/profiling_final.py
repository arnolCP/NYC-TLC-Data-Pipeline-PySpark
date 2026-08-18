"""
PROFILING FINAL DE SILVER - VERSIÓN PRODUCCIÓN
- Lee datos de SILVER cleaned (data/silver/cleaned/)
- Genera perfil POST-limpieza
- Compara con perfil de BRONZE (si existe)
- Logs en data/logs/profiling_final_manifest.jsonl
- Incremental: Solo procesa años nuevos
"""
import os
import json
import argparse
import time
import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# CONFIGURACIÓN
# ============================================================
ROOT_DIR = Path(__file__).parent.parent.parent.parent

if os.name == 'nt':
    pass  # Hadoop lo configura command_runner.py
    pass  # Hadoop lo configura command_runner.py

# Tipos de taxi activos (excluye FHVHV por defecto)
TAXI_TYPES_ACTIVE = ["fhv", "green", "yellow"]

class ProfilingFinal:
    def __init__(self, spark):
        self.spark = spark
        self.pipeline_id = f"profiling_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.pipeline_name = "profiling_final"
        self.pipeline_version = "1.0.0"
        self.start_time = datetime.now()
        
        # Rutas
        self.input_dir = ROOT_DIR / "data" / "silver" / "cleaned"
        self.profile_dir = ROOT_DIR / "data" / "silver" / "profiling_final" / "_profiles"
        self.metadata_dir = ROOT_DIR / "data" / "silver" / "profiling_final" / "_metadata"
        self.log_dir = ROOT_DIR / "data" / "logs"
        self.log_path = self.log_dir / "profiling_final_manifest.jsonl"
        self.metadata_path = self.metadata_dir / "metadata.jsonl"
        self.tracker_path = self.metadata_dir / "profiling_final_tracker.json"
        
        # Directorio de perfiles de BRONZE
        self.bronze_profile_dir = ROOT_DIR / "data" / "silver" / "profiling_inicial" / "_profiles"
        
        # Crear directorios
        for path in [self.profile_dir, self.metadata_dir, self.log_dir]:
            path.mkdir(parents=True, exist_ok=True)
        
        # Inicializar tracker
        if not self.tracker_path.exists():
            with open(self.tracker_path, "w", encoding="utf-8") as f:
                json.dump({}, f, indent=2)
        
        # Estadísticas
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
    
    def _get_processed_years(self, taxi_type):
        if not self.tracker_path.exists():
            return []
        try:
            with open(self.tracker_path, "r", encoding="utf-8") as f:
                tracker = json.load(f)
            return tracker.get(taxi_type, [])
        except:
            return []
    
    def _save_processed_years(self, taxi_type, years):
        try:
            with open(self.tracker_path, "r", encoding="utf-8") as f:
                tracker = json.load(f)
        except:
            tracker = {}
        
        if taxi_type not in tracker:
            tracker[taxi_type] = []
        for year in years:
            if year not in tracker[taxi_type]:
                tracker[taxi_type].append(year)
        tracker[taxi_type] = sorted(tracker[taxi_type])
        
        with open(self.tracker_path, "w", encoding="utf-8") as f:
            json.dump(tracker, f, indent=2, default=str)
    
    def _get_available_years(self, taxi_type):
        base_path = self.input_dir / taxi_type
        if not base_path.exists():
            return []
        years = []
        for year_dir in base_path.glob("year=*"):
            try:
                year = int(year_dir.name.split("=")[1])
                years.append(year)
            except:
                continue
        return sorted(years)
    
    def _read_parquet_safe(self, input_path):
        """Lee archivos Parquet de forma segura (uno por uno)"""
        try:
            parquet_files = list(input_path.glob("*.parquet"))
            if not parquet_files:
                return None
            
            df = self.spark.read.parquet(str(parquet_files[0]))
            for file_path in parquet_files[1:]:
                try:
                    df_temp = self.spark.read.parquet(str(file_path))
                    df = df.union(df_temp)
                except:
                    continue
            return df
        except:
            return None
    
    def _load_bronze_profile(self, taxi_type, year):
        """Carga el perfil de BRONZE si existe"""
        # La ruta correcta es: data/silver/profiling_inicial/_profiles/{taxi_type}/{taxi_type}_{year}_profile.json
        bronze_path = self.bronze_profile_dir / taxi_type / f"{taxi_type}_{year}_profile.json"
        
        self._log_info(f"      Buscando perfil de BRONZE en: {bronze_path}")
        
        if bronze_path.exists():
            try:
                with open(bronze_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self._log_info(f"      Error al leer perfil de BRONZE: {str(e)[:50]}")
                return None
        else:
            self._log_info(f"      No existe perfil de BRONZE en: {bronze_path}")
            return None
    
    def generate_profile(self, df, taxi_type, year):
        """Genera perfil de SILVER y compara con BRONZE"""
        self._log_info(f"   Generando perfil de SILVER...")
        
        total_records = df.count()
        
        if total_records == 0:
            self._log_info(f"      No hay registros para perfilar")
            return None
        
        # 1. Generar perfil de SILVER
        profile = {
            "taxi_type": taxi_type,
            "year": year,
            "stage": "silver",
            "total_records": total_records,
            "columns": {},
            "quality_score": 100.0,
            "issues": [],
            "improvement": {}
        }
        
        # 2. Analizar cada columna
        for col_name in df.columns:
            col_type = str(dict(df.dtypes)[col_name])
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_pct = (null_count / total_records) * 100 if total_records > 0 else 0
            
            distinct_count = df.select(col_name).distinct().count()
            distinct_pct = (distinct_count / total_records) * 100 if total_records > 0 else 0
            
            # Detectar problemas
            if null_pct > 10:
                profile["issues"].append(f"Alto % de nulos en {col_name}: {null_pct:.1f}%")
                profile["quality_score"] -= 5
            
            if distinct_count == 1:
                profile["issues"].append(f"Columna constante: {col_name}")
                profile["quality_score"] -= 10
            
            profile["columns"][col_name] = {
                "data_type": col_type,
                "null_count": null_count,
                "null_pct": round(null_pct, 2),
                "distinct_count": distinct_count,
                "distinct_pct": round(distinct_pct, 2)
            }
        
        profile["quality_score"] = max(0, min(100, profile["quality_score"]))
        
        # 3. Cargar perfil de BRONZE para comparar
        bronze_profile = self._load_bronze_profile(taxi_type, year)
        
        if bronze_profile:
            self._log_info(f"      Perfil de BRONZE encontrado para comparar")
            
            # Calcular mejora en nulos
            bronze_null_pcts = {}
            if "columns" in bronze_profile:
                for col, data in bronze_profile["columns"].items():
                    if "null_pct" in data:
                        bronze_null_pcts[col] = data["null_pct"]
            
            for col, data in profile["columns"].items():
                if col in bronze_null_pcts:
                    bronze_null = bronze_null_pcts[col]
                    silver_null = data["null_pct"]
                    improvement = bronze_null - silver_null
                    profile["improvement"][col] = {
                        "bronze_null_pct": bronze_null,
                        "silver_null_pct": silver_null,
                        "improvement_pct": round(improvement, 2),
                        "improved": improvement > 0
                    }
            
            # Calcular mejora promedio
            improvements = [v["improvement_pct"] for v in profile["improvement"].values()]
            avg_improvement = sum(improvements) / len(improvements) if improvements else 0
            profile["avg_improvement"] = round(avg_improvement, 2)
            
            self._log_info(f"      Mejora promedio en nulos: {profile['avg_improvement']:.1f}%")
        else:
            self._log_info(f"      No se encontró perfil de BRONZE para comparar")
        
        return profile
    
    def process_year(self, taxi_type, year, force=False):
        """Procesa UN AÑO completo de UN tipo de taxi"""
        self._log_info(f"\nProcesando: {taxi_type.upper()} - {year}")
        start_time = time.time()
        
        processed_years = self._get_processed_years(taxi_type)
        if year in processed_years and not force:
            self._log_info(f"   {year} ya procesado. Usa --force para reprocesar.")
            return None
        
        # 1. Leer datos de SILVER cleaned
        input_path = self.input_dir / taxi_type / f"year={year}"
        if not input_path.exists():
            self._log_error(f"   No existe: {input_path}")
            return None
        
        self._log_info(f"   Leyendo datos de: {input_path}")
        df = self._read_parquet_safe(input_path)
        
        if df is None:
            self._log_error(f"   No se pudieron leer los datos")
            return None
        
        try:
            original_count = df.count()
        except:
            self._log_error(f"   Error al contar registros")
            return None
        
        self._log_info(f"   Registros: {original_count:,}")
        
        # 2. Generar perfil
        profile = self.generate_profile(df, taxi_type, year)
        
        if profile is None:
            self._log_error(f"   No se pudo generar el perfil")
            return None
        
        # 3. Guardar perfil
        profile_path = self.profile_dir / f"{taxi_type}_{year}_profile.json"
        with open(profile_path, "w", encoding="utf-8") as f:
            json.dump(profile, f, indent=2, default=str)
        
        elapsed = time.time() - start_time
        
        self._log_info(f"\n   {taxi_type.upper()} {year} COMPLETADO")
        self._log_info(f"   Perfil: {profile_path}")
        self._log_info(f"   Calidad: {profile['quality_score']:.1f}%")
        self._log_info(f"   Mejora vs BRONZE: {profile.get('avg_improvement', 'N/A')}")
        self._log_info(f"   Tiempo: {elapsed:.2f} segundos")
        
        # 4. Actualizar tracker
        self._save_processed_years(taxi_type, [year])
        
        # 5. Registrar estadísticas
        self.execution_stats["processed"].append({
            "taxi_type": taxi_type,
            "year": year,
            "records": original_count,
            "quality_score": profile["quality_score"],
            "improvement": profile.get("avg_improvement", None),
            "execution_time": round(elapsed, 2)
        })
        
        self._log_audit(
            taxi_type=taxi_type,
            year=year,
            status="completed",
            message=f"Perfil generado: {profile_path}",
            file_path=str(profile_path),
            metrics={
                "records_processed": original_count,
                "quality_score": profile["quality_score"],
                "improvement": profile.get("avg_improvement", None),
                "execution_time_seconds": round(elapsed, 2)
            }
        )
        
        return profile
    
    def process(self, taxi_types, years, force=False):
        """Procesa todos los tipos y años solicitados"""
        self._log_info(f"Procesando {len(taxi_types)} tipos de taxi")
        
        for taxi_type in taxi_types:
            self._log_info(f"\n{'='*50}")
            self._log_info(f"Categoria: {taxi_type.upper()}")
            self._log_info(f"{'='*50}")
            
            available_years = self._get_available_years(taxi_type)
            if not available_years:
                self._log_info(f"   No hay datos disponibles para {taxi_type}")
                continue
            
            years_to_process = [y for y in years if y in available_years]
            if not years_to_process:
                self._log_info(f"   No hay años solicitados disponibles")
                continue
            
            for year in years_to_process:
                try:
                    self.process_year(taxi_type, year, force)
                except Exception as e:
                    self._log_error(f"   Error en {taxi_type} {year}: {str(e)}")
                    self.execution_stats["errors"].append({
                        "taxi_type": taxi_type,
                        "year": year,
                        "error": str(e)
                    })
                    self._log_audit(
                        taxi_type=taxi_type,
                        year=year,
                        status="failed",
                        message="Error en profiling final",
                        error=str(e)
                    )
        
        self.execution_stats["finished_at"] = datetime.now().isoformat()
        self.execution_stats["total_processed"] = len(self.execution_stats["processed"])
        self.execution_stats["total_errors"] = len(self.execution_stats["errors"])
        
        self._save_metadata()
        self._show_summary()
        
        return self.execution_stats
    
    def _log_audit(self, taxi_type, year, status, message, file_path=None, error=None, metrics=None):
        log_entry = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "trip_type": taxi_type,
            "year": year,
            "file_name": file_path.split("\\")[-1] if file_path else None,
            "local_path": file_path,
            "status": status,
            "error": error,
            "metrics": metrics or {}
        }
        
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    
    def _save_metadata(self):
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
        print(f"\n{'='*70}")
        print(f"RESUMEN DE PROFILING FINAL")
        print(f"{'='*70}")
        print(f"Pipeline ID: {self.pipeline_id}")
        print(f"Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'-'*70}")
        
        if self.execution_stats["processed"]:
            print(f"Procesados: {len(self.execution_stats['processed'])}")
            for item in self.execution_stats["processed"]:
                impr = f"+{item['improvement']}%" if item.get('improvement') else "N/A"
                print(f"   - {item['taxi_type']} {item['year']}: Score {item['quality_score']:.1f}% ({impr})")
        
        if self.execution_stats["errors"]:
            print(f"Errores: {len(self.execution_stats['errors'])}")
            for item in self.execution_stats["errors"]:
                print(f"   - {item['taxi_type']} {item['year']}: {item['error']}")
        
        print(f"\nUbicaciones:")
        print(f"   Perfiles: {self.profile_dir}")
        print(f"   Metadata: {self.metadata_dir}")
        print(f"   Logs: {self.log_path}")
        print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(
        description="Profiling Final de SILVER - Produccion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS:
  # Procesar TODOS los tipos activos
  python profiling_final.py --taxi all --years 2023 2024 2025
  
  # Procesar solo Yellow 2023
  python profiling_final.py --taxi yellow --years 2023
  
  # Forzar reprocesamiento
  python profiling_final.py --taxi yellow --years 2023 --force
        """
    )
    
    parser.add_argument("--taxi", nargs="+",
                       choices=["fhv", "fhvhv", "green", "yellow", "all"],
                       default=["all"],
                       help="Tipos de taxi a procesar (default: all)")
    
    parser.add_argument("--years", nargs="+", type=int,
                       default=[2023, 2024, 2025],
                       help="Años a procesar (default: 2023 2024 2025)")
    
    parser.add_argument("--force", action="store_true",
                       help="Forzar reprocesamiento")
    
    args = parser.parse_args()
    
    if "all" in args.taxi:
        taxi_types = TAXI_TYPES_ACTIVE.copy()
        print("[INFO] NOTA: 'all' excluye FHVHV (demasiado grande)")
    else:
        taxi_types = args.taxi
    
    spark = SparkSession.builder \
        .appName("ProfilingFinal") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    print(f"\nIniciando profiling final de SILVER...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tipos a procesar: {taxi_types}")
    
    profiler = ProfilingFinal(spark)
    profiler.process(taxi_types, args.years, args.force)
    
    print(f"\nPROFILING FINAL COMPLETADO")
    print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()

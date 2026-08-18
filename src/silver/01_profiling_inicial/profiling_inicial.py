"""
PROFILING BRONZE - VERSIÓN PRODUCCIÓN
- Procesa meses en memoria (sin guardar JSONs mensuales)
- UN SOLO tracker: month_tracker.json
- UN SOLO perfil consolidado por año
- Incremental por mes
- Logs en data/logs/profile_inicial_manifest.jsonl
- Optimizado para producción
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

# Parche de Hadoop para Windows (solo necesario en Windows)
if os.name == 'nt':
    pass  # Hadoop lo configura command_runner.py
    pass  # Hadoop lo configura command_runner.py

# Configuración de Spark
SPARK_CONFIG = {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "50",
    "spark.sql.adaptive.skewJoin.enabled": "true"
}

# ============================================================
# CLASE PRINCIPAL
# ============================================================
class BronzeProfiler:
    def __init__(self, spark):
        self.spark = spark
        self.pipeline_id = f"bronze_profiling_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.pipeline_name = "bronze_profiling_nivel_2_3"
        self.pipeline_version = "1.0.0"
        self.start_time = datetime.now()
        
        # Rutas
        self.base_dir = ROOT_DIR / "data" / "silver" / "profiling_inicial"
        self.profile_dir = self.base_dir / "_profiles"
        self.metadata_dir = self.base_dir / "_metadata"
        self.log_dir = ROOT_DIR / "data" / "logs"
        self.log_path = self.log_dir / "profile_inicial_manifest.jsonl"
        self.metadata_path = self.metadata_dir / "metadata.jsonl"
        self.month_tracker_path = self.metadata_dir / "month_tracker.json"
        
        # Crear directorios
        for path in [self.profile_dir, self.metadata_dir, self.log_dir]:
            path.mkdir(parents=True, exist_ok=True)
        
        # Inicializar tracker si no existe
        if not self.month_tracker_path.exists():
            with open(self.month_tracker_path, "w", encoding="utf-8") as f:
                json.dump({}, f, indent=2)
        
        # Categorías
        self.taxi_types = ["fhv", "fhvhv", "green", "yellow", "lookup"]
        
        # Rutas de datos
        self.lookup_path = ROOT_DIR / "data" / "bronze" / "files" / "lookup" / "taxi_zone_lookup.csv"
        self.trip_data_base = ROOT_DIR / "data" / "bronze" / "files" / "trip_data"
        
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
        
        self._log_info(f"Iniciando profiling - Pipeline ID: {self.pipeline_id}")
    
    def _log_info(self, message):
        """Log informativo en consola"""
        print(f"[INFO] {message}")
    
    def _log_error(self, message):
        """Log de error en consola"""
        print(f"[ERROR] {message}", file=sys.stderr)
    
    def profile_month(self, df_month, taxi_type, year, month):
        """Genera perfil de UN MES (en memoria)"""
        total_records = df_month.count()
        
        if total_records == 0:
            return None
        
        # Muestreo (10% para velocidad)
        if total_records > 1000000:
            df_sample = df_month.sample(fraction=0.1, seed=42)
            sample_size = df_sample.count()
        else:
            df_sample = df_month
            sample_size = total_records
        
        profile = {
            "month": month,
            "total_records": total_records,
            "sample_size": sample_size,
            "columns": {},
            "quality_score": 100.0
        }
        
        columns = [str(col) for col in df_sample.columns]
        
        for col_name in columns:
            col_name_str = str(col_name)
            col_type = str(dict(df_sample.dtypes).get(col_name_str, 'unknown'))
            
            null_count = df_sample.filter(F.col(col_name_str).isNull()).count()
            null_pct = (null_count / sample_size) * 100 if sample_size > 0 else 0
            distinct_count = df_sample.select(col_name_str).distinct().count()
            distinct_pct = (distinct_count / sample_size) * 100 if sample_size > 0 else 0
            
            stats = {}
            if col_type in ['double', 'float', 'int', 'bigint']:
                try:
                    stats_df = df_sample.select(
                        F.min(F.col(col_name_str)).alias("min"),
                        F.max(F.col(col_name_str)).alias("max"),
                        F.mean(F.col(col_name_str)).alias("mean"),
                        F.stddev(F.col(col_name_str)).alias("stddev")
                    ).collect()[0]
                    stats = {
                        "min": float(stats_df.min) if stats_df.min is not None else None,
                        "max": float(stats_df.max) if stats_df.max is not None else None,
                        "mean": float(stats_df.mean) if stats_df.mean is not None else None,
                        "stddev": float(stats_df.stddev) if stats_df.stddev is not None else None
                    }
                except Exception:
                    stats = {"error": "No se pudo calcular estadísticas"}
            
            if null_pct > 30:
                profile["quality_score"] -= 5
            elif null_pct > 15:
                profile["quality_score"] -= 2
            if distinct_count == 1 and distinct_pct == 100:
                profile["quality_score"] -= 10
            
            profile["columns"][col_name_str] = {
                "data_type": col_type,
                "null_count": null_count,
                "null_pct": round(null_pct, 2),
                "distinct_count": distinct_count,
                "distinct_pct": round(distinct_pct, 2),
                "min_value": stats.get("min"),
                "max_value": stats.get("max"),
                "mean_value": stats.get("mean"),
                "stddev_value": stats.get("stddev")
            }
        
        profile["quality_score"] = max(0, min(100, profile["quality_score"]))
        
        return profile
    
    def get_processed_months(self, taxi_type, year):
        """Obtiene la lista de meses ya procesados desde el tracker único"""
        if not self.month_tracker_path.exists():
            return []
        
        try:
            with open(self.month_tracker_path, "r", encoding="utf-8") as f:
                tracker = json.load(f)
            return tracker.get(taxi_type, {}).get(str(year), [])
        except:
            return []
    
    def save_processed_months(self, taxi_type, year, months):
        """Guarda la lista de meses procesados en el tracker único"""
        try:
            with open(self.month_tracker_path, "r", encoding="utf-8") as f:
                tracker = json.load(f)
        except:
            tracker = {}
        
        if taxi_type not in tracker:
            tracker[taxi_type] = {}
        
        tracker[taxi_type][str(year)] = sorted(months)
        
        with open(self.month_tracker_path, "w", encoding="utf-8") as f:
            json.dump(tracker, f, indent=2, default=str)
    
    def get_available_months(self, taxi_type, year):
        """Obtiene la lista de meses disponibles en BRONZE"""
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
    
    def consolidate_profiles(self, taxi_type, year, month_profiles):
        """Consolida perfiles de múltiples meses en un solo JSON"""
        if not month_profiles:
            return None
        
        consolidated = {
            "taxi_type": taxi_type,
            "year": year,
            "months_processed": sorted([mp["month"] for mp in month_profiles]),
            "consolidated_columns": {},
            "total_records": 0,
            "quality_score": 0.0
        }
        
        # 1. Unificar columnas
        all_columns = set()
        for mp in month_profiles:
            all_columns.update(mp["columns"].keys())
        
        # 2. Inicializar estructura consolidada
        for col_name in all_columns:
            consolidated["consolidated_columns"][col_name] = {
                "data_type": None,
                "total_null_count": 0,
                "null_pct_avg": 0,
                "min_values": [],
                "max_values": [],
                "mean_values": [],
                "stddev_values": [],
                "distinct_values": []
            }
        
        # 3. Agregar datos de cada mes
        quality_scores = []
        for mp in month_profiles:
            quality_scores.append(mp["quality_score"])
            consolidated["total_records"] += mp["total_records"]
            
            for col_name, col_data in mp["columns"].items():
                if col_name in consolidated["consolidated_columns"]:
                    cc = consolidated["consolidated_columns"][col_name]
                    
                    cc["total_null_count"] += col_data["null_count"]
                    
                    if col_data["data_type"] and not cc["data_type"]:
                        cc["data_type"] = col_data["data_type"]
                    
                    if col_data.get("min_value") is not None:
                        cc["min_values"].append(col_data["min_value"])
                    if col_data.get("max_value") is not None:
                        cc["max_values"].append(col_data["max_value"])
                    if col_data.get("mean_value") is not None:
                        cc["mean_values"].append(col_data["mean_value"])
                    if col_data.get("stddev_value") is not None:
                        cc["stddev_values"].append(col_data["stddev_value"])
                    if col_data.get("distinct_count") is not None:
                        cc["distinct_values"].append(col_data["distinct_count"])
        
        # 4. Calcular promedios consolidados
        total_records_all = consolidated["total_records"]
        
        for col_name, cc in consolidated["consolidated_columns"].items():
            if total_records_all > 0:
                cc["null_pct_avg"] = round((cc["total_null_count"] / total_records_all) * 100, 2)
            
            if cc["min_values"]:
                cc["min"] = min(cc["min_values"])
            if cc["max_values"]:
                cc["max"] = max(cc["max_values"])
            if cc["mean_values"]:
                cc["mean"] = sum(cc["mean_values"]) / len(cc["mean_values"])
            if cc["stddev_values"]:
                cc["stddev"] = sum(cc["stddev_values"]) / len(cc["stddev_values"])
            if cc["distinct_values"]:
                cc["distinct_avg"] = round(sum(cc["distinct_values"]) / len(cc["distinct_values"]), 2)
            
            # Limpiar listas temporales
            for key in ["min_values", "max_values", "mean_values", "stddev_values", "distinct_values"]:
                if key in cc:
                    del cc[key]
        
        # 5. Calcular calidad consolidada
        consolidated["quality_score"] = round(sum(quality_scores) / len(quality_scores), 2)
        
        return consolidated
    
    def process_year_incremental(self, taxi_type, year, force=False):
        """Procesa UN AÑO de forma INCREMENTAL (solo meses nuevos)"""
        self._log_info(f"Procesando: {taxi_type.upper()} - {year}")
        start_time = time.time()
        
        # 1. Obtener meses disponibles en BRONZE
        available_months = self.get_available_months(taxi_type, year)
        
        if not available_months:
            self._log_info(f"   ⚠️ No hay meses disponibles para {taxi_type} {year}")
            return None
        
        # 2. Obtener meses ya procesados
        processed_months = self.get_processed_months(taxi_type, year)
        
        # 3. Determinar meses pendientes
        if force:
            pending_months = available_months
        else:
            pending_months = [m for m in available_months if m not in processed_months]
        
        if not pending_months:
            self._log_info(f"   ✅ {taxi_type} {year} - Todos los meses ya están procesados")
            profile_path = self.profile_dir / taxi_type / f"{taxi_type}_{year}_profile.json"
            if profile_path.exists():
                with open(profile_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            return None
        
        self._log_info(f"   📊 Procesando {len(pending_months)} meses pendientes...")
        
        # 4. Procesar meses pendientes
        month_profiles = []
        
        # Si el perfil consolidado ya existe, reprocesamos todo para consistencia
        profile_path = self.profile_dir / taxi_type / f"{taxi_type}_{year}_profile.json"
        if profile_path.exists():
            self._log_info(f"   🔄 Perfil existente encontrado, reprocesando todos los meses para consistencia")
            pending_months = available_months
        
        for month in pending_months:
            month_dir = self.trip_data_base / f"tipo_dataset={taxi_type}" / f"anio={year}" / f"mes={month:02d}"
            
            if not month_dir.exists():
                self._log_info(f"      ⚠️ Mes {month:02d}: No existe carpeta")
                continue
            
            parquet_files = list(month_dir.glob("*.parquet"))
            if not parquet_files:
                parquet_files = list(month_dir.glob("*.snappy.parquet"))
            if not parquet_files:
                parquet_files = list(month_dir.glob("*parquet"))
            
            if not parquet_files:
                self._log_info(f"      ⚠️ Mes {month:02d}: No hay archivos Parquet")
                continue
            
            try:
                df = self.spark.read.parquet(str(parquet_files[0]))
                month_profile = self.profile_month(df, taxi_type, year, month)
                
                if month_profile:
                    month_profiles.append(month_profile)
                    self._log_info(f"      ✅ Mes {month:02d}: {month_profile['total_records']:,} registros")
                    
            except Exception as e:
                self._log_error(f"      ❌ Error en mes {month:02d}: {str(e)[:80]}")
                continue
        
        if not month_profiles:
            self._log_error(f"   ❌ No se procesó ningún mes")
            return None
        
        # 5. Consolidar todos los meses
        self._log_info(f"   📊 Consolidando {len(month_profiles)} meses...")
        consolidated = self.consolidate_profiles(taxi_type, year, month_profiles)
        
        if not consolidated:
            return None
        
        # 6. Guardar perfil consolidado
        profile_path = self.profile_dir / taxi_type
        profile_path.mkdir(parents=True, exist_ok=True)
        
        json_path = profile_path / f"{taxi_type}_{year}_profile.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(consolidated, f, indent=2, default=str)
        
        # 7. Actualizar tracker
        all_processed = sorted(set([mp["month"] for mp in month_profiles]))
        self.save_processed_months(taxi_type, year, all_processed)
        
        elapsed = time.time() - start_time
        
        self._log_info(f"    Perfil consolidado guardado: {json_path}")
        self._log_info(f"    Total registros: {consolidated['total_records']:,}")
        self._log_info(f"    Calidad promedio: {consolidated['quality_score']:.1f}%")
        self._log_info(f"    Tiempo: {elapsed:.2f} segundos")
        
        # Registrar en estadísticas
        self.execution_stats["processed"].append({
            "taxi_type": taxi_type,
            "year": year,
            "records": consolidated["total_records"],
            "quality_score": consolidated["quality_score"],
            "execution_time": round(elapsed, 2)
        })
        self._log_audit(
            taxi_type=taxi_type,
            year=year,
            status="completed",
            message=f"Perfil consolidado generado: {json_path}",
            file_path=str(json_path),
            metrics={
                "records_processed": consolidated.get("total_records", 0),
                "quality_score": consolidated.get("quality_score", 0),
                "execution_time_seconds": round(elapsed, 2),
                "months_processed": len(consolidated.get("months_processed", []))
            }
        )   
        return consolidated
    
    def profile_lookup(self):
        """Genera perfil de taxi_zone_lookup (archivo CSV)"""
        taxi_type = "lookup"
        year = 0
        
        self._log_info(f"Perfilando: {taxi_type.upper()}")
        profile_start = time.time()
        
        if not self.lookup_path.exists():
            self._log_error(f"   ❌ No existe: {self.lookup_path}")
            return None
        
        try:
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(str(self.lookup_path))
            columns = [str(col) for col in df.columns]
            total_records = df.count()
        except Exception as e:
            self._log_error(f"   ❌ Error al leer CSV: {str(e)}")
            return None
        
        if total_records == 0:
            self._log_error(f"   ⚠️ Archivo vacío")
            return None
        
        self._log_info(f"   Registros totales: {total_records:,}")
        
        profile_data = {
            "taxi_type": taxi_type,
            "year": year,
            "total_records": total_records,
            "quality_score": 100.0,
            "columns": {}
        }
        
        for col_name in columns:
            col_name_str = str(col_name)
            col_type = str(dict(df.dtypes).get(col_name_str, 'unknown'))
            
            null_count = df.filter(F.col(col_name_str).isNull()).count()
            null_pct = (null_count / total_records) * 100 if total_records > 0 else 0
            distinct_count = df.select(col_name_str).distinct().count()
            distinct_pct = (distinct_count / total_records) * 100 if total_records > 0 else 0
            
            stats = {}
            if col_type in ['double', 'float', 'int', 'bigint']:
                try:
                    stats_df = df.select(
                        F.min(F.col(col_name_str)).alias("min"),
                        F.max(F.col(col_name_str)).alias("max"),
                        F.mean(F.col(col_name_str)).alias("mean")
                    ).collect()[0]
                    stats = {
                        "min": float(stats_df.min) if stats_df.min is not None else None,
                        "max": float(stats_df.max) if stats_df.max is not None else None,
                        "mean": float(stats_df.mean) if stats_df.mean is not None else None
                    }
                except:
                    stats = {"error": "No se pudo calcular estadísticas"}
            
            if null_pct > 30:
                profile_data["quality_score"] -= 5
            elif null_pct > 15:
                profile_data["quality_score"] -= 2
            if distinct_count == 1 and distinct_pct == 100:
                profile_data["quality_score"] -= 10
            
            profile_data["columns"][col_name_str] = {
                "data_type": col_type,
                "null_count": int(null_count),
                "null_pct": float(round(null_pct, 2)),
                "distinct_count": int(distinct_count),
                "distinct_pct": float(round(distinct_pct, 2)),
                "min_value": float(stats.get("min")) if stats.get("min") is not None else None,
                "max_value": float(stats.get("max")) if stats.get("max") is not None else None,
                "mean_value": float(stats.get("mean")) if stats.get("mean") is not None else None,
                "stddev_value": None
            }
        
        profile_data["quality_score"] = max(0, min(100, profile_data["quality_score"]))
        
        taxi_profile_dir = self.profile_dir / taxi_type
        taxi_profile_dir.mkdir(parents=True, exist_ok=True)
        
        json_path = taxi_profile_dir / f"{taxi_type}_profile.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(profile_data, f, indent=2, default=str)
        
        self._log_info(f"   ✅ Perfil guardado: {json_path}")
        self._log_info(f"   📊 Score de calidad: {profile_data['quality_score']:.1f}%")
        
        self.execution_stats["processed"].append({
            "taxi_type": taxi_type,
            "year": year,
            "records": total_records,
            "quality_score": profile_data["quality_score"],
            "execution_time": round(time.time() - profile_start, 2)
        })
        self._log_audit(
            taxi_type=taxi_type,
            year=year,
            status="completed",
            message=f"Perfil generado: {json_path}",
            file_path=str(json_path),
            metrics={
                "records_processed": total_records,
                "quality_score": profile_data["quality_score"],
                "execution_time_seconds": round(time.time() - profile_start, 2)
            }
        )
        
        return profile_data
    
    def _log_audit(self, taxi_type, year, status, message, file_path=None, error=None, metrics=None):
        """Registra auditoría en JSONL con formato consistente con ingesta"""
        if not metrics:
            for item in self.execution_stats.get("processed", []):
                if item.get("taxi_type") == taxi_type and item.get("year") == year:
                    metrics = {
                        "records_processed": item.get("records", 0),
                        "quality_score": item.get("quality_score", 0),
                        "execution_time_seconds": item.get("execution_time", 0),
                        "months_processed": item.get("months_processed", 0)
                    }
                    break
        
        if not file_path:
            if taxi_type == "lookup":
                file_path = f"..\\..\\data\\silver\\profiling_inicial\\_profiles\\lookup\\lookup_profile.json"
            else:
                file_path = f"..\\..\\data\\silver\\profiling_inicial\\_profiles\\{taxi_type}\\{taxi_type}_{year}_profile.json"
        
        size_bytes = 0
        size_gb = 0
        full_path = ROOT_DIR / file_path.replace("..\\..\\", "")
        if full_path.exists():
            size_bytes = full_path.stat().st_size
            size_gb = round(size_bytes / (1024**3), 6)
        
        log_entry = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "year": year if year != 0 else None,
            "month": None,
            "trip_type": taxi_type,
            "file_name": file_path.split("\\")[-1] if file_path else f"{taxi_type}_{year}_profile.json",
            "url": None,
            "local_path": file_path,
            "status": status,
            "size_bytes": size_bytes,
            "size_gb": size_gb,
            "sha256": None,
            "error": error,
            "metrics": metrics or {}
        }
        
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    
    def get_existing_profiles(self, taxi_type):
        existing = []
        taxi_profile_dir = self.profile_dir / taxi_type
        if taxi_profile_dir.exists():
            for file in taxi_profile_dir.glob("*.json"):
                try:
                    if taxi_type == "lookup":
                        if file.stem == "lookup_profile":
                            existing.append(0)
                    else:
                        year = int(file.stem.split("_")[1])
                        existing.append(year)
                except:
                    continue
        return existing
    
    def get_available_years(self, taxi_type):
        base_path = self.trip_data_base / f"tipo_dataset={taxi_type}"
        if not base_path.exists():
            return []
        years = []
        for year_dir in base_path.glob("anio=*"):
            try:
                years.append(int(year_dir.name.split("=")[1]))
            except:
                continue
        return sorted(years)
    
    def process(self, taxi_types, years, force=False):
        self._log_info(f"Iniciando procesamiento de {len(taxi_types)} tipos de taxi")
        
        for taxi_type in taxi_types:
            self._log_info(f"\n{'='*50}")
            self._log_info(f"📂 {taxi_type.upper()}")
            self._log_info(f"{'='*50}")
            
            if taxi_type == "lookup":
                existing = self.get_existing_profiles(taxi_type)
                if 0 in existing and not force:
                    self._log_info(f"   ✅ lookup ya está procesado")
                    continue
                try:
                    self.profile_lookup()
                except Exception as e:
                    self._log_error(f"   ❌ Error: {str(e)}")
                    self.execution_stats["errors"].append({"taxi_type": taxi_type, "year": 0, "error": str(e)})
                continue
            
            available_years = self.get_available_years(taxi_type)
            if not available_years:
                self._log_info(f"   ⚠️ No hay datos disponibles para {taxi_type}")
                continue
            
            years_to_process = [y for y in years if y in available_years]
            if not years_to_process:
                self._log_info(f"   ⚠️ No hay años solicitados disponibles")
                continue
            
            existing_years = self.get_existing_profiles(taxi_type)
            
            for year in years_to_process:
                if year in existing_years and not force:
                    self._log_info(f"\n    {year} ya procesado - saltando")
                    continue
                
                try:
                    self.process_year_incremental(taxi_type, year, force)
                except Exception as e:
                    self._log_error(f"   ❌ Error en {taxi_type} {year}: {str(e)}")
                    self.execution_stats["errors"].append({"taxi_type": taxi_type, "year": year, "error": str(e)})
                    self._log_audit(
                        taxi_type=taxi_type,
                        year=year,
                        status="failed",
                        message="Error en profiling",
                        error=str(e)
                    )
        
        self.execution_stats["finished_at"] = datetime.now().isoformat()
        self.execution_stats["total_processed"] = len(self.execution_stats["processed"])
        self.execution_stats["total_errors"] = len(self.execution_stats["errors"])
        
        self._save_metadata()
        self._show_summary()
        
        return self.execution_stats
    
    def _save_metadata(self):
        """Guarda metadata de ejecución en UN SOLO archivo JSONL"""
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
        print(f" RESUMEN DE PROFILING")
        print(f"{'='*70}")
        print(f"Pipeline ID: {self.pipeline_id}")
        print(f"Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'-'*70}")
        
        if self.execution_stats["processed"]:
            print(f"✅ Procesados: {len(self.execution_stats['processed'])}")
            for item in self.execution_stats["processed"]:
                year_str = item['year'] if item['year'] != 0 else "N/A"
                print(f"   - {item['taxi_type']} {year_str}: {item['records']:,} registros, Score: {item['quality_score']:.1f}%")
        
        if self.execution_stats["errors"]:
            print(f"❌ Errores: {len(self.execution_stats['errors'])}")
            for item in self.execution_stats["errors"]:
                year_str = item['year'] if item['year'] != 0 else "N/A"
                print(f"   - {item['taxi_type']} {year_str}: {item['error']}")
        
        print(f"\n📁 Ubicaciones:")
        print(f"   Perfiles (JSON): {self.profile_dir}")
        print(f"   Metadata: {self.metadata_dir}")
        print(f"   Logs: {self.log_path}")
        print(f"{'='*70}")


# ============================================================
# PUNTO DE ENTRADA
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Profiling BRONZE - Versión Producción",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS:
  # Procesar TODAS las categorías (2023-2025)
  python profiling_inicial.py
  
  # Procesar solo green
  python profiling_inicial.py --taxi green --years 2023 2024 2025
  
  # Procesar solo lookup
  python profiling_inicial.py --taxi lookup
  
  # Procesar solo 2026 (incremental)
  python profiling_inicial.py --years 2026
  
  # Forzar reprocesamiento
  python profiling_inicial.py --force
        """
    )
    
    parser.add_argument("--taxi", nargs="+",
                       choices=["fhv", "fhvhv", "green", "yellow", "lookup", "all"],
                       default=["all"],
                       help="Tipos de taxi a procesar (default: all)")
    
    parser.add_argument("--years", nargs="+", type=int,
                       default=[2023, 2024, 2025],
                       help="Años a procesar (default: 2023 2024 2025)")
    
    parser.add_argument("--force", action="store_true",
                       help="Forzar reprocesamiento de todos los años")
    
    args = parser.parse_args()
    
    if "all" in args.taxi:
        taxi_types = ["fhv", "fhvhv", "green", "yellow", "lookup"]
    else:
        taxi_types = args.taxi
    
    # Crear sesión Spark
    spark = SparkSession.builder \
        .appName("BronzeProfiler") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
    
    print(f"\n Iniciando profiling...")
    print(f" Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    profiler = BronzeProfiler(spark)
    profiler.process(taxi_types, args.years, args.force)
    
    print(f"\n PROFILING COMPLETADO")
    print(f" Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()

"""
BRONZE TO SILVER - VERSIÓN PRODUCCIÓN CON CALIDAD DE DATOS
- Consolidación: Une todos los meses de un año en un solo DataFrame
- Normalización: Aplica esquema unificado (tipos de datos consistentes)
- LIMPIEZA CON DIMENSIONES DE CALIDAD:
  - Completitud: Tratar nulos en columnas críticas
  - Exactitud: Corregir tipos de dato
  - Unicidad: Eliminar duplicados
  - Razonabilidad: Validar rangos de valores
  - Validez: Verificar códigos contra catálogos
  - Integridad: Enriquecer con lookup
- Enriquecimiento: Añade columnas derivadas (hora, día, duración, etc.)
- Guarda en SILVER (1 archivo Parquet por año en varias particiones)
- Genera REPORTE DE CALIDAD por año
- Logs en data/logs/cleaning_manifest.jsonl

NOTA: FHVHV está EXCLUIDO por defecto (demasiado grande para 16GB RAM)
Para procesar FHVHV usar: --taxi fhvhv --years 2023
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
from pyspark.sql.types import *
from pyspark.sql import DataFrame

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
    "spark.driver.memory": "8g",
    "spark.executor.memory": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.skewJoin.enabled": "true"
}

class BronzeToSilver:
    def __init__(self, spark):
        self.spark = spark
        self.pipeline_id = f"bronze_to_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.pipeline_name = "bronze_to_silver"
        self.pipeline_version = "1.0.0"
        self.start_time = datetime.now()
        
        # Rutas
        self.base_dir = ROOT_DIR / "data" / "silver" / "cleaning"
        self.output_dir = ROOT_DIR / "data" / "silver" / "cleaned"
        self.metadata_dir = self.base_dir / "_metadata"
        self.quality_dir = self.base_dir / "_quality_reports"
        self.log_dir = ROOT_DIR / "data" / "logs"
        self.log_path = self.log_dir / "cleaning_manifest.jsonl"
        self.metadata_path = self.metadata_dir / "metadata.jsonl"
        self.tracker_path = self.metadata_dir / "cleaning_tracker.json"
        
        # Directorio de esquemas unificados
        self.schemas_dir = ROOT_DIR / "data" / "silver" / "unified_schema" / "_schemas"
        
        # Ruta del lookup
        self.lookup_path = ROOT_DIR / "data" / "bronze" / "files" / "lookup" / "taxi_zone_lookup.csv"
        self.lookup_parquet_path = ROOT_DIR / "data" / "silver" / "lookup" / "taxi_zone_lookup.parquet"
        
        # Crear directorios
        for path in [self.output_dir, self.metadata_dir, self.quality_dir, self.log_dir]:
            path.mkdir(parents=True, exist_ok=True)
        
        self.lookup_parquet_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Rutas de datos
        self.trip_data_base = ROOT_DIR / "data" / "bronze" / "files" / "trip_data"
        self.taxi_types = ["fhv", "fhvhv", "green", "yellow"]
        
        # Cargar lookup
        self.lookup_df = self._load_lookup()
        
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
    
    def _load_lookup(self):
        """Carga el Taxi Zone Lookup desde CSV y lo guarda en Parquet"""
        self._log_info("Cargando Taxi Zone Lookup...")
        
        if self.lookup_parquet_path.exists():
            self._log_info(f"   Lookup encontrado en Parquet: {self.lookup_parquet_path}")
            return self.spark.read.parquet(str(self.lookup_parquet_path))
        
        if not self.lookup_path.exists():
            self._log_error(f"   Lookup no encontrado: {self.lookup_path}")
            return None
        
        self._log_info(f"   Cargando desde CSV: {self.lookup_path}")
        
        try:
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(str(self.lookup_path))
            for col in df.columns:
                new_col = col.lower().replace(" ", "_")
                if new_col != col:
                    df = df.withColumnRenamed(col, new_col)
            
            df.write.mode("overwrite").parquet(str(self.lookup_parquet_path))
            self._log_info(f"   Lookup guardado en Parquet: {self.lookup_parquet_path}")
            self._log_info(f"   Registros: {df.count():,}")
            return df
        except Exception as e:
            self._log_error(f"   Error cargando lookup: {str(e)}")
            return None
    
    def _get_processed_years(self, taxi_type):
        """Obtiene años ya procesados desde el tracker"""
        if not self.tracker_path.exists():
            return []
        try:
            with open(self.tracker_path, "r", encoding="utf-8") as f:
                tracker = json.load(f)
            return tracker.get(taxi_type, [])
        except:
            return []
    
    def _save_processed_years(self, taxi_type, years):
        """Guarda años procesados en el tracker"""
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
        """Obtiene años disponibles en BRONZE"""
        base_path = self.trip_data_base / f"tipo_dataset={taxi_type}"
        if not base_path.exists():
            return []
        years = []
        for year_dir in base_path.glob("anio=*"):
            try:
                year = int(year_dir.name.split("=")[1])
                years.append(year)
            except:
                continue
        return sorted(years)
    
    def _load_unified_schema(self, taxi_type):
        """Carga el esquema unificado para un tipo de taxi"""
        schema_path = self.schemas_dir / f"{taxi_type}_unified_schema.json"
        if not schema_path.exists():
            self._log_error(f"Esquema unificado no encontrado: {schema_path}")
            self._log_info("Ejecuta primero generate_unified_schema.py")
            return None
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            self._log_error(f"Error cargando esquema: {str(e)}")
            return None
    
    def consolidate_year(self, taxi_type, year):
        """PASO 1: Consolidar (unir todos los meses del año)"""
        self._log_info(f"   [1/7] Consolidando año {year}...")
        
        year_path = self.trip_data_base / f"tipo_dataset={taxi_type}" / f"anio={year}"
        if not year_path.exists():
            self._log_error(f"      No existe: {year_path}")
            return None
        
        parquet_files = list(year_path.glob("mes=*/*.parquet"))
        if not parquet_files:
            parquet_files = list(year_path.glob("mes=*/*.snappy.parquet"))
        if not parquet_files:
            self._log_error(f"      No hay archivos Parquet en: {year_path}")
            return None
        
        self._log_info(f"      Encontrados {len(parquet_files)} archivos")
        
        df = None
        for file_path in parquet_files:
            try:
                df_month = self.spark.read.parquet(str(file_path))
                if df is None:
                    df = df_month
                else:
                    df = df.union(df_month)
            except Exception as e:
                self._log_error(f"      Error leyendo {file_path.name}: {str(e)[:80]}")
                continue
        
        if df is None:
            self._log_error("      No se pudo leer ningún archivo")
            return None
        
        total_records = df.count()
        self._log_info(f"      Registros consolidados: {total_records:,}")
        
        return df
    
    def normalize_schema(self, df, taxi_type):
        """PASO 2: Normalizar (aplicar esquema unificado)"""
        self._log_info(f"   [2/7] Normalizando esquema...")
        
        unified_schema = self._load_unified_schema(taxi_type)
        if not unified_schema:
            return None
        
        self._log_info(f"      Esquema unificado: {len(unified_schema)} columnas")
        
        for col_name, col_type in unified_schema.items():
            if col_name in df.columns:
                try:
                    df = df.withColumn(col_name, F.col(col_name).cast(col_type))
                except Exception as e:
                    self._log_error(f"      Error casteando {col_name}: {str(e)[:50]}")
                    df = df.withColumn(col_name, F.col(col_name).cast("string"))
                    df = df.withColumn(col_name, F.col(col_name).cast(col_type))
            else:
                df = df.withColumn(col_name, F.lit(None).cast(col_type))
        
        self._log_info(f"      Columnas finales: {len(df.columns)}")
        
        return df
    
    def clean_data_with_quality_dimensions(self, df, taxi_type, year):
        """Limpieza con dimensiones de calidad - ADAPTADO POR TIPO DE TAXI"""
        self._log_info(f"   [3/7] Limpiando datos con dimensiones de calidad...")
        
        original_count = df.count()
        quality_report = {
            "taxi_type": taxi_type,
            "year": year,
            "original_records": original_count,
            "dimensions": {},
            "records_removed": 0,
            "final_records": 0
        }
        
        # ================================================================
        # DIMENSIÓN 1: COMPLETITUD
        # ================================================================
        self._log_info("      [Completitud] Eliminando nulos en columnas críticas...")
        removed_completitud = 0
        
        critical_cols_map = {
            "yellow": ["tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount"],
            "green": ["lpep_pickup_datetime", "lpep_dropoff_datetime", "total_amount"],
            "fhv": ["pickup_datetime"],
            "fhvhv": ["pickup_datetime"]
        }
        
        critical_cols = critical_cols_map.get(taxi_type, ["pickup_datetime"])
        critical_cols = [col for col in critical_cols if col in df.columns]
        
        self._log_info(f"         Columnas críticas: {critical_cols}")
        
        for col in critical_cols:
            before = df.count()
            df = df.filter(F.col(col).isNotNull())
            removed = before - df.count()
            removed_completitud += removed
            if removed > 0:
                self._log_info(f"         {col}: eliminados {removed:,} nulos")
        
        quality_report["dimensions"]["completitud"] = {
            "description": "Eliminación de nulos en columnas críticas",
            "columns_checked": critical_cols,
            "records_removed": removed_completitud
        }
        
        # ================================================================
        # DIMENSIÓN 2: UNICIDAD
        # ================================================================
        self._log_info("      [Unicidad] Eliminando registros duplicados...")
        before = df.count()
        df = df.dropDuplicates()
        removed_unicidad = before - df.count()
        
        quality_report["dimensions"]["unicidad"] = {
            "description": "Eliminación de registros duplicados exactos",
            "records_removed": removed_unicidad
        }
        
        # ================================================================
        # DIMENSIÓN 3: EXACTITUD
        # ================================================================
        self._log_info("      [Exactitud] Corrigiendo tipos y valores negativos...")
        removed_exactitud = 0
        
        for col in ["fare_amount", "tip_amount", "tolls_amount"]:
            if col in df.columns:
                before = df.count()
                df = df.filter(F.col(col) >= 0)
                removed = before - df.count()
                removed_exactitud += removed
                if removed > 0:
                    self._log_info(f"         {col}: eliminados {removed:,} valores negativos")
        
        quality_report["dimensions"]["exactitud"] = {
            "description": "Corrección de tipos y eliminación de valores negativos",
            "records_removed": removed_exactitud
        }
        
        # ================================================================
        # DIMENSIÓN 4: RAZONABILIDAD (SOLO si existen las columnas Y tienen datos)
        # ================================================================
        self._log_info("      [Razonabilidad] Validando rangos de valores...")
        removed_razonabilidad = 0
        
        # 4a. Distancias (0.1 - 500 km)
        if "trip_distance" in df.columns:
            before = df.count()
            df = df.filter((F.col("trip_distance") >= 0.1) & (F.col("trip_distance") <= 500))
            removed = before - df.count()
            removed_razonabilidad += removed
            if removed > 0:
                self._log_info(f"         trip_distance: eliminados {removed:,} fuera de rango (0.1-500)")
        
        # 4b. Pasajeros (0 - 6)
        if "passenger_count" in df.columns:
            before = df.count()
            df = df.filter((F.col("passenger_count") >= 0) & (F.col("passenger_count") <= 6))
            removed = before - df.count()
            removed_razonabilidad += removed
            if removed > 0:
                self._log_info(f"         passenger_count: eliminados {removed:,} fuera de rango (0-6)")
        
        # 4c. Duración máxima 24 horas (SOLO si dropoff_datetime existe Y tiene datos)
        if "dropoff_datetime" in df.columns:
            non_null_count = df.filter(F.col("dropoff_datetime").isNotNull()).count()
            if non_null_count > 0:
                before = df.count()
                df = df.filter(
                    (F.unix_timestamp(F.col("dropoff_datetime")) - 
                    F.unix_timestamp(F.col("pickup_datetime"))) <= 86400
                )
                removed = before - df.count()
                removed_razonabilidad += removed
                if removed > 0:
                    self._log_info(f"         trip_duration: eliminados {removed:,} con duración > 24h")
            else:
                self._log_info(f"         trip_duration: saltado (dropoff_datetime solo tiene NULLs para {taxi_type})")
        else:
            self._log_info(f"         trip_duration: saltado (dropoff_datetime no disponible para {taxi_type})")
        
        quality_report["dimensions"]["razonabilidad"] = {
            "description": "Validación de rangos: distancia, pasajeros, duración",
            "records_removed": removed_razonabilidad
        }
        
        # ================================================================
        # DIMENSIÓN 5: VALIDEZ
        # ================================================================
        self._log_info("      [Validez] Verificando códigos contra catálogos...")
        removed_validez = 0
        
        if "payment_type" in df.columns:
            before = df.count()
            df = df.filter((F.col("payment_type").between(1, 6)) | F.col("payment_type").isNull())
            removed = before - df.count()
            removed_validez += removed
            if removed > 0:
                self._log_info(f"         payment_type: eliminados {removed:,} códigos inválidos")
        
        if "pulocationid" in df.columns:
            before = df.count()
            df = df.filter((F.col("pulocationid").between(1, 263)) | F.col("pulocationid").isNull())
            removed = before - df.count()
            removed_validez += removed
            if removed > 0:
                self._log_info(f"         pulocationid: eliminados {removed:,} IDs inválidos")
        
        if "dolocationid" in df.columns:
            before = df.count()
            df = df.filter((F.col("dolocationid").between(1, 263)) | F.col("dolocationid").isNull())
            removed = before - df.count()
            removed_validez += removed
            if removed > 0:
                self._log_info(f"         dolocationid: eliminados {removed:,} IDs inválidos")
        
        quality_report["dimensions"]["validez"] = {
            "description": "Validación de códigos: payment_type, location_ids",
            "records_removed": removed_validez
        }
        
        # ================================================================
        # DIMENSIÓN 6: OPORTUNIDAD
        # ================================================================
        self._log_info("      [Oportunidad] Verificando rango de fechas...")
        removed_oportunidad = 0
        
        if "pickup_datetime" in df.columns:
            before = df.count()
            df = df.filter(
                (F.col("pickup_datetime") >= "2020-01-01") & 
                (F.col("pickup_datetime") <= "2027-12-31")
            )
            removed = before - df.count()
            removed_oportunidad += removed
            if removed > 0:
                self._log_info(f"         fechas: eliminados {removed:,} fuera de rango (2020-2027)")
        
        quality_report["dimensions"]["oportunidad"] = {
            "description": "Verificación de fechas en rango 2020-2027",
            "records_removed": removed_oportunidad
        }
        
        # ================================================================
        # RESUMEN
        # ================================================================
        total_removed = (
            removed_completitud + removed_unicidad + removed_exactitud + 
            removed_razonabilidad + removed_validez + removed_oportunidad
        )
        
        quality_report["records_removed"] = total_removed
        quality_report["final_records"] = original_count - total_removed
        quality_report["removed_percentage"] = round((total_removed / original_count) * 100, 2) if original_count > 0 else 0
        
        self._log_info(f"      RESUMEN LIMPIEZA:")
        self._log_info(f"         Originales: {original_count:,}")
        self._log_info(f"         Eliminados: {total_removed:,} ({quality_report['removed_percentage']}%)")
        self._log_info(f"         Finales: {quality_report['final_records']:,}")
        
        self._save_quality_report(quality_report, taxi_type, year)
        
        return df, quality_report
    
    def _save_quality_report(self, report, taxi_type, year):
        """Guarda el reporte de calidad en JSON"""
        report_path = self.quality_dir / f"{taxi_type}_{year}_quality_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        self._log_info(f"      Reporte de calidad guardado: {report_path}")
    
    def _enrich_with_lookup(self, df):
        """Enriquece los datos con nombres de zonas y boroughs usando el lookup"""
        if self.lookup_df is None:
            self._log_info("   Lookup no disponible, saltando enriquecimiento")
            return df
        
        self._log_info(f"   [4/7] Enriqueciendo con Lookup...")
        
        lookup_pickup = self.lookup_df.select(
            F.col("locationid").alias("pickup_locationid"),
            F.col("zone").alias("pickup_zone"),
            F.col("borough").alias("pickup_borough"),
            F.col("service_zone").alias("pickup_service_zone")
        )
        
        lookup_dropoff = self.lookup_df.select(
            F.col("locationid").alias("dropoff_locationid"),
            F.col("zone").alias("dropoff_zone"),
            F.col("borough").alias("dropoff_borough"),
            F.col("service_zone").alias("dropoff_service_zone")
        )
        
        if "pulocationid" in df.columns:
            df = df.join(
                lookup_pickup,
                df.pulocationid == lookup_pickup.pickup_locationid,
                "left"
            ).drop("pickup_locationid")
            self._log_info("      Añadidas: pickup_zone, pickup_borough, pickup_service_zone")
        
        if "dolocationid" in df.columns:
            df = df.join(
                lookup_dropoff,
                df.dolocationid == lookup_dropoff.dropoff_locationid,
                "left"
            ).drop("dropoff_locationid")
            self._log_info("      Añadidas: dropoff_zone, dropoff_borough, dropoff_service_zone")
        
        return df
    
    def enrich_data(self, df, taxi_type):
        """PASO 5: Enriquecer (añadir columnas derivadas)"""
        self._log_info(f"   [5/7] Enriqueciendo datos...")
        
        added_cols = []
        
        if "pickup_datetime" in df.columns:
            df = df.withColumn("pickup_hour", F.hour(F.col("pickup_datetime")))
            df = df.withColumn("pickup_day", F.dayofweek(F.col("pickup_datetime")))
            df = df.withColumn("pickup_month", F.month(F.col("pickup_datetime")))
            df = df.withColumn("pickup_year", F.year(F.col("pickup_datetime")))
            df = df.withColumn("pickup_date", F.to_date(F.col("pickup_datetime")))
            added_cols.extend(["pickup_hour", "pickup_day", "pickup_month", "pickup_year", "pickup_date"])
        
        if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
            df = df.withColumn(
                "trip_duration_minutes",
                (F.unix_timestamp(F.col("dropoff_datetime")) - 
                 F.unix_timestamp(F.col("pickup_datetime"))) / 60
            )
            added_cols.append("trip_duration_minutes")
        
        df = df.withColumn("taxi_type", F.lit(taxi_type))
        added_cols.append("taxi_type")
        
        self._log_info(f"      Añadidas {len(added_cols)} columnas: {', '.join(added_cols)}")
        
        return df
    
    def clean_spark_metadata(self, output_path):
        """
        Elimina archivos _SUCCESS, .crc y cualquier otro que no sea .parquet
        """
        if not output_path.exists():
            return
        
        # Eliminar _SUCCESS
        success_file = output_path / "_SUCCESS"
        if success_file.exists():
            success_file.unlink()
            self._log_info(f"      Eliminado: {success_file.name}")
        
        # Eliminar todos los archivos que no terminan en .parquet
        for file in output_path.glob("*"):
            if file.is_file() and not file.name.endswith(".parquet"):
                file.unlink()
                self._log_info(f"      Eliminado: {file.name}")
    
    def process_year(self, taxi_type, year, force=False):
        """Procesa UN AÑO COMPLETO - MES POR MES (para datasets grandes)"""
        self._log_info(f"\nProcesando: {taxi_type.upper()} - {year}")
        
        # Si es FHVHV, usar método optimizado
        if taxi_type == "fhvhv":
            return self.process_fhvhv_optimized(year, force)
        
        start_time = time.time()    

        processed_years = self._get_processed_years(taxi_type)
        if year in processed_years and not force:
            self._log_info(f"   {year} ya procesado. Usa --force para reprocesar.")
            return None
        
        # Para FHVHV, procesar mes por mes
        if taxi_type == "fhvhv":
            return self._process_year_month_by_month(taxi_type, year, force)
        else:
            return self._process_year_consolidated(taxi_type, year, force)

    def _process_year_month_by_month(self, taxi_type, year, force=False):
        """Procesa año MES POR MES (para datasets grandes como FHVHV)"""
        self._log_info(f"   Procesando MES POR MES (datos masivos)...")
        
        start_time = time.time()

        # Obtener meses disponibles
        year_path = self.trip_data_base / f"tipo_dataset={taxi_type}" / f"anio={year}"
        if not year_path.exists():
            self._log_error(f"   No existe: {year_path}")
            return None
        
        months = []
        for month_dir in sorted(year_path.glob("mes=*")):
            try:
                month = int(month_dir.name.split("=")[1])
                months.append(month)
            except:
                continue
        
        if not months:
            self._log_error(f"   No hay meses disponibles para {taxi_type} {year}")
            return None
        
        self._log_info(f"   Meses a procesar: {months}")
        
        # Procesar cada mes individualmente
        month_dfs = []
        total_removed = 0
        total_final = 0
        
        for month in months:
            month_start = time.time()
            self._log_info(f"\n   --- Procesando mes {month:02d} ---")
            
            # 1. Consolidar mes
            month_path = year_path / f"mes={month:02d}"
            parquet_files = list(month_path.glob("*.parquet"))
            if not parquet_files:
                self._log_error(f"      No hay archivos en mes {month:02d}")
                continue
            
            df = None
            for file_path in parquet_files:
                try:
                    df_month = self.spark.read.parquet(str(file_path))
                    if df is None:
                        df = df_month
                    else:
                        df = df.union(df_month)
                except Exception as e:
                    self._log_error(f"      Error leyendo {file_path.name}: {str(e)[:80]}")
                    continue
            
            if df is None:
                self._log_error(f"      No se pudo leer mes {month:02d}")
                continue
            
            month_records = df.count()
            self._log_info(f"      Registros: {month_records:,}")
            
            # 2. Normalizar
            df = self.normalize_schema(df, taxi_type)
            if df is None:
                continue
            
            # 3. Limpiar
            df, quality_report = self.clean_data_with_quality_dimensions(df, taxi_type, year)
            
            # 4. Lookup
            df = self._enrich_with_lookup(df)
            
            # 5. Enriquecer
            df = self.enrich_data(df, taxi_type)
            
            month_dfs.append(df)
            total_removed += quality_report["records_removed"]
            total_final += df.count()
            
            month_elapsed = time.time() - month_start
            self._log_info(f"      Mes {month:02d} completado en {month_elapsed:.2f}s")
        
        if not month_dfs:
            self._log_error(f"   No se procesó ningún mes para {taxi_type} {year}")
            return None
        
        # 6. Consolidar TODOS los meses procesados
        self._log_info(f"\n   Consolidando {len(month_dfs)} meses...")
        df_final = month_dfs[0]
        for df_month in month_dfs[1:]:
            df_final = df_final.union(df_month)
        
        final_count = df_final.count()
        self._log_info(f"   Registros finales consolidados: {final_count:,}")
        
        # 7. Guardar
        output_path = self.output_dir / taxi_type / f"year={year}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        parquet_path = output_path / f"{taxi_type}_{year}_cleaned.parquet"
        df_final.write.mode("overwrite").parquet(str(parquet_path))
        
        self.clean_spark_metadata(output_path)
        
        elapsed = time.time() - start_time
        
        self._log_info(f"\n   {taxi_type.upper()} {year} COMPLETADO")
        self._log_info(f"   Registros finales: {final_count:,}")
        self._log_info(f"   Archivo: {parquet_path}")
        self._log_info(f"   Tiempo total: {elapsed:.2f} segundos")
        
        self._save_processed_years(taxi_type, [year])
        
        self.execution_stats["processed"].append({
            "taxi_type": taxi_type,
            "year": year,
            "records": final_count,
            "quality_score": 100 - ((total_removed / (total_final + total_removed)) * 100) if (total_final + total_removed) > 0 else 0,
            "execution_time": round(elapsed, 2)
        })
        
        self._log_audit(
            taxi_type=taxi_type,
            year=year,
            status="completed",
            message=f"Archivo generado: {parquet_path}",
            file_path=str(parquet_path),
            metrics={
                "records_processed": final_count,
                "records_removed": total_removed,
                "execution_time_seconds": round(elapsed, 2)
            }
        )
        
        return df_final
    
    def _process_year_consolidated(self, taxi_type, year, force=False):
        """Procesa año CONSOLIDADO (para datasets normales)"""
        self._log_info(f"   Procesando año consolidado...")
        
        start_time = time.time()
        
        df = self.consolidate_year(taxi_type, year)
        if df is None:
            return None
        
        df = self.normalize_schema(df, taxi_type)
        if df is None:
            return None
        
        df, quality_report = self.clean_data_with_quality_dimensions(df, taxi_type, year)
        
        df = self._enrich_with_lookup(df)
        
        df = self.enrich_data(df, taxi_type)
        
        output_path = self.output_dir / taxi_type / f"year={year}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        parquet_path = output_path / f"{taxi_type}_{year}_cleaned.parquet"
        df.write.mode("overwrite").parquet(str(parquet_path))
        
        self.clean_spark_metadata(output_path)
        
        elapsed = time.time() - start_time
        final_count = df.count()
        
        self._log_info(f"\n   {taxi_type.upper()} {year} COMPLETADO")
        self._log_info(f"   Registros finales: {final_count:,}")
        self._log_info(f"   Archivo: {parquet_path}")
        self._log_info(f"   Tiempo: {elapsed:.2f} segundos")
        
        self._save_processed_years(taxi_type, [year])
        
        self.execution_stats["processed"].append({
            "taxi_type": taxi_type,
            "year": year,
            "records": final_count,
            "quality_score": 100 - quality_report["removed_percentage"],
            "execution_time": round(elapsed, 2)
        })
        
        self._log_audit(
            taxi_type=taxi_type,
            year=year,
            status="completed",
            message=f"Archivo generado: {parquet_path}",
            file_path=str(parquet_path),
            metrics={
                "records_processed": final_count,
                "records_removed": quality_report["records_removed"],
                "removed_percentage": quality_report["removed_percentage"],
                "execution_time_seconds": round(elapsed, 2)
            }
        )
        
        return df

    def process_fhvhv_optimized(self, year, force=False):
        """
        Procesa FHVHV de forma OPTIMIZADA (más rápido)
        - Sin validaciones no críticas
        - dropDuplicates solo con columnas clave
        - Un solo count al final
        """
        taxi_type = "fhvhv"
        self._log_info(f"\nProcesando: FHVHV - {year} (OPTIMIZADO)")
        start_time = time.time()
        
        processed_years = self._get_processed_years(taxi_type)
        if year in processed_years and not force:
            self._log_info(f"   {year} ya procesado. Usa --force para reprocesar.")
            return None
        
        # 1. Consolidar TODOS los meses del año
        year_path = self.trip_data_base / f"tipo_dataset={taxi_type}" / f"anio={year}"
        if not year_path.exists():
            self._log_error(f"   No existe: {year_path}")
            return None
        
        self._log_info(f"   Leyendo todos los meses del año...")
        parquet_files = list(year_path.glob("mes=*/*.parquet"))
        if not parquet_files:
            parquet_files = list(year_path.glob("mes=*/*.snappy.parquet"))
        
        self._log_info(f"   Archivos encontrados: {len(parquet_files)}")
        
        # Leer todos los meses (puede ser pesado pero una sola vez)
        df = None
        for file_path in parquet_files:
            try:
                df_month = self.spark.read.parquet(str(file_path))
                if df is None:
                    df = df_month
                else:
                    df = df.union(df_month)
            except Exception as e:
                self._log_error(f"      Error en {file_path.name}: {str(e)[:80]}")
                continue
        
        if df is None:
            self._log_error("   No se pudo leer ningún archivo")
            return None
        
        original_count = df.count()
        self._log_info(f"   Registros totales: {original_count:,}")
        
        # 2. Normalizar (solo columnas necesarias)
        unified_schema = self._load_unified_schema(taxi_type)
        if unified_schema:
            for col_name, col_type in unified_schema.items():
                if col_name in df.columns:
                    try:
                        df = df.withColumn(col_name, F.col(col_name).cast(col_type))
                    except:
                        df = df.withColumn(col_name, F.col(col_name).cast("string"))
                        df = df.withColumn(col_name, F.col(col_name).cast(col_type))
                else:
                    df = df.withColumn(col_name, F.lit(None).cast(col_type))
        
        # 3. Validaciones CRÍTICAS SOLO
        self._log_info("   Aplicando validaciones críticas...")
        removed = 0
        
        # 3a. Completitud (pickup_datetime)
        before = df.count()
        df = df.filter(F.col("pickup_datetime").isNotNull())
        removed += before - df.count()
        self._log_info(f"      pickup_datetime: eliminados {before - df.count():,} nulos")
        
        # 3b. Oportunidad (fechas en rango)
        before = df.count()
        df = df.filter(
            (F.col("pickup_datetime") >= "2020-01-01") & 
            (F.col("pickup_datetime") <= "2027-12-31")
        )
        removed += before - df.count()
        self._log_info(f"      fechas: eliminados {before - df.count():,} fuera de rango")
        
        # 3c. Unicidad (SOLO con columnas clave)
        self._log_info("   Eliminando duplicados (con columnas clave)...")
        before = df.count()
        df = df.dropDuplicates(subset=["pickup_datetime", "PULocationID", "DOLocationID"])
        removed += before - df.count()
        self._log_info(f"      duplicados: eliminados {before - df.count():,}")
        
        # 4. Enriquecer con lookup
        df = self._enrich_with_lookup(df)
        
        # 5. Enriquecer con columnas derivadas
        df = self.enrich_data(df, taxi_type)
        
        # 6. Guardar
        output_path = self.output_dir / taxi_type / f"year={year}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        parquet_path = output_path / f"{taxi_type}_{year}_cleaned.parquet"
        df.write.mode("overwrite").parquet(str(parquet_path))
        self.clean_spark_metadata(output_path)
        
        final_count = df.count()
        elapsed = time.time() - start_time
        
        self._log_info(f"\n   FHVHV {year} COMPLETADO (OPTIMIZADO)")
        self._log_info(f"   Originales: {original_count:,}")
        self._log_info(f"   Eliminados: {removed:,}")
        self._log_info(f"   Finales: {final_count:,}")
        self._log_info(f"   Tiempo: {elapsed:.2f} segundos")
        
        self._save_processed_years(taxi_type, [year])
        
        self.execution_stats["processed"].append({
            "taxi_type": taxi_type,
            "year": year,
            "records": final_count,
            "quality_score": 100 - ((removed / original_count) * 100) if original_count > 0 else 0,
            "execution_time": round(elapsed, 2)
        })
        
        return df

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
                        message="Error en procesamiento",
                        error=str(e)
                    )
        
        self.execution_stats["finished_at"] = datetime.now().isoformat()
        self.execution_stats["total_processed"] = len(self.execution_stats["processed"])
        self.execution_stats["total_errors"] = len(self.execution_stats["errors"])
        
        self._save_metadata()
        self._show_summary()
        
        return self.execution_stats
    
    def _log_audit(self, taxi_type, year, status, message, file_path=None, error=None, metrics=None):
        """Registra auditoría en JSONL"""
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
        """Guarda metadata de ejecución en JSONL"""
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
        print(f"RESUMEN DE BRONZE TO SILVER")
        print(f"{'='*70}")
        print(f"Pipeline ID: {self.pipeline_id}")
        print(f"Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'-'*70}")
        
        if self.execution_stats["processed"]:
            print(f"Procesados: {len(self.execution_stats['processed'])}")
            for item in self.execution_stats["processed"]:
                print(f"   - {item['taxi_type']} {item['year']}: {item['records']:,} registros (calidad: {item['quality_score']:.1f}%)")
        
        if self.execution_stats["errors"]:
            print(f"Errores: {len(self.execution_stats['errors'])}")
            for item in self.execution_stats["errors"]:
                print(f"   - {item['taxi_type']} {item['year']}: {item['error']}")
        
        print(f"\nUbicaciones:")
        print(f"   SILVER: {self.output_dir}")
        print(f"   Lookup Parquet: {self.lookup_parquet_path}")
        print(f"   Metadata: {self.metadata_dir}")
        print(f"   Quality Reports: {self.quality_dir}")
        print(f"   Logs: {self.log_path}")
        print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(
        description="Bronze to Silver - Produccion con Calidad de Datos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS:
  # Procesar TODOS los tipos activos (FHV, Green, Yellow) - RECOMENDADO
  python bronze_to_silver.py --taxi all --years 2023 2024 2025
  
  # Procesar solo Yellow 2023
  python bronze_to_silver.py --taxi yellow --years 2023
  
  # Procesar FHVHV (solo si tienes >32GB RAM)
  python bronze_to_silver.py --taxi fhvhv --years 2023
  
  # Forzar reprocesamiento
  python bronze_to_silver.py --taxi yellow --years 2023 --force

NOTA: FHVHV está EXCLUIDO por defecto de 'all' debido a su tamaño masivo.
      Para procesar FHVHV, especifícalo explícitamente: --taxi fhvhv
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
    
    # ============================================================
    # CORRECCIÓN: Definir qué tipos se procesan con "all"
    # ============================================================
    # TIPOS ACTIVOS: los que se procesan con "all" (excluye FHVHV)
    ACTIVE_TYPES = ["fhv", "green", "yellow"]
    
    # TODOS LOS TIPOS: incluye FHVHV (para uso futuro)
    ALL_TYPES = ["fhv", "fhvhv", "green", "yellow"]
    
    # Determinar qué tipos procesar
    if "all" in args.taxi:
        taxi_types = ACTIVE_TYPES.copy()  # ← EXCLUYE FHVHV
        print("[INFO] NOTA: 'all' excluye FHVHV (demasiado grande para 16GB RAM)")
        print("[INFO] Para procesar FHVHV usar: --taxi fhvhv")
    else:
        taxi_types = args.taxi
    
    # Verificar si FHVHV está incluido y advertir
    if "fhvhv" in taxi_types:
        print("[ADVERTENCIA] FHVHV es un dataset masivo (>232M registros/año)")
        print("[ADVERTENCIA] Puede tomar mucho tiempo y requerir >32GB RAM")
        print("[ADVERTENCIA] ¿Estás seguro? Presiona Ctrl+C para cancelar o espera...")
        time.sleep(2)  # Dar tiempo para cancelar
    
    # Configuración optimizada para datos masivos
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()
    
    print(f"\nIniciando Bronze to Silver...")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tipos a procesar: {taxi_types}")
    
    processor = BronzeToSilver(spark)
    
    processor.process(taxi_types, args.years, args.force)
    
    print(f"\nBRONZE TO SILVER COMPLETADO")
    print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()

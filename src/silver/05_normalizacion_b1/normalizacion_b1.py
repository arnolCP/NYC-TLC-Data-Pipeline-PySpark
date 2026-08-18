"""Normaliza los datasets Silver limpios en una capa canónica para Gold y análisis.

La etapa B1 recibe los Parquet producidos por ``04_limpieza`` y conserva una
salida ligera, consistente entre tipos de taxi, en
``data/silver/trip_data_normalized``.  Está diseñada para ejecutarse tanto de
forma aislada como desde ``src.pipeline``.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


ROOT_DIR = Path(__file__).resolve().parents[3]
CLEANED_BASE_DIR = ROOT_DIR / "data" / "silver" / "cleaned"
OUTPUT_BASE_DIR = ROOT_DIR / "data" / "silver" / "trip_data_normalized"
AUDIT_LOG_PATH = ROOT_DIR / "data" / "logs" / "silver_b_normalization_audit.jsonl"
TAXI_TYPES = ("fhv", "fhvhv", "green", "yellow")
HADOOP_DLL_DIRECTORY = None

PICKUP_DATETIME_CANDIDATES = ("pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime")
DROPOFF_DATETIME_CANDIDATES = ("dropoff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime")
PICKUP_LOCATION_CANDIDATES = ("pickup_location_id", "pulocationid", "pu_location_id", "pu_locationid")
DROPOFF_LOCATION_CANDIDATES = ("dropoff_location_id", "dolocationid", "do_location_id", "do_locationid")
DISTANCE_CANDIDATES = ("distancia_millas", "trip_distance", "distance_miles")
BASE_AMOUNT_CANDIDATES = ("monto_base", "fare_amount", "base_passenger_fare")
TOTAL_AMOUNT_CANDIDATES = ("monto_total", "total_amount", "base_passenger_fare")
TIP_CANDIDATES = ("propina", "tip_amount", "tips")
TOLLS_CANDIDATES = ("peajes", "tolls_amount", "tolls")
PAYMENT_CANDIDATES = ("payment_type", "paymenttype")
PASSENGER_CANDIDATES = ("passenger_count", "passengercount")
VENDOR_CANDIDATES = ("vendor_id", "vendorid")
RATECODE_CANDIDATES = ("ratecode_id", "ratecodeid")
SHARED_RIDE_CANDIDATES = ("shared_ride_flag", "sr_flag")

OPTIONAL_DOUBLE_COLUMNS = {
    "extra": ("extra",),
    "mta_tax": ("mta_tax",),
    "improvement_surcharge": ("improvement_surcharge",),
    "congestion_surcharge": ("congestion_surcharge",),
    "airport_fee": ("airport_fee", "airportfee"),
    "cbd_congestion_fee": ("cbd_congestion_fee",),
    "ehail_fee": ("ehail_fee",),
}

CANONICAL_ORDER = (
    "tipo_dataset", "anio", "pickup_datetime", "dropoff_datetime",
    "pickup_location_id", "dropoff_location_id", "distancia_millas",
    "duracion_segundos", "duracion_minutos", "monto_base", "monto_total",
    "propina", "peajes", "extra", "mta_tax", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "ehail_fee",
    "payment_type", "passenger_count", "vendor_id", "ratecode_id", "trip_type",
    "shared_ride_flag", "pickup_date", "pickup_hour", "pickup_day_of_week",
    "pickup_month",
)

ESSENTIAL_EXTRA_COLUMNS = (
    "pickup_zone", "pickup_borough", "pickup_service_zone", "dropoff_zone",
    "dropoff_borough", "dropoff_service_zone", "store_and_fwd_flag",
    "dispatching_base_num", "affiliated_base_number",
)


def clean_column_name(name: str) -> str:
    """Convierte cualquier nombre de columna a snake_case seguro."""
    normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")


def parquet_part_files(path: Path) -> list[str]:
    """Lista partes Parquet concretas, compatible con rutas Windows/Spark."""
    if path.is_file() and path.suffix.lower() == ".parquet":
        return [str(path)]
    return [str(file) for file in sorted(path.rglob("*.parquet")) if file.is_file()]


def parse_cleaned_dataset_path(path: Path) -> tuple[str, int | None]:
    """Extrae tipo y año de ``cleaned/<taxi>/year=YYYY/<taxi>_YYYY_cleaned``."""
    taxi_type = path.parent.parent.name.lower()
    year_match = re.fullmatch(r"year=(\d{4})", path.parent.name.lower())
    year = int(year_match.group(1)) if year_match else None
    file_match = re.fullmatch(r"([a-z0-9]+)_(\d{4})_cleaned\.parquet", path.name.lower())
    if file_match:
        taxi_type, year = file_match.group(1), int(file_match.group(2))
    return taxi_type, year


def discover_datasets(taxi_types: Iterable[str], years: Iterable[int]) -> list[Path]:
    wanted_taxis = set(taxi_types)
    wanted_years = set(years)
    datasets: list[Path] = []
    for candidate in sorted(CLEANED_BASE_DIR.glob("*/*/*_cleaned.parquet")):
        taxi_type, year = parse_cleaned_dataset_path(candidate)
        if taxi_type in wanted_taxis and year in wanted_years:
            datasets.append(candidate)
    return datasets


def append_audit(record: dict[str, Any]) -> None:
    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    complete_record = {
        "pipeline": "silver_b_normalization",
        "executed_at": datetime.now().isoformat(timespec="seconds"),
        **record,
    }
    with AUDIT_LOG_PATH.open("a", encoding="utf-8") as file:
        file.write(json.dumps(complete_record, ensure_ascii=False, default=str) + "\n")


def create_spark():
    """Crea una sesión Spark respetando la configuración Hadoop del proyecto."""
    from pyspark.sql import SparkSession

    global HADOOP_DLL_DIRECTORY

    if os.name == "nt":
        hadoop_home = ROOT_DIR / "tools" / "hadoop"
        hadoop_bin = hadoop_home / "bin"
        required_files = (hadoop_bin / "winutils.exe", hadoop_bin / "hadoop.dll")
        missing_files = [str(path) for path in required_files if not path.is_file()]
        if missing_files:
            raise FileNotFoundError("Faltan utilidades Hadoop: " + ", ".join(missing_files))

        # Igual que command_runner.py: no se debe heredar un HADOOP_HOME global.
        # PATH se modifica siempre (no con setdefault), para que la JVM encuentre
        # hadoop.dll al iniciar y Spark pueda escribir Parquet en Windows.
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)
        os.environ["PATH"] = str(hadoop_bin) + os.pathsep + os.environ.get("PATH", "")
        if hasattr(os, "add_dll_directory"):
            HADOOP_DLL_DIRECTORY = os.add_dll_directory(str(hadoop_bin))

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("SILVER_B1_NORMALIZATION")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "128")
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalize_columns(df, functions):
    """Normaliza nombres, preservando columnas con nombres potencialmente repetidos."""
    aliases = []
    used_names: set[str] = set()
    for original_name in df.columns:
        base_name = clean_column_name(original_name)
        new_name = base_name
        suffix = 2
        while new_name in used_names:
            new_name = f"{base_name}_{suffix}"
            suffix += 1
        used_names.add(new_name)
        escaped = original_name.replace("`", "``")
        aliases.append(functions.col(f"`{escaped}`").alias(new_name))
    return df.select(*aliases)


def first_existing(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    available = set(columns)
    return next((candidate for candidate in candidates if candidate in available), None)


def column_expr_or_null(df, functions, candidates: Iterable[str], data_type: str):
    source_column = first_existing(df.columns, candidates)
    if source_column is None:
        return functions.lit(None).cast(data_type), None
    return functions.col(source_column).cast(data_type), source_column


def ensure_canonical_column(df, functions, name: str, expression, created_columns: list[str]):
    if name in df.columns:
        return df.withColumn(name, functions.col(name))
    created_columns.append(name)
    return df.withColumn(name, expression)


def normalize_trip_dataset(spark, dataset_path: Path, force: bool) -> dict[str, Any]:
    """Transforma un dataset cleaned en su representación B1 canónica."""
    from pyspark.sql import functions as F

    taxi_type, year = parse_cleaned_dataset_path(dataset_path)
    if year is None:
        raise ValueError(f"No se pudo determinar el año de {dataset_path}")

    output_path = OUTPUT_BASE_DIR / f"{taxi_type}_{year}.parquet"
    if output_path.exists() and not force:
        return {
            "status": "skipped",
            "dataset": f"{taxi_type}_{year}.parquet",
            "taxi_type": taxi_type,
            "year": year,
            "input_path": str(dataset_path),
            "output_path": str(output_path),
            "reason": "La salida ya existe; use --force para regenerarla.",
        }

    started_at = datetime.now()
    input_files = parquet_part_files(dataset_path)
    if not input_files:
        raise FileNotFoundError(f"No se encontraron archivos Parquet en {dataset_path}")

    created_columns: list[str] = []
    df = normalize_columns(spark.read.parquet(*input_files), F)
    original_columns = list(df.columns)
    input_records = int(df.count())

    sources: dict[str, str | None] = {}
    canonical_sources = {
        "pickup_datetime": (PICKUP_DATETIME_CANDIDATES, "timestamp"),
        "dropoff_datetime": (DROPOFF_DATETIME_CANDIDATES, "timestamp"),
        "pickup_location_id": (PICKUP_LOCATION_CANDIDATES, "int"),
        "dropoff_location_id": (DROPOFF_LOCATION_CANDIDATES, "int"),
        "distancia_millas": (DISTANCE_CANDIDATES, "double"),
        "monto_base": (BASE_AMOUNT_CANDIDATES, "double"),
        "monto_total": (TOTAL_AMOUNT_CANDIDATES, "double"),
        "propina": (TIP_CANDIDATES, "double"),
        "peajes": (TOLLS_CANDIDATES, "double"),
        "payment_type": (PAYMENT_CANDIDATES, "int"),
        "passenger_count": (PASSENGER_CANDIDATES, "double"),
        "vendor_id": (VENDOR_CANDIDATES, "string"),
        "ratecode_id": (RATECODE_CANDIDATES, "int"),
        "shared_ride_flag": (SHARED_RIDE_CANDIDATES, "int"),
    }

    df = ensure_canonical_column(df, F, "tipo_dataset", F.lit(taxi_type), created_columns)
    df = ensure_canonical_column(df, F, "anio", F.lit(year).cast("int"), created_columns)
    for canonical_name, (candidates, data_type) in canonical_sources.items():
        expression, source = column_expr_or_null(df, F, candidates, data_type)
        sources[canonical_name] = source
        df = ensure_canonical_column(df, F, canonical_name, expression, created_columns)

    for canonical_name, candidates in OPTIONAL_DOUBLE_COLUMNS.items():
        expression, _ = column_expr_or_null(df, F, candidates, "double")
        df = ensure_canonical_column(df, F, canonical_name, expression, created_columns)

    df = df.withColumn("pickup_datetime", F.to_timestamp(F.col("pickup_datetime")))
    df = df.withColumn("dropoff_datetime", F.to_timestamp(F.col("dropoff_datetime")))
    if "duracion_segundos" not in df.columns:
        created_columns.append("duracion_segundos")
        df = df.withColumn(
            "duracion_segundos",
            F.when(
                F.col("pickup_datetime").isNotNull() & F.col("dropoff_datetime").isNotNull(),
                F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"),
            ).cast("long"),
        )
    if "duracion_minutos" not in df.columns:
        created_columns.append("duracion_minutos")
        df = df.withColumn("duracion_minutos", (F.col("duracion_segundos") / F.lit(60.0)).cast("double"))

    derived_columns = {
        "pickup_date": F.to_date("pickup_datetime"),
        "pickup_hour": F.hour("pickup_datetime"),
        "pickup_day_of_week": F.dayofweek("pickup_datetime"),
        "pickup_month": F.month("pickup_datetime"),
    }
    for canonical_name, expression in derived_columns.items():
        df = ensure_canonical_column(df, F, canonical_name, expression, created_columns)

    int_columns = {
        "pickup_location_id", "dropoff_location_id", "payment_type", "ratecode_id",
        "trip_type", "shared_ride_flag",
    }
    numeric_columns = {
        "pickup_location_id", "dropoff_location_id", "distancia_millas", "monto_base",
        "monto_total", "propina", "peajes", "extra", "mta_tax", "improvement_surcharge",
        "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "ehail_fee",
        "payment_type", "passenger_count", "ratecode_id", "trip_type", "shared_ride_flag",
    }
    for column in numeric_columns.intersection(df.columns):
        df = df.withColumn(column, F.col(column).cast("int" if column in int_columns else "double"))

    ordered_columns = [column for column in CANONICAL_ORDER if column in df.columns]
    extras = [column for column in ESSENTIAL_EXTRA_COLUMNS if column in df.columns and column not in ordered_columns]
    df = df.select(*ordered_columns, *extras)

    OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(output_path))
    record = {
        "status": "completed",
        "dataset": f"{taxi_type}_{year}.parquet",
        "taxi_type": taxi_type,
        "year": year,
        "input_path": str(dataset_path),
        "input_files": len(input_files),
        "output_path": str(output_path),
        "input_records": input_records,
        "output_records": input_records,
        "original_columns": len(original_columns),
        "output_columns": len(df.columns),
        "created_columns": sorted(set(created_columns)),
        "source_columns": sources,
        "duration_seconds": round((datetime.now() - started_at).total_seconds(), 2),
    }
    del df
    gc.collect()
    return record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normaliza datasets Silver con el modelo canónico B1.")
    parser.add_argument("--taxi", nargs="+", default=["all"], choices=["all", *TAXI_TYPES])
    parser.add_argument("--years", nargs="+", required=True, type=int, help="Años a normalizar.")
    parser.add_argument("--force", action="store_true", help="Regenera las salidas B1 ya existentes.")
    parser.add_argument("--dry-run", action="store_true", help="Muestra los datasets detectados sin iniciar Spark.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    selected_taxis = TAXI_TYPES if "all" in args.taxi else tuple(args.taxi)
    datasets = discover_datasets(selected_taxis, args.years)
    if not datasets:
        print(
            f"No se encontraron datasets cleaned para taxi={', '.join(selected_taxis)} "
            f"y años={', '.join(map(str, args.years))}.",
            file=sys.stderr,
        )
        return 1

    print("Datasets B1 seleccionados:")
    for dataset in datasets:
        taxi_type, year = parse_cleaned_dataset_path(dataset)
        print(f"- {dataset.relative_to(ROOT_DIR)} (taxi={taxi_type}, año={year})")
    if args.dry_run:
        return 0

    spark = create_spark()
    failures = 0
    try:
        for dataset in datasets:
            taxi_type, year = parse_cleaned_dataset_path(dataset)
            try:
                result = normalize_trip_dataset(spark, dataset, args.force)
                append_audit(result)
                print(
                    f"[{result['status'].upper()}] {result['dataset']} | "
                    f"{result.get('output_columns', 0)} columnas | "
                    f"{result.get('duration_seconds', 0)} s"
                )
            except Exception as error:
                failures += 1
                error_record = {
                    "status": "failed",
                    "dataset": f"{taxi_type}_{year}.parquet",
                    "taxi_type": taxi_type,
                    "year": year,
                    "input_path": str(dataset),
                    "error": str(error),
                }
                append_audit(error_record)
                print(f"[FAILED] {dataset.name}: {error}", file=sys.stderr)
    finally:
        spark.stop()

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

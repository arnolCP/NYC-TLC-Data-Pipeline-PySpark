"""Publica un modelo estrella Gold incremental en Parquet.

Consume los datasets canónicos de B1 desde ``data/silver/trip_data_normalized``
y publica dimensiones y ``fact_viajes_agregados`` para Power BI. El manifiesto
evita reprocesar fuentes Silver que no hayan cambiado.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT_DIR = Path(__file__).resolve().parents[2]
NORMALIZED_BASE_DIR = ROOT_DIR / "data" / "silver" / "trip_data_normalized"
LOOKUP_DIR = ROOT_DIR / "data" / "lookup" / "taxi_zone_lookup"
GOLD_ROOT = ROOT_DIR / "data" / "gold" / "modelo_estrella"
DIMENSIONS_DIR = GOLD_ROOT / "dimensiones"
FACTS_DIR = GOLD_ROOT / "hechos"
FACT_OUTPUT_DIR = FACTS_DIR / "fact_viajes_agregados.parquet"
CONTROL_DIR = GOLD_ROOT / "_control"
STAGING_SOURCE_DIR = GOLD_ROOT / "_staging" / "sources"
MANIFEST_PATH = CONTROL_DIR / "process_manifest.json"
AUDIT_LOG_PATH = ROOT_DIR / "data" / "logs" / "gold_parquet_audit.jsonl"
TAXI_TYPES = ("fhv", "fhvhv", "green", "yellow")
HADOOP_DLL_DIRECTORY = None

PAYMENT_ROWS = [(-1, "No informado / No aplica"), (0, "Flex Fare trip"), (1, "Credit card"), (2, "Cash"), (3, "No charge"), (4, "Dispute"), (5, "Unknown"), (6, "Voided trip")]
RATECODE_ROWS = [(-1, "No informado / No aplica"), (1, "Standard rate"), (2, "JFK"), (3, "Newark"), (4, "Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride"), (99, "Null / unknown")]
TRIP_TYPE_ROWS = [(-1, "No informado / No aplica"), (1, "Street-hail"), (2, "Dispatch")]
SHARED_RIDE_ROWS = [(-1, "No aplica"), (0, "No compartido / no informado"), (1, "Compartido")]
TAXI_ROWS = [("desconocido", "Tipo desconocido"), ("yellow", "Yellow Taxi"), ("green", "Green Taxi / SHL"), ("fhv", "For-Hire Vehicle"), ("fhvhv", "High-Volume For-Hire Vehicle")]
VENDOR_LABELS = {"1": "Creative Mobile Technologies, LLC", "2": "Curb Mobility, LLC", "6": "Myle Technologies Inc", "7": "Helix"}

FACT_KEYS = ["time_id", "hora_id", "tipo_dataset", "pickup_location_id", "dropoff_location_id", "payment_type", "ratecode_id", "trip_type", "shared_ride_flag", "provider_key"]
SOURCE_METRICS = ["passenger_count", "distancia_millas", "duracion_minutos", "monto_base", "monto_total", "propina", "peajes", "extra", "mta_tax", "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "ehail_fee"]
TOTAL_COLUMNS = {"passenger_count": "total_pasajeros", "distancia_millas": "total_distancia_millas", "duracion_minutos": "total_duracion_minutos", "monto_base": "total_monto_base", "monto_total": "total_monto_total", "propina": "total_propina", "peajes": "total_peajes", "extra": "total_extra", "mta_tax": "total_mta_tax", "improvement_surcharge": "total_improvement_surcharge", "congestion_surcharge": "total_congestion_surcharge", "airport_fee": "total_airport_fee", "cbd_congestion_fee": "total_cbd_congestion_fee", "ehail_fee": "total_ehail_fee"}
AVERAGE_COLUMNS = {"distancia_millas": "promedio_distancia_millas", "duracion_minutos": "promedio_duracion_minutos", "monto_total": "promedio_monto_total", "propina": "promedio_propina"}
REQUIRED_TYPES = {"tipo_dataset": "string", "anio": "int", "pickup_datetime": "timestamp", "pickup_month": "int", "pickup_hour": "int", "pickup_location_id": "int", "dropoff_location_id": "int", "payment_type": "int", "ratecode_id": "int", "trip_type": "int", "shared_ride_flag": "int", "vendor_id": "string", "dispatching_base_num": "string", "affiliated_base_number": "string", **{metric: "double" for metric in SOURCE_METRICS}}


def append_audit(record: dict[str, Any]) -> None:
    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"pipeline": "gold_modelo_estrella", "timestamp_utc": datetime.now(timezone.utc).isoformat(), **record}
    with AUDIT_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def parquet_files(path: Path) -> list[Path]:
    candidates = [path] if path.is_file() else list(path.rglob("*.parquet"))
    return sorted(file for file in candidates if file.is_file())


def source_identity(path: Path) -> tuple[str, int | None]:
    match = re.match(r"^(yellow|green|fhv|fhvhv)_(\d{4})", path.stem.lower())
    return (match.group(1), int(match.group(2))) if match else ("desconocido", None)


def select_sources(taxis: Iterable[str], years: Iterable[int]) -> list[Path]:
    selected_taxis, selected_years = set(taxis), set(years)
    return [
        path for path in sorted(NORMALIZED_BASE_DIR.glob("*.parquet"))
        if path.is_file() or path.is_dir()
        if source_identity(path)[0] in selected_taxis and source_identity(path)[1] in selected_years
    ]


def source_fingerprint(path: Path) -> dict[str, Any]:
    files = parquet_files(path)
    if not files:
        raise FileNotFoundError(f"No hay archivos Parquet dentro de {path}")
    digest, total_bytes, latest_mtime_ns = hashlib.sha256(), 0, 0
    for file in files:
        stat = file.stat()
        relative = file.relative_to(path) if path.is_dir() else Path(file.name)
        digest.update(relative.as_posix().encode("utf-8"))
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(str(stat.st_mtime_ns).encode("ascii"))
        total_bytes += stat.st_size
        latest_mtime_ns = max(latest_mtime_ns, stat.st_mtime_ns)
    return {"fingerprint": digest.hexdigest(), "part_files": len(files), "total_bytes": total_bytes, "latest_mtime_ns": latest_mtime_ns}


def source_id_for(path: Path) -> str:
    relative = path.relative_to(NORMALIZED_BASE_DIR).as_posix()
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", path.stem).strip("_").lower()
    return f"{slug}_{hashlib.sha1(relative.encode('utf-8')).hexdigest()[:8]}"


def load_manifest() -> dict[str, Any]:
    if not MANIFEST_PATH.exists():
        return {"version": 1, "sources": {}}
    with MANIFEST_PATH.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    manifest.setdefault("version", 1)
    manifest.setdefault("sources", {})
    return manifest


def save_manifest(manifest: dict[str, Any]) -> None:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    temporary = MANIFEST_PATH.with_suffix(".json.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, ensure_ascii=False)
    os.replace(temporary, MANIFEST_PATH)


def prune_stale_staging(manifest: dict[str, Any]) -> list[str]:
    """Elimina contribuciones de fuentes Silver que ya no existen.

    Evita que un staging heredado (por ejemplo, de otro equipo o de años que
    ya no están en ``trip_data_normalized``) siga alimentando Gold.
    """
    current_ids = {
        source_id_for(path)
        for path in NORMALIZED_BASE_DIR.glob("*.parquet")
        if path.is_file() or path.is_dir()
    }
    removed: list[str] = []
    for source_id in list(manifest["sources"]):
        if source_id not in current_ids:
            target = STAGING_SOURCE_DIR / source_id
            if target.exists():
                shutil.rmtree(target)
            manifest["sources"].pop(source_id, None)
            removed.append(source_id)
    for target in STAGING_SOURCE_DIR.iterdir() if STAGING_SOURCE_DIR.exists() else []:
        if target.is_dir() and not target.name.startswith("_tmp_") and target.name not in current_ids:
            shutil.rmtree(target)
            if target.name not in removed:
                removed.append(target.name)
    return removed


def create_spark():
    global HADOOP_DLL_DIRECTORY
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if os.name == "nt":
        hadoop_home = ROOT_DIR / "tools" / "hadoop"
        hadoop_bin = hadoop_home / "bin"
        required = (hadoop_bin / "winutils.exe", hadoop_bin / "hadoop.dll")
        missing = [str(path) for path in required if not path.is_file()]
        if missing:
            raise FileNotFoundError("Faltan utilidades Hadoop: " + ", ".join(missing))
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)
        os.environ["PATH"] = str(hadoop_bin) + os.pathsep + os.environ.get("PATH", "")
        if hasattr(os, "add_dll_directory"):
            HADOOP_DLL_DIRECTORY = os.add_dll_directory(str(hadoop_bin))

    spark = (SparkSession.builder.master("local[2]").appName("GOLD_MODELO_ESTRELLA")
        .config("spark.ui.enabled", "false").config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g").config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.python.worker.reuse", "true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalize_columns(df, F):
    result = df
    for original in df.columns:
        normalized = re.sub(r"[^a-z0-9]+", "_", original.strip().lower()).strip("_")
        if normalized != original:
            result = result.withColumnRenamed(original, normalized)
    return result


def ensure_columns(df, F, definitions: dict[str, str]):
    result = df
    for name, data_type in definitions.items():
        result = result.withColumn(name, (F.col(name) if name in result.columns else F.lit(None)).cast(data_type))
    return result


def non_empty_string(F, name: str):
    value = F.trim(F.col(name).cast("string"))
    return F.when(F.length(value) > 0, value)


def prepare_source(df, F, source_path: Path):
    taxi_type, source_year = source_identity(source_path)
    prepared = ensure_columns(normalize_columns(df, F), F, REQUIRED_TYPES)
    prepared = prepared.withColumn("tipo_dataset", F.coalesce(non_empty_string(F, "tipo_dataset"), F.lit(taxi_type)))
    if source_year is not None:
        prepared = prepared.withColumn("anio", F.coalesce(F.col("anio"), F.lit(source_year)))
    valid_month = F.col("anio").between(1900, 2200) & F.col("pickup_month").between(1, 12)
    prepared = prepared.withColumn("time_id", F.coalesce(F.date_format("pickup_datetime", "yyyy-MM"), F.when(valid_month, F.format_string("%04d-%02d", F.col("anio"), F.col("pickup_month"))), F.lit("SIN_FECHA")))
    hour_value = F.coalesce(F.col("pickup_hour"), F.hour("pickup_datetime"))
    prepared = prepared.withColumn("hora_id", F.when(hour_value.between(0, 23), hour_value.cast("int")).otherwise(F.lit(-1)))
    for key in ("pickup_location_id", "dropoff_location_id", "payment_type", "ratecode_id", "trip_type", "shared_ride_flag"):
        prepared = prepared.withColumn(key, F.coalesce(F.col(key).cast("int"), F.lit(-1)))
    provider_id = F.coalesce(non_empty_string(F, "vendor_id"), non_empty_string(F, "dispatching_base_num"), non_empty_string(F, "affiliated_base_number"), F.lit("NO_INFO"))
    return prepared.withColumn("provider_id", provider_id).withColumn("provider_key", F.concat_ws("|", F.col("tipo_dataset"), F.col("provider_id")))


def build_fact_contribution(prepared, F):
    aggregations = [F.count(F.lit(1)).cast("long").alias("cantidad_viajes")]
    aggregations.extend(F.sum(F.coalesce(F.col(source).cast("double"), F.lit(0.0))).alias(total) for source, total in TOTAL_COLUMNS.items())
    aggregations.extend(F.count(F.when(F.col(source).isNotNull(), F.lit(1))).cast("long").alias(f"_count_{source}") for source in AVERAGE_COLUMNS)
    return prepared.groupBy(*FACT_KEYS).agg(*aggregations)


def stage_source(spark, F, source_info: dict[str, Any]) -> dict[str, Any]:
    from pyspark import StorageLevel

    source_path, source_id = source_info["path"], source_info["source_id"]
    target_path, temporary = STAGING_SOURCE_DIR / source_id, STAGING_SOURCE_DIR / f"_tmp_{source_id}"
    if temporary.exists():
        shutil.rmtree(temporary)
    prepared = prepare_source(spark.read.parquet(str(source_path)), F, source_path).persist(StorageLevel.DISK_ONLY)
    try:
        input_rows = int(prepared.count())
        build_fact_contribution(prepared, F).repartition(4, "time_id").write.mode("overwrite").parquet(str(temporary / "fact_contribution.parquet"))
        prepared.select("provider_key", "provider_id", "tipo_dataset").dropDuplicates(["provider_key"]).coalesce(1).write.mode("overwrite").parquet(str(temporary / "provider_candidates.parquet"))
        if target_path.exists():
            shutil.rmtree(target_path)
        temporary.replace(target_path)
        return {"input_rows": input_rows, "stage_path": target_path.relative_to(GOLD_ROOT).as_posix()}
    finally:
        prepared.unpersist()
        if temporary.exists():
            shutil.rmtree(temporary)


def complete_numeric_catalog(spark, fact, F, key: str, rows: list[tuple], label: str):
    catalog = spark.createDataFrame(rows, schema=f"{key} int, {label} string")
    return fact.select(F.col(key).cast("int").alias(key)).distinct().join(catalog, key, "left").withColumn(label, F.coalesce(F.col(label), F.concat(F.lit("Codigo "), F.col(key)))).orderBy(key)


def complete_string_catalog(spark, fact, F, key: str, rows: list[tuple], label: str):
    catalog = spark.createDataFrame(rows, schema=f"{key} string, {label} string")
    return fact.select(F.col(key).cast("string").alias(key)).distinct().join(catalog, key, "left").withColumn(label, F.coalesce(F.col(label), F.concat(F.lit("Tipo "), F.col(key)))).orderBy(key)


def build_time_dimension(fact, F):
    names = [(1, "Enero"), (2, "Febrero"), (3, "Marzo"), (4, "Abril"), (5, "Mayo"), (6, "Junio"), (7, "Julio"), (8, "Agosto"), (9, "Septiembre"), (10, "Octubre"), (11, "Noviembre"), (12, "Diciembre")]
    month_names = F.create_map(*[item for month, name in names for item in (F.lit(month), F.lit(name))])
    valid = F.col("time_id").rlike(r"^\d{4}-(0[1-9]|1[0-2])$")
    return (fact.select("time_id").distinct().withColumn("fecha_inicio_mes", F.when(valid, F.to_date(F.concat(F.col("time_id"), F.lit("-01")))))
        .withColumn("anio", F.when(valid, F.substring("time_id", 1, 4).cast("int")).otherwise(F.lit(-1)))
        .withColumn("mes", F.when(valid, F.substring("time_id", 6, 2).cast("int")).otherwise(F.lit(-1)))
        .withColumn("nombre_mes", F.when(valid, F.element_at(month_names, F.col("mes"))).otherwise(F.lit("Sin fecha")))
        .withColumn("trimestre", F.when(valid, F.ceil(F.col("mes") / F.lit(3.0)).cast("int")).otherwise(F.lit(-1)))
        .withColumn("etiqueta_mes", F.when(valid, F.concat_ws(" ", F.col("nombre_mes"), F.col("anio"))).otherwise(F.lit("Sin fecha")))
        .withColumn("orden_mes", F.when(valid, F.col("anio") * 100 + F.col("mes")).otherwise(F.lit(-1)))
        .select("time_id", "fecha_inicio_mes", "anio", "mes", "nombre_mes", "trimestre", "etiqueta_mes", "orden_mes").orderBy("orden_mes"))


def build_zone_dimensions(spark, fact, F):
    lookup_paths = [path for path in sorted(LOOKUP_DIR.glob("*.parquet")) if path.is_file() or path.is_dir()]
    if not lookup_paths:
        raise FileNotFoundError(f"No hay lookup Parquet en {LOOKUP_DIR}")
    lookup = normalize_columns(spark.read.parquet(*[str(path) for path in lookup_paths]), F)
    zones = lookup.select(F.col("locationid").cast("int").alias("location_id"), F.col("borough").cast("string").alias("borough"), F.col("zone").cast("string").alias("zone"), F.col("service_zone").cast("string").alias("service_zone")).dropDuplicates(["location_id"])
    pickup = fact.select("pickup_location_id").distinct().join(zones, F.col("pickup_location_id") == F.col("location_id"), "left").select("pickup_location_id", F.coalesce("borough", F.lit("No informado")).alias("pickup_borough"), F.coalesce("zone", F.lit("No informado")).alias("pickup_zone"), F.coalesce("service_zone", F.lit("No informado")).alias("pickup_service_zone"))
    dropoff = fact.select("dropoff_location_id").distinct().join(zones, F.col("dropoff_location_id") == F.col("location_id"), "left").select("dropoff_location_id", F.coalesce("borough", F.lit("No informado")).alias("dropoff_borough"), F.coalesce("zone", F.lit("No informado")).alias("dropoff_zone"), F.coalesce("service_zone", F.lit("No informado")).alias("dropoff_service_zone"))
    return pickup, dropoff


def build_provider_dimension(spark, F):
    paths = list(STAGING_SOURCE_DIR.glob("*/provider_candidates.parquet"))
    providers = spark.read.parquet(*[str(path) for path in paths]).groupBy("provider_key").agg(F.first("provider_id", ignorenulls=True).alias("provider_id"), F.first("tipo_dataset", ignorenulls=True).alias("tipo_dataset"))
    vendor_map = F.create_map(*[item for key, value in VENDOR_LABELS.items() for item in (F.lit(key), F.lit(value))])
    return (providers.withColumn("tipo_proveedor", F.when(F.col("tipo_dataset") == "fhv", "Base FHV").otherwise("Vendor TLC"))
        .withColumn("proveedor", F.when(F.col("provider_id") == "NO_INFO", "No informado").when(F.element_at(vendor_map, F.col("provider_id")).isNotNull(), F.element_at(vendor_map, F.col("provider_id"))).when(F.col("tipo_dataset") == "fhv", F.concat(F.lit("Base "), F.col("provider_id"))).otherwise(F.concat(F.lit("Vendor "), F.col("provider_id"))))
        .select("provider_key", "provider_id", "tipo_dataset", "tipo_proveedor", "proveedor"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construye el modelo estrella Gold incremental en Parquet.")
    parser.add_argument("--taxi", nargs="+", choices=["all", *TAXI_TYPES], default=["all"])
    parser.add_argument("--years", nargs="+", required=True, type=int)
    parser.add_argument("--force-reprocess", action="store_true", help="Regenera staging de todas las fuentes seleccionadas.")
    parser.add_argument("--force-rebuild", action="store_true", help="Republica Gold aunque Silver no haya cambiado.")
    parser.add_argument("--dry-run", action="store_true", help="Lista fuentes sin iniciar Spark.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    taxis = TAXI_TYPES if "all" in args.taxi else tuple(args.taxi)
    sources = select_sources(taxis, args.years)
    if not sources:
        print("No hay fuentes B1 seleccionadas para construir Gold.", file=sys.stderr)
        return 1
    print("Fuentes Gold seleccionadas:")
    for source in sources:
        print(f"- {source.relative_to(ROOT_DIR)}")
    if args.dry_run:
        return 0

    DIMENSIONS_DIR.mkdir(parents=True, exist_ok=True)
    FACTS_DIR.mkdir(parents=True, exist_ok=True)
    STAGING_SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    stale_sources = prune_stale_staging(manifest)
    if stale_sources:
        append_audit({"status": "staging_pruned", "removed_sources": stale_sources})
    infos, pending = [], []
    for path in sources:
        info = {"path": path, "source_id": source_id_for(path), **source_fingerprint(path)}
        infos.append(info)
        previous = manifest["sources"].get(info["source_id"])
        stage_exists = (STAGING_SOURCE_DIR / info["source_id"] / "fact_contribution.parquet").exists()
        if args.force_reprocess or previous is None or previous.get("fingerprint") != info["fingerprint"] or not stage_exists:
            pending.append(info)

    spark = create_spark()
    from pyspark.sql import functions as F
    staged_updates: dict[str, Any] = {}
    try:
        for info in pending:
            started = datetime.now(timezone.utc)
            try:
                stats = stage_source(spark, F, info)
                staged_updates[info["source_id"]] = {"path": info["path"].relative_to(ROOT_DIR).as_posix(), "fingerprint": info["fingerprint"], "part_files": info["part_files"], "total_bytes": info["total_bytes"], "latest_mtime_ns": info["latest_mtime_ns"], "processed_at_utc": datetime.now(timezone.utc).isoformat(), **stats}
                append_audit({"status": "staged", "source": info["path"].name, "source_id": info["source_id"], "input_rows": stats["input_rows"], "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2)})
            except Exception as error:
                append_audit({"status": "failed", "source": info["path"].name, "source_id": info["source_id"], "error": str(error)})
                raise

        stage_fact_paths = sorted(STAGING_SOURCE_DIR.glob("*/fact_contribution.parquet"))
        publish_needed = bool(pending) or args.force_rebuild or not FACT_OUTPUT_DIR.exists()
        if not publish_needed:
            append_audit({"status": "gold_skipped", "reason": "No hay fuentes Silver modificadas.", "selected_sources": [path.name for path in sources]})
            print("Gold vigente: no hubo cambios en Silver.")
            return 0
        if not stage_fact_paths:
            raise RuntimeError("No existen contribuciones en staging para construir Gold.")

        contributions = spark.read.parquet(*[str(path) for path in stage_fact_paths])
        additive = ["cantidad_viajes", *TOTAL_COLUMNS.values(), *[f"_count_{source}" for source in AVERAGE_COLUMNS]]
        fact = contributions.groupBy(*FACT_KEYS).agg(*[F.sum(F.col(name)).alias(name) for name in additive])
        for source, average in AVERAGE_COLUMNS.items():
            fact = fact.withColumn(average, F.when(F.col(f"_count_{source}") > 0, F.col(TOTAL_COLUMNS[source]) / F.col(f"_count_{source}")))
        fact = fact.select(*FACT_KEYS, "cantidad_viajes", *TOTAL_COLUMNS.values(), *AVERAGE_COLUMNS.values()).cache()
        fact_count = int(fact.count())

        pickup, dropoff = build_zone_dimensions(spark, fact, F)
        dimensions = {
            "dim_tiempo": build_time_dimension(fact, F),
            "dim_hora": fact.select("hora_id").distinct().withColumn("hora", F.when(F.col("hora_id") >= 0, F.col("hora_id"))).withColumn("franja_horaria", F.when(F.col("hora_id") == -1, "Sin hora").when(F.col("hora_id").between(0, 5), "Madrugada").when(F.col("hora_id").between(6, 11), "Manana").when(F.col("hora_id").between(12, 17), "Tarde").otherwise("Noche")),
            "dim_tipo_taxi": complete_string_catalog(spark, fact, F, "tipo_dataset", TAXI_ROWS, "tipo_taxi"),
            "dim_zona_pickup": pickup, "dim_zona_dropoff": dropoff,
            "dim_pago": complete_numeric_catalog(spark, fact, F, "payment_type", PAYMENT_ROWS, "forma_pago"),
            "dim_ratecode": complete_numeric_catalog(spark, fact, F, "ratecode_id", RATECODE_ROWS, "ratecode"),
            "dim_trip_type": complete_numeric_catalog(spark, fact, F, "trip_type", TRIP_TYPE_ROWS, "tipo_viaje"),
            "dim_shared_ride": complete_numeric_catalog(spark, fact, F, "shared_ride_flag", SHARED_RIDE_ROWS, "viaje_compartido"),
            "dim_proveedor": build_provider_dimension(spark, F),
        }
        keys = {"dim_tiempo": "time_id", "dim_hora": "hora_id", "dim_tipo_taxi": "tipo_dataset", "dim_zona_pickup": "pickup_location_id", "dim_zona_dropoff": "dropoff_location_id", "dim_pago": "payment_type", "dim_ratecode": "ratecode_id", "dim_trip_type": "trip_type", "dim_shared_ride": "shared_ride_flag", "dim_proveedor": "provider_key"}
        dimension_rows = {}
        for name, dimension in dimensions.items():
            rows, unique = int(dimension.count()), int(dimension.select(keys[name]).distinct().count())
            if rows != unique:
                raise ValueError(f"{name} contiene claves duplicadas en {keys[name]}")
            dimension_rows[name] = rows
            dimension.coalesce(1).write.mode("overwrite").parquet(str(DIMENSIONS_DIR / f"{name}.parquet"))
        fact.repartition(16, "time_id").write.mode("overwrite").parquet(str(FACT_OUTPUT_DIR))
        manifest["sources"].update(staged_updates)
        manifest.update({"last_gold_publish_utc": datetime.now(timezone.utc).isoformat(), "fact_rows": fact_count, "dimension_rows": dimension_rows})
        save_manifest(manifest)
        append_audit({"status": "gold_published", "processed_sources": [info["path"].name for info in pending], "fact_rows": fact_count, "dimension_rows": dimension_rows, "gold_root": str(GOLD_ROOT)})
        fact.unpersist()
        print(f"Gold publicado: {FACT_OUTPUT_DIR} ({fact_count:,} filas agregadas).")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

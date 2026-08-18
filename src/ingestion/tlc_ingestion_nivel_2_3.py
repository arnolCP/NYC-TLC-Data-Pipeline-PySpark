#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TLC Trip Record Data - Pipeline de Ingesta Nivel 2 y Nivel 3

Nivel 2 - Data Ingestion Layer
- Construcción de catálogo de archivos TLC.
- Descarga idempotente de archivos Parquet mensuales.
- Descarga de Taxi Zone Lookup Table.
- Manifest y auditoría de ingesta.
- Validación de metadata, hash, tamaño, formato y lectura con PySpark.

Nivel 3 - Bronze / Raw Data Layer
- Conserva archivos originales en zona raw.
- Copia archivos válidos a zona bronze sin modificar su contenido.
- Mantiene trazabilidad técnica en JSONL y Parquet.

Ejemplos:
    # Prueba rápida, igual que el notebook actual
    python tlc_ingestion_nivel_2_3.py --years 2023 --max-files 4

    # Ejecución completa para los años solicitados
    python tlc_ingestion_nivel_2_3.py --years 2023 2024 2025 --max-files none

    # Día del examen, incluyendo 2026 dinámico según archivos disponibles
    python tlc_ingestion_nivel_2_3.py --years 2023 2024 2025 2026 --max-files none
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Falta pyarrow. Instala dependencias con: pip install pandas pyarrow pyspark rich tqdm"
    ) from exc

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
except Exception:  # pragma: no cover
    Console = None
    Panel = None
    Table = None

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover

    def tqdm(iterable: Iterable, **_: Any) -> Iterable:
        return iterable


# ============================================================
# 0. CONSTANTES
# ============================================================

PIPELINE_NAME = "tlc_ingestion_nivel_2_3"
PIPELINE_VERSION = "1.1.0"
LOCAL_TZ = "America/Lima"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
SOURCE_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
FORBIDDEN_COOLDOWN_SECONDS = 300

TRIP_TYPE_PREFIX = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "fhvhv": "fhvhv_tripdata",
}

LOOKUP_EXPECTED_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]

# La raíz se deriva de la ubicación del archivo, no del directorio actual.
# tlc_ingestion_nivel_2_3.py -> ingestion -> src -> raíz del repositorio.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WORKSPACE_DIR = PROJECT_ROOT / "data"

console = Console() if Console else None


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================


@dataclass(frozen=True)
class PipelineConfig:
    years: list[int]
    trip_types: list[str]
    max_files: int | None
    overwrite_raw: bool
    strict_completeness: bool
    include_lookup: bool
    workspace_dir: Path
    forbidden_cooldown_seconds: int = FORBIDDEN_COOLDOWN_SECONDS
    local_tz: str = LOCAL_TZ
    pipeline_name: str = PIPELINE_NAME
    pipeline_version: str = PIPELINE_VERSION

    @property
    def raw_dir(self) -> Path:
        return self.workspace_dir / "raw"

    @property
    def bronze_dir(self) -> Path:
        return self.workspace_dir / "bronze"

    @property
    def bronze_files_dir(self) -> Path:
        return self.bronze_dir / "files"

    @property
    def bronze_metadata_dir(self) -> Path:
        return self.bronze_dir / "_metadata"

    @property
    def log_dir(self) -> Path:
        return self.workspace_dir / "logs"

    @property
    def spark_staging_dir(self) -> Path:
        return self.workspace_dir / "_spark_staging"

    @property
    def manifest_jsonl(self) -> Path:
        return self.log_dir / "tlc_ingestion_manifest.jsonl"

    @property
    def selected_trip_types(self) -> list[str]:
        if "all" in self.trip_types:
            return list(TRIP_TYPE_PREFIX.keys())

        invalid = [item for item in self.trip_types if item not in TRIP_TYPE_PREFIX]
        if invalid:
            raise ValueError(f"Tipos de viaje inválidos: {invalid}")

        return self.trip_types

    @property
    def closed_years(self) -> list[int]:
        return [year for year in self.years if year in [2023, 2024, 2025]]

    @property
    def dynamic_years(self) -> list[int]:
        return [year for year in self.years if year >= 2026]


def parse_max_files(value: str | None) -> int | None:
    if value is None:
        return None

    value_clean = str(value).strip().lower()
    if value_clean in {"none", "null", "no", "full", "all", "0"}:
        return None

    parsed = int(value_clean)
    if parsed < 1:
        raise argparse.ArgumentTypeError("--max-files debe ser >= 1 o 'none'.")
    return parsed


def parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(
        description="Pipeline TLC Nivel 2 y Nivel 3: ingesta, validación, raw y bronze."
    )

    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2023],
        help="Años a procesar. Ejemplo: --years 2023 2024 2025 2026",
    )
    parser.add_argument(
        "--trip-types",
        nargs="+",
        default=["all"],
        choices=["all", "yellow", "green", "fhv", "fhvhv"],
        help="Tipos de dataset a procesar.",
    )
    parser.add_argument(
        "--max-files",
        type=parse_max_files,
        default=4,
        help="Límite de archivos para prueba. Usa 'none' para ejecución completa.",
    )
    parser.add_argument(
        "--workspace-dir",
        type=Path,
        default=DEFAULT_WORKSPACE_DIR,
        help=(
            "Directorio base donde se crearán raw, bronze y logs. "
            "Por defecto usa <raíz-del-proyecto>/data."
        ),
    )
    parser.add_argument(
        "--overwrite-raw",
        action="store_true",
        help="Fuerza re-descarga aunque el archivo ya exista en raw.",
    )
    parser.add_argument(
        "--no-strict-completeness",
        action="store_true",
        help="No falla si faltan archivos esperados en años cerrados.",
    )
    parser.add_argument(
        "--no-lookup",
        action="store_true",
        help="No descarga Taxi Zone Lookup Table.",
    )
    parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=5,
        help="Minutos de espera antes de reintentar una descarga bloqueada con HTTP 403.",
    )

    args = parser.parse_args()

    if args.cooldown_minutes < 0:
        raise argparse.ArgumentTypeError("--cooldown-minutes debe ser >= 0.")

    return PipelineConfig(
        years=args.years,
        trip_types=args.trip_types,
        max_files=args.max_files,
        overwrite_raw=args.overwrite_raw,
        strict_completeness=not args.no_strict_completeness,
        include_lookup=not args.no_lookup,
        workspace_dir=args.workspace_dir.resolve(),
        forbidden_cooldown_seconds=args.cooldown_minutes * 60,
    )


# ============================================================
# 2. PRESENTACIÓN Y UTILIDADES
# ============================================================


def print_panel(title: str, message: Any, style: str = "blue") -> None:
    if console and Panel:
        console.print(Panel(str(message), title=title, style=style))
    else:
        print(f"\n[{title}] {message}\n")


def format_table_value(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, (list, tuple, set)):
        return str(list(value))

    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)

    try:
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            return ""
    except Exception:
        pass

    return str(value)


def print_dataframe(df: pd.DataFrame, title: str, max_rows: int = 40) -> None:
    if df is None or df.empty:
        print_panel(title, "Sin registros.", "yellow")
        return

    show_df = df.head(max_rows).copy()

    if console and Table:
        table = Table(title=title)
        for column in show_df.columns:
            table.add_column(str(column), overflow="fold")
        for _, row in show_df.iterrows():
            table.add_row(*[format_table_value(value) for value in row])
        console.print(table)
        if len(df) > max_rows:
            console.print(f"Mostrando {max_rows} de {len(df)} registros.")
    else:
        print(f"\n{title}")
        print(show_df.to_string(index=False))
        if len(df) > max_rows:
            print(f"Mostrando {max_rows} de {len(df)} registros.")


def ensure_directories(config: PipelineConfig) -> None:
    for directory in [
        config.raw_dir,
        config.bronze_files_dir,
        config.bronze_metadata_dir,
        config.log_dir,
        config.spark_staging_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)


def now_local(config: PipelineConfig) -> datetime:
    return datetime.now(ZoneInfo(config.local_tz))


def sha256_file(path: Path, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(block_size), b""):
            digest.update(block)
    return digest.hexdigest()


def append_manifest(config: PipelineConfig, record: dict[str, Any]) -> None:
    config.manifest_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with config.manifest_jsonl.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def has_curl() -> bool:
    return shutil.which("curl") is not None


def url_exists(url: str) -> bool:
    """Valida si una URL existe usando curl; si no hay curl, usa urllib con HEAD."""
    if has_curl():
        cmd = [
            "curl",
            "-L",
            "--fail",
            "--silent",
            "--show-error",
            "--head",
            "--connect-timeout",
            "20",
            "--max-time",
            "40",
            "-A",
            "Mozilla/5.0",
            "-e",
            SOURCE_PAGE,
            url,
        ]
        completed = subprocess.run(cmd, capture_output=True, text=True)
        return completed.returncode == 0

    try:
        request = urllib.request.Request(
            url,
            method="HEAD",
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": SOURCE_PAGE,
            },
        )
        with urllib.request.urlopen(request, timeout=40) as response:
            return 200 <= response.status < 400
    except Exception:
        return False


def download_with_curl(url: str, target_path: Path) -> None:
    cmd = [
        "curl",
        "-L",
        "--fail",
        "--silent",
        "--show-error",
        "--retry",
        "5",
        "--retry-delay",
        "5",
        "--retry-all-errors",
        "--connect-timeout",
        "30",
        "--max-time",
        "0",
        "-H",
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "-H",
        "Accept: application/octet-stream,*/*",
        "-H",
        f"Referer: {SOURCE_PAGE}",
        "-o",
        str(target_path),
        url,
    ]

    completed = subprocess.run(cmd, capture_output=True, text=True)
    if completed.returncode != 0:
        err = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(err)


def download_with_urllib(url: str, target_path: Path) -> None:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": SOURCE_PAGE,
        },
    )

    with (
        urllib.request.urlopen(request, timeout=120) as response,
        target_path.open("wb") as file,
    ):
        shutil.copyfileobj(response, file)


def is_http_403_error(error: Any) -> bool:
    error_message = str(error).lower()
    return "403" in error_message or "forbidden" in error_message


def _download_once(url: str, part_path: Path) -> None:
    if has_curl():
        download_with_curl(url, part_path)
    else:
        download_with_urllib(url, part_path)


def download_file(
    url: str,
    target_path: Path,
    overwrite: bool,
    cooldown_seconds: int = FORBIDDEN_COOLDOWN_SECONDS,
) -> str:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = target_path.with_suffix(target_path.suffix + ".part")

    if target_path.exists() and target_path.stat().st_size > 0 and not overwrite:
        return "skipped_existing"

    if part_path.exists():
        part_path.unlink()

    try:
        _download_once(url, part_path)
    except Exception as first_error:
        if not is_http_403_error(first_error) or cooldown_seconds <= 0:
            raise

        minutes = round(cooldown_seconds / 60, 2)
        print_panel(
            "HTTP 403",
            f"Bloqueo temporal detectado. Esperando {minutes} minuto(s) antes de reintentar el mismo archivo.",
            "yellow",
        )
        time.sleep(cooldown_seconds)

        if part_path.exists():
            part_path.unlink()

        try:
            _download_once(url, part_path)
        except Exception as second_error:
            raise RuntimeError(
                f"Falló luego de esperar {minutes} minuto(s). "
                f"Primer error: {first_error}. Segundo error: {second_error}"
            ) from second_error

    if not part_path.exists() or part_path.stat().st_size == 0:
        raise RuntimeError("Descarga vacía.")

    part_path.replace(target_path)
    return "downloaded"


# ============================================================
# 3. JAVA Y SPARK
# ============================================================


def first_existing_path(paths: list[str]) -> str | None:
    for candidate in paths:
        path = Path(candidate)
        if path.exists():
            return str(path)
    return None


def configure_java_and_spark_env() -> None:
    if platform.system().lower() == "windows":
        detected_java = first_existing_path(
            [
                r"C:\Program Files\Java\jdk-17",
                r"C:\Program Files\Java\jdk-21",
                r"C:\Program Files\Eclipse Adoptium\jdk-17",
                r"C:\Program Files\Eclipse Adoptium\jdk-21",
            ]
        )

        if detected_java:
            os.environ["JAVA_HOME"] = detected_java
            os.environ["PATH"] = detected_java + r"\bin;" + os.environ["PATH"]

    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


def start_spark():
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Falta PySpark. Instala dependencias con: pip install pyspark"
        ) from exc

    spark_start = time.perf_counter()
    spark = (
        SparkSession.builder.appName("TLC_Ingestion_Nivel_2_3")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.files.ignoreCorruptFiles", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    spark.range(1).collect()

    elapsed = round(time.perf_counter() - spark_start, 2)
    print_panel(
        "Spark activo",
        f"Versión: {spark.version} | Inicio: {elapsed} segundos",
        "green",
    )
    return spark


# ============================================================
# 4. CATÁLOGO DE ARCHIVOS TLC
# ============================================================


def build_trip_file_record(
    config: PipelineConfig, year: int, month: int, trip_type: str
) -> dict[str, Any]:
    prefix = TRIP_TYPE_PREFIX[trip_type]
    file_name = f"{prefix}_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{file_name}"

    local_path = (
        config.raw_dir
        / "trip_data"
        / f"tipo_dataset={trip_type}"
        / f"anio={year}"
        / f"mes={month:02d}"
        / file_name
    )

    return {
        "source_kind": "trip_data",
        "year": int(year),
        "month": int(month),
        "trip_type": trip_type,
        "file_name": file_name,
        "url": url,
        "local_path": str(local_path),
        "format": "parquet",
    }


def build_trip_catalog(config: PipelineConfig) -> pd.DataFrame:
    records = []
    current = datetime.now()
    current_year = current.year
    current_month = current.month

    for year in config.years:
        for month in range(1, 13):
            if year > current_year:
                continue
            if year == current_year and month > current_month:
                continue

            for trip_type in config.selected_trip_types:
                record = build_trip_file_record(config, year, month, trip_type)

                # Para 2026 en adelante no se asume cobertura completa.
                # Se consulta la existencia para procesar automáticamente solo lo publicado.
                if year in config.dynamic_years:
                    exists = url_exists(record["url"])
                    if exists is False:
                        continue

                records.append(record)

    return pd.DataFrame(records)


def build_lookup_record(config: PipelineConfig) -> dict[str, Any]:
    local_path = config.raw_dir / "lookup" / "taxi_zone_lookup.csv"
    return {
        "source_kind": "taxi_zone_lookup",
        "year": None,
        "month": None,
        "trip_type": "lookup",
        "file_name": "taxi_zone_lookup.csv",
        "url": LOOKUP_URL,
        "local_path": str(local_path),
        "format": "csv",
    }


def apply_test_limit(catalog_df: pd.DataFrame, max_files: int | None) -> pd.DataFrame:
    if max_files is None:
        return catalog_df.copy()
    return catalog_df.head(max_files).copy()


# ============================================================
# 5. VALIDACIÓN DE COBERTURA
# ============================================================


def validate_closed_year_coverage(
    config: PipelineConfig, full_catalog_df: pd.DataFrame
) -> None:
    expected_records = []

    for year in config.closed_years:
        for month in range(1, 13):
            for trip_type in config.selected_trip_types:
                expected_records.append(
                    {
                        "year": int(year),
                        "month": int(month),
                        "trip_type": trip_type,
                    }
                )

    expected_df = pd.DataFrame(expected_records)

    if expected_df.empty:
        print_panel(
            "Validación de cobertura", "No hay años cerrados para validar.", "yellow"
        )
        return

    current_df = full_catalog_df[["year", "month", "trip_type"]].drop_duplicates()
    missing_df = (
        expected_df.merge(
            current_df, on=["year", "month", "trip_type"], how="left", indicator=True
        )
        .query("_merge == 'left_only'")
        .drop(columns=["_merge"])
    )

    if missing_df.empty:
        print_panel(
            "Validación de cobertura", "Cobertura completa para años cerrados.", "green"
        )
        return

    print_dataframe(missing_df, "Archivos faltantes para años cerrados", max_rows=80)
    if config.strict_completeness and config.max_files is None:
        raise RuntimeError("Cobertura incompleta para años cerrados.")

    print_panel(
        "Validación de cobertura",
        "Hay faltantes, pero no se detuvo porque estás en modo prueba o strict_completeness está desactivado.",
        "yellow",
    )


# ============================================================
# 6. DESCARGA RAW
# ============================================================


def download_raw_files(
    config: PipelineConfig,
    catalog_df: pd.DataFrame,
    run_id: str,
    run_ts: str,
) -> pd.DataFrame:
    download_results = []

    for _, row in tqdm(
        catalog_df.iterrows(), total=len(catalog_df), desc="Descargando raw"
    ):
        local_path = Path(row["local_path"])

        result = {
            "pipeline_id": run_id,
            "pipeline_name": config.pipeline_name,
            "pipeline_version": config.pipeline_version,
            "started_at": run_ts,
            "source_kind": row["source_kind"],
            "year": row.get("year"),
            "month": row.get("month"),
            "trip_type": row.get("trip_type"),
            "file_name": row["file_name"],
            "url": row["url"],
            "local_path": str(local_path),
            "format": row["format"],
            "status": None,
            "size_bytes": 0,
            "size_gb": 0.0,
            "sha256": None,
            "error": None,
        }

        try:
            result["status"] = download_file(
                row["url"],
                local_path,
                config.overwrite_raw,
                config.forbidden_cooldown_seconds,
            )
            result["size_bytes"] = int(local_path.stat().st_size)
            result["size_gb"] = round(result["size_bytes"] / (1024**3), 4)
            result["sha256"] = sha256_file(local_path)

        except Exception as error:
            result["status"] = "failed"
            result["error"] = str(error)
            part_path = local_path.with_suffix(local_path.suffix + ".part")
            if part_path.exists():
                part_path.unlink()

        result["finished_at"] = now_local(config).isoformat()
        append_manifest(config, result)
        download_results.append(result)

    download_results_df = pd.DataFrame(download_results)

    download_summary = download_results_df.groupby(
        ["source_kind", "status"], as_index=False
    ).agg(files=("file_name", "count"), gb=("size_gb", "sum"))
    print_dataframe(download_summary, "Resultado de descarga", max_rows=30)

    failed_downloads = download_results_df[download_results_df["status"] == "failed"]
    if not failed_downloads.empty:
        print_dataframe(
            failed_downloads[
                ["source_kind", "year", "month", "trip_type", "file_name", "error"]
            ],
            "Errores de descarga",
            max_rows=50,
        )
        raise RuntimeError(f"Fallaron {len(failed_downloads)} descarga(s).")

    print_panel("Manifest actualizado", config.manifest_jsonl, "green")
    return download_results_df


# ============================================================
# 7. METADATA VALIDATOR
# ============================================================


def parquet_signature_ok(file_path: Path) -> bool:
    if not file_path.exists() or file_path.stat().st_size < 8:
        return False

    with file_path.open("rb") as file:
        start = file.read(4)
        file.seek(-4, os.SEEK_END)
        end = file.read(4)

    return start == b"PAR1" and end == b"PAR1"


def spark_read_path_candidates(path: Path) -> list[str]:
    path = path.resolve()
    candidates = [str(path), path.as_uri()]

    if platform.system().lower() == "windows":
        candidates.append("file:///" + path.as_posix().replace(":", ":", 1))

    unique = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def read_schema_with_spark(spark: Any, path: Path) -> dict[str, Any]:
    last_error = None

    for candidate in spark_read_path_candidates(path):
        try:
            df_spark = spark.read.parquet(candidate)
            return {
                "spark_status": "valid",
                "spark_schema_json": df_spark.schema.json(),
                "spark_read_path": candidate,
                "spark_error": None,
            }
        except Exception as error:
            last_error = str(error)

    return {
        "spark_status": "invalid",
        "spark_schema_json": None,
        "spark_read_path": None,
        "spark_error": last_error,
    }


def validate_trip_parquet_files(
    config: PipelineConfig, spark: Any, catalog_df: pd.DataFrame, run_id: str
) -> pd.DataFrame:
    available_files_df = catalog_df[catalog_df["source_kind"] == "trip_data"].copy()
    available_files_df["exists_local"] = available_files_df["local_path"].apply(
        lambda value: Path(value).exists()
    )
    available_files_df = available_files_df[available_files_df["exists_local"]].copy()

    metadata_records = []

    for _, row in tqdm(
        available_files_df.iterrows(),
        total=len(available_files_df),
        desc="Validando Parquet",
    ):
        file_path = Path(row["local_path"])

        result = {
            "pipeline_id": run_id,
            "source_kind": "trip_data",
            "year": int(row["year"]),
            "month": int(row["month"]),
            "trip_type": row["trip_type"],
            "file_name": row["file_name"],
            "local_path": str(file_path),
            "format": "parquet",
            "validation_status": "invalid",
            "size_bytes": 0,
            "size_gb": 0.0,
            "records": None,
            "sha256": None,
            "spark_schema_json": None,
            "spark_read_path": None,
            "validation_error": None,
        }

        try:
            if not file_path.exists():
                raise RuntimeError("El archivo no existe localmente.")

            size_bytes = file_path.stat().st_size
            if size_bytes == 0:
                raise RuntimeError("El archivo está vacío.")
            if file_path.suffix.lower() != ".parquet":
                raise RuntimeError("El archivo no tiene extensión .parquet.")
            if not parquet_signature_ok(file_path):
                raise RuntimeError("La firma Parquet no es válida.")

            pq_file = pq.ParquetFile(file_path)
            result["size_bytes"] = int(size_bytes)
            result["size_gb"] = round(size_bytes / (1024**3), 4)
            result["records"] = int(pq_file.metadata.num_rows)
            result["sha256"] = sha256_file(file_path)

            spark_validation = read_schema_with_spark(spark, file_path)
            result["spark_schema_json"] = spark_validation["spark_schema_json"]
            result["spark_read_path"] = spark_validation["spark_read_path"]

            if spark_validation["spark_status"] != "valid":
                raise RuntimeError(
                    "PySpark no pudo leer el archivo: "
                    + str(spark_validation["spark_error"])
                )

            result["validation_status"] = "valid"

        except Exception as error:
            result["validation_error"] = str(error)

        metadata_records.append(result)

    metadata_df = pd.DataFrame(metadata_records)
    if metadata_df.empty:
        raise RuntimeError("No hay archivos Parquet locales para validar.")

    print_dataframe(
        metadata_df[
            [
                "year",
                "month",
                "trip_type",
                "file_name",
                "validation_status",
                "size_gb",
                "records",
                "validation_error",
            ]
        ],
        "Metadata Parquet validada",
        max_rows=80,
    )

    invalid_df = metadata_df[metadata_df["validation_status"] != "valid"]
    if not invalid_df.empty:
        print_dataframe(
            invalid_df[["year", "month", "trip_type", "file_name", "validation_error"]],
            "Errores de validación Parquet",
            max_rows=80,
        )
        raise RuntimeError(f"Hay {len(invalid_df)} archivo(s) Parquet inválidos.")

    valid_metadata_df = metadata_df[metadata_df["validation_status"] == "valid"].copy()
    print_panel(
        "Metadata validator",
        f"Archivos Parquet válidos: {len(valid_metadata_df)}",
        "green",
    )
    return valid_metadata_df


def validate_lookup_csv(
    config: PipelineConfig, lookup_record: dict[str, Any] | None, run_id: str
) -> pd.DataFrame:
    if lookup_record is None:
        return pd.DataFrame()

    file_path = Path(lookup_record["local_path"])
    result = {
        "pipeline_id": run_id,
        "source_kind": "taxi_zone_lookup",
        "year": None,
        "month": None,
        "trip_type": "lookup",
        "file_name": lookup_record["file_name"],
        "local_path": str(file_path),
        "format": "csv",
        "validation_status": "invalid",
        "size_bytes": 0,
        "size_gb": 0.0,
        "records": None,
        "sha256": None,
        "spark_schema_json": None,
        "spark_read_path": None,
        "validation_error": None,
    }

    try:
        if not file_path.exists():
            raise RuntimeError("El lookup no existe localmente.")
        size_bytes = file_path.stat().st_size
        if size_bytes == 0:
            raise RuntimeError("El lookup está vacío.")
        if file_path.suffix.lower() != ".csv":
            raise RuntimeError("El lookup no tiene extensión .csv.")

        # Validación básica con csv.Sniffer para confirmar que se puede leer como CSV.
        with file_path.open("r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(2048)
            f.seek(0)
            csv.Sniffer().sniff(sample)

        lookup_df = pd.read_csv(file_path)
        missing_columns = [
            col for col in LOOKUP_EXPECTED_COLUMNS if col not in lookup_df.columns
        ]
        if missing_columns:
            raise RuntimeError(
                f"Faltan columnas esperadas en lookup: {missing_columns}"
            )
        if lookup_df.empty:
            raise RuntimeError("El lookup no contiene registros.")
        if lookup_df["LocationID"].duplicated().any():
            raise RuntimeError("LocationID contiene duplicados en lookup.")

        result["size_bytes"] = int(size_bytes)
        result["size_gb"] = round(size_bytes / (1024**3), 4)
        result["records"] = int(len(lookup_df))
        result["sha256"] = sha256_file(file_path)
        result["validation_status"] = "valid"

    except Exception as error:
        result["validation_error"] = str(error)

    lookup_metadata_df = pd.DataFrame([result])
    print_dataframe(
        lookup_metadata_df[
            [
                "source_kind",
                "file_name",
                "validation_status",
                "records",
                "validation_error",
            ]
        ],
        "Metadata Lookup validada",
        max_rows=10,
    )

    if result["validation_status"] != "valid":
        raise RuntimeError(
            "Taxi Zone Lookup inválido: " + str(result["validation_error"])
        )

    return lookup_metadata_df


# ============================================================
# 8. CARGA BRONZE
# ============================================================


def bronze_output_path(config: PipelineConfig, row: pd.Series) -> Path:
    if row["source_kind"] == "trip_data":
        return (
            config.bronze_files_dir
            / "trip_data"
            / f"tipo_dataset={row['trip_type']}"
            / f"anio={int(row['year'])}"
            / f"mes={str(int(row['month'])).zfill(2)}"
            / row["file_name"]
        )

    if row["source_kind"] == "taxi_zone_lookup":
        return config.bronze_files_dir / "lookup" / row["file_name"]

    raise ValueError(f"source_kind no soportado: {row['source_kind']}")


def load_bronze(
    config: PipelineConfig,
    valid_metadata_df: pd.DataFrame,
    run_id: str,
    run_date: str,
    run_time: str,
) -> pd.DataFrame:
    bronze_audit_records = []

    for _, row in valid_metadata_df.iterrows():
        input_path = Path(row["local_path"]).resolve()
        output_file = bronze_output_path(config, row)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if output_file.exists():
            output_file.unlink()

        # Bronze conserva el archivo publicado sin modificar su contenido.
        shutil.copy2(input_path, output_file)

        bronze_audit_records.append(
            {
                "pipeline_id": run_id,
                "pipeline_name": config.pipeline_name,
                "pipeline_version": config.pipeline_version,
                "fecha_ingesta": run_date,
                "hora_ingesta": run_time,
                "source_kind": row["source_kind"],
                "anio": None if pd.isna(row.get("year")) else int(row["year"]),
                "mes": None if pd.isna(row.get("month")) else int(row["month"]),
                "tipo_dataset": row["trip_type"],
                "nombre_archivo": row["file_name"],
                "formato": row["format"],
                "raw_path": str(input_path),
                "bronze_path": str(output_file),
                "size_bytes": int(row["size_bytes"]),
                "size_gb": float(row["size_gb"]),
                "records": None if pd.isna(row.get("records")) else int(row["records"]),
                "hash_archivo": row["sha256"],
                "spark_schema_json": row.get("spark_schema_json"),
                "spark_read_path": row.get("spark_read_path"),
                "bronze_status": "loaded",
            }
        )

    bronze_audit_df = pd.DataFrame(bronze_audit_records)

    bronze_audit_jsonl = config.bronze_metadata_dir / "bronze_audit.jsonl"
    bronze_audit_parquet = config.bronze_metadata_dir / "bronze_audit.parquet"

    bronze_audit_df.to_json(
        bronze_audit_jsonl,
        orient="records",
        lines=True,
        force_ascii=False,
    )

    pq.write_table(pa.Table.from_pandas(bronze_audit_df), bronze_audit_parquet)

    print_dataframe(
        bronze_audit_df[
            [
                "source_kind",
                "anio",
                "mes",
                "tipo_dataset",
                "nombre_archivo",
                "records",
                "bronze_status",
            ]
        ],
        "Carga Bronze",
        max_rows=80,
    )

    print_panel("Bronze generado", config.bronze_dir, "green")
    return bronze_audit_df


# ============================================================
# 9. VALIDACIONES FINALES Y RESUMEN
# ============================================================


def validate_final_completeness(
    config: PipelineConfig,
    run_catalog_df: pd.DataFrame,
    download_results_df: pd.DataFrame,
    valid_metadata_df: pd.DataFrame,
    bronze_audit_df: pd.DataFrame,
) -> None:
    expected_count = len(run_catalog_df)
    downloaded_count = len(
        download_results_df[
            download_results_df["status"].isin(["downloaded", "skipped_existing"])
        ]
    )
    validated_count = len(
        valid_metadata_df[valid_metadata_df["validation_status"] == "valid"]
    )
    bronze_count = len(bronze_audit_df[bronze_audit_df["bronze_status"] == "loaded"])

    checks_df = pd.DataFrame(
        [
            ["archivos_en_catalogo_ejecucion", expected_count],
            ["descargados_o_existentes", downloaded_count],
            ["validados", validated_count],
            ["cargados_bronze", bronze_count],
        ],
        columns=["control", "valor"],
    )
    print_dataframe(checks_df, "Control final de completitud", max_rows=20)

    if (
        downloaded_count != expected_count
        or validated_count != expected_count
        or bronze_count != expected_count
    ):
        raise RuntimeError(
            "La ejecución no está completa: catálogo, descarga, validación y bronze no tienen el mismo conteo."
        )

    if config.max_files is not None:
        print_panel(
            "Modo prueba activo",
            f"Solo se procesaron {config.max_files} archivo(s) de trip data. Para producción usa --max-files none.",
            "yellow",
        )


def print_final_summary(
    config: PipelineConfig,
    run_id: str,
    catalog_df: pd.DataFrame,
    run_catalog_df: pd.DataFrame,
    download_results_df: pd.DataFrame,
    valid_metadata_df: pd.DataFrame,
    bronze_audit_df: pd.DataFrame,
) -> None:
    final_summary = pd.DataFrame(
        [
            ["pipeline_id", run_id],
            ["workspace_dir", config.workspace_dir],
            ["raw_dir", config.raw_dir],
            ["bronze_files_dir", config.bronze_files_dir],
            ["bronze_metadata_dir", config.bronze_metadata_dir],
            ["manifest_jsonl", config.manifest_jsonl],
            ["archivos_catalogados_total_trip_data", len(catalog_df)],
            ["archivos_catalogados_ejecucion", len(run_catalog_df)],
            ["archivos_descargados_o_existentes", len(download_results_df)],
            ["archivos_validados", len(valid_metadata_df)],
            ["archivos_bronze", len(bronze_audit_df)],
            ["bronze_audit_jsonl", config.bronze_metadata_dir / "bronze_audit.jsonl"],
            [
                "bronze_audit_parquet",
                config.bronze_metadata_dir / "bronze_audit.parquet",
            ],
        ],
        columns=["métrica", "valor"],
    )

    print_dataframe(final_summary, "Resumen final", max_rows=30)
    print_panel("Ejecución finalizada", "Nivel 2 y Nivel 3 completados.", "green")


def print_active_config(config: PipelineConfig) -> None:
    config_df = pd.DataFrame(
        [
            ["YEARS", config.years],
            ["TRIP_TYPES", config.trip_types],
            ["SELECTED_TRIP_TYPES", config.selected_trip_types],
            ["MAX_FILES", config.max_files],
            ["OVERWRITE_RAW", config.overwrite_raw],
            ["STRICT_COMPLETENESS", config.strict_completeness],
            ["INCLUDE_LOOKUP", config.include_lookup],
            ["FORBIDDEN_COOLDOWN_SECONDS", config.forbidden_cooldown_seconds],
            ["WORKSPACE_DIR", config.workspace_dir],
            ["RAW_DIR", config.raw_dir],
            ["BRONZE_DIR", config.bronze_dir],
            ["MANIFEST_JSONL", config.manifest_jsonl],
        ],
        columns=["parámetro", "valor"],
    )
    print_dataframe(config_df, "Parámetros activos", max_rows=30)


# ============================================================
# 10. MAIN
# ============================================================


def main() -> int:
    config = parse_args()
    ensure_directories(config)

    run_dt = now_local(config)
    run_ts = run_dt.isoformat()
    run_id = run_dt.strftime("tlc_ingestion_%Y%m%d_%H%M%S")
    run_date = run_dt.strftime("%Y-%m-%d")
    run_time = run_dt.strftime("%H:%M:%S")

    print_active_config(config)

    configure_java_and_spark_env()
    print_panel("JAVA_HOME", os.environ.get("JAVA_HOME", "No configurado"), "blue")
    print_panel(
        "HADOOP_HOME",
        os.environ.get("HADOOP_HOME", "No configurado. No se usará Spark write local."),
        "blue",
    )

    spark = None
    try:
        spark = start_spark()

        full_trip_catalog_df = build_trip_catalog(config)
        if full_trip_catalog_df.empty:
            raise RuntimeError(
                "El catálogo de trip data quedó vacío. Revisa years/trip-types o conectividad."
            )

        summary_df = (
            full_trip_catalog_df.groupby(["year", "trip_type"], as_index=False)
            .agg(files=("file_name", "count"))
            .sort_values(["year", "trip_type"])
        )
        print_dataframe(summary_df, "Catálogo detectado", max_rows=100)
        print_dataframe(
            full_trip_catalog_df, "Archivos trip data catalogados", max_rows=40
        )

        validate_closed_year_coverage(config, full_trip_catalog_df)

        run_trip_catalog_df = apply_test_limit(full_trip_catalog_df, config.max_files)

        lookup_record = build_lookup_record(config) if config.include_lookup else None
        run_catalog_parts = [run_trip_catalog_df]
        if lookup_record is not None:
            run_catalog_parts.append(pd.DataFrame([lookup_record]))

        run_catalog_df = pd.concat(run_catalog_parts, ignore_index=True)
        print_dataframe(run_catalog_df, "Catálogo de ejecución", max_rows=80)

        download_results_df = download_raw_files(config, run_catalog_df, run_id, run_ts)

        trip_metadata_df = validate_trip_parquet_files(
            config, spark, run_trip_catalog_df, run_id
        )
        lookup_metadata_df = validate_lookup_csv(config, lookup_record, run_id)

        valid_metadata_df = pd.concat(
            [trip_metadata_df, lookup_metadata_df], ignore_index=True
        )
        bronze_audit_df = load_bronze(
            config, valid_metadata_df, run_id, run_date, run_time
        )

        validate_final_completeness(
            config,
            run_catalog_df,
            download_results_df,
            valid_metadata_df,
            bronze_audit_df,
        )

        print_final_summary(
            config,
            run_id,
            full_trip_catalog_df,
            run_catalog_df,
            download_results_df,
            valid_metadata_df,
            bronze_audit_df,
        )
        return 0

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print_panel("ERROR", str(exc), "red")
        raise

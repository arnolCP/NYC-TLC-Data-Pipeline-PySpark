"""
ANÁLISIS DE CALIDAD INTEGRADO CON LA AUDITORÍA CENTRAL

Lee:
    data/silver/cleaned/<taxi_type>/year=<year>/

Genera y conserva:
    data/silver/quality/_quality_reports/<taxi>_<year>_quality_report.json
    data/silver/quality/_metadata/metadata.jsonl
    data/silver/quality/_metadata/quality_tracker.json
    data/logs/quality_manifest.jsonl

Además registra en la auditoría central:
    audit/runtime/events/audit_events.jsonl

Las vistas Parquet se regeneran al terminar el pipeline:
    audit/runtime/views/quality_results.parquet
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.audit.audit_manager import AuditManager


ROOT_DIR = Path(__file__).resolve().parents[3]

TAXI_TYPES_ACTIVE = [
    "fhv",
    "green",
    "yellow",
]

QUALITY_THRESHOLDS = {
    "completitud": 0.90,
    "precision": 0.85,
    "unicidad": 0.80,
    "validez": 0.90,
    "consistencia": 0.90,
    "overall": 0.85,
}

QUALITY_WEIGHTS = {
    "completitud": 0.30,
    "precision": 0.25,
    "unicidad": 0.15,
    "validez": 0.20,
    "consistencia": 0.10,
}


class QualityAnalyzer:
    def __init__(
        self,
        spark: SparkSession,
        audit: AuditManager,
        pipeline_id: str,
        stage_run_id: str,
        stage_name: str,
    ) -> None:
        self.spark = spark
        self.audit = audit

        self.pipeline_id = pipeline_id
        self.stage_run_id = stage_run_id
        self.stage_name = stage_name

        self.pipeline_name = "quality_analysis"
        self.pipeline_version = "2.0.0"
        self.start_time = datetime.now()

        self.input_dir = ROOT_DIR / "data" / "silver" / "cleaned"

        self.base_dir = ROOT_DIR / "data" / "silver" / "quality"

        self.metadata_dir = self.base_dir / "_metadata"

        self.reports_dir = self.base_dir / "_quality_reports"

        self.log_dir = ROOT_DIR / "data" / "logs"

        self.log_path = self.log_dir / "quality_manifest.jsonl"

        self.metadata_path = self.metadata_dir / "metadata.jsonl"

        self.tracker_path = self.metadata_dir / "quality_tracker.json"

        for path in (
            self.reports_dir,
            self.metadata_dir,
            self.log_dir,
        ):
            path.mkdir(
                parents=True,
                exist_ok=True,
            )

        if not self.tracker_path.exists():
            self._write_json_atomic(
                self.tracker_path,
                {},
            )

        self.execution_stats: dict[str, Any] = {
            "pipeline_id": self.pipeline_id,
            "stage_run_id": self.stage_run_id,
            "stage_name": self.stage_name,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "root_dir": str(ROOT_DIR),
            "processed": [],
            "skipped": [],
            "errors": [],
        }

        self._log_info(f"Pipeline ID central: {self.pipeline_id}")

        self._log_info(f"Stage run ID: {self.stage_run_id}")

    @staticmethod
    def _write_json_atomic(
        path: Path,
        payload: Any,
    ) -> None:
        temporary_path = path.with_suffix(path.suffix + ".tmp")

        with temporary_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                payload,
                file,
                indent=2,
                ensure_ascii=False,
                default=str,
            )

        temporary_path.replace(path)

    @staticmethod
    def _append_jsonl(
        path: Path,
        payload: dict[str, Any],
    ) -> None:
        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with path.open(
            "a",
            encoding="utf-8",
        ) as file:
            file.write(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    default=str,
                )
                + "\n"
            )

    @staticmethod
    def _log_info(
        message: str,
    ) -> None:
        print(
            f"[INFO] {message}",
        )

    @staticmethod
    def _log_error(
        message: str,
    ) -> None:
        print(
            f"[ERROR] {message}",
            file=sys.stderr,
        )

    def _load_tracker(
        self,
    ) -> dict[str, list[int]]:
        if not self.tracker_path.exists():
            return {}

        try:
            with self.tracker_path.open(
                "r",
                encoding="utf-8",
            ) as file:
                tracker = json.load(file)

            if not isinstance(tracker, dict):
                return {}

            normalized: dict[str, list[int]] = {}

            for taxi_type, years in tracker.items():
                if not isinstance(years, list):
                    continue

                normalized[str(taxi_type)] = sorted({int(year) for year in years})

            return normalized

        except (
            OSError,
            ValueError,
            TypeError,
            json.JSONDecodeError,
        ):
            return {}

    def _get_processed_years(
        self,
        taxi_type: str,
    ) -> list[int]:
        tracker = self._load_tracker()
        return tracker.get(
            taxi_type,
            [],
        )

    def _save_processed_year(
        self,
        taxi_type: str,
        year: int,
    ) -> None:
        tracker = self._load_tracker()

        taxi_years = set(
            tracker.get(
                taxi_type,
                [],
            )
        )

        taxi_years.add(
            int(year),
        )

        tracker[taxi_type] = sorted(
            taxi_years,
        )

        self._write_json_atomic(
            self.tracker_path,
            tracker,
        )

    def _get_available_years(
        self,
        taxi_type: str,
    ) -> list[int]:
        taxi_path = self.input_dir / taxi_type

        if not taxi_path.exists():
            return []

        available_years: list[int] = []

        for year_directory in taxi_path.glob("year=*"):
            try:
                year = int(
                    year_directory.name.split(
                        "=",
                        maxsplit=1,
                    )[1]
                )

                available_years.append(
                    year,
                )

            except (
                IndexError,
                ValueError,
            ):
                continue

        return sorted(
            set(
                available_years,
            )
        )

    def _read_parquet_safe(
        self,
        input_path: Path,
    ) -> DataFrame | None:
        parquet_files = sorted(input_path.rglob("*.parquet"))

        if not parquet_files:
            return None

        try:
            paths = [str(file_path) for file_path in parquet_files]

            return self.spark.read.parquet(*paths)

        except Exception as error:
            self._log_error(f"No se pudieron leer los Parquet de {input_path}: {error}")

            return None

    @staticmethod
    def _first_existing_column(
        dataframe: DataFrame,
        candidates: list[str],
    ) -> str | None:
        columns_by_lowercase = {column.lower(): column for column in dataframe.columns}

        for candidate in candidates:
            real_name = columns_by_lowercase.get(candidate.lower())

            if real_name is not None:
                return real_name

        return None

    def _register_dimension(
        self,
        dataset_name: str,
        taxi_type: str,
        year: int,
        dimension_name: str,
        score: float,
        threshold: float,
        records_evaluated: int,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        normalized_score = max(
            0.0,
            min(
                1.0,
                float(score),
            ),
        )

        records_failed = round(records_evaluated * (1.0 - normalized_score))

        return self.audit.record_quality_rule(
            pipeline_id=self.pipeline_id,
            stage_name=self.stage_name,
            stage_run_id=self.stage_run_id,
            dataset_name=dataset_name,
            rule_id=(f"DQ_{taxi_type.upper()}_{year}_{dimension_name.upper()}"),
            rule_name=(f"{dimension_name.capitalize()} de {taxi_type} {year}"),
            records_evaluated=records_evaluated,
            records_failed=records_failed,
            allowed_failure_rate=(1.0 - threshold),
            metadata={
                "taxi_type": taxi_type,
                "year": year,
                "dimension": dimension_name,
                "score": normalized_score,
                "threshold": threshold,
                **(metadata or {}),
            },
        )

    def _calculate_completeness(
        self,
        dataframe: DataFrame,
        total_records: int,
    ) -> tuple[float, dict[str, Any]]:
        total_columns = len(dataframe.columns)

        total_cells = total_records * total_columns

        if total_cells == 0:
            return 0.0, {
                "total_columns": total_columns,
                "total_cells": total_cells,
                "null_cells": 0,
            }

        null_expressions = [
            F.sum(
                F.when(
                    F.col(column_name).isNull(),
                    F.lit(1),
                ).otherwise(
                    F.lit(0),
                )
            ).alias(
                column_name,
            )
            for column_name in dataframe.columns
        ]

        null_row = dataframe.agg(*null_expressions).first()

        null_cells = sum(int(value or 0) for value in null_row)

        score = 1.0 - (null_cells / total_cells)

        return score, {
            "total_columns": total_columns,
            "total_cells": total_cells,
            "null_cells": null_cells,
        }

    def _calculate_precision(
        self,
        dataframe: DataFrame,
        total_records: int,
    ) -> tuple[float, dict[str, Any]]:
        checks: list[
            tuple[
                str,
                Any,
            ]
        ] = []

        total_amount_column = self._first_existing_column(
            dataframe,
            [
                "total_amount",
            ],
        )

        if total_amount_column:
            checks.append(
                (
                    "total_amount_between_0_and_1000",
                    (
                        F.col(total_amount_column).between(
                            0,
                            1000,
                        )
                    ),
                )
            )

        trip_distance_column = self._first_existing_column(
            dataframe,
            [
                "trip_distance",
            ],
        )

        if trip_distance_column:
            checks.append(
                (
                    "trip_distance_between_0_1_and_500",
                    (
                        F.col(trip_distance_column).between(
                            0.1,
                            500,
                        )
                    ),
                )
            )

        if not checks:
            return 1.0, {
                "checks": [],
                "note": ("No existían columnas aplicables; se conserva score 1.0."),
            }

        check_results: dict[str, float] = {}

        for check_name, condition in checks:
            valid_count = dataframe.filter(condition).count()

            check_results[check_name] = (
                valid_count / total_records if total_records > 0 else 0.0
            )

        score = min(check_results.values())

        return score, {
            "checks": check_results,
        }

    def _calculate_uniqueness(
        self,
        dataframe: DataFrame,
        total_records: int,
    ) -> tuple[float, dict[str, Any]]:
        key_candidates = [
            "pickup_datetime",
            "dropoff_datetime",
            "taxi_type",
            "pulocationid",
            "dolocationid",
        ]

        columns_by_lowercase = {column.lower(): column for column in dataframe.columns}

        key_columns = [
            columns_by_lowercase[candidate]
            for candidate in key_candidates
            if candidate in columns_by_lowercase
        ]

        if not key_columns:
            return 1.0, {
                "key_columns": [],
                "note": ("No existían columnas clave; se conserva score 1.0."),
            }

        unique_records = dataframe.select(*key_columns).distinct().count()

        score = unique_records / total_records if total_records > 0 else 0.0

        return score, {
            "key_columns": key_columns,
            "unique_records": unique_records,
            "duplicate_records": (total_records - unique_records),
        }

    def _calculate_validity(
        self,
        dataframe: DataFrame,
        total_records: int,
    ) -> tuple[float, dict[str, Any]]:
        checks: list[
            tuple[
                str,
                Any,
            ]
        ] = []

        payment_type_column = self._first_existing_column(
            dataframe,
            [
                "payment_type",
            ],
        )

        if payment_type_column:
            checks.append(
                (
                    "payment_type_between_1_and_6_or_null",
                    (
                        F.col(payment_type_column).between(
                            1,
                            6,
                        )
                        | F.col(payment_type_column).isNull()
                    ),
                )
            )

        pickup_location_column = self._first_existing_column(
            dataframe,
            [
                "pulocationid",
                "PULocationID",
            ],
        )

        if pickup_location_column:
            checks.append(
                (
                    "pickup_location_between_1_and_263_or_null",
                    (
                        F.col(pickup_location_column).between(
                            1,
                            263,
                        )
                        | F.col(pickup_location_column).isNull()
                    ),
                )
            )

        dropoff_location_column = self._first_existing_column(
            dataframe,
            [
                "dolocationid",
                "DOLocationID",
            ],
        )

        if dropoff_location_column:
            checks.append(
                (
                    "dropoff_location_between_1_and_263_or_null",
                    (
                        F.col(dropoff_location_column).between(
                            1,
                            263,
                        )
                        | F.col(dropoff_location_column).isNull()
                    ),
                )
            )

        if not checks:
            return 1.0, {
                "checks": [],
                "note": ("No existían columnas aplicables; se conserva score 1.0."),
            }

        check_results: dict[str, float] = {}

        for check_name, condition in checks:
            valid_count = dataframe.filter(condition).count()

            check_results[check_name] = (
                valid_count / total_records if total_records > 0 else 0.0
            )

        score = min(check_results.values())

        return score, {
            "checks": check_results,
        }

    @staticmethod
    def _calculate_consistency(
        dataframe: DataFrame,
    ) -> tuple[float, dict[str, Any]]:
        accepted_types = {
            "tinyint",
            "smallint",
            "int",
            "bigint",
            "float",
            "double",
            "decimal",
            "string",
            "boolean",
            "date",
            "timestamp",
        }

        dtypes = list(dataframe.dtypes)

        if not dtypes:
            return 0.0, {
                "total_columns": 0,
                "accepted_columns": 0,
                "unsupported_types": [],
            }

        accepted_columns = 0
        unsupported_types: list[dict[str, str]] = []

        for column_name, data_type in dtypes:
            normalized_type = data_type.split(
                "(",
                maxsplit=1,
            )[0].lower()

            if normalized_type in accepted_types:
                accepted_columns += 1
            else:
                unsupported_types.append(
                    {
                        "column": column_name,
                        "type": data_type,
                    }
                )

        score = accepted_columns / len(dtypes)

        return score, {
            "total_columns": len(dtypes),
            "accepted_columns": accepted_columns,
            "unsupported_types": unsupported_types,
        }

    def analyze_quality(
        self,
        dataframe: DataFrame,
        taxi_type: str,
        year: int,
    ) -> dict[str, Any] | None:
        self._log_info("Analizando calidad...")

        dataframe = dataframe.cache()

        try:
            total_records = dataframe.count()

            if total_records == 0:
                self._log_info("No hay registros para analizar.")
                return None

            dataset_name = f"silver_cleaned_{taxi_type}_{year}"

            quality_report: dict[str, Any] = {
                "pipeline_id": self.pipeline_id,
                "stage_run_id": self.stage_run_id,
                "taxi_type": taxi_type,
                "year": year,
                "total_records": total_records,
                "dimensions": {},
                "overall_score": 0.0,
                "overall_passed": False,
                "issues": [],
            }

            dimension_calculators = {
                "completitud": (
                    lambda: self._calculate_completeness(
                        dataframe,
                        total_records,
                    )
                ),
                "precision": (
                    lambda: self._calculate_precision(
                        dataframe,
                        total_records,
                    )
                ),
                "unicidad": (
                    lambda: self._calculate_uniqueness(
                        dataframe,
                        total_records,
                    )
                ),
                "validez": (
                    lambda: self._calculate_validity(
                        dataframe,
                        total_records,
                    )
                ),
                "consistencia": (
                    lambda: self._calculate_consistency(
                        dataframe,
                    )
                ),
            }

            for (
                dimension_name,
                calculator,
            ) in dimension_calculators.items():
                self._log_info(f"[{dimension_name.capitalize()}] Calculando...")

                score, details = calculator()

                threshold = QUALITY_THRESHOLDS[dimension_name]

                passed = score >= threshold

                central_status = self._register_dimension(
                    dataset_name=dataset_name,
                    taxi_type=taxi_type,
                    year=year,
                    dimension_name=dimension_name,
                    score=score,
                    threshold=threshold,
                    records_evaluated=total_records,
                    metadata={
                        "details": details,
                    },
                )

                quality_report["dimensions"][dimension_name] = {
                    "score": round(
                        score,
                        6,
                    ),
                    "threshold": threshold,
                    "passed": passed,
                    "central_audit_status": (central_status),
                    "details": details,
                }

                if not passed:
                    quality_report["issues"].append(
                        f"{dimension_name.capitalize()} baja: {score:.2%}"
                    )

                self._log_info(
                    f"{dimension_name.capitalize()}: "
                    f"{score:.2%} "
                    f"{'APROBADO' if passed else 'RECHAZADO'}"
                )

            overall_score = sum(
                quality_report["dimensions"][dimension_name]["score"] * weight
                for (
                    dimension_name,
                    weight,
                ) in QUALITY_WEIGHTS.items()
            )

            overall_passed = overall_score >= QUALITY_THRESHOLDS["overall"]

            quality_report["overall_score"] = round(
                overall_score,
                6,
            )

            quality_report["overall_passed"] = overall_passed

            self._register_dimension(
                dataset_name=dataset_name,
                taxi_type=taxi_type,
                year=year,
                dimension_name="overall",
                score=overall_score,
                threshold=QUALITY_THRESHOLDS["overall"],
                records_evaluated=total_records,
                metadata={
                    "weights": QUALITY_WEIGHTS,
                    "issues": quality_report["issues"],
                },
            )

            self._log_info(f"Score total: {overall_score:.2%}")

            self._log_info(
                f"Resultado general: {'APROBADO' if overall_passed else 'RECHAZADO'}"
            )

            return quality_report

        finally:
            dataframe.unpersist()

    def process_year(
        self,
        taxi_type: str,
        year: int,
        force: bool = False,
    ) -> dict[str, Any] | None:
        self._log_info(f"Procesando: {taxi_type.upper()} - {year}")

        start_time = time.perf_counter()

        processed_years = self._get_processed_years(
            taxi_type,
        )

        if year in processed_years and not force:
            message = (
                f"{taxi_type} {year} ya fue procesado. Usa --force para reprocesarlo."
            )

            self._log_info(
                message,
            )

            self.execution_stats["skipped"].append(
                {
                    "taxi_type": taxi_type,
                    "year": year,
                    "reason": ("already_processed"),
                }
            )

            return None

        input_path = self.input_dir / taxi_type / f"year={year}"

        if not input_path.exists():
            message = f"No existe la entrada: {input_path}"

            self._log_error(
                message,
            )

            self.execution_stats["errors"].append(
                {
                    "taxi_type": taxi_type,
                    "year": year,
                    "error": message,
                }
            )

            self._log_local_audit(
                taxi_type=taxi_type,
                year=year,
                status="failed",
                message=message,
                error=message,
            )

            return None

        dataframe = self._read_parquet_safe(input_path)

        if dataframe is None:
            message = f"No se pudieron leer los datos de {input_path}"

            self._log_error(
                message,
            )

            self.execution_stats["errors"].append(
                {
                    "taxi_type": taxi_type,
                    "year": year,
                    "error": message,
                }
            )

            self._log_local_audit(
                taxi_type=taxi_type,
                year=year,
                status="failed",
                message=message,
                error=message,
            )

            return None

        quality_report = self.analyze_quality(
            dataframe,
            taxi_type,
            year,
        )

        if quality_report is None:
            message = "No se pudo generar el reporte de calidad."

            self._log_error(
                message,
            )

            return None

        report_path = self.reports_dir / (f"{taxi_type}_{year}_quality_report.json")

        self._write_json_atomic(
            report_path,
            quality_report,
        )

        elapsed_seconds = time.perf_counter() - start_time

        self._save_processed_year(
            taxi_type,
            year,
        )

        processed_entry = {
            "taxi_type": taxi_type,
            "year": year,
            "records": quality_report["total_records"],
            "quality_score": quality_report["overall_score"],
            "passed": quality_report["overall_passed"],
            "execution_time_seconds": round(
                elapsed_seconds,
                3,
            ),
            "report_path": str(
                report_path,
            ),
        }

        self.execution_stats["processed"].append(
            processed_entry,
        )

        self._log_local_audit(
            taxi_type=taxi_type,
            year=year,
            status="completed",
            message=("Reporte de calidad generado."),
            file_path=str(
                report_path,
            ),
            metrics={
                "records_analyzed": (quality_report["total_records"]),
                "quality_score": (quality_report["overall_score"]),
                "passed": (quality_report["overall_passed"]),
                "execution_time_seconds": (
                    round(
                        elapsed_seconds,
                        3,
                    )
                ),
            },
        )

        self.audit.record_file(
            pipeline_id=self.pipeline_id,
            stage_name=self.stage_name,
            stage_run_id=self.stage_run_id,
            file_name=report_path.name,
            status="SUCCESS",
            target_path=str(
                report_path,
            ),
            metadata={
                "artifact_type": ("quality_report"),
                "taxi_type": taxi_type,
                "year": year,
                "records": quality_report["total_records"],
                "quality_score": quality_report["overall_score"],
                "passed": quality_report["overall_passed"],
            },
        )

        self._log_info(f"{taxi_type.upper()} {year} completado.")

        self._log_info(f"Reporte: {report_path}")

        self._log_info(f"Score: {quality_report['overall_score']:.2%}")

        return quality_report

    def process(
        self,
        taxi_types: list[str],
        years: list[int],
        force: bool = False,
    ) -> dict[str, Any]:
        self._log_info(f"Procesando {len(taxi_types)} tipos de taxi.")

        for taxi_type in taxi_types:
            available_years = self._get_available_years(
                taxi_type,
            )

            if not available_years:
                self._log_info(f"No hay datos disponibles para {taxi_type}.")
                continue

            years_to_process = [year for year in years if year in available_years]

            unavailable_years = [year for year in years if year not in available_years]

            for unavailable_year in unavailable_years:
                self.execution_stats["skipped"].append(
                    {
                        "taxi_type": taxi_type,
                        "year": unavailable_year,
                        "reason": ("year_not_available"),
                    }
                )

            for year in years_to_process:
                try:
                    self.process_year(
                        taxi_type,
                        year,
                        force,
                    )

                except Exception as error:
                    error_message = str(error)

                    self._log_error(f"Error en {taxi_type} {year}: {error_message}")

                    self.execution_stats["errors"].append(
                        {
                            "taxi_type": taxi_type,
                            "year": year,
                            "error": error_message,
                        }
                    )

                    self._log_local_audit(
                        taxi_type=taxi_type,
                        year=year,
                        status="failed",
                        message=("Error en análisis de calidad."),
                        error=error_message,
                    )

        self.execution_stats["finished_at"] = datetime.now().isoformat()

        self.execution_stats["total_processed"] = len(self.execution_stats["processed"])

        self.execution_stats["total_skipped"] = len(self.execution_stats["skipped"])

        self.execution_stats["total_errors"] = len(self.execution_stats["errors"])

        self._save_metadata()
        self._show_summary()

        return self.execution_stats

    def _log_local_audit(
        self,
        taxi_type: str,
        year: int,
        status: str,
        message: str,
        file_path: str | None = None,
        error: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        log_entry = {
            "pipeline_id": self.pipeline_id,
            "stage_run_id": self.stage_run_id,
            "stage_name": self.stage_name,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "trip_type": taxi_type,
            "year": year,
            "file_name": (Path(file_path).name if file_path else None),
            "local_path": file_path,
            "status": status,
            "message": message,
            "error": error,
            "metrics": metrics or {},
        }

        self._append_jsonl(
            self.log_path,
            log_entry,
        )

    def _save_metadata(
        self,
    ) -> None:
        metadata_entry = {
            "pipeline_id": self.pipeline_id,
            "stage_run_id": self.stage_run_id,
            "stage_name": self.stage_name,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "started_at": self.start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "root_dir": str(
                ROOT_DIR,
            ),
            "total_processed": len(self.execution_stats["processed"]),
            "total_skipped": len(self.execution_stats["skipped"]),
            "total_errors": len(self.execution_stats["errors"]),
            "profiles": self.execution_stats["processed"],
            "skipped": self.execution_stats["skipped"],
            "errors": self.execution_stats["errors"],
        }

        self._append_jsonl(
            self.metadata_path,
            metadata_entry,
        )

        self._log_info(f"Metadata: {self.metadata_path}")

    def _show_summary(
        self,
    ) -> None:
        print("\n" + "=" * 70)

        print("RESUMEN DE ANÁLISIS DE CALIDAD")

        print("=" * 70)

        print(f"Pipeline ID: {self.pipeline_id}")

        print(f"Stage run ID: {self.stage_run_id}")

        print(f"Procesados: {len(self.execution_stats['processed'])}")

        print(f"Omitidos: {len(self.execution_stats['skipped'])}")

        print(f"Errores: {len(self.execution_stats['errors'])}")

        for item in self.execution_stats["processed"]:
            status = "APROBADO" if item["passed"] else "RECHAZADO"

            print(
                f"  - {item['taxi_type']} "
                f"{item['year']}: "
                f"{item['quality_score']:.2%} "
                f"({status})"
            )

        print(f"Reportes: {self.reports_dir}")

        print(f"Metadata: {self.metadata_dir}")

        print(f"Manifest: {self.log_path}")

        print("=" * 70)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Análisis de calidad integrado con la auditoría central.")
    )

    parser.add_argument(
        "--taxi",
        nargs="+",
        choices=[
            "fhv",
            "fhvhv",
            "green",
            "yellow",
            "all",
        ],
        default=[
            "all",
        ],
    )

    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        required=True,
        help=(
            "Años que deben analizarse. "
            "Se exige explícitamente para "
            "evitar analizar años incorrectos."
        ),
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=("Reprocesa años ya registrados en el tracker."),
    )

    return parser.parse_args()


def get_central_audit_context() -> tuple[
    AuditManager,
    str,
    str,
    str,
]:
    pipeline_id = os.environ.get("PIPELINE_ID")

    stage_run_id = os.environ.get("STAGE_RUN_ID")

    stage_name = os.environ.get(
        "STAGE_NAME",
        "SILVER_05_QUALITY_ANALYSIS",
    )

    if not pipeline_id:
        raise RuntimeError(
            "Falta PIPELINE_ID. Ejecuta este script mediante `python -m src.pipeline`."
        )

    if not stage_run_id:
        raise RuntimeError(
            "Falta STAGE_RUN_ID. Ejecuta este script mediante `python -m src.pipeline`."
        )

    audit = AuditManager(
        audit_root=(ROOT_DIR / "audit" / "runtime"),
    )

    return (
        audit,
        pipeline_id,
        stage_run_id,
        stage_name,
    )


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("QualityAnalyzer")
        .config(
            "spark.driver.memory",
            "8g",
        )
        .config(
            "spark.executor.memory",
            "8g",
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            ("spark.sql.adaptive.coalescePartitions.enabled"),
            "true",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "200",
        )
        .getOrCreate()
    )


def main() -> int:
    arguments = parse_arguments()

    if "all" in arguments.taxi:
        taxi_types = TAXI_TYPES_ACTIVE.copy()
    else:
        taxi_types = list(dict.fromkeys(arguments.taxi))

    (
        audit,
        pipeline_id,
        stage_run_id,
        stage_name,
    ) = get_central_audit_context()

    spark = create_spark_session()

    try:
        print("Iniciando análisis de calidad...")

        print(f"Años solicitados: {arguments.years}")

        print(f"Tipos solicitados: {taxi_types}")

        analyzer = QualityAnalyzer(
            spark=spark,
            audit=audit,
            pipeline_id=pipeline_id,
            stage_run_id=stage_run_id,
            stage_name=stage_name,
        )

        execution_stats = analyzer.process(
            taxi_types=taxi_types,
            years=arguments.years,
            force=arguments.force,
        )

        if execution_stats["total_errors"] > 0:
            print(
                "El análisis terminó con errores técnicos.",
                file=sys.stderr,
            )

            return 1

        if execution_stats["total_processed"] == 0:
            print(
                "No se procesó ningún dataset. Revisa años disponibles o usa --force.",
                file=sys.stderr,
            )

            return 2

        print("ANÁLISIS DE CALIDAD COMPLETADO")

        return 0

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

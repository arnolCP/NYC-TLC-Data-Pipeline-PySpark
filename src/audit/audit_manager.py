from __future__ import annotations

import json
import os
import socket
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def now_iso() -> str:
    """Devuelve la fecha actual en UTC con formato ISO 8601."""
    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds",
    )


class AuditManager:
    """
    Gestor central de auditoría.

    Los eventos históricos se guardan en:

        audit/runtime/events/audit_events.jsonl

    Las vistas analíticas se guardan en:

        audit/runtime/views/pipeline_runs.parquet
        audit/runtime/views/stage_runs.parquet
        audit/runtime/views/file_runs.parquet
        audit/runtime/views/quality_results.parquet
        audit/runtime/views/latest_pipeline_run.parquet
        audit/runtime/views/latest_stage_runs.parquet
        audit/runtime/views/latest_successful_pipeline.parquet
        audit/runtime/views/latest_successful_stage_runs.parquet
    """

    VALID_PIPELINE_STATUSES = {
        "RUNNING",
        "SUCCESS",
        "PARTIAL_SUCCESS",
        "FAILED",
    }

    VALID_STAGE_STATUSES = {
        "RUNNING",
        "SUCCESS",
        "SKIPPED",
        "FAILED",
    }

    def __init__(
        self,
        audit_root: str | Path = "audit/runtime",
        pipeline_name: str = "nyc_tlc_pipeline",
        pipeline_version: str = "1.2.0",
        environment: str = "local",
    ) -> None:
        self.root = Path(audit_root).resolve()
        self.events_dir = self.root / "events"
        self.views_dir = self.root / "views"
        self.events_file = self.events_dir / "audit_events.jsonl"

        self.pipeline_name = pipeline_name
        self.pipeline_version = pipeline_version
        self.environment = environment

        self.events_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.views_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    @staticmethod
    def new_pipeline_id() -> str:
        timestamp = datetime.now(
            timezone.utc,
        ).strftime(
            "%Y%m%dT%H%M%SZ",
        )

        random_part = uuid.uuid4().hex[:8]

        return f"{timestamp}_{random_part}"

    @staticmethod
    def new_stage_run_id(
        stage_name: str,
    ) -> str:
        normalized_name = "".join(
            character.lower() if character.isalnum() else "_"
            for character in stage_name
        ).strip("_")

        random_part = uuid.uuid4().hex[:8]

        return f"{normalized_name}_{random_part}"

    def _write_event(
        self,
        event_type: str,
        pipeline_id: str,
        **fields: Any,
    ) -> None:
        event = {
            "event_id": uuid.uuid4().hex,
            "event_type": event_type,
            "occurred_at": now_iso(),
            "pipeline_id": pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "environment": self.environment,
            "host_name": socket.gethostname(),
            "process_id": os.getpid(),
            **fields,
        }

        serialized_event = json.dumps(
            event,
            ensure_ascii=False,
            default=str,
        )

        with self.events_file.open(
            "a",
            encoding="utf-8",
        ) as file:
            file.write(
                serialized_event + "\n",
            )

    def start_pipeline(
        self,
        pipeline_id: str,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        self._write_event(
            "PIPELINE_STARTED",
            pipeline_id,
            status="RUNNING",
            payload={
                "parameters": parameters or {},
            },
        )

    def finish_pipeline(
        self,
        pipeline_id: str,
        status: str,
        message: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        if status not in self.VALID_PIPELINE_STATUSES:
            raise ValueError(
                f"Estado de pipeline inválido: {status}. "
                f"Permitidos: "
                f"{sorted(self.VALID_PIPELINE_STATUSES)}"
            )

        self._write_event(
            "PIPELINE_FINISHED",
            pipeline_id,
            status=status,
            message=message,
            payload={
                "metrics": metrics or {},
            },
        )

    def start_stage(
        self,
        pipeline_id: str,
        stage_name: str,
        command: list[str] | None = None,
    ) -> str:
        stage_run_id = self.new_stage_run_id(
            stage_name,
        )

        self._write_event(
            "STAGE_STARTED",
            pipeline_id,
            stage_name=stage_name,
            stage_run_id=stage_run_id,
            status="RUNNING",
            payload={
                "command": command or [],
            },
        )

        return stage_run_id

    def finish_stage(
        self,
        pipeline_id: str,
        stage_name: str,
        stage_run_id: str,
        status: str,
        duration_seconds: float,
        return_code: int,
        message: str | None = None,
        stdout_log: str | None = None,
        stderr_log: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        if status not in self.VALID_STAGE_STATUSES:
            raise ValueError(
                f"Estado de etapa inválido: {status}. "
                f"Permitidos: "
                f"{sorted(self.VALID_STAGE_STATUSES)}"
            )

        self._write_event(
            "STAGE_FINISHED",
            pipeline_id,
            stage_name=stage_name,
            stage_run_id=stage_run_id,
            status=status,
            message=message,
            payload={
                "duration_seconds": round(
                    float(duration_seconds),
                    6,
                ),
                "return_code": int(return_code),
                "stdout_log": stdout_log,
                "stderr_log": stderr_log,
                "metrics": metrics or {},
            },
        )

    def record_file(
        self,
        pipeline_id: str,
        stage_name: str,
        stage_run_id: str,
        file_name: str,
        status: str,
        target_path: str,
        source_hash: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._write_event(
            "FILE_PROCESSED",
            pipeline_id,
            stage_name=stage_name,
            stage_run_id=stage_run_id,
            status=status,
            payload={
                "file_name": file_name,
                "target_path": target_path,
                "source_hash": source_hash,
                "metadata": metadata or {},
            },
        )

    def record_quality_rule(
        self,
        pipeline_id: str,
        stage_name: str,
        stage_run_id: str,
        dataset_name: str,
        rule_id: str,
        rule_name: str,
        records_evaluated: int,
        records_failed: int,
        allowed_failure_rate: float,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        if records_evaluated < 0:
            raise ValueError("records_evaluated no puede ser negativo.")

        if records_failed < 0:
            raise ValueError("records_failed no puede ser negativo.")

        if records_failed > records_evaluated:
            raise ValueError("records_failed no puede ser mayor que records_evaluated.")

        if not 0 <= allowed_failure_rate <= 1:
            raise ValueError("allowed_failure_rate debe estar entre 0 y 1.")

        if records_evaluated == 0:
            failure_rate = 0.0
        else:
            failure_rate = records_failed / records_evaluated

        status = "PASSED" if failure_rate <= allowed_failure_rate else "FAILED"

        self._write_event(
            "QUALITY_RULE_EVALUATED",
            pipeline_id,
            stage_name=stage_name,
            stage_run_id=stage_run_id,
            status=status,
            payload={
                "dataset_name": dataset_name,
                "rule_id": rule_id,
                "rule_name": rule_name,
                "records_evaluated": int(
                    records_evaluated,
                ),
                "records_failed": int(
                    records_failed,
                ),
                "failure_rate": float(
                    failure_rate,
                ),
                "allowed_failure_rate": float(
                    allowed_failure_rate,
                ),
                "metadata": metadata or {},
            },
        )

        return status

    def read_events(
        self,
    ) -> list[dict[str, Any]]:
        if not self.events_file.exists():
            return []

        events: list[dict[str, Any]] = []

        with self.events_file.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line_number, line in enumerate(
                file,
                start=1,
            ):
                stripped_line = line.strip()

                if not stripped_line:
                    continue

                try:
                    event = json.loads(
                        stripped_line,
                    )

                    events.append(
                        event,
                    )

                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"JSON inválido en "
                        f"{self.events_file}, "
                        f"línea {line_number}: "
                        f"{error}"
                    ) from error

        return events

    @staticmethod
    def _write_parquet(
        dataframe: pd.DataFrame,
        output_path: Path,
    ) -> Path:
        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        dataframe.to_parquet(
            output_path,
            index=False,
        )

        return output_path

    @staticmethod
    def _convert_datetime_column(
        dataframe: pd.DataFrame,
        column_name: str,
    ) -> pd.DataFrame:
        result = dataframe.copy()

        if column_name in result.columns:
            result[column_name] = pd.to_datetime(
                result[column_name],
                utc=True,
                errors="coerce",
            )

        return result

    def _build_latest_views(
        self,
        pipeline_dataframe: pd.DataFrame,
        stage_dataframe: pd.DataFrame,
    ) -> dict[str, Path]:
        outputs: dict[str, Path] = {}

        if pipeline_dataframe.empty:
            return outputs

        ordered_pipeline_dataframe = self._convert_datetime_column(
            pipeline_dataframe,
            "started_at",
        ).sort_values(
            by=[
                "started_at",
                "pipeline_id",
            ],
            ascending=[
                True,
                True,
            ],
            na_position="first",
        )

        latest_pipeline_dataframe = ordered_pipeline_dataframe.tail(1).copy()

        latest_pipeline_id = str(latest_pipeline_dataframe.iloc[0]["pipeline_id"])

        outputs["latest_pipeline_run"] = self._write_parquet(
            latest_pipeline_dataframe,
            self.views_dir / "latest_pipeline_run.parquet",
        )

        if not stage_dataframe.empty:
            latest_stage_dataframe = stage_dataframe[
                stage_dataframe["pipeline_id"].astype(str) == latest_pipeline_id
            ].copy()

            latest_stage_dataframe = self._convert_datetime_column(
                latest_stage_dataframe,
                "started_at",
            ).sort_values(
                by=[
                    "started_at",
                    "stage_name",
                ],
                ascending=[
                    True,
                    True,
                ],
                na_position="last",
            )

            outputs["latest_stage_runs"] = self._write_parquet(
                latest_stage_dataframe,
                self.views_dir / "latest_stage_runs.parquet",
            )

        successful_pipeline_dataframe = ordered_pipeline_dataframe[
            ordered_pipeline_dataframe["status"] == "SUCCESS"
        ].copy()

        if successful_pipeline_dataframe.empty:
            return outputs

        latest_successful_pipeline_dataframe = successful_pipeline_dataframe.tail(
            1
        ).copy()

        latest_successful_pipeline_id = str(
            latest_successful_pipeline_dataframe.iloc[0]["pipeline_id"]
        )

        outputs["latest_successful_pipeline"] = self._write_parquet(
            latest_successful_pipeline_dataframe,
            self.views_dir / "latest_successful_pipeline.parquet",
        )

        if not stage_dataframe.empty:
            latest_successful_stage_dataframe = stage_dataframe[
                stage_dataframe["pipeline_id"].astype(str)
                == latest_successful_pipeline_id
            ].copy()

            latest_successful_stage_dataframe = self._convert_datetime_column(
                latest_successful_stage_dataframe,
                "started_at",
            ).sort_values(
                by=[
                    "started_at",
                    "stage_name",
                ],
                ascending=[
                    True,
                    True,
                ],
                na_position="last",
            )

            outputs["latest_successful_stage_runs"] = self._write_parquet(
                latest_successful_stage_dataframe,
                self.views_dir / ("latest_successful_stage_runs.parquet"),
            )

        return outputs

    def export_parquet_views(
        self,
    ) -> dict[str, Path]:
        events = self.read_events()

        if not events:
            return {}

        pipeline_state: dict[
            str,
            dict[str, Any],
        ] = {}

        stage_state: dict[
            str,
            dict[str, Any],
        ] = {}

        file_rows: list[dict[str, Any]] = []

        quality_rows: list[dict[str, Any]] = []

        for event in events:
            event_type = event.get(
                "event_type",
            )

            pipeline_id = str(
                event["pipeline_id"],
            )

            payload = event.get("payload") or {}

            if event_type == "PIPELINE_STARTED":
                pipeline_state[pipeline_id] = {
                    "pipeline_id": pipeline_id,
                    "pipeline_name": event.get(
                        "pipeline_name",
                    ),
                    "pipeline_version": event.get(
                        "pipeline_version",
                    ),
                    "environment": event.get(
                        "environment",
                    ),
                    "host_name": event.get(
                        "host_name",
                    ),
                    "process_id": event.get(
                        "process_id",
                    ),
                    "started_at": event.get(
                        "occurred_at",
                    ),
                    "finished_at": None,
                    "status": event.get(
                        "status",
                    ),
                    "message": event.get(
                        "message",
                    ),
                    "parameters_json": json.dumps(
                        payload.get(
                            "parameters",
                            {},
                        ),
                        ensure_ascii=False,
                        default=str,
                    ),
                    "metrics_json": "{}",
                }

            elif event_type == "PIPELINE_FINISHED":
                pipeline_row = pipeline_state.setdefault(
                    pipeline_id,
                    {
                        "pipeline_id": pipeline_id,
                        "pipeline_name": event.get(
                            "pipeline_name",
                        ),
                        "pipeline_version": event.get(
                            "pipeline_version",
                        ),
                        "environment": event.get(
                            "environment",
                        ),
                        "host_name": event.get(
                            "host_name",
                        ),
                        "process_id": event.get(
                            "process_id",
                        ),
                        "started_at": None,
                        "parameters_json": "{}",
                    },
                )

                pipeline_row.update(
                    {
                        "finished_at": event.get(
                            "occurred_at",
                        ),
                        "status": event.get(
                            "status",
                        ),
                        "message": event.get(
                            "message",
                        ),
                        "metrics_json": json.dumps(
                            payload.get(
                                "metrics",
                                {},
                            ),
                            ensure_ascii=False,
                            default=str,
                        ),
                    }
                )

            elif event_type == "STAGE_STARTED":
                stage_run_id = str(
                    event["stage_run_id"],
                )

                stage_state[stage_run_id] = {
                    "pipeline_id": pipeline_id,
                    "stage_run_id": stage_run_id,
                    "stage_name": event.get(
                        "stage_name",
                    ),
                    "started_at": event.get(
                        "occurred_at",
                    ),
                    "finished_at": None,
                    "status": event.get(
                        "status",
                    ),
                    "duration_seconds": None,
                    "return_code": None,
                    "message": event.get(
                        "message",
                    ),
                    "command_json": json.dumps(
                        payload.get(
                            "command",
                            [],
                        ),
                        ensure_ascii=False,
                        default=str,
                    ),
                    "metrics_json": "{}",
                    "stdout_log": None,
                    "stderr_log": None,
                }

            elif event_type == "STAGE_FINISHED":
                stage_run_id = str(
                    event["stage_run_id"],
                )

                stage_row = stage_state.setdefault(
                    stage_run_id,
                    {
                        "pipeline_id": pipeline_id,
                        "stage_run_id": stage_run_id,
                        "stage_name": event.get(
                            "stage_name",
                        ),
                        "started_at": None,
                        "command_json": "[]",
                    },
                )

                stage_row.update(
                    {
                        "finished_at": event.get(
                            "occurred_at",
                        ),
                        "status": event.get(
                            "status",
                        ),
                        "duration_seconds": payload.get(
                            "duration_seconds",
                        ),
                        "return_code": payload.get(
                            "return_code",
                        ),
                        "message": event.get(
                            "message",
                        ),
                        "metrics_json": json.dumps(
                            payload.get(
                                "metrics",
                                {},
                            ),
                            ensure_ascii=False,
                            default=str,
                        ),
                        "stdout_log": payload.get(
                            "stdout_log",
                        ),
                        "stderr_log": payload.get(
                            "stderr_log",
                        ),
                    }
                )

            elif event_type == "FILE_PROCESSED":
                file_rows.append(
                    {
                        "event_id": event.get(
                            "event_id",
                        ),
                        "occurred_at": event.get(
                            "occurred_at",
                        ),
                        "pipeline_id": pipeline_id,
                        "stage_run_id": event.get(
                            "stage_run_id",
                        ),
                        "stage_name": event.get(
                            "stage_name",
                        ),
                        "status": event.get(
                            "status",
                        ),
                        "file_name": payload.get(
                            "file_name",
                        ),
                        "target_path": payload.get(
                            "target_path",
                        ),
                        "source_hash": payload.get(
                            "source_hash",
                        ),
                        "metadata_json": json.dumps(
                            payload.get(
                                "metadata",
                                {},
                            ),
                            ensure_ascii=False,
                            default=str,
                        ),
                    }
                )

            elif event_type == ("QUALITY_RULE_EVALUATED"):
                quality_rows.append(
                    {
                        "event_id": event.get(
                            "event_id",
                        ),
                        "occurred_at": event.get(
                            "occurred_at",
                        ),
                        "pipeline_id": pipeline_id,
                        "stage_run_id": event.get(
                            "stage_run_id",
                        ),
                        "stage_name": event.get(
                            "stage_name",
                        ),
                        "status": event.get(
                            "status",
                        ),
                        "dataset_name": payload.get(
                            "dataset_name",
                        ),
                        "rule_id": payload.get(
                            "rule_id",
                        ),
                        "rule_name": payload.get(
                            "rule_name",
                        ),
                        "records_evaluated": payload.get(
                            "records_evaluated",
                        ),
                        "records_failed": payload.get(
                            "records_failed",
                        ),
                        "failure_rate": payload.get(
                            "failure_rate",
                        ),
                        "allowed_failure_rate": payload.get(
                            "allowed_failure_rate",
                        ),
                        "metadata_json": json.dumps(
                            payload.get(
                                "metadata",
                                {},
                            ),
                            ensure_ascii=False,
                            default=str,
                        ),
                    }
                )

        pipeline_dataframe = pd.DataFrame(
            list(
                pipeline_state.values(),
            )
        )

        stage_dataframe = pd.DataFrame(
            list(
                stage_state.values(),
            )
        )

        file_dataframe = pd.DataFrame(
            file_rows,
        )

        quality_dataframe = pd.DataFrame(
            quality_rows,
        )

        outputs: dict[str, Path] = {}

        historical_views = {
            "pipeline_runs": pipeline_dataframe,
            "stage_runs": stage_dataframe,
            "file_runs": file_dataframe,
            "quality_results": quality_dataframe,
        }

        for view_name, dataframe in historical_views.items():
            if dataframe.empty:
                continue

            output_path = self.views_dir / f"{view_name}.parquet"

            outputs[view_name] = self._write_parquet(
                dataframe,
                output_path,
            )

        outputs.update(
            self._build_latest_views(
                pipeline_dataframe,
                stage_dataframe,
            )
        )

        return outputs

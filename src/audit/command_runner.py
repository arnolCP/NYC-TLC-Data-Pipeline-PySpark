from __future__ import annotations

import hashlib
import os
import queue
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from src.audit.audit_manager import AuditManager


@dataclass(frozen=True)
class CommandResult:
    stage_name: str
    status: str
    return_code: int
    duration_seconds: float
    artifacts_detected: int


def file_hash(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    """
    Calcula el hash SHA-256 de un archivo.
    """
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            digest.update(chunk)

    return digest.hexdigest()


class AuditedCommandRunner:
    """
    Ejecuta una etapa del pipeline y registra:

    - inicio y fin;
    - stdout y stderr;
    - duración;
    - código de retorno;
    - artefactos creados o modificados;
    - validación de salidas obligatorias;
    - configuración del Hadoop local en Windows.

    En Windows espera:

        tools/hadoop/bin/winutils.exe
        tools/hadoop/bin/hadoop.dll
    """

    def __init__(
        self,
        project_root: str | Path,
        audit: AuditManager,
        pipeline_id: str,
        logs_root: str = "logs/pipeline",
        live_output: bool = False,
    ) -> None:
        self.project_root = Path(
            project_root,
        ).resolve()

        self.audit = audit
        self.pipeline_id = pipeline_id

        self.live_output = live_output

        self.logs_dir = (self.project_root / logs_root / pipeline_id).resolve()

        self.logs_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _execute_process(
        self,
        command: Sequence[str],
        child_environment: dict[str, str],
        stdout_file: Any,
        stderr_file: Any,
        stage_name: str,
    ) -> int:
        """Ejecuta una etapa y, opcionalmente, replica su salida en consola."""
        if not self.live_output:
            completed_process = subprocess.run(
                list(command),
                cwd=(self.project_root),
                env=(child_environment),
                stdout=stdout_file,
                stderr=stderr_file,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            return completed_process.returncode

        child_environment = {
            **child_environment,
            "PYTHONUNBUFFERED": "1",
        }
        process = subprocess.Popen(
            list(command),
            cwd=(self.project_root),
            env=(child_environment),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        messages: queue.Queue[tuple[str, str]] = queue.Queue()

        def copy_stream(
            stream: Any,
            destination: Any,
            stream_name: str,
        ) -> None:
            try:
                for line in iter(stream.readline, ""):
                    destination.write(line)
                    destination.flush()
                    messages.put((stream_name, line))
            finally:
                stream.close()

        readers = [
            threading.Thread(
                target=copy_stream,
                args=(process.stdout, stdout_file, "OUT"),
                daemon=True,
            ),
            threading.Thread(
                target=copy_stream,
                args=(process.stderr, stderr_file, "ERR"),
                daemon=True,
            ),
        ]
        for reader in readers:
            reader.start()

        while any(reader.is_alive() for reader in readers) or not messages.empty():
            try:
                stream_name, line = messages.get(timeout=0.15)
            except queue.Empty:
                continue
            print(
                f"  [{stage_name}][{stream_name}] {line}",
                end="",
                flush=True,
            )

        for reader in readers:
            reader.join()
        return process.wait()

    @staticmethod
    def _safe_name(
        value: str,
    ) -> str:
        return "".join(
            character.lower() if character.isalnum() else "_" for character in value
        ).strip("_")

    def _resolve_path(
        self,
        value: str | Path,
    ) -> Path:
        path = Path(value)

        if not path.is_absolute():
            path = self.project_root / path

        return path.resolve()

    def _snapshot(
        self,
        roots: Sequence[str],
        patterns: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        """
        Obtiene una fotografía de los artefactos existentes.
        """
        result: dict[
            str,
            dict[str, Any],
        ] = {}

        for root_name in roots:
            root_path = self._resolve_path(
                root_name,
            )

            if not root_path.exists():
                continue

            for pattern in patterns:
                for path in root_path.rglob(
                    pattern,
                ):
                    if not path.is_file():
                        continue

                    resolved_path = path.resolve()

                    stat = resolved_path.stat()

                    result[str(resolved_path)] = {
                        "path": resolved_path,
                        "size": stat.st_size,
                        "mtime": stat.st_mtime_ns,
                        "hash": file_hash(
                            resolved_path,
                        ),
                    }

        return result

    @staticmethod
    def _detect_changed_artifacts(
        before: dict[
            str,
            dict[str, Any],
        ],
        after: dict[
            str,
            dict[str, Any],
        ],
    ) -> list[
        tuple[
            str,
            dict[str, Any],
        ]
    ]:
        """
        Detecta archivos creados o modificados.
        """
        changed: list[
            tuple[
                str,
                dict[str, Any],
            ]
        ] = []

        for (
            path_key,
            current,
        ) in after.items():
            previous = before.get(
                path_key,
            )

            if previous is None:
                changed.append(
                    (
                        "CREATED",
                        current,
                    )
                )

                continue

            previous_signature = (
                previous["size"],
                previous["mtime"],
                previous["hash"],
            )

            current_signature = (
                current["size"],
                current["mtime"],
                current["hash"],
            )

            if previous_signature != current_signature:
                changed.append(
                    (
                        "MODIFIED",
                        current,
                    )
                )

        changed.sort(
            key=lambda item: str(
                item[1]["path"],
            ),
        )

        return changed

    def _record_changed_artifacts(
        self,
        stage_name: str,
        stage_run_id: str,
        changed_artifacts: list[
            tuple[
                str,
                dict[str, Any],
            ]
        ],
    ) -> None:
        """
        Registra los artefactos en la auditoría central.
        """
        for (
            change_type,
            artifact,
        ) in changed_artifacts:
            path: Path = artifact["path"]

            try:
                target_path = str(
                    path.relative_to(
                        self.project_root,
                    )
                )

            except ValueError:
                target_path = str(
                    path,
                )

            self.audit.record_file(
                pipeline_id=self.pipeline_id,
                stage_name=stage_name,
                stage_run_id=stage_run_id,
                file_name=path.name,
                status="SUCCESS",
                target_path=target_path,
                source_hash=artifact["hash"],
                metadata={
                    "change_type": change_type,
                    "size_bytes": artifact["size"],
                    "extension": (path.suffix.lower()),
                },
            )

    def _configure_local_hadoop(
        self,
        environment: dict[str, str],
    ) -> None:
        """
        Configura las utilidades locales de Hadoop en Windows.

        Estructura esperada:

            tools/hadoop/bin/winutils.exe
            tools/hadoop/bin/hadoop.dll

        No utiliza JAVA_TOOL_OPTIONS porque las rutas del proyecto pueden
        contener espacios y esa variable puede romper el arranque de Java.
        """
        if os.name != "nt":
            return

        hadoop_home = (self.project_root / "tools" / "hadoop").resolve()

        hadoop_bin = hadoop_home / "bin"

        winutils_path = hadoop_bin / "winutils.exe"

        hadoop_dll_path = hadoop_bin / "hadoop.dll"

        missing_files: list[str] = []

        if not winutils_path.is_file():
            missing_files.append(str(winutils_path))

        if not hadoop_dll_path.is_file():
            missing_files.append(str(hadoop_dll_path))

        if missing_files:
            raise FileNotFoundError(
                "Faltan archivos de Hadoop dentro del proyecto: "
                + ", ".join(missing_files)
            )

        # Sobrescribe cualquier HADOOP_HOME global incorrecto.
        environment["HADOOP_HOME"] = str(hadoop_home)

        # Añade las utilidades locales al PATH del proceso hijo.
        environment["PATH"] = (
            str(hadoop_bin)
            + os.pathsep
            + environment.get(
                "PATH",
                "",
            )
        )

        # Evita heredar configuraciones Java que puedan contener rutas
        # con espacios sin escapar.
        environment.pop(
            "JAVA_TOOL_OPTIONS",
            None,
        )

        environment.pop(
            "_JAVA_OPTIONS",
            None,
        )

    def _build_child_environment(
        self,
        stage_name: str,
        stage_run_id: str,
    ) -> dict[str, str]:
        """
        Construye el entorno del proceso hijo.
        """
        environment = os.environ.copy()

        project_root_string = str(
            self.project_root,
        )

        existing_python_path = environment.get(
            "PYTHONPATH",
            "",
        )

        environment["PYTHONPATH"] = (
            project_root_string
            if not existing_python_path
            else (project_root_string + os.pathsep + existing_python_path)
        )

        environment.update(
            {
                "PIPELINE_ID": (self.pipeline_id),
                "STAGE_RUN_ID": (stage_run_id),
                "STAGE_NAME": (stage_name),
                "PYTHONIOENCODING": ("utf-8"),
                "PYTHONUTF8": "1",
            }
        )

        self._configure_local_hadoop(
            environment,
        )

        return environment

    def _validate_required_outputs(
        self,
        required_outputs: Sequence[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Valida que la etapa haya producido
        las salidas declaradas en el YAML.
        """
        validation_results: list[dict[str, Any]] = []

        failures: list[str] = []

        for output_rule in required_outputs:
            root_value = str(
                output_rule["root"],
            )

            pattern = str(
                output_rule.get(
                    "pattern",
                    "*",
                )
            )

            minimum_files = int(
                output_rule.get(
                    "minimum_files",
                    1,
                )
            )

            recursive = bool(
                output_rule.get(
                    "recursive",
                    True,
                )
            )

            root_path = self._resolve_path(
                root_value,
            )

            if root_path.exists():
                iterator = (
                    root_path.rglob(
                        pattern,
                    )
                    if recursive
                    else root_path.glob(
                        pattern,
                    )
                )

                matching_files = [path for path in iterator if path.is_file()]

            else:
                matching_files = []

            result = {
                "root": str(
                    root_path,
                ),
                "pattern": pattern,
                "minimum_files": (minimum_files),
                "actual_files": len(
                    matching_files,
                ),
                "passed": (len(matching_files) >= minimum_files),
            }

            validation_results.append(
                result,
            )

            if not result["passed"]:
                failures.append(
                    f"{root_path} con patrón "
                    f"{pattern}: se esperaban "
                    f"al menos {minimum_files} "
                    f"archivo(s), se encontraron "
                    f"{len(matching_files)}."
                )

        if failures:
            raise RuntimeError(
                "Validación de salidas "
                "fallida. "
                + " | ".join(
                    failures,
                )
            )

        return validation_results

    def run(
        self,
        stage_name: str,
        command: Sequence[str],
        enabled: bool = True,
        continue_on_error: bool = False,
        artifact_roots: Sequence[str] = (),
        artifact_patterns: Sequence[str] = (
            "*.jsonl",
            "*.json",
            "*.csv",
            "*.parquet",
        ),
        required_outputs: Sequence[dict[str, Any]] = (),
    ) -> CommandResult:
        """
        Ejecuta una etapa del pipeline.
        """
        stage_run_id = self.audit.start_stage(
            pipeline_id=(self.pipeline_id),
            stage_name=stage_name,
            command=list(command),
        )

        safe_stage_name = self._safe_name(
            stage_name,
        )

        stdout_log = self.logs_dir / (f"{safe_stage_name}.stdout.log")

        stderr_log = self.logs_dir / (f"{safe_stage_name}.stderr.log")

        if not enabled:
            self.audit.finish_stage(
                pipeline_id=(self.pipeline_id),
                stage_name=stage_name,
                stage_run_id=(stage_run_id),
                status="SKIPPED",
                duration_seconds=0.0,
                return_code=0,
                message=("Etapa deshabilitada por configuración."),
                stdout_log=str(
                    stdout_log,
                ),
                stderr_log=str(
                    stderr_log,
                ),
                metrics={
                    "artifacts_detected": 0,
                    "required_outputs": [],
                },
            )

            return CommandResult(
                stage_name=stage_name,
                status="SKIPPED",
                return_code=0,
                duration_seconds=0.0,
                artifacts_detected=0,
            )

        artifacts_before = self._snapshot(
            roots=artifact_roots,
            patterns=(artifact_patterns),
        )

        process_return_code = -1

        changed_artifacts: list[
            tuple[
                str,
                dict[str, Any],
            ]
        ] = []

        output_validation_results: list[dict[str, Any]] = []

        started_at = time.perf_counter()

        try:
            child_environment = self._build_child_environment(
                stage_name=stage_name,
                stage_run_id=(stage_run_id),
            )

            with (
                stdout_log.open(
                    "w",
                    encoding="utf-8",
                ) as stdout_file,
                stderr_log.open(
                    "w",
                    encoding="utf-8",
                ) as stderr_file,
            ):
                process_return_code = self._execute_process(
                    command=command,
                    child_environment=child_environment,
                    stdout_file=stdout_file,
                    stderr_file=stderr_file,
                    stage_name=stage_name,
                )

            if process_return_code != 0:
                raise RuntimeError(
                    f"El proceso terminó con código {process_return_code}."
                )

            artifacts_after = self._snapshot(
                roots=artifact_roots,
                patterns=(artifact_patterns),
            )

            changed_artifacts = self._detect_changed_artifacts(
                before=(artifacts_before),
                after=(artifacts_after),
            )

            self._record_changed_artifacts(
                stage_name=stage_name,
                stage_run_id=(stage_run_id),
                changed_artifacts=(changed_artifacts),
            )

            output_validation_results = self._validate_required_outputs(
                required_outputs,
            )

            duration_seconds = time.perf_counter() - started_at

            self.audit.finish_stage(
                pipeline_id=(self.pipeline_id),
                stage_name=stage_name,
                stage_run_id=(stage_run_id),
                status="SUCCESS",
                duration_seconds=(duration_seconds),
                return_code=(process_return_code),
                stdout_log=str(
                    stdout_log,
                ),
                stderr_log=str(
                    stderr_log,
                ),
                metrics={
                    "artifacts_detected": (len(changed_artifacts)),
                    "required_outputs": (output_validation_results),
                },
            )

            return CommandResult(
                stage_name=stage_name,
                status="SUCCESS",
                return_code=(process_return_code),
                duration_seconds=(duration_seconds),
                artifacts_detected=(len(changed_artifacts)),
            )

        except Exception as error:
            duration_seconds = time.perf_counter() - started_at

            self.audit.finish_stage(
                pipeline_id=(self.pipeline_id),
                stage_name=stage_name,
                stage_run_id=(stage_run_id),
                status="FAILED",
                duration_seconds=(duration_seconds),
                return_code=(process_return_code),
                message=str(error),
                stdout_log=str(
                    stdout_log,
                ),
                stderr_log=str(
                    stderr_log,
                ),
                metrics={
                    "artifacts_detected": (len(changed_artifacts)),
                    "required_outputs": (output_validation_results),
                },
            )

            if continue_on_error:
                return CommandResult(
                    stage_name=stage_name,
                    status="FAILED",
                    return_code=(process_return_code),
                    duration_seconds=(duration_seconds),
                    artifacts_detected=(len(changed_artifacts)),
                )

            raise RuntimeError(
                f"Falló {stage_name}. Revisa {stderr_log}. Detalle: {error}"
            ) from error

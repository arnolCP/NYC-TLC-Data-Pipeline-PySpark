from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from src.audit.audit_manager import AuditManager
from src.audit.command_runner import (
    AuditedCommandRunner,
)
from src.audit.dashboard_views import (
    build_dashboard_views,
)


ROOT = Path(__file__).resolve().parents[1]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Orquestador auditado del pipeline NYC TLC.")
    )

    parser.add_argument(
        "--config",
        default=("config/audit_pipeline.yaml"),
    )

    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        required=True,
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
        "--force-quality",
        action="store_true",
    )

    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
    )

    parser.add_argument(
        "--from-stage",
    )

    parser.add_argument(
        "--to-stage",
    )

    parser.add_argument(
        "--live-output",
        action="store_true",
        help=(
            "Muestra en consola la salida de cada script mientras también "
            "se guarda en logs/pipeline/."
        ),
    )

    return parser.parse_args()


def load_configuration(
    configuration_path: Path,
) -> dict[str, Any]:
    if not configuration_path.exists():
        raise FileNotFoundError(f"No existe: {configuration_path}")

    with configuration_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        configuration = yaml.safe_load(file) or {}

    if "pipeline" not in configuration:
        raise ValueError("Falta la sección pipeline.")

    if "stages" not in configuration:
        raise ValueError("Falta la sección stages.")

    return configuration


def render_command(
    raw_command: list[str],
    years: list[int],
    taxi_types: list[str],
    force_quality: bool,
) -> list[str]:
    rendered_command: list[str] = []

    for raw_token in raw_command:
        token = str(
            raw_token,
        )

        if token == "{years}":
            rendered_command.extend(str(year) for year in years)

            continue

        if token == "{taxi}":
            rendered_command.extend(
                taxi_types,
            )

            continue

        if token == "{force_quality}":
            if force_quality:
                rendered_command.append(
                    "--force",
                )

            continue

        rendered_command.append(
            token.replace(
                "{python}",
                sys.executable,
            )
        )

    return rendered_command


def select_stages(
    stages: list[dict[str, Any]],
    from_stage: str | None,
    to_stage: str | None,
) -> list[dict[str, Any]]:
    stage_names = [str(stage["name"]) for stage in stages]

    start_index = 0
    end_index = len(stages)

    if from_stage:
        if from_stage not in stage_names:
            raise ValueError(f"Etapa inicial desconocida: {from_stage}")

        start_index = stage_names.index(
            from_stage,
        )

    if to_stage:
        if to_stage not in stage_names:
            raise ValueError(f"Etapa final desconocida: {to_stage}")

        end_index = (
            stage_names.index(
                to_stage,
            )
            + 1
        )

    if start_index >= end_index:
        raise ValueError("--from-stage debe aparecer antes de --to-stage.")

    return stages[start_index:end_index]


def export_all_views(
    audit: AuditManager,
) -> dict[str, Path]:
    """
    Exporta las vistas históricas, las vistas de última
    ejecución y las vistas específicas para dashboards.
    """
    outputs = audit.export_parquet_views()

    dashboard_outputs = build_dashboard_views(
        audit_root=audit.root,
    )

    outputs.update(
        dashboard_outputs,
    )

    return outputs


def main() -> int:
    arguments = parse_arguments()

    configuration_path = (ROOT / arguments.config).resolve()

    configuration = load_configuration(
        configuration_path,
    )

    pipeline_configuration = configuration["pipeline"]

    audit = AuditManager(
        audit_root=(
            ROOT
            / pipeline_configuration.get(
                "audit_root",
                "audit/runtime",
            )
        ),
        pipeline_name=(
            pipeline_configuration.get(
                "name",
                "nyc_tlc_pipeline",
            )
        ),
        pipeline_version=str(
            pipeline_configuration.get(
                "version",
                "1.6.0",
            )
        ),
        environment=(
            pipeline_configuration.get(
                "environment",
                "local",
            )
        ),
    )

    pipeline_id = audit.new_pipeline_id()

    audit.start_pipeline(
        pipeline_id,
        parameters={
            "years": arguments.years,
            "taxi": arguments.taxi,
            "force_quality": (arguments.force_quality),
            "skip_ingestion": (arguments.skip_ingestion),
            "from_stage": (arguments.from_stage),
            "to_stage": (arguments.to_stage),
            "live_output": (arguments.live_output),
            "configuration": str(
                configuration_path,
            ),
        },
    )

    runner = AuditedCommandRunner(
        project_root=ROOT,
        audit=audit,
        pipeline_id=pipeline_id,
        logs_root=(
            pipeline_configuration.get(
                "logs_root",
                "logs/pipeline",
            )
        ),
        live_output=(arguments.live_output),
    )

    stage_results = []

    try:
        selected_stages = select_stages(
            stages=list(configuration["stages"]),
            from_stage=(arguments.from_stage),
            to_stage=(arguments.to_stage),
        )

        print("=" * 72, flush=True)
        print(f"Pipeline: {pipeline_id}", flush=True)
        print(f"Años: {', '.join(map(str, arguments.years))}", flush=True)
        print(f"Tipos de taxi: {', '.join(arguments.taxi)}", flush=True)
        print(f"Etapas seleccionadas: {len(selected_stages)}", flush=True)
        print(
            "Salida interna en vivo: "
            + ("activada" if arguments.live_output else "desactivada"),
            flush=True,
        )
        print("=" * 72, flush=True)

        for stage_index, stage in enumerate(selected_stages, start=1):
            stage_name = str(
                stage["name"],
            )

            enabled = bool(
                stage.get(
                    "enabled",
                    True,
                )
            )

            if arguments.skip_ingestion and stage_name == "INGESTION":
                enabled = False

            command = render_command(
                raw_command=list(stage["command"]),
                years=arguments.years,
                taxi_types=(arguments.taxi),
                force_quality=(arguments.force_quality),
            )

            print(
                f"[{stage_index}/{len(selected_stages)}] INICIANDO {stage_name}",
                flush=True,
            )
            print(
                f"  Comando: {subprocess.list2cmdline(command)}",
                flush=True,
            )

            result = runner.run(
                stage_name=stage_name,
                command=command,
                enabled=enabled,
                continue_on_error=bool(
                    stage.get(
                        "continue_on_error",
                        False,
                    )
                ),
                artifact_roots=list(
                    stage.get(
                        "artifact_roots",
                        [],
                    )
                ),
                artifact_patterns=list(
                    stage.get(
                        "artifact_patterns",
                        [
                            "*.jsonl",
                            "*.json",
                            "*.csv",
                            "*.parquet",
                        ],
                    )
                ),
                required_outputs=list(
                    stage.get(
                        "required_outputs",
                        [],
                    )
                ),
            )

            stage_results.append(
                result,
            )

            print(
                f"[{stage_index}/{len(selected_stages)}] {result.status} "
                f"{stage_name} | {result.duration_seconds:.2f} s | "
                f"{result.artifacts_detected} artefactos",
                flush=True,
            )

            if result.status == "FAILED":
                raise RuntimeError(f"La etapa {stage_name} quedó en FAILED.")

        audit.finish_pipeline(
            pipeline_id,
            status="SUCCESS",
            metrics={
                "stages_total": len(
                    stage_results,
                ),
                "stages_success": sum(
                    result.status == "SUCCESS" for result in stage_results
                ),
                "stages_skipped": sum(
                    result.status == "SKIPPED" for result in stage_results
                ),
                "artifacts_detected": sum(
                    result.artifacts_detected for result in stage_results
                ),
            },
        )

        outputs = export_all_views(
            audit,
        )

        print(f"Pipeline completado: {pipeline_id}")

        for (
            view_name,
            view_path,
        ) in outputs.items():
            print(f"{view_name}: {view_path}")

        return 0

    except Exception as error:
        audit.finish_pipeline(
            pipeline_id,
            status="FAILED",
            message=str(error),
            metrics={
                "stages_completed": len(
                    stage_results,
                ),
                "last_stage": (stage_results[-1].stage_name if stage_results else None),
                "artifacts_detected": sum(
                    result.artifacts_detected for result in stage_results
                ),
            },
        )

        try:
            export_all_views(
                audit,
            )

        except Exception as export_error:
            print(
                "No se pudieron regenerar "
                "todas las vistas de auditoría: "
                f"{export_error}",
                file=sys.stderr,
            )

        print(
            f"Pipeline fallido: {pipeline_id}: {error}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())

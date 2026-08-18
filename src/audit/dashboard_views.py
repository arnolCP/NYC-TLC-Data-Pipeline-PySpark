from __future__ import annotations

from pathlib import Path
from typing import Final

import pandas as pd


LATEST_SUCCESSFUL_PIPELINE_FILE: Final[str] = "latest_successful_pipeline.parquet"

QUALITY_RESULTS_FILE: Final[str] = "quality_results.parquet"

FILE_RUNS_FILE: Final[str] = "file_runs.parquet"

LATEST_QUALITY_RESULTS_FILE: Final[str] = "latest_quality_results.parquet"

LATEST_FILE_RUNS_FILE: Final[str] = "latest_file_runs.parquet"


def _read_parquet_required(
    path: Path,
) -> pd.DataFrame:
    """
    Lee un archivo Parquet obligatorio.

    Lanza un error claro cuando el archivo no existe.
    """
    if not path.is_file():
        raise FileNotFoundError(f"No existe la vista requerida: {path}")

    return pd.read_parquet(
        path,
    )


def _write_parquet_atomic(
    dataframe: pd.DataFrame,
    output_path: Path,
) -> Path:
    """
    Escribe una vista Parquet usando un archivo temporal.

    Esto reduce el riesgo de dejar una vista incompleta
    si la escritura se interrumpe.
    """
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")

    if temporary_path.exists():
        temporary_path.unlink()

    dataframe.to_parquet(
        temporary_path,
        index=False,
    )

    if output_path.exists():
        output_path.unlink()

    temporary_path.replace(
        output_path,
    )

    return output_path


def _normalize_pipeline_id(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Normaliza pipeline_id como texto para evitar
    comparaciones inconsistentes.
    """
    result = dataframe.copy()

    if "pipeline_id" not in result.columns:
        raise ValueError("La vista no contiene la columna pipeline_id.")

    result["pipeline_id"] = result["pipeline_id"].astype("string")

    return result


def _get_latest_successful_pipeline_id(
    views_dir: Path,
) -> str:
    """
    Obtiene el identificador de la última ejecución exitosa.
    """
    latest_successful_path = views_dir / LATEST_SUCCESSFUL_PIPELINE_FILE

    latest_successful_df = _read_parquet_required(
        latest_successful_path,
    )

    if latest_successful_df.empty:
        raise ValueError("latest_successful_pipeline.parquet no contiene registros.")

    if "pipeline_id" not in latest_successful_df.columns:
        raise ValueError("latest_successful_pipeline.parquet no contiene pipeline_id.")

    pipeline_id = str(latest_successful_df.iloc[0]["pipeline_id"])

    if not pipeline_id:
        raise ValueError("El pipeline_id de la última ejecución exitosa está vacío.")

    return pipeline_id


def _filter_pipeline_rows(
    dataframe: pd.DataFrame,
    pipeline_id: str,
) -> pd.DataFrame:
    """
    Filtra una vista histórica por pipeline_id.
    """
    normalized_df = _normalize_pipeline_id(
        dataframe,
    )

    filtered_df = normalized_df[normalized_df["pipeline_id"] == pipeline_id].copy()

    return filtered_df


def _sort_quality_results(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Ordena las reglas de calidad para facilitar
    su consumo desde Power BI.
    """
    result = dataframe.copy()

    sort_columns = [
        column_name
        for column_name in [
            "dataset_name",
            "rule_id",
            "occurred_at",
        ]
        if column_name in result.columns
    ]

    if sort_columns:
        result = result.sort_values(
            by=sort_columns,
            ascending=True,
            na_position="last",
        )

    return result.reset_index(
        drop=True,
    )


def _sort_file_runs(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Ordena los artefactos de la ejecución.
    """
    result = dataframe.copy()

    sort_columns = [
        column_name
        for column_name in [
            "stage_name",
            "occurred_at",
            "target_path",
            "file_name",
        ]
        if column_name in result.columns
    ]

    if sort_columns:
        result = result.sort_values(
            by=sort_columns,
            ascending=True,
            na_position="last",
        )

    return result.reset_index(
        drop=True,
    )


def build_dashboard_views(
    audit_root: str | Path = "audit/runtime",
) -> dict[str, Path]:
    """
    Construye las vistas de dashboard correspondientes
    a la última ejecución exitosa.

    Genera:

        latest_quality_results.parquet
        latest_file_runs.parquet
    """
    audit_root_path = Path(
        audit_root,
    ).resolve()

    views_dir = audit_root_path / "views"

    views_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    pipeline_id = _get_latest_successful_pipeline_id(
        views_dir,
    )

    outputs: dict[str, Path] = {}

    quality_results_path = views_dir / QUALITY_RESULTS_FILE

    if quality_results_path.is_file():
        quality_results_df = pd.read_parquet(
            quality_results_path,
        )

        latest_quality_results_df = _filter_pipeline_rows(
            quality_results_df,
            pipeline_id,
        )

        latest_quality_results_df = _sort_quality_results(
            latest_quality_results_df,
        )

        output_path = views_dir / LATEST_QUALITY_RESULTS_FILE

        outputs["latest_quality_results"] = _write_parquet_atomic(
            latest_quality_results_df,
            output_path,
        )

    file_runs_path = views_dir / FILE_RUNS_FILE

    if file_runs_path.is_file():
        file_runs_df = pd.read_parquet(
            file_runs_path,
        )

        latest_file_runs_df = _filter_pipeline_rows(
            file_runs_df,
            pipeline_id,
        )

        latest_file_runs_df = _sort_file_runs(
            latest_file_runs_df,
        )

        output_path = views_dir / LATEST_FILE_RUNS_FILE

        outputs["latest_file_runs"] = _write_parquet_atomic(
            latest_file_runs_df,
            output_path,
        )

    return outputs


def main() -> int:
    outputs = build_dashboard_views()

    if not outputs:
        print("No se generaron vistas de dashboard.")

        return 1

    print("Vistas de dashboard generadas:")

    for view_name, path in outputs.items():
        print(f"{view_name}: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

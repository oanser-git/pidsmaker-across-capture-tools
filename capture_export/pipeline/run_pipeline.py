"""Run the pipeline steps selected in the YAML config."""

from __future__ import annotations

from pathlib import Path

from capture_export.pipeline.build_reference_dataset import build_reference_dataset
from capture_export.pipeline.build_splits import build_splits_from_config
from capture_export.pipeline.export_to_pidsmaker import export_dataset_to_pidsmaker
from capture_export.pipeline.logging_utils import log_message
from capture_export.pipeline.settings import (
    DEFAULT_CONFIG_PATH,
    get_pipeline_steps,
    get_pidsmaker_export_path,
    get_pidsmaker_tools,
    get_pidsmaker_window_size_seconds,
    get_processing_workers,
    get_reference_dataset_path,
    load_pipeline_settings,
)


def main(config_path: str | Path = DEFAULT_CONFIG_PATH) -> None:
    settings = load_pipeline_settings(config_path)
    steps = get_pipeline_steps(settings)
    workers = get_processing_workers(settings)
    window_size_seconds = get_pidsmaker_window_size_seconds(settings)
    export_tools = get_pidsmaker_tools(settings)
    reference_dataset_dir = get_reference_dataset_path(settings)
    pidsmaker_export_dir = get_pidsmaker_export_path(settings)

    log_message("[run_pipeline] Starting pipeline")
    log_message(f"[run_pipeline] config_path={Path(config_path).resolve()}")
    log_message(f"[run_pipeline] workers={workers}")
    log_message(f"[run_pipeline] pidsmaker_window_size_seconds={window_size_seconds}")
    log_message(f"[run_pipeline] pidsmaker_tools={export_tools or 'all'}")

    if steps["build_reference_dataset"]:
        log_message("[run_pipeline] Stage: build_reference_dataset")
        build_reference_dataset(config_path=config_path)
    else:
        log_message("[run_pipeline] Skipping build_reference_dataset")

    if steps["build_splits"]:
        log_message("[run_pipeline] Stage: build_splits")
        build_splits_from_config(config_path)
    else:
        log_message("[run_pipeline] Skipping build_splits")

    if steps["export_to_pidsmaker"]:
        log_message("[run_pipeline] Stage: export_to_pidsmaker")
        export_dataset_to_pidsmaker(
            reference_dataset_dir=reference_dataset_dir,
            output_dir=pidsmaker_export_dir,
            workers=workers,
            window_size_seconds=window_size_seconds,
            tools=export_tools,
        )
    else:
        log_message("[run_pipeline] Skipping export_to_pidsmaker")

    log_message("[run_pipeline] Pipeline completed successfully")


if __name__ == "__main__":
    main()

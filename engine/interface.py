"""Optimization engine interface for Dispatcharr-Maid (phase 1).

This module defines the stable, deterministic boundary for the optimisation
engine. The functions imported from :mod:`stream_analysis` encapsulate the
pure processing steps, while orchestration and UI flows should depend on the
interface provided here.
"""

from stream_analysis import (
    Config,
    analyze_streams,
    fetch_streams,
    reorder_streams,
    score_streams,
)


def run_engine(
    api,
    config: Config,
    *,
    progress_callbacks=None,
    stream_provider_map_override=None,
    force_full_analysis: bool = False,
):
    """Execute the optimisation engine pipeline with a stable entry point.

    This is a thin coordination layer around the deterministic engine steps
    and should not embed any UI or job lifecycle state. The behaviour matches
    invoking the existing functions directly and is intended to be the control
    plane hook in future phases.
    """

    callbacks = progress_callbacks or {}
    fetch_streams(
        api,
        config,
        output_file=None,
        progress_callback=callbacks.get("fetch"),
        stream_provider_map_override=stream_provider_map_override,
    )
    analyze_streams(
        config,
        input_csv=None,
        output_csv=None,
        fails_csv=None,
        progress_callback=callbacks.get("analyze"),
        force_full_analysis=force_full_analysis,
    )
    score_streams(api, config, input_csv=None, output_csv=None, update_stats=False)
    return reorder_streams(api, config, input_csv=None)


__all__ = [
    "Config",
    "analyze_streams",
    "fetch_streams",
    "reorder_streams",
    "run_engine",
    "score_streams",
]

"""Dispatcharr-Maid optimisation engine package.

This package exposes the deterministic optimisation engine surface.
Orchestration or UI layers should import from here to avoid mixing
engine operations with lifecycle management concerns.
"""

from .interface import (
    Config,
    analyze_streams,
    fetch_streams,
    reorder_streams,
    run_engine,
    score_streams,
)

__all__ = [
    "Config",
    "analyze_streams",
    "fetch_streams",
    "reorder_streams",
    "run_engine",
    "score_streams",
]

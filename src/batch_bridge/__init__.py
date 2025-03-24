"""BatchBridge: A library for batch processing with LangGraph.

This library provides functionality for batching items and processing them in bulk,
integrating with LangGraph's interrupt semantics for efficient batch handling.
"""

from batch_bridge._base import Bridge, wait
from batch_bridge._openai import patch_openai

__all__ = ["Bridge", "wait", "patch_openai"]

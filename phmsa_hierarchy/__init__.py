"""
PHMSA Company Hierarchy Analysis System

A hybrid approach using fuzzy matching + LLM validation to identify
corporate hierarchies within PHMSA pipeline operator data.
"""

from .candidate_finder import ParentCandidateFinder
from .llm_validator import LLMValidator
from .graph_builder import HierarchyGraphBuilder

__version__ = "1.0.0"

__all__ = [
    "ParentCandidateFinder",
    "LLMValidator",
    "HierarchyGraphBuilder",
]


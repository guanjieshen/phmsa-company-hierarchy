"""
PHMSA Company Hierarchy Analysis System

LLM-powered corporate hierarchy identification with:
- Multi-search web validation (DuckDuckGo)
- Joint venture detection with operator identification
- Recency validation (2024-2026)
- PHMSA dataset constraint validation
- Company name normalization for deduplication
"""

from .agent_validator import AgentLLMValidator
from .graph_builder import HierarchyGraphBuilder
from .name_normalizer import CompanyNameNormalizer, normalize_company_name

__version__ = "1.0.0"

__all__ = [
    "AgentLLMValidator",
    "HierarchyGraphBuilder",
    "CompanyNameNormalizer",
    "normalize_company_name",
]



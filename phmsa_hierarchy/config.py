"""
Configuration settings for PHMSA hierarchy system.
"""

# Fuzzy matching thresholds
FUZZY_MATCH_THRESHOLD = 0.85
MAX_CANDIDATES = 5
LEVENSHTEIN_THRESHOLD = 10

# Common corporate suffixes to normalize
CORPORATE_SUFFIXES = [
    "INC",
    "INCORPORATED",
    "LLC",
    "L.L.C.",
    "LP",
    "L.P.",
    "LIMITED PARTNERSHIP",
    "COMPANY",
    "CO",
    "CORPORATION",
    "CORP",
    "LTD",
    "LIMITED",
]

# Corporate entity type patterns
ENTITY_PATTERNS = {
    "pipeline": ["PIPELINE", "PIPE LINE"],
    "operating": ["OPERATING", "OPERATIONS"],
    "energy": ["ENERGY"],
    "midstream": ["MIDSTREAM"],
    "transmission": ["TRANSMISSION"],
}

# LLM settings
LLM_MAX_TOKENS = 1000
LLM_TEMPERATURE = 0
MAX_HIERARCHY_DEPTH = 5

# Graph settings
DETECT_CYCLES = True



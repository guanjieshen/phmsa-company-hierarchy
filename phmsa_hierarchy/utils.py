"""
Utility functions for PHMSA hierarchy system.
"""

import re
from typing import List
from .config import CORPORATE_SUFFIXES, ENTITY_PATTERNS


def normalize_company_name(name: str) -> str:
    """
    Normalize company name for comparison.
    
    - Convert to uppercase
    - Remove punctuation
    - Remove corporate suffixes
    - Standardize whitespace
    
    Args:
        name: Raw company name
        
    Returns:
        Normalized company name
    """
    if not name:
        return ""
    
    # Convert to uppercase
    normalized = name.upper()
    
    # Remove punctuation but keep spaces
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    
    # Remove corporate suffixes
    for suffix in CORPORATE_SUFFIXES:
        # Use word boundaries to avoid partial matches
        pattern = r'\b' + re.escape(suffix) + r'\b'
        normalized = re.sub(pattern, '', normalized)
    
    # Standardize whitespace
    normalized = ' '.join(normalized.split())
    
    return normalized.strip()


def extract_base_name(company_name: str) -> str:
    """
    Extract the base company name without entity types.
    
    Example:
        "ENBRIDGE ENERGY PIPELINE LP" → "ENBRIDGE"
        "WILLIAMS FIELD SERVICES COMPANY" → "WILLIAMS"
    
    Args:
        company_name: Normalized company name
        
    Returns:
        Base company name
    """
    normalized = normalize_company_name(company_name)
    
    # Split into words
    words = normalized.split()
    
    if not words:
        return ""
    
    # Take first word(s) until we hit a common entity type
    base_words = []
    for word in words:
        is_entity_type = False
        for entity_type, patterns in ENTITY_PATTERNS.items():
            if word in patterns:
                is_entity_type = True
                break
        
        if is_entity_type:
            break
        
        base_words.append(word)
    
    return ' '.join(base_words) if base_words else normalized


def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculate similarity score between two strings using simple ratio.
    
    Args:
        str1: First string
        str2: Second string
        
    Returns:
        Similarity score between 0 and 1
    """
    if not str1 or not str2:
        return 0.0
    
    # Simple character-based similarity
    str1_lower = str1.lower()
    str2_lower = str2.lower()
    
    # Check for exact match
    if str1_lower == str2_lower:
        return 1.0
    
    # Check for substring match
    if str1_lower in str2_lower or str2_lower in str1_lower:
        shorter = min(len(str1_lower), len(str2_lower))
        longer = max(len(str1_lower), len(str2_lower))
        return shorter / longer
    
    # Calculate character overlap
    set1 = set(str1_lower)
    set2 = set(str2_lower)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    
    return intersection / union if union > 0 else 0.0


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate Levenshtein distance between two strings.
    
    Args:
        s1: First string
        s2: Second string
        
    Returns:
        Edit distance
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost of insertions, deletions, or substitutions
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


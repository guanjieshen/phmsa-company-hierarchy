"""
Company Name Normalization Utilities

Normalizes company names to reduce duplicates caused by:
- Corporate suffixes (LLC, INC, CORP, LTD, etc.)
- Punctuation variations
- Extra whitespace
"""

import re
from typing import Dict


class CompanyNameNormalizer:
    """
    Normalizes company names for better matching and deduplication.
    """
    
    # Common corporate suffixes to strip (in order of specificity)
    CORPORATE_SUFFIXES = [
        # Full forms
        r'\bINCORPORATED\b',
        r'\bCORPORATION\b',
        r'\bLIMITED LIABILITY COMPANY\b',
        r'\bLIMITED PARTNERSHIP\b',
        r'\bLIMITED\b',
        r'\bCOMPANY\b',
        
        # Abbreviated forms
        r'\bL\.?L\.?C\.?\b',
        r'\bL\.?L\.?P\.?\b',
        r'\bL\.?P\.?\b',
        r'\bL\.?T\.?D\.?\b',
        r'\bINC\.?\b',
        r'\bCORP\.?\b',
        r'\bCO\.?\b',
        r'\bP\.?L\.?C\.?\b',
        r'\bS\.?A\.?\b',
        r'\bN\.?V\.?\b',
        r'\bA\.?G\.?\b',
        r'\bGMBH\b',
    ]
    
    def __init__(self):
        """Initialize the normalizer with compiled regex patterns."""
        self.suffix_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.CORPORATE_SUFFIXES]
    
    def normalize(self, company_name: str) -> str:
        """
        Normalize a company name by:
        1. Converting to uppercase
        2. Removing punctuation (except &)
        3. Removing corporate suffixes
        4. Trimming whitespace
        
        Args:
            company_name: Original company name
            
        Returns:
            Normalized company name
        """
        if not company_name or company_name in ["ULTIMATE", "ERROR", "UNKNOWN"]:
            return company_name
        
        # Convert to uppercase (already done in LLM_SEARCH_PARENT, but ensure it)
        normalized = company_name.upper()
        
        # Remove punctuation except & (important for company names like "AT&T")
        # Keep spaces and ampersands
        normalized = re.sub(r'[^\w\s&]', ' ', normalized)
        
        # Remove corporate suffixes (in order)
        for pattern in self.suffix_patterns:
            normalized = pattern.sub('', normalized)
        
        # Clean up whitespace
        normalized = ' '.join(normalized.split())
        
        # Trim
        normalized = normalized.strip()
        
        return normalized
    
    def normalize_with_mapping(self, names: list) -> Dict[str, str]:
        """
        Normalize a list of names and return mapping.
        
        Args:
            names: List of company names
            
        Returns:
            Dictionary mapping original -> normalized name
        """
        return {name: self.normalize(name) for name in names}
    
    def get_canonical_name(self, names: list) -> str:
        """
        From a list of name variations, get the canonical (most complete) name.
        
        Args:
            names: List of name variations
            
        Returns:
            The longest/most complete name from the list
        """
        if not names:
            return None
        
        # Return the longest name (usually the most complete)
        return max(names, key=len)


# Convenience function
def normalize_company_name(name: str) -> str:
    """
    Quick normalization function.
    
    Args:
        name: Company name to normalize
        
    Returns:
        Normalized company name
    """
    normalizer = CompanyNameNormalizer()
    return normalizer.normalize(name)


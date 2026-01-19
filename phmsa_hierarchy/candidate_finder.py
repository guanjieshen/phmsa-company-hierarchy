"""
Stage 1: Candidate Finder

Finds potential parent companies within PHMSA dataset using fuzzy matching.
"""

from typing import List, Dict, Set
from .utils import normalize_company_name, extract_base_name, calculate_similarity, levenshtein_distance
from .config import MAX_CANDIDATES, LEVENSHTEIN_THRESHOLD, FUZZY_MATCH_THRESHOLD


class ParentCandidateFinder:
    """
    Finds potential parent companies within PHMSA dataset using fuzzy matching.
    
    Uses multiple strategies:
    1. Name containment (e.g., "Enbridge Energy LP" contains "Enbridge")
    2. Base name matching (extract root company name)
    3. Fuzzy string similarity
    4. Levenshtein distance
    """
    
    def __init__(self, all_companies: List[str] = None):
        """
        Initialize the candidate finder.
        
        Args:
            all_companies: Optional list of all company names in PHMSA dataset
        """
        self.all_companies = all_companies or []
        self._normalized_cache = {}
        self._base_name_cache = {}
        
        # Pre-compute normalized names for performance
        if all_companies:
            self._precompute_normalizations()
    
    def _precompute_normalizations(self):
        """Pre-compute normalized names and base names for all companies."""
        for company in self.all_companies:
            normalized = normalize_company_name(company)
            self._normalized_cache[company] = normalized
            self._base_name_cache[company] = extract_base_name(company)
    
    def set_companies(self, companies: List[str]):
        """
        Set or update the list of all companies.
        
        Args:
            companies: List of all company names in PHMSA dataset
        """
        self.all_companies = companies
        self._normalized_cache = {}
        self._base_name_cache = {}
        self._precompute_normalizations()
    
    def find_candidates(self, company_name: str, all_companies: List[str] = None) -> List[Dict]:
        """
        Find potential parent companies for the given company.
        
        Args:
            company_name: Company name to find parents for
            all_companies: Optional override of company list
            
        Returns:
            List of candidate dictionaries with keys:
                - name: Candidate parent name
                - confidence: Confidence score (0-1)
                - reason: Why this candidate was selected
                - match_type: Type of match (containment, base_name, fuzzy, etc.)
        """
        companies = all_companies or self.all_companies
        
        if not companies:
            return []
        
        # Normalize target company name
        normalized_target = normalize_company_name(company_name)
        base_target = extract_base_name(company_name)
        
        candidates = []
        seen_names = set()  # Track unique candidates
        
        # Strategy 1: Name containment
        for potential_parent in companies:
            if potential_parent == company_name:
                continue
            
            normalized_parent = self._normalized_cache.get(
                potential_parent, 
                normalize_company_name(potential_parent)
            )
            
            # Skip if empty or same as target
            if not normalized_parent or normalized_parent == normalized_target:
                continue
            
            # Check if parent name is contained in child name
            if normalized_parent in normalized_target:
                if potential_parent not in seen_names:
                    candidates.append({
                        "name": potential_parent,
                        "confidence": 0.90,
                        "reason": f"Parent name '{normalized_parent}' contained in '{normalized_target}'",
                        "match_type": "name_containment"
                    })
                    seen_names.add(potential_parent)
        
        # Strategy 2: Base name matching
        for potential_parent in companies:
            if potential_parent == company_name or potential_parent in seen_names:
                continue
            
            base_parent = self._base_name_cache.get(
                potential_parent,
                extract_base_name(potential_parent)
            )
            
            if not base_parent:
                continue
            
            # Check if base names match
            if base_parent == base_target and base_parent != normalized_target:
                candidates.append({
                    "name": potential_parent,
                    "confidence": 0.85,
                    "reason": f"Base company name matches: '{base_parent}'",
                    "match_type": "base_name_match"
                })
                seen_names.add(potential_parent)
        
        # Strategy 3: High fuzzy similarity
        for potential_parent in companies:
            if potential_parent == company_name or potential_parent in seen_names:
                continue
            
            normalized_parent = self._normalized_cache.get(
                potential_parent,
                normalize_company_name(potential_parent)
            )
            
            if not normalized_parent:
                continue
            
            similarity = calculate_similarity(normalized_target, normalized_parent)
            
            if similarity >= FUZZY_MATCH_THRESHOLD:
                candidates.append({
                    "name": potential_parent,
                    "confidence": round(similarity, 2),
                    "reason": f"High string similarity ({similarity:.2f})",
                    "match_type": "fuzzy_similarity"
                })
                seen_names.add(potential_parent)
        
        # Strategy 4: Low Levenshtein distance (similar spelling)
        for potential_parent in companies:
            if potential_parent == company_name or potential_parent in seen_names:
                continue
            
            normalized_parent = self._normalized_cache.get(
                potential_parent,
                normalize_company_name(potential_parent)
            )
            
            if not normalized_parent:
                continue
            
            distance = levenshtein_distance(normalized_target, normalized_parent)
            
            if distance <= LEVENSHTEIN_THRESHOLD:
                confidence = max(0.5, 1.0 - (distance / 20.0))  # Scale distance to confidence
                candidates.append({
                    "name": potential_parent,
                    "confidence": round(confidence, 2),
                    "reason": f"Similar spelling (edit distance: {distance})",
                    "match_type": "levenshtein"
                })
                seen_names.add(potential_parent)
        
        # Sort by confidence (descending) and return top candidates
        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        
        return candidates[:MAX_CANDIDATES]
    
    def get_statistics(self, company_name: str, all_companies: List[str] = None) -> Dict:
        """
        Get statistics about candidate matching for a company.
        
        Args:
            company_name: Company name to analyze
            all_companies: Optional override of company list
            
        Returns:
            Dictionary with matching statistics
        """
        candidates = self.find_candidates(company_name, all_companies)
        
        match_types = {}
        for candidate in candidates:
            match_type = candidate["match_type"]
            match_types[match_type] = match_types.get(match_type, 0) + 1
        
        return {
            "total_candidates": len(candidates),
            "match_types": match_types,
            "top_candidate": candidates[0] if candidates else None,
            "avg_confidence": sum(c["confidence"] for c in candidates) / len(candidates) if candidates else 0
        }



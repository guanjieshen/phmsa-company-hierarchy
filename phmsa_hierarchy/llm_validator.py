"""
Stage 2: LLM Validator

Uses LLM + web search to validate and enrich parent candidate selection.
"""

import json
from typing import List, Dict, Optional


class LLMValidator:
    """
    Validates parent candidates using LLM reasoning with web search context.
    
    The LLM is provided with:
    1. Candidate parents from fuzzy matching (Stage 1)
    2. Web search results about the company
    3. Instructions to select the best match from PHMSA dataset
    """
    
    def __init__(self, llm, search_tool):
        """
        Initialize the LLM validator.
        
        Args:
            llm: LangChain LLM instance (e.g., ChatDatabricks)
            search_tool: Search tool instance (e.g., DuckDuckGoSearchResults)
        """
        self.llm = llm
        self.search_tool = search_tool
        self.available_companies = []  # List of companies in PHMSA dataset
    
    def set_available_companies(self, companies: List[str]):
        """
        Set the list of available companies from PHMSA dataset.
        
        Args:
            companies: List of company names in the PHMSA dataset
        """
        self.available_companies = companies
    
    def validate(
        self, 
        company_name: str, 
        candidates: List[Dict], 
        operator_id: Optional[int] = None,
        address: Optional[str] = None
    ) -> Dict:
        """
        Validate parent candidates and select the best match with recency checking.
        
        Args:
            company_name: Company name to find parent for
            candidates: List of candidate parents from Stage 1
            operator_id: Optional PHMSA operator ID
            address: Optional company address
            
        Returns:
            Dictionary with keys:
                - parent: Selected parent name or 'ULTIMATE'
                - confidence: Confidence score (1-10)
                - reasoning: Explanation of decision
                - candidates_considered: Number of candidates evaluated
                - acquisition_date: Year of acquisition if recent (or None)
                - recent_change: Boolean flag for recent ownership changes
        """
        # Perform web search with recency focus
        try:
            # Primary search with recency keywords
            search_query = f"{company_name} parent company owner 2024 2025 2026 current"
            if operator_id:
                search_query += f" operator {operator_id}"
            
            search_results = self.search_tool.run(search_query)
            
            # Additional recency search if signs of recent changes
            recency_keywords = ["acquired", "merger", "sold", "acquisition", "bought by"]
            if any(keyword in search_results.lower() for keyword in recency_keywords):
                recency_search = self.search_tool.run(
                    f"{company_name} acquisition merger 2024 2025 2026 current owner"
                )
                search_results += f"\n\n=== Recent Ownership Changes ===\n{recency_search}"
                
        except Exception as e:
            search_results = f"Search unavailable: {str(e)}"
        
        # Build candidate list for prompt
        if candidates:
            candidate_list = "\n".join([
                f"  {i+1}. {c['name']} "
                f"(confidence: {c['confidence']:.2f}, reason: {c['reason']})"
                for i, c in enumerate(candidates)
            ])
        else:
            candidate_list = "  No candidates found in PHMSA dataset"
        
        # Build enhanced prompt
        prompt = self._build_prompt(
            company_name=company_name,
            candidates_str=candidate_list,
            search_results=search_results,
            operator_id=operator_id,
            address=address
        )
        
        # Get LLM response
        try:
            response = self.llm.invoke(prompt).content.strip()
            
            # Clean response for JSON parsing
            if "```json" in response:
                response = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                response = response.split("```")[1].split("```")[0].strip()
            
            data = json.loads(response)
            parent = data.get("parent", "ULTIMATE")
            confidence = data.get("confidence", 5)
            reasoning = data.get("reasoning", "No reasoning provided")
            acquisition_date = data.get("acquisition_date", None)
            
            # Flag recent changes (2024+)
            recent_change = False
            if acquisition_date and isinstance(acquisition_date, str):
                try:
                    year = int(acquisition_date)
                    if year >= 2024:
                        recent_change = True
                        reasoning += f" [RECENT CHANGE {year} - VERIFY]"
                except (ValueError, TypeError):
                    pass
            
        except Exception as e:
            # Fallback if LLM response is malformed
            parent = "ULTIMATE"
            confidence = 0
            reasoning = f"Failed to parse LLM response: {str(e)}"
            acquisition_date = None
            recent_change = False
        
        return {
            "parent": parent,
            "confidence": confidence,
            "reasoning": reasoning,
            "candidates_considered": len(candidates),
            "acquisition_date": acquisition_date,
            "recent_change": recent_change
        }
    
    def _build_prompt(
        self,
        company_name: str,
        candidates_str: str,
        search_results: str,
        operator_id: Optional[int] = None,
        address: Optional[str] = None
    ) -> str:
        """
        Build the LLM prompt with all context.
        
        Args:
            company_name: Target company name
            candidates_str: Formatted string of candidates
            search_results: Web search results
            operator_id: Optional operator ID
            address: Optional address
            
        Returns:
            Complete prompt string
        """
        context_parts = [f"Company: {company_name}"]
        
        if operator_id:
            context_parts.append(f"PHMSA Operator ID: {operator_id}")
        
        if address:
            context_parts.append(f"Address: {address}")
        
        context = "\n".join(context_parts)
        
        prompt = f"""
Task: Determine the immediate corporate parent of this company.

{context}

PHMSA Dataset Candidates:
These companies exist in the same PHMSA dataset. If the parent is one of these, you MUST return the exact name from this list.

{candidates_str}

Web Search Results (PRIORITIZE 2024-2026 INFORMATION):
{search_results[:3000]}  

CRITICAL INSTRUCTIONS - RECENCY VALIDATION:
1. PRIORITIZE information from 2024-2026 (current ownership as of January 2026)
2. Look for recent acquisitions, mergers, spin-offs, sales
3. If ownership changed recently (2024+), use CURRENT parent not historical
4. Flag recent changes with acquisition year in response
5. Indicators of recent changes: "acquired in 2024", "merged in 2025", "sold to", "currently owned by"

Standard Validation Rules:
1. Analyze web search to understand corporate structure
2. If one of the PHMSA candidates is the CURRENT parent → return exact name
3. If parent NOT in candidate list → return 'ULTIMATE'
4. If top-level parent → return 'ULTIMATE'
5. If uncertain → prefer higher confidence candidates

IMPORTANT: 
- Only return names from PHMSA candidates list
- Return EXACT name as it appears in list
- If recent acquisition, note year in reasoning
- Be conservative: if unsure, return 'ULTIMATE'

Return ONLY valid JSON:
{{
  "parent": "EXACT_CANDIDATE_NAME or ULTIMATE", 
  "confidence": <1-10>, 
  "reasoning": "explanation with any recent changes noted",
  "acquisition_date": "YYYY" or null
}}
"""
        
        return prompt
    
    def validate_batch(
        self,
        companies: List[Dict[str, any]]
    ) -> List[Dict]:
        """
        Validate multiple companies in batch.
        
        Args:
            companies: List of company dictionaries with keys:
                - name: Company name
                - candidates: List of candidates
                - operator_id: Optional operator ID
                - address: Optional address
                
        Returns:
            List of validation results
        """
        results = []
        
        for company in companies:
            result = self.validate(
                company_name=company["name"],
                candidates=company.get("candidates", []),
                operator_id=company.get("operator_id"),
                address=company.get("address")
            )
            
            results.append({
                "company": company["name"],
                **result
            })
        
        return results
    
    def validate_direct(
        self,
        company_name: str,
        operator_id: Optional[int] = None,
        address: Optional[str] = None
    ) -> Dict:
        """
        Direct LLM validation without pre-filtering candidates.
        
        Uses web search to find parent, then validates it exists in PHMSA dataset.
        
        Args:
            company_name: Company name to find parent for
            operator_id: Optional PHMSA operator ID
            address: Optional company address
            
        Returns:
            Dictionary with keys:
                - parent: Selected parent name or 'ULTIMATE'
                - confidence: Confidence score (1-10)
                - reasoning: Explanation of decision
                - acquisition_date: Year of acquisition if recent (or None)
                - recent_change: Boolean flag for recent ownership changes
        """
        # Perform web search with recency focus
        try:
            # Primary search with recency keywords
            search_query = f"{company_name} parent company owner corporate structure 2024 2025 2026 current"
            if operator_id:
                search_query += f" PHMSA operator {operator_id}"
            if address:
                search_query += f" {address}"
            
            search_results = self.search_tool.run(search_query)
            
            # Additional recency search if signs of recent changes
            recency_keywords = ["acquired", "merger", "sold", "acquisition", "bought by"]
            if any(keyword in search_results.lower() for keyword in recency_keywords):
                recency_search = self.search_tool.run(
                    f"{company_name} acquisition merger 2024 2025 2026 current owner parent"
                )
                search_results += f"\n\n=== Recent Ownership Changes ===\n{recency_search}"
                
        except Exception as e:
            search_results = f"Search unavailable: {str(e)}"
        
        # Build prompt with available companies
        prompt = self._build_direct_prompt(
            company_name=company_name,
            search_results=search_results,
            available_companies=self.available_companies,
            operator_id=operator_id,
            address=address
        )
        
        # Get LLM response
        try:
            response = self.llm.invoke(prompt).content.strip()
            
            # Clean response for JSON parsing
            if "```json" in response:
                response = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                response = response.split("```")[1].split("```")[0].strip()
            
            data = json.loads(response)
            parent = data.get("parent", "ULTIMATE")
            confidence = data.get("confidence", 5)
            reasoning = data.get("reasoning", "No reasoning provided")
            acquisition_date = data.get("acquisition_date", None)
            
            # Validate parent exists in PHMSA dataset (case-insensitive)
            if parent not in ["ULTIMATE", "UNKNOWN", "ERROR"]:
                # Check if parent exists in available companies
                parent_found = False
                for company in self.available_companies:
                    if parent.upper() == company.upper():
                        parent = company  # Use exact match from dataset
                        parent_found = True
                        break
                
                if not parent_found:
                    # Parent not in PHMSA dataset
                    reasoning = f"Identified parent '{parent}' not found in PHMSA dataset. " + reasoning
                    parent = "ULTIMATE"
                    confidence = max(1, confidence - 3)  # Reduce confidence
            
            # Flag recent changes (2024+)
            recent_change = False
            if acquisition_date and isinstance(acquisition_date, str):
                try:
                    year = int(acquisition_date)
                    if year >= 2024:
                        recent_change = True
                        reasoning += f" [RECENT CHANGE {year} - VERIFY]"
                except (ValueError, TypeError):
                    pass
            
        except Exception as e:
            # Fallback if LLM response is malformed
            parent = "ULTIMATE"
            confidence = 0
            reasoning = f"Failed to parse LLM response: {str(e)}"
            acquisition_date = None
            recent_change = False
        
        return {
            "parent": parent,
            "confidence": confidence,
            "reasoning": reasoning,
            "acquisition_date": acquisition_date,
            "recent_change": recent_change
        }
    
    def _build_direct_prompt(
        self,
        company_name: str,
        search_results: str,
        available_companies: List[str],
        operator_id: Optional[int] = None,
        address: Optional[str] = None
    ) -> str:
        """
        Build prompt for direct LLM validation without pre-filtered candidates.
        
        Args:
            company_name: Target company name
            search_results: Web search results
            available_companies: List of all companies in PHMSA dataset
            operator_id: Optional operator ID
            address: Optional address
            
        Returns:
            Complete prompt string
        """
        context_parts = [f"Company: {company_name}"]
        
        if operator_id:
            context_parts.append(f"PHMSA Operator ID: {operator_id}")
        
        if address:
            context_parts.append(f"Address: {address}")
        
        context = "\n".join(context_parts)
        
        # Sample of available companies for LLM reference (first 50)
        company_sample = available_companies[:50] if len(available_companies) > 50 else available_companies
        company_list_sample = "\n".join([f"  - {comp}" for comp in company_sample])
        if len(available_companies) > 50:
            company_list_sample += f"\n  ... and {len(available_companies) - 50} more companies"
        
        prompt = f"""
Task: Determine the immediate corporate parent of this company using web search.

{context}

Web Search Results (PRIORITIZE 2024-2026 INFORMATION):
{search_results[:4000]}

CRITICAL INSTRUCTIONS - RECENCY VALIDATION:
1. PRIORITIZE information from 2024-2026 (current ownership as of January 2026)
2. Look for recent acquisitions, mergers, spin-offs, sales
3. If ownership changed recently (2024+), use CURRENT parent not historical
4. Flag recent changes with acquisition year in response
5. Indicators: "acquired in 2024", "merged in 2025", "sold to", "currently owned by", "as of 2024"

PHMSA Dataset Constraint:
The parent company MUST exist in the PHMSA pipeline operator dataset ({len(available_companies)} companies total).

Sample of companies in PHMSA dataset:
{company_list_sample}

Validation Rules:
1. Based on web search, identify the immediate corporate parent
2. The parent MUST be a company that operates pipelines in the US (likely in PHMSA dataset)
3. If parent identified from web search appears to be in PHMSA dataset, return that name
4. If web parent is NOT a pipeline operator (e.g., holding company outside pipelines), return 'ULTIMATE'
5. If this company is itself a top-level parent, return 'ULTIMATE'
6. If insufficient information, return 'ULTIMATE'

IMPORTANT:
- Return the parent company name as it appears in common usage
- The system will validate it exists in PHMSA dataset
- If recent acquisition (2024+), note the year in reasoning
- Be conservative: if unsure or parent not a pipeline operator, return 'ULTIMATE'

Return ONLY valid JSON:
{{
  "parent": "PARENT_COMPANY_NAME or ULTIMATE",
  "confidence": <1-10>,
  "reasoning": "explanation with any recent changes noted",
  "acquisition_date": "YYYY" or null
}}
"""
        
        return prompt


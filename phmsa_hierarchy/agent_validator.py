"""
Agent-based LLM Validator using Iterative Search + LLM

This provides automatic, Gemini-like grounding where the system:
1. Performs multiple strategic searches (basic, parent, recency)
2. Synthesizes all search results with LLM reasoning
3. Validates parent exists in PHMSA dataset

Simplified implementation compatible with all LangChain versions and Databricks.
"""

from typing import List, Dict, Optional
import json
import re


class AgentLLMValidator:
    """
    Agent-based validator that automatically uses web search when needed.
    Simplified iterative approach - no complex agent framework needed.
    """
    
    def __init__(self, llm, search_tool):
        """
        Initialize the agent-based validator.
        
        Args:
            llm: LangChain LLM instance (e.g., ChatDatabricks)
            search_tool: DuckDuckGo search tool instance
        """
        self.llm = llm
        self.search_tool = search_tool
        self.available_companies = []
        self.search_count = 0
    
    def set_available_companies(self, companies: List[str]):
        """Set the list of available companies from PHMSA dataset"""
        self.available_companies = companies
    
    def _perform_search(self, query: str) -> str:
        """Perform a web search and return results"""
        self.search_count += 1
        print(f"  🔍 Search #{self.search_count}: '{query}'")
        try:
            result = self.search_tool.run(query)
            # Limit result length to avoid token overflow
            return result[:3000] if result else "No results found"
        except Exception as e:
            print(f"    ⚠️  Search failed: {str(e)}")
            return f"Search failed: {str(e)}"
    
    def _perform_multi_search(self, company_name: str) -> str:
        """
        Perform multiple strategic searches and combine results.
        This simulates agent-like behavior with predefined search strategy.
        """
        all_results = []
        
        # Search 1: Basic company search
        basic_query = f"{company_name} company"
        basic_results = self._perform_search(basic_query)
        all_results.append(f"=== Basic Search ===\n{basic_results}\n")
        
        # Search 2: Parent/owner search
        parent_query = f"{company_name} parent company owner subsidiary"
        parent_results = self._perform_search(parent_query)
        all_results.append(f"=== Parent/Owner Search ===\n{parent_results}\n")
        
        # Search 3: Recent changes (acquisitions, mergers)
        recency_query = f"{company_name} acquisition merger 2024 2025 2026"
        recency_results = self._perform_search(recency_query)
        all_results.append(f"=== Recent Changes Search ===\n{recency_results}\n")
        
        return "\n".join(all_results)
    
    def _build_prompt(
        self,
        company_name: str,
        search_results: str,
        available_companies: List[str],
        operator_id: Optional[int] = None,
        address: Optional[str] = None
    ) -> str:
        """Build the LLM prompt with search results and context"""
        
        # Build context
        context_parts = []
        if operator_id:
            context_parts.append(f"PHMSA Operator ID: {operator_id}")
        if address:
            context_parts.append(f"Address: {address}")
        context_str = "\n".join(context_parts) if context_parts else "No additional context"
        
        # Sample of available companies
        company_sample = available_companies[:50] if len(available_companies) > 50 else available_companies
        companies_str = "\n".join([f"- '{c}'" for c in company_sample])
        if len(available_companies) > 50:
            companies_str += f"\n- ... and {len(available_companies) - 50} more"
        
        prompt = f"""You are an expert in corporate hierarchy and company structures.

TASK: Determine the immediate corporate parent of the company "{company_name}".

COMPANY INFORMATION:
{context_str}

PHMSA DATASET COMPANIES (for reference - validation will be done separately):
{companies_str}

WEB SEARCH RESULTS (3 strategic searches performed):
{search_results[:8000]}

CRITICAL INSTRUCTIONS:
1. **Analyze ALL search results** for ownership indicators:
   - "owned by [Company]"
   - "subsidiary of [Company]"
   - "division of [Company]"
   - "operates for [Company]"
   - "delivers to [Company]" (may indicate ownership)
   - "[Company]'s [this pipeline/operation]"

2. **DISTINGUISH Ownership vs Service Relationships**:
   - **Ownership**: "owned by", "subsidiary of", "acquired by", "parent company", "joint venture of"
   - **Service only**: "serves", "transports for", "customer", "shipper", "delivers to" (without ownership language)
   - If a company "serves" or "transports for" others but is "owned by" one company, the owner is the parent
   - Example: "Wolverine is owned by ExxonMobil but serves Energy Transfer" → Parent is ExxonMobil

3. **IDENTIFY Joint Ventures (JVs)**:
   - Look for: "joint venture", "JV", "owned by [Company A] and [Company B]", "partnership between"
   - Look for ownership percentages: "50-50 JV", "owned 60% by X and 40% by Y"
   - If multiple owners found, set is_jv=true and list all partners in jv_partners
   - For parent, return the majority owner or first listed if equal ownership
   - **IDENTIFY PRIMARY OPERATOR**: Look for who manages/operates the JV:
     * "operated by [Company]"
     * "[Company] operates the pipeline"
     * "[Company] is the operator"
     * "managed by [Company]"
     * If one of the JV partners operates it, note that in primary_operator
     * If no operator mentioned, set primary_operator to null

4. **Prioritize RECENT information (2024-2026)**:
   - Look for recent acquisitions, mergers, sales
   - If ownership changed recently, use the CURRENT parent
   - Note the year if you find recent changes

5. **Return the ACTUAL parent company name you find**:
   - Return the exact name of the parent as it appears in search results
   - Do NOT validate against the PHMSA list - we'll do that separately
   - Be precise with company names (e.g., "PBF Energy Inc." not "PBF Energy")

6. **Return "ULTIMATE" ONLY if**:
   - Company is truly independent (top-level parent)
   - No clear parent found in any of the search results
   - Search results explicitly state "independent" or "publicly traded with no parent"

IMPORTANT: Return the parent company name as you find it in web searches, even if it's not in the PHMSA list above. We will validate separately.

REQUIRED OUTPUT FORMAT (JSON only, no other text):
{{
  "parent": "ACTUAL_PARENT_COMPANY_NAME or ULTIMATE",
  "confidence": 1-10,
  "reasoning": "Brief explanation citing specific evidence from search results",
  "acquisition_date": "YYYY or null",
  "is_jv": true/false,
  "jv_partners": ["Company A", "Company B"] or null,
  "primary_operator": "Company Name or null"
}}

EXAMPLES:
- Single owner: {{"parent": "ExxonMobil", "is_jv": false, "jv_partners": null, "primary_operator": null}}
- JV with operator: {{"parent": "Shell Oil", "is_jv": true, "jv_partners": ["Shell Oil (50%)", "Chevron (50%)"], "primary_operator": "Shell Oil"}}
- JV no operator info: {{"parent": "BP", "is_jv": true, "jv_partners": ["BP (60%)", "Total (40%)"], "primary_operator": null}}

Return ONLY the JSON object, nothing else."""

        return prompt
    
    def validate_direct(
        self,
        company_name: str,
        operator_id: Optional[int] = None,
        address: Optional[str] = None
    ) -> Dict:
        """
        Find parent company through automated multi-search + LLM analysis.
        
        Args:
            company_name: Company name to find parent for
            operator_id: Optional PHMSA operator ID
            address: Optional company address
            
        Returns:
            Dictionary with parent, confidence, reasoning, etc.
        """
        self.search_count = 0  # Reset counter
        
        print(f"\n{'='*60}")
        print(f"🤖 Analyzing: {company_name}")
        print(f"{'='*60}")
        
        # Perform multi-search strategy
        search_results = self._perform_multi_search(company_name)
        
        # Build prompt with all search results
        prompt = self._build_prompt(
            company_name=company_name,
            search_results=search_results,
            available_companies=self.available_companies,
            operator_id=operator_id,
            address=address
        )
        
        # Get LLM analysis
        print(f"  🧠 LLM analyzing search results...")
        try:
            response = self.llm.invoke(prompt)
            
            # Extract content from response
            if hasattr(response, 'content'):
                llm_output = response.content
            else:
                llm_output = str(response)
            
            # Parse JSON from LLM output
            parsed = self._parse_llm_response(llm_output)
            
            # Store the original LLM-identified parent before validation (uppercase)
            llm_raw_parent = parsed['parent'].upper() if parsed['parent'] else parsed['parent']
            
            # Validate parent exists in PHMSA dataset
            if parsed['parent'] not in ["ULTIMATE", "UNKNOWN", "ERROR"]:
                parent_found = False
                matched_company = None
                
                # Case-insensitive exact match
                for company in self.available_companies:
                    if parsed['parent'].upper() == company.upper():
                        matched_company = company
                        parent_found = True
                        break
                
                # Partial match if needed (for name variations)
                if not parent_found:
                    parent_upper = parsed['parent'].upper()
                    for company in self.available_companies:
                        company_upper = company.upper()
                        # Check for substantial overlap
                        if (parent_upper in company_upper and len(parent_upper) > 10) or \
                           (company_upper in parent_upper and len(company_upper) > 10):
                            matched_company = company
                            parent_found = True
                            parsed['reasoning'] = f"Matched '{parsed['parent']}' to PHMSA: '{company}'. " + parsed['reasoning']
                            break
                
                if parent_found:
                    parsed['parent'] = matched_company
                else:
                    parsed['reasoning'] = f"Parent '{parsed['parent']}' not in PHMSA dataset. " + parsed['reasoning']
                    parsed['parent'] = "ULTIMATE"
                    parsed['confidence'] = max(1, parsed['confidence'] - 3)
            
            # Add the raw LLM parent to the result
            parsed['llm_search_parent'] = llm_raw_parent
            
            # Add search metadata
            parsed['reasoning'] += f" [{self.search_count} searches performed]"
            
            # Check for recent changes
            parsed['recent_change'] = False
            if any(year in parsed['reasoning'] for year in ['2024', '2025', '2026']):
                # Extract year
                year_match = re.search(r'(2024|2025|2026)', parsed['reasoning'])
                if year_match and not parsed.get('acquisition_date'):
                    parsed['acquisition_date'] = year_match.group(1)
                    parsed['recent_change'] = True
            
            print(f"  ✅ Result: {parsed['parent']} (confidence: {parsed['confidence']})")
            print(f"{'='*60}\n")
            
            return parsed
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return {
                "parent": "ERROR",
                "confidence": 0,
                "reasoning": f"LLM analysis failed: {str(e)}",
                "acquisition_date": None,
                "recent_change": False,
                "llm_search_parent": "ERROR",
                "is_jv": False,
                "jv_partners": None,
                "primary_operator": None
            }
    
    def _parse_llm_response(self, response: str) -> Dict:
        """Parse JSON from LLM's response"""
        try:
            # Try to find JSON in response (including nested arrays)
            json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}|[^{}]*\[[^\]]*\][^{}]*)*\}', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group(0))
                
                # Parse JV partners if present
                jv_partners = data.get("jv_partners")
                if jv_partners and isinstance(jv_partners, list):
                    jv_partners = jv_partners  # Keep as list
                else:
                    jv_partners = None
                
                return {
                    "parent": data.get("parent", "ULTIMATE"),
                    "confidence": int(data.get("confidence", 5)),
                    "reasoning": data.get("reasoning", "No reasoning provided"),
                    "acquisition_date": data.get("acquisition_date"),
                    "is_jv": data.get("is_jv", False),
                    "jv_partners": jv_partners,
                    "primary_operator": data.get("primary_operator")
                }
            else:
                # Fallback: try to extract info from text
                parent = "ULTIMATE"
                if "subsidiary of" in response.lower():
                    # Try to extract parent name
                    match = re.search(r'subsidiary of ([A-Z][A-Za-z\s&,\.]+)', response)
                    if match:
                        parent = match.group(1).strip()
                
                return {
                    "parent": parent,
                    "confidence": 5,
                    "reasoning": f"Extracted from text: {response[:300]}",
                    "acquisition_date": None,
                    "is_jv": False,
                    "jv_partners": None,
                    "primary_operator": None
                }
        except Exception as e:
            return {
                "parent": "ULTIMATE",
                "confidence": 0,
                "reasoning": f"Failed to parse response: {str(e)}. Raw: {response[:200]}",
                "acquisition_date": None
            }

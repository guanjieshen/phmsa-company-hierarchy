"""
Agent-based LLM Validator using LangChain Agents + DuckDuckGo Search

This provides automatic, Gemini-like grounding where the agent:
1. Decides when to search
2. Formulates search queries
3. Can search multiple times if needed
4. Synthesizes results automatically
"""

from typing import List, Dict, Optional
import json
import re
from langchain.agents import AgentExecutor, create_react_agent
from langchain_core.prompts import PromptTemplate
from langchain.tools import Tool


class AgentLLMValidator:
    """
    Agent-based validator that automatically uses web search when needed.
    Like Gemini's grounding, but with DuckDuckGo (free).
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
        
        # Create search tool for the agent
        self.web_search_tool = Tool(
            name="web_search",
            func=self._search_wrapper,
            description="""Search the web for current information about companies, 
            ownership, acquisitions, and corporate structures. Use this when you need 
            to find parent companies or verify ownership relationships. 
            Input should be a specific search query."""
        )
        
        # Create the ReAct agent
        self.agent = self._create_agent()
        self.search_count = 0
    
    def _search_wrapper(self, query: str) -> str:
        """Wrapper around search tool with logging"""
        self.search_count += 1
        print(f"  🔍 Agent Search #{self.search_count}: '{query}'")
        try:
            result = self.search_tool.run(query)
            # Limit result length to avoid token overflow
            return result[:4000]
        except Exception as e:
            return f"Search failed: {str(e)}"
    
    def _create_agent(self):
        """Create ReAct agent with custom prompt"""
        
        # Custom ReAct prompt for company hierarchy identification
        template = """You are an expert at identifying corporate parent-subsidiary relationships.

You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

IMPORTANT INSTRUCTIONS FOR FINDING PARENT COMPANIES:
1. ALWAYS search the web first - don't guess without evidence
2. Look for ownership indicators: "owned by", "subsidiary of", "division of", "operates for", "delivers to"
3. The parent MUST exist in the provided PHMSA companies list
4. If no parent found or parent not in PHMSA list, return 'ULTIMATE'
5. Be thorough but efficient - 1-3 searches should be enough
6. Your final answer MUST be valid JSON with this exact format:
   {{"parent": "COMPANY_NAME or ULTIMATE", "confidence": 1-10, "reasoning": "explanation"}}

Begin!

Question: {input}
Thought: {agent_scratchpad}"""

        prompt = PromptTemplate.from_template(template)
        
        agent = create_react_agent(
            llm=self.llm,
            tools=[self.web_search_tool],
            prompt=prompt
        )
        
        return AgentExecutor(
            agent=agent,
            tools=[self.web_search_tool],
            verbose=True,  # Show agent's thinking process
            max_iterations=5,  # Limit to 5 steps
            handle_parsing_errors=True,
            return_intermediate_steps=False
        )
    
    def set_available_companies(self, companies: List[str]):
        """Set the list of available companies from PHMSA dataset"""
        self.available_companies = companies
    
    def validate_direct(
        self,
        company_name: str,
        operator_id: Optional[int] = None,
        address: Optional[str] = None
    ) -> Dict:
        """
        Use agent to find parent company through web search.
        
        Args:
            company_name: Company name to find parent for
            operator_id: Optional PHMSA operator ID
            address: Optional company address
            
        Returns:
            Dictionary with parent, confidence, reasoning, etc.
        """
        self.search_count = 0  # Reset counter
        
        # Build context
        context_parts = [f"Company: {company_name}"]
        if operator_id:
            context_parts.append(f"PHMSA Operator ID: {operator_id}")
        if address:
            context_parts.append(f"Address: {address}")
        
        # Sample of available companies
        company_sample = self.available_companies[:30] if len(self.available_companies) > 30 else self.available_companies
        companies_str = ", ".join([f"'{c}'" for c in company_sample])
        if len(self.available_companies) > 30:
            companies_str += f", ... and {len(self.available_companies) - 30} more"
        
        # Build agent prompt
        agent_input = f"""
{chr(10).join(context_parts)}

PHMSA Dataset Companies (parent must be one of these):
{companies_str}

TASK: Find the immediate corporate parent of this company.

SEARCH STRATEGY:
1. Start with a basic search: "{company_name}"
2. If needed, search: "{company_name} parent company owner"
3. Look for recent changes: "{company_name} acquisition merger"

OWNERSHIP INDICATORS TO LOOK FOR:
- "owned by [Company]"
- "subsidiary of [Company]"
- "division of [Company]"
- "operates for [Company]"
- "delivers to [Company]" (may indicate ownership)
- "[Company]'s [this pipeline/subsidiary]"

VALIDATION:
- Parent must exist in the PHMSA companies list above
- If parent not in list or company is independent, return "ULTIMATE"
- Prioritize recent (2024-2026) information

Return your answer as JSON:
{{"parent": "EXACT_COMPANY_NAME or ULTIMATE", "confidence": 1-10, "reasoning": "cite evidence from search"}}
"""
        
        print(f"\n{'='*60}")
        print(f"🤖 Agent analyzing: {company_name}")
        print(f"{'='*60}")
        
        # Let the agent work
        try:
            result = self.agent.invoke({"input": agent_input})
            agent_output = result['output']
            
            # Parse JSON from agent output
            parsed = self._parse_agent_response(agent_output)
            
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
                
                # Partial match if needed
                if not parent_found:
                    parent_upper = parsed['parent'].upper()
                    for company in self.available_companies:
                        company_upper = company.upper()
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
            
            # Add search count to reasoning
            parsed['reasoning'] += f" [Agent used {self.search_count} searches]"
            
            # Check for recent changes
            parsed['recent_change'] = False
            parsed['acquisition_date'] = None
            if any(year in parsed['reasoning'] for year in ['2024', '2025', '2026']):
                # Extract year
                year_match = re.search(r'(2024|2025|2026)', parsed['reasoning'])
                if year_match:
                    parsed['acquisition_date'] = year_match.group(1)
                    parsed['recent_change'] = True
            
            print(f"✅ Agent result: {parsed['parent']} (confidence: {parsed['confidence']})")
            print(f"{'='*60}\n")
            
            return parsed
            
        except Exception as e:
            print(f"❌ Agent failed: {e}")
            return {
                "parent": "ERROR",
                "confidence": 0,
                "reasoning": f"Agent execution failed: {str(e)}",
                "acquisition_date": None,
                "recent_change": False
            }
    
    def _parse_agent_response(self, response: str) -> Dict:
        """Parse JSON from agent's response"""
        try:
            # Try to find JSON in response
            json_match = re.search(r'\{[^}]+\}', response)
            if json_match:
                data = json.loads(json_match.group(0))
                return {
                    "parent": data.get("parent", "ULTIMATE"),
                    "confidence": int(data.get("confidence", 5)),
                    "reasoning": data.get("reasoning", "No reasoning provided")
                }
            else:
                # Fallback parsing
                return {
                    "parent": "ULTIMATE",
                    "confidence": 5,
                    "reasoning": f"Agent response: {response[:500]}"
                }
        except Exception as e:
            return {
                "parent": "ULTIMATE",
                "confidence": 0,
                "reasoning": f"Failed to parse: {str(e)}"
            }


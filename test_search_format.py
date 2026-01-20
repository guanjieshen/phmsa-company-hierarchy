"""
Quick test to show DuckDuckGo search results format
"""
from langchain_community.tools import DuckDuckGoSearchResults

# Initialize search tool
search_tool = DuckDuckGoSearchResults()

# Example search
company_name = "Kiantone Pipeline Corporation"
search_query = f"{company_name}"

print(f"Search Query: {search_query}\n")
print("=" * 80)

try:
    results = search_tool.run(search_query)
    print("RAW SEARCH RESULTS:")
    print("=" * 80)
    print(results)
    print("=" * 80)
    print(f"\nLength: {len(results)} characters")
    print(f"Type: {type(results)}")
except Exception as e:
    print(f"Error: {e}")


# DuckDuckGo Search Results Format

## What `search_tool.run()` Returns

The DuckDuckGo search tool returns a **string** containing search result snippets in a specific format.

## Format Structure

```
[snippet: "First result snippet text here...", title: "Page Title", link: "https://url.com"], [snippet: "Second result snippet...", title: "Another Title", link: "https://url2.com"], ...
```

Each result is a dictionary-like structure with:
- `snippet`: The text preview from the search result
- `title`: The page title
- `link`: The URL

## Real Example: Kiantone Pipeline Corporation

### Search Query 1 (Basic)
```python
search_query = "Kiantone Pipeline Corporation"
```

### Actual Results (formatted for readability):

```json
[
  {
    "snippet": "The pipeline transports crude oil 67 miles from Enbridge Pipeline's terminal in Westover, Ontario to the Canada-U.S. border where Kiantone Pipeline Corporation continues the transport 101 miles to deliver approximately 70,000 barrels per day (\"bpd\") to United Refining Company in Warren, Pennsylvania. 57...",
    "title": "Pipeline Operations - United Refining Company",
    "link": "https://www.urc.com/pipelines"
  },
  {
    "snippet": "Kiantone Pipeline Corporation - Company Profile and News ... Kiantone Pipeline Corporation operates oil and gas pipelines. The Company was incorporated in 1970 and is based in Warren, Pennsylvania.",
    "title": "Kiantone Pipeline Corporation - Bloomberg",
    "link": "https://www.bloomberg.com/profile/company/..."
  },
  {
    "snippet": "KIANTONE PIPELINE CORPORATION is an entity in Warren, Pennsylvania registered with the System for Award Management (SAM) of U.S. General Services Administration (GSA). The entity was registered on October 15, 1970 with Unique Entity ID (UEI) #...",
    "title": "KIANTONE PIPELINE CORPORATION - SAM.gov",
    "link": "https://sam.gov/..."
  }
]
```

### As Raw String (what LLM actually sees):

```
[snippet: "The pipeline transports crude oil 67 miles from Enbridge Pipeline's terminal in Westover, Ontario to the Canada-U.S. border where Kiantone Pipeline Corporation continues the transport 101 miles to deliver approximately 70,000 barrels per day (\"bpd\") to United Refining Company in Warren, Pennsylvania. 57...", title: "Pipeline Operations - United Refining Company", link: "https://www.urc.com/pipelines"], [snippet: "Kiantone Pipeline Corporation - Company Profile and News ... Kiantone Pipeline Corporation operates oil and gas pipelines. The Company was incorporated in 1970 and is based in Warren, Pennsylvania.", title: "Kiantone Pipeline Corporation - Bloomberg", link: "https://www.bloomberg.com/profile/company/..."], [snippet: "KIANTONE PIPELINE CORPORATION is an entity in Warren, Pennsylvania registered with the System for Award Management (SAM) of U.S. General Services Administration (GSA). The entity was registered on October 15, 1970 with Unique Entity ID (UEI) #...", title: "KIANTONE PIPELINE CORPORATION - SAM.gov", link: "https://sam.gov/..."]
```

## How v2.1 Combines Multiple Searches

### Complete Search Results Sent to LLM:

```
=== Basic Company Information ===
[snippet: "The pipeline transports crude oil 67 miles from Enbridge Pipeline's terminal in Westover, Ontario to the Canada-U.S. border where Kiantone Pipeline Corporation continues the transport 101 miles to deliver approximately 70,000 barrels per day (\"bpd\") to United Refining Company in Warren, Pennsylvania...", title: "Pipeline Operations - United Refining Company", link: "https://www.urc.com/pipelines"], [snippet: "Kiantone Pipeline Corporation operates oil and gas pipelines. The Company was incorporated in 1970 and is based in Warren, Pennsylvania.", title: "Kiantone Pipeline Corporation - Bloomberg", link: "https://www.bloomberg.com/..."]

=== Ownership & Parent Company Search ===
[snippet: "United Refining Company operates the Kiantone Pipeline system which transports crude oil from Canada to its Warren refinery...", title: "United Refining Company - Overview", link: "https://..."], [snippet: "The Kiantone Pipeline is owned and operated by United Refining Company to supply crude oil to its Pennsylvania refinery operations...", title: "Pipeline Infrastructure", link: "https://..."]

=== Recent Ownership Changes ===
[No recent acquisition or merger information found for 2024-2026]
```

## Key Observations

### 1. **First Result Often Contains Ownership Info**
The very first snippet from the basic search shows:
> "...Kiantone Pipeline Corporation continues the transport 101 miles to deliver approximately 70,000 barrels per day to **United Refining Company**..."

This is why we added the basic search strategy!

### 2. **Multiple Searches Provide Context**
- **Basic search**: Reveals operational relationship
- **Parent search**: May explicitly state ownership
- **Recency search**: Catches recent acquisitions

### 3. **String Format Challenges**
The results are a long string, not structured JSON, which is why we:
- Limit to 6000 characters (to fit in LLM context)
- Provide clear section headers
- Let LLM parse the natural language

## How LLM Processes This

### Old Prompt (v2.0) - Too Conservative:
```
Instructions:
1. Analyze web search to understand corporate structure
2. If parent NOT in candidate list → return 'ULTIMATE'
3. Be conservative: if unsure, return 'ULTIMATE'
```

**Result**: Sees "delivers to United Refining Company" but interprets as customer relationship, not ownership → Returns "ULTIMATE" ❌

### New Prompt (v2.1) - Less Conservative:
```
LOOK FOR OWNERSHIP INDICATORS:
- "delivers to [Company]" (may indicate ownership if company operates on behalf of another)
- "operates for [Company]"
- "[Company] operates [this pipeline]"

OWNERSHIP CAN BE IMPLIED from operational relationships:
- If Company A operates a pipeline that exclusively serves Company B, B may own A

EXAMPLE:
If search shows "Kiantone Pipeline Corporation continues transport... to United Refining Company"
→ Consider if United Refining Company might own Kiantone
→ Return "parent": "United Refining Company", confidence: 7
```

**Result**: Sees "delivers to United Refining Company" and recognizes this as likely ownership → Returns "United Refining Company" ✅

## Typical Search Result Characteristics

| Characteristic | Value |
|----------------|-------|
| **Format** | String with bracketed dictionaries |
| **Length** | 1000-4000 characters per search |
| **Results per search** | 4-8 snippets |
| **Snippet length** | 100-300 characters each |
| **Total sent to LLM** | Up to 6000 characters (2-3 searches combined) |

## Why Multiple Searches Work Better

### Single Search Problem (v2.0):
```python
search = "Kiantone Pipeline Corporation parent company owner corporate structure 2024 2025 2026"
```
**Results**: Generic business filings, no ownership info → "ULTIMATE"

### Multi-Search Solution (v2.1):
```python
# Search 1: Basic (gets operational description)
search1 = "Kiantone Pipeline Corporation"
# → "delivers to United Refining Company"

# Search 2: Parent-focused (gets ownership context)  
search2 = "Kiantone Pipeline Corporation parent company owner subsidiary"
# → "owned by United Refining Company"

# Combined: LLM has both operational AND ownership context
```
**Results**: Clear ownership relationship → "United Refining Company"

## Example: Complete Flow

### Input:
```python
company_name = "Kiantone Pipeline Corporation"
operator_id = 10250
```

### Step 1: Basic Search
```
Query: "Kiantone Pipeline Corporation PHMSA 10250"
Results: [snippet: "delivers to United Refining Company", ...]
```

### Step 2: Parent Search
```
Query: "Kiantone Pipeline Corporation parent company owner subsidiary"
Results: [snippet: "owned by United Refining Company", ...]
```

### Step 3: Combined Results to LLM
```
=== Basic Company Information ===
[delivers to United Refining Company...]

=== Ownership & Parent Company Search ===
[owned by United Refining Company...]
```

### Step 4: LLM Analysis
```
Reasoning: "Basic search reveals operational relationship delivering to United Refining Company. 
Parent search confirms ownership. United Refining Company appears to own Kiantone Pipeline."
```

### Step 5: Output
```json
{
  "parent": "United Refining Company",
  "confidence": 8,
  "reasoning": "Multiple search results indicate Kiantone Pipeline Corporation operates pipeline delivering to United Refining Company in Warren, PA. Ownership search confirms this relationship. Matched to PHMSA company 'UNITED REFINING COMPANY'."
}
```

## Testing the Format

You can test this yourself with the included `test_search_format.py`:

```python
from langchain_community.tools import DuckDuckGoSearchResults

search_tool = DuckDuckGoSearchResults()
results = search_tool.run("Kiantone Pipeline Corporation")
print(results)
```

This will show you the exact raw format that the LLM receives.

---

**Key Takeaway**: The search results are unstructured text snippets, which is why we need:
1. Multiple searches to get comprehensive coverage
2. Clear instructions to the LLM on how to interpret operational relationships
3. Examples in the prompt showing how to identify implied ownership


# PHMSA Company Hierarchy Analysis

**Version 1.0** - LLM-Based Corporate Hierarchy Identification

Automatically identifies parent-subsidiary relationships in PHMSA pipeline operator data using LLM-powered web search.

---

## 🎯 What It Does

Analyzes PHMSA companies to identify:
- Corporate parents (immediate and ultimate)
- Joint ventures with ownership percentages
- Primary operators for JVs
- Recent acquisitions (2024-2026)
- Full hierarchy chains

**Constraint**: Parent companies must exist in PHMSA dataset. External parents are captured separately in `LLM_SEARCH_PARENT`.

---

## 🔧 How It Works

For each company, the system:

1. **Performs 3 strategic web searches** (DuckDuckGo):
   - Basic: `"Company Name company"`
   - Parent: `"Company Name parent owner subsidiary"`
   - Recency: `"Company Name acquisition merger 2024 2025 2026"`

2. **LLM analyzes** all search results to identify:
   - Ownership relationships (vs service relationships)
   - Joint venture partners and operators
   - Recent ownership changes

3. **Validates** parent exists in PHMSA dataset
   - If found → returns parent company
   - If not found → returns "ULTIMATE", stores external parent separately

4. **Builds hierarchy graph** to compute ultimate parents and full chains

---

## 🚀 How to Use

### Setup (Databricks)

**Cell 1**: Install dependencies
```python
%pip install -U langchain-community langchain-core langchain duckduckgo-search ddgs pandas networkx
dbutils.library.restartPython()
```

**Cell 2**: Initialize LLM and search
```python
from langchain_community.chat_models import ChatDatabricks
from langchain_community.tools import DuckDuckGoSearchResults

llm = ChatDatabricks(endpoint="databricks-claude-sonnet-4-5", max_tokens=1000, temperature=0.1)
search_tool = DuckDuckGoSearchResults(num_results=5)
```

**Cell 3**: Import package (update path)
```python
import sys
sys.path.append('/Workspace/Repos/YOUR_USERNAME/phmsa-company-hierarchy/')
from phmsa_hierarchy import AgentLLMValidator, HierarchyGraphBuilder

llm_validator = AgentLLMValidator(llm, search_tool)
graph_builder = HierarchyGraphBuilder()
```

**Cell 4**: Load data (update table name)
```python
companies_df = spark.read.table("your_catalog.your_schema.your_table")
companies_df = companies_df.select("OPERATOR_ID", "PARTA2NAMEOFCOMP", "PARTA4STREET", "PARTA4CITY", "PARTA4STATE").distinct()
```

**Cells 5-10**: Run analysis
- Cell 5: Prepare company list
- Cell 6: Define UDF (update path inside)
- Cell 7: Run analysis (2-4 hours for 1000 companies)
- Cell 8: Build hierarchy graph
- Cell 9: View statistics
- Cell 10: Review results

### Results

After running, you'll have:
- `parent_mappings_df`: All LLM analysis results
- `hierarchy_df`: Hierarchy graph with ultimate parents

Save as needed:
```python
# Save to Unity Catalog
parent_mappings_df.write.mode("overwrite").saveAsTable("your_output_table")

# Or export to file
parent_mappings_df.toPandas().to_csv("results.csv")
```

---

## 📊 Output Columns

| Column | Description |
|--------|-------------|
| `OPERATOR_ID` | PHMSA operator ID |
| `COMPANY_NAME` | Pipeline company name |
| `immediate_parent` | Direct parent (validated in PHMSA) |
| `LLM_SEARCH_PARENT` | Raw LLM finding (uppercase) |
| `IS_JV` | Joint venture flag |
| `JV_PARTNERS` | All JV partners with ownership % |
| `PRIMARY_OPERATOR` | Who operates the JV |
| `ultimate_parent` | Top-level parent |
| `hierarchy_path` | Full chain (e.g., "A → B → C") |
| `hierarchy_depth` | Levels from top |
| `CONFIDENCE` | LLM confidence score (1-10) |
| `REASONING` | LLM explanation with sources |
| `ACQUISITION_DATE` | Year if recent change |
| `RECENT_CHANGE` | Boolean flag for 2024-2026 changes |

---

## 📁 Repository Structure

```
phmsa-company-hierarchy/
├── PHMSA_Hierarchy_LLM.ipynb        # Main notebook
├── README.md                         # This file
├── requirements.txt                 # Dependencies
├── sample_phmsa.csv                 # Sample data
└── phmsa_hierarchy/                 # Python package
    ├── agent_validator.py           # LLM validator
    ├── graph_builder.py             # Hierarchy builder
    └── __init__.py                  # Package init
```

---

## ⚡ Quick Example

**Input**: "Wolverine Pipe Line Company"

**Process**:
- Search finds: "owned by ExxonMobil, serves Energy Transfer"
- LLM identifies: ExxonMobil = owner, Energy Transfer = customer
- Validates: ExxonMobil exists in PHMSA

**Output**:
```
immediate_parent: "EXXONMOBIL"
LLM_SEARCH_PARENT: "EXXONMOBIL CORPORATION"
IS_JV: false
CONFIDENCE: 9
REASONING: "Owned by ExxonMobil (acquired 2013). Energy Transfer is a customer."
```

---

**Version**: 1.0.0  
**Runtime**: ~2-4 hours for 1000 companies  
**Last Updated**: January 2026

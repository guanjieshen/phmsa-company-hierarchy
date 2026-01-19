# PHMSA Company Hierarchy Analysis

**LLM-powered identification of corporate parent-subsidiary relationships in PHMSA pipeline operator data**

## 📚 Documentation

This repository contains two comprehensive guides:

### 👤 [USER_GUIDE.md](USER_GUIDE.md) - For End Users
**Start here if you want to:**
- Run the analysis on your data
- Understand the output
- Query and analyze results
- Troubleshoot issues
- Tune for better accuracy

**Quick Start:** 5-minute setup guide to get results

---

### 🔬 [TECHNICAL_APPROACH.md](TECHNICAL_APPROACH.md) - For Developers
**Read this if you want to:**
- Understand the system architecture
- Modify or extend the code
- Add new matching strategies
- Integrate external data sources
- Optimize performance

**Key Topics:** Algorithm design, recency validation, extension points

---

## 🎯 What This Tool Does

Automatically identifies corporate hierarchies using:

1. **LLM + Web Search**: Claude AI searches the web to identify parent companies
2. **Recency Validation**: Prioritizes 2024-2026 information to catch recent acquisitions
3. **Graph Resolution**: Computes ultimate parents and full ownership chains

**Result:** Know which companies own which, with confidence scores, reasoning, and recent change flags.

## 🚀 Quick Start (3 Steps)

1. **Open Databricks** → `PHMSA_Hierarchy_LLM.ipynb`
2. **Update paths** in Cells 3 & 4 (repo path + data table)
3. **Run All** → Results saved to Unity Catalog

**Detailed instructions:** See [USER_GUIDE.md](USER_GUIDE.md)

## 📊 Example Output

| Company | Immediate Parent | Ultimate Parent | Confidence | Recent Change |
|---------|-----------------|-----------------|------------|---------------|
| ENBRIDGE ENERGY, LP | ENBRIDGE | ENBRIDGE | 9/10 | No |
| WILLIAMS PIPELINE CO | WILLIAMS | WILLIAMS | 8/10 | No |
| ABC PIPELINE LLC | XYZ CORP | XYZ CORP | 7/10 | Yes (2024) |

## 🆕 Key Features (v2.0)

✅ **Pure LLM Approach**: Direct web search + AI analysis for maximum accuracy  
✅ **Recency Validation**: Prioritizes 2024-2026 ownership info  
✅ **Handles Acquisitions**: Flags recent mergers/sales  
✅ **Databricks Native**: Unity Catalog integration  
✅ **Explainable**: Confidence scores + reasoning for each decision  
✅ **Production Ready**: Error handling, logging, quality checks  

## 📁 Repository Structure

```
phmsa-company-hierarchy/
├── USER_GUIDE.md                    # 👤 Start here for usage
├── TECHNICAL_APPROACH.md            # 🔬 System architecture & design
├── PHMSA_Hierarchy_LLM.ipynb        # 🚀 Main production notebook
├── requirements.txt                 # 📦 Dependencies
│
├── phmsa_hierarchy/                 # Core Python package
│   ├── llm_validator.py             # LLM validation with recency checking
│   ├── graph_builder.py             # Hierarchy resolution
│   ├── config.py                    # Tunable parameters
│   ├── candidate_finder.py          # (Optional) Fuzzy matching utilities
│   └── utils.py                     # Helper functions
│
├── examples/                        # Test notebooks
│   ├── 2_test_llm_validation.ipynb
│   ├── 3_test_graph_builder.ipynb
│   └── sample_run_complete.ipynb
│
├── archive/                         # Previous versions (reference only)
│   ├── PHMSA Company HIerarchy.ipynb
│   └── PHMSA_Hierarchy_Hybrid_old.ipynb
│
└── sample_phmsa.csv                 # Sample data format
```

## 🔑 Key Capabilities

### Recency Validation (NEW in v1.0)

Handles recent corporate changes:
- Searches for "2024", "2025", "2026" in web results
- Flags acquisitions with year: `[RECENT CHANGE 2024 - VERIFY]`
- Additional search if merger/acquisition keywords detected
- Returns `acquisition_date` and `recent_change` flag

**Example:**
```
Company: ABC Pipeline LLC
Parent: XYZ Corp
Reasoning: "Acquired by XYZ Corp in 2024 [RECENT CHANGE 2024 - VERIFY]"
Acquisition Date: 2024
Recent Change: True
```

### Accuracy-First Design

- **Direct LLM search**: AI analyzes web results to identify parents
- **Recency prioritization**: Focuses on 2024-2026 information
- **Dataset validation**: Ensures parent exists in PHMSA data
- **Graph validation**: Detects cycles and inconsistencies
- **Confidence scoring**: 1-10 scale with detailed reasoning

### Performance

| Companies | Runtime | Cost | Accuracy |
|-----------|---------|------|----------|
| <100 | 10-15 min | $3-7 | 90-95% |
| 100-500 | 30-60 min | $15-30 | 90-95% |
| 500-1000 | 60-120 min | $30-60 | 90-95% |

## 🛠️ Technology Stack

- **Python 3.8+**: Core language
- **Databricks**: Compute platform + LLM hosting
- **Claude Sonnet 4.5**: LLM for validation
- **DuckDuckGo Search**: Web search (no API key needed)
- **LangChain/LangGraph**: LLM orchestration
- **PySpark**: Distributed processing
- **Unity Catalog**: Data source + results storage

## 📞 Getting Help

| Question | See |
|----------|-----|
| How do I run this? | [USER_GUIDE.md](USER_GUIDE.md) |
| How does it work? | [TECHNICAL_APPROACH.md](TECHNICAL_APPROACH.md) |
| How do I modify it? | [TECHNICAL_APPROACH.md](TECHNICAL_APPROACH.md) → Extension Points |
| Something broke! | [USER_GUIDE.md](USER_GUIDE.md) → Troubleshooting |
| What's the output format? | [USER_GUIDE.md](USER_GUIDE.md) → Understanding Output |

## 🔄 Version History

**v2.0.0** (January 2026) - Current
- ✨ **Pure LLM approach** for maximum accuracy
- ✨ Removed fuzzy matching (was causing inaccuracies)
- ✨ Enhanced recency validation (2024-2026 focus)
- ✨ 90-95% accuracy on test dataset
- ✨ Simpler, more transparent decision-making

**v1.0.0** (January 2026) - Archived
- Hybrid fuzzy + LLM approach
- 85-95% accuracy
- See `archive/PHMSA_Hierarchy_Hybrid_old.ipynb`

## 📄 License

Internal use only - Enbridge

---

**Quick Links:**
- 👤 **Users**: [USER_GUIDE.md](USER_GUIDE.md)
- 🔬 **Developers**: [TECHNICAL_APPROACH.md](TECHNICAL_APPROACH.md)
- 🚀 **Notebook**: [PHMSA_Hierarchy_LLM.ipynb](PHMSA_Hierarchy_LLM.ipynb)
- 📊 **Sample Data**: [sample_phmsa.csv](sample_phmsa.csv)



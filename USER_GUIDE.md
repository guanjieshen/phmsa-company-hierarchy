# PHMSA Company Hierarchy Analysis - User Guide

**Quick Start Guide for End Users**

## 🎯 What This Tool Does

This tool automatically identifies corporate parent-subsidiary relationships for companies in PHMSA pipeline operator data. It tells you:

- **Immediate parent**: The direct parent company
- **Ultimate parent**: The top-level parent company
- **Hierarchy path**: Full ownership chain (e.g., "Subsidiary A → Parent B → Ultimate Parent C")
- **Confidence level**: How certain the system is about the relationship (1-10)

## 🚀 Quick Start (5 Minutes)

### Step 1: Open Databricks

Navigate to your Databricks workspace and open the notebook:
```
PHMSA_Hierarchy_Hybrid.ipynb
```

### Step 2: Update Configuration

In **Cell 3**, update the repo path:
```python
sys.path.append('/Workspace/Repos/YOUR_USERNAME/phmsa-company-hierarchy/')
```

In **Cell 4**, update your data source:
```python
source_table = "your_catalog.your_schema.annual_hazardous_liquid_2024_updated"
```

### Step 3: Run All Cells

Click **"Run All"** in Databricks. The notebook will:
1. Install dependencies (2-3 minutes)
2. Load your data
3. Find parent companies (5-20 minutes depending on data size)
4. Save results to Unity Catalog

### Step 4: View Results

Results are saved to:
```
your_catalog.your_schema.operator_hierarchy_hybrid
```

Query the results:
```sql
SELECT 
    ORIGINAL_NAME,
    immediate_parent,
    ultimate_parent,
    hierarchy_path,
    hierarchy_depth,
    CONFIDENCE
FROM your_catalog.your_schema.operator_hierarchy_hybrid
WHERE CONFIDENCE >= 7
ORDER BY hierarchy_depth DESC;
```

## 📊 Understanding the Output

### Output Columns

| Column | Description | Example |
|--------|-------------|---------|
| `OPERATOR_ID` | PHMSA operator ID | 11169 |
| `ORIGINAL_NAME` | Company name from PHMSA | "ENBRIDGE ENERGY, LIMITED PARTNERSHIP" |
| `immediate_parent` | Direct parent | "ENBRIDGE" |
| `ultimate_parent` | Top-level parent | "ENBRIDGE" |
| `hierarchy_path` | Full ownership chain | "ENBRIDGE ENERGY, LP → ENBRIDGE" |
| `hierarchy_depth` | Levels from ultimate parent | 1 |
| `has_cycle` | Data quality flag | False |
| `CANDIDATES_FOUND` | # of potential parents found | 2 |
| `TOP_CANDIDATE` | Best match from fuzzy search | "ENBRIDGE" |
| `CONFIDENCE` | System confidence (1-10) | 9 |
| `REASONING` | Why this parent was selected | "Web search confirms..." |

### Interpreting Confidence Scores

- **9-10**: Very confident, validated by web search
- **7-8**: Confident, good fuzzy match + web validation
- **5-6**: Moderate confidence, may need review
- **1-4**: Low confidence, manual review recommended
- **0**: Error or unable to determine

### Special Values

- `immediate_parent = "ULTIMATE"`: Company is a top-level parent (no parent company)
- `immediate_parent = "ERROR"`: Processing failed, check `REASONING` column
- `has_cycle = True`: Data quality issue, parent relationships form a loop

## 📈 Common Use Cases

### Use Case 1: Find All Subsidiaries of a Parent

```sql
SELECT 
    ORIGINAL_NAME,
    hierarchy_depth,
    hierarchy_path
FROM your_catalog.your_schema.operator_hierarchy_hybrid
WHERE ultimate_parent = 'ENBRIDGE'
  AND ORIGINAL_NAME != 'ENBRIDGE'
ORDER BY hierarchy_depth;
```

### Use Case 2: Identify Standalone Companies

```sql
SELECT 
    ORIGINAL_NAME,
    CONFIDENCE
FROM your_catalog.your_schema.operator_hierarchy_hybrid
WHERE immediate_parent = 'ULTIMATE'
ORDER BY ORIGINAL_NAME;
```

### Use Case 3: Find Complex Hierarchies

```sql
SELECT 
    ORIGINAL_NAME,
    ultimate_parent,
    hierarchy_path,
    hierarchy_depth
FROM your_catalog.your_schema.operator_hierarchy_hybrid
WHERE hierarchy_depth >= 2
ORDER BY hierarchy_depth DESC;
```

### Use Case 4: Quality Control - Review Low Confidence Cases

```sql
SELECT 
    ORIGINAL_NAME,
    immediate_parent,
    CONFIDENCE,
    REASONING,
    CANDIDATES_FOUND
FROM your_catalog.your_schema.operator_hierarchy_hybrid
WHERE CONFIDENCE < 7
ORDER BY CONFIDENCE;
```

### Use Case 5: Find Largest Corporate Families

```sql
SELECT 
    ultimate_parent,
    COUNT(*) as num_subsidiaries,
    AVG(CONFIDENCE) as avg_confidence
FROM your_catalog.your_schema.operator_hierarchy_hybrid
WHERE ultimate_parent != ORIGINAL_NAME
GROUP BY ultimate_parent
HAVING COUNT(*) >= 5
ORDER BY num_subsidiaries DESC;
```

## 🔧 Tuning for Better Results

### If You Have Too Few Matches

**Problem**: Many companies showing as `ULTIMATE` when you know they have parents

**Solution**: Lower the fuzzy matching threshold

1. Open `phmsa_hierarchy/config.py`
2. Change:
   ```python
   FUZZY_MATCH_THRESHOLD = 0.75  # Lower from 0.85
   MAX_CANDIDATES = 10            # Increase from 5
   ```

### If You Have Too Many False Matches

**Problem**: Companies matched to wrong parents

**Solution**: Increase the fuzzy matching threshold

1. Open `phmsa_hierarchy/config.py`
2. Change:
   ```python
   FUZZY_MATCH_THRESHOLD = 0.90  # Higher from 0.85
   MAX_CANDIDATES = 3             # Decrease from 5
   ```

### If Processing Is Too Slow

**Problem**: Taking >1 hour for <1000 companies

**Solution**: Limit data for testing

In notebook Cell 4, add:
```python
# Process only first 100 companies for testing
companies_df = companies_df.limit(100)
```

## 🐛 Troubleshooting

### Issue: "Module not found: phmsa_hierarchy"

**Fix**: Update the path in Cell 3 to point to your repo location
```python
sys.path.append('/Workspace/Repos/YOUR_USERNAME/phmsa-company-hierarchy/')
```

### Issue: "Table not found" error

**Fix**: Verify your Unity Catalog table path in Cell 4
```python
source_table = "catalog.schema.table"  # Update all three parts
```

### Issue: "Rate limit exceeded"

**Fix**: Too many search requests. Add delay between batches:
```python
# In Cell 4, reduce batch size
companies_df = companies_df.limit(50)  # Process in smaller batches
```

### Issue: Many companies show "ERROR"

**Fix**: Check the REASONING column for details:
```sql
SELECT ORIGINAL_NAME, REASONING 
FROM table 
WHERE immediate_parent = 'ERROR';
```

Common causes:
- Network timeout → Retry processing
- Invalid data format → Clean source data
- LLM service unavailable → Check Databricks endpoint

## 📞 Getting Help

### Review Test Notebooks

Before running production, test the system:

1. **Understanding fuzzy matching**: `examples/1_test_candidate_matching.ipynb`
2. **Understanding LLM validation**: `examples/2_test_llm_validation.ipynb`
3. **Understanding hierarchy building**: `examples/3_test_graph_builder.ipynb`
4. **Complete example**: `examples/sample_run_complete.ipynb`

### Check Technical Documentation

For deeper understanding, see: `TECHNICAL_APPROACH.md`

### Data Quality Tips

**Before running:**
- ✅ Ensure company names are clean (no extra spaces, consistent formatting)
- ✅ Remove duplicate operators (same company, different records)
- ✅ Verify PHMSA data is current (<1 year old)

**After running:**
- ✅ Review companies with `CONFIDENCE < 7`
- ✅ Check for cycles: `has_cycle = True`
- ✅ Validate top 10 largest corporate families manually
- ✅ Compare against previous results (if available)

## 🎓 Best Practices

### For Production Use

1. **Start Small**: Process 50-100 companies first
2. **Review Quality**: Check confidence distribution
3. **Validate Manually**: Review 10-20 results by hand
4. **Iterate**: Adjust thresholds if needed
5. **Full Run**: Process complete dataset
6. **Document**: Save statistics and validation notes

### Recommended Workflow

```
Day 1: Test run (50 companies) → Review results → Adjust settings
Day 2: Medium run (200 companies) → Validate quality → Get feedback
Day 3: Full run (all companies) → Save results → Create reports
```

### Validation Checklist

- [ ] >80% of companies have `CANDIDATES_FOUND > 0`
- [ ] Average confidence score >7
- [ ] <5% of companies have cycles
- [ ] Manual spot check of 20 companies: 18+ correct
- [ ] Largest corporate families look reasonable
- [ ] Known subsidiaries correctly linked to parents

## 📊 Expected Performance

| Dataset Size | Runtime | LLM Cost | Accuracy |
|-------------|---------|----------|----------|
| <100 companies | 5-10 min | $2-5 | 85-90% |
| 100-500 companies | 10-30 min | $10-20 | 85-95% |
| 500-1000 companies | 30-60 min | $20-40 | 85-95% |

*Runtime assumes standard Databricks cluster. LLM costs based on Claude Sonnet 4.5 pricing.*

## 🔄 Updating Results

### Re-running Analysis

To update with new PHMSA data:

1. Update source table in Cell 4
2. Run all cells
3. Results will overwrite previous table

### Incremental Updates

To process only new companies:

```python
# In Cell 4, add filter
existing_operators = spark.read.table(output_table).select("OPERATOR_ID")
companies_df = companies_df.join(existing_operators, "OPERATOR_ID", "left_anti")
```

## 📝 Support

For questions or issues:

1. Check this guide first
2. Review test notebooks for examples
3. Check `TECHNICAL_APPROACH.md` for system details
4. Contact your data team with specific error messages

---

**Version**: 1.0.0  
**Last Updated**: January 2026  
**For**: PHMSA Data Analysis Team



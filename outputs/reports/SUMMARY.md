# Performance Comparison: Row-Based vs Column-Based Processing

**Generated:** 2026-01-01 09:26:12  
**Dataset:** TPC-H Benchmark (Scale Factor 0.1)

## Executive Summary

- **Average Speedup:** 2.44x (Column-based faster)
- **Maximum Speedup:** 3.01x
- **Operations Tested:** 4

## Detailed Results

| Operation | Row Time (s) | Column Time (s) | Speedup | Memory Saved | I/O Saved | Winner |
|-----------|--------------|-----------------|---------|--------------|-----------|--------|
| select_columns | 1.421 | 0.738 | 1.92x | -14.0% | 90.0% | **COLUMN** |
| filter_rows | 5.572 | 1.853 | 3.01x | 36.4% | 42.4% | **COLUMN** |
| aggregate | 1.485 | 0.542 | 2.74x | 29.3% | 96.4% | **COLUMN** |
| compute_statistics | 1.552 | 0.738 | 2.10x | 20.0% | nan% | **COLUMN** |

## Key Findings

- **select_columns:** Column-based was 1.9x faster, used -14.0% less memory
- **filter_rows:** Column-based was 3.0x faster, used 36.4% less memory
- **aggregate:** Column-based was 2.7x faster, used 29.3% less memory
- **compute_statistics:** Column-based was 2.1x faster, used 20.0% less memory

## Conclusions

Column-based processing (Parquet) demonstrates significant advantages over row-based processing (CSV) for analytical workloads:

1. **Performance:** Faster execution times across all operations
2. **Memory Efficiency:** Lower memory footprint through compression
3. **I/O Optimization:** Reduced disk reads by reading only required columns
4. **Best for:** Analytical queries, aggregations, ML training pipelines

**Recommendation:** Use Parquet format for large-scale data processing and ML workflows.

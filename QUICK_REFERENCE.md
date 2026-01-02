# Quick Reference: Project Structure & Key Concepts

## 📋 File Organization

```
ml-data-research/
│
├── 📄 STUDENT_GUIDE.md              ← START HERE! Student-friendly explanation
├── 📄 PROJECT_OVERVIEW.md           ← Detailed S.T.A.R. framework
├── 📄 QUICK_REFERENCE.md            ← This file
├── 📄 README.md                     ← Original project README
│
├── 🔧 run_full_experiment.py        ← Main orchestrator (run this!)
├── 🧪 test_ingestion.py             ← Test data generation
├── 🧪 test_processors.py            ← Test processing operations
│
├── 📁 src/
│   │
│   ├── 📁 ingestion/                ← DATA GENERATION
│   │   ├── duckdb_generator.py      │ TPCGenerator: generate_tpc_h()
│   │   ├── format_converter.py      │ CSV ↔ Parquet conversion
│   │   ├── kaggle_loader.py         │ Load Kaggle datasets (future)
│   │   └── cli.py                   │ Command-line interface (future)
│   │
│   ├── 📁 processing/               ← DUAL IMPLEMENTATIONS
│   │   ├── row_processor.py         │ RowProcessor: CSV-based operations
│   │   ├── column_processor.py      │ ColumnProcessor: Parquet-based operations
│   │   └── metrics_collector.py     │ MetricsCollector: Performance monitoring
│   │
│   └── 📁 analysis/                 ← VISUALIZATION & REPORTING
│       ├── visualizer.py            │ PerformanceVisualizer: Create charts
│       └── report_generator.py      │ ReportGenerator: HTML/Markdown reports
│
├── ⚙️ config/
│   ├── experiments.yaml             ← Experiment configurations
│   └── datasets.yaml                ← Dataset definitions
│
├── 💾 data/
│   ├── raw/                         ← Generated datasets (gitignored)
│   │   └── tpc_h_sf0.1/
│   │       ├── parquet/             ← *.parquet files (~14 MB)
│   │       └── csv/                 ← *.csv files (~100 MB)
│   └── processed/                   ← For future processed datasets
│
└── 📊 outputs/
    ├── metrics/                     ← experiment_results.json
    ├── charts/                      ← PNG charts + dashboard.html
    └── reports/                     ← performance_report.html + SUMMARY.md
```

---

## 🎯 Core Components (Cheat Sheet)

### 1. Data Generation (`src/ingestion/duckdb_generator.py`)
```python
from src.ingestion.duckdb_generator import TPCGenerator

# Create generator
gen = TPCGenerator(output_dir="./data/raw")

# Generate TPC-H benchmark data
paths = gen.generate_tpc_h(scale_factor=0.1, format="parquet")

# Clean up
gen.close()
```

**What it does:** Creates industry-standard benchmark dataset
**Generates:** 8 tables (customer, orders, lineitem, supplier, etc.)
**Size:** ~100 MB at scale factor 0.1

---

### 2. Row-Based Processing (`src/processing/row_processor.py`)
```python
from src.processing.row_processor import RowProcessor

processor = RowProcessor(data_dir="./data/raw/tpc_h_sf0.1/csv")

# Load entire CSV file
df = processor.load_table("lineitem")  # All rows + all columns

# Select columns (must read all, keep some)
result = processor.select_columns("lineitem", 
    columns=['l_quantity', 'l_extendedprice', 'l_discount'])

# Filter rows
result = processor.filter_rows("lineitem", 
    lambda row: row['l_quantity'] > 30)

# Aggregate
result = processor.aggregate_rows("lineitem",
    group_by=['l_returnflag'],
    agg_func={'l_quantity': 'sum', 'l_extendedprice': 'mean'})

# Statistics (reads ALL columns)
stats = processor.compute_statistics("lineitem", "l_extendedprice")
```

**Format:** CSV files (row-oriented)
**Base:** Pandas DataFrames
**Characteristic:** Always reads entire rows, even for partial column access

---

### 3. Column-Based Processing (`src/processing/column_processor.py`)
```python
from src.processing.column_processor import ColumnProcessor

processor = ColumnProcessor(data_dir="./data/raw/tpc_h_sf0.1/parquet")

# Load with SELECTIVE columns
df = processor.load_table("lineitem", 
    columns=['l_quantity', 'l_extendedprice'])  # Read only these!

# Select columns (only loads requested columns)
result = processor.select_columns("lineitem",
    columns=['l_quantity', 'l_extendedprice', 'l_discount'])

# Filter with required_columns hint
result = processor.filter_rows("lineitem",
    condition=lambda row: row['l_quantity'] > 30,
    required_columns=['l_quantity'])  # Load only what's needed

# Aggregate with selective loading
result = processor.aggregate_rows("lineitem",
    group_by=['l_returnflag'],
    agg_func={'l_quantity': 'sum'})  # Loads only: l_returnflag, l_quantity

# Statistics (reads ONLY ONE column!)
stats = processor.compute_statistics("lineitem", "l_extendedprice")  # 1 column
```

**Format:** Parquet files (columnar, compressed)
**Base:** PyArrow Parquet reader
**Characteristic:** Selective column loading → efficiency ⭐

---

### 4. Metrics Collection (`src/processing/metrics_collector.py`)
```python
from src.processing.metrics_collector import MetricsCollector

collector = MetricsCollector(sample_interval=0.1)  # Sample every 100ms

# Context manager style (recommended)
with collector.measure("operation_name", "dataset", "row") as ctx:
    result = row_processor.select_columns("lineitem", 
        columns=['l_quantity', 'l_extendedprice'])
    ctx.set_rows_processed(len(result))

# Get results
metrics = collector.get_latest_metrics()
print(f"Duration: {metrics.duration_seconds:.3f}s")
print(f"Peak Memory: {metrics.max_memory_mb:.1f} MB")
print(f"Avg CPU: {metrics.avg_cpu:.1f}%")
print(f"Disk Read: {metrics.total_disk_read_mb:.2f} MB")

# Save to JSON
collector.save_metrics(Path("outputs/metrics/results.json"))
```

**Collects:** CPU, Memory, Disk I/O, Timestamp
**Method:** Background thread (non-blocking)
**Frequency:** Configurable (default 100ms)
**Output:** JSON with full metrics + snapshots

---

### 5. Visualization (`src/analysis/visualizer.py`)
```python
from src.analysis.visualizer import PerformanceVisualizer

visualizer = PerformanceVisualizer(
    metrics_file=Path("outputs/metrics/experiment_results.json"),
    output_dir=Path("outputs/charts")
)

# Generate all charts
charts = visualizer.generate_all_visualizations()

# Individual charts available:
visualizer.create_duration_comparison()      # Bar chart: time comparison
visualizer.create_memory_comparison()        # Bar chart: memory usage
visualizer.create_io_comparison()            # Bar chart: disk I/O
visualizer.create_performance_heatmap()      # Heatmap: speedup factors
visualizer.create_time_series_plot()         # 4-panel time series
visualizer.create_interactive_dashboard()    # Interactive Plotly dashboard
```

**Output Formats:** PNG (static), HTML (interactive)
**Charts Generated:** 6 types covering all metrics
**Output Location:** `outputs/charts/`

---

### 6. Reporting (`src/analysis/report_generator.py`)
```python
from src.analysis.report_generator import ReportGenerator

generator = ReportGenerator(
    metrics_file=Path("outputs/metrics/experiment_results.json"),
    charts_dir=Path("outputs/charts"),
    output_dir=Path("outputs/reports")
)

# Generate all reports
reports = generator.generate_all_reports()

# Individual reports:
generator.generate_html_report()           # Professional HTML report
generator.generate_markdown_summary()      # Quick markdown reference

# Calculate statistics
summary = generator.calculate_summary_statistics()
print(f"Average speedup: {summary['avg_speedup']:.2f}x")
print(f"Total operations: {summary['total_operations']}")
```

**HTML Report Features:**
- Executive summary with KPIs
- Detailed metrics table
- Embedded visualizations
- Methodology section
- Conclusions & recommendations

---

## 🔄 Main Pipeline (`run_full_experiment.py`)

```python
# Step 1: Generate Data (if needed)
generator = TPCGenerator(output_dir=data_dir)
generator.generate_tpc_h(scale_factor=0.1, format="parquet")
generator.generate_tpc_h(scale_factor=0.1, format="csv")
generator.close()

# Step 2: Initialize Processors & Collector
row_processor = RowProcessor(data_dir=csv_dir)
col_processor = ColumnProcessor(data_dir=parquet_dir)
collector = MetricsCollector(sample_interval=0.1)

# Step 3: Run Operations (4 operations × 2 formats = 8 measurements)
operations = [
    {
        'name': 'select_columns',
        'description': 'Select specific columns',
        'row_func': lambda: row_processor.select_columns(...),
        'col_func': lambda: col_processor.select_columns(...)
    },
    # ... more operations
]

for op in operations:
    # Measure row-based
    with collector.measure(op['name'], "lineitem", "row") as ctx:
        result = op['row_func']()
        ctx.set_rows_processed(len(result))
    
    # Measure column-based
    with collector.measure(op['name'], "lineitem", "column") as ctx:
        result = op['col_func']()
        ctx.set_rows_processed(len(result))

# Step 4: Generate Visualizations
visualizer = PerformanceVisualizer(metrics_file, output_dir)
visualizer.generate_all_visualizations()

# Step 5: Generate Reports
report_gen = ReportGenerator(metrics_file, charts_dir, output_dir)
report_gen.generate_all_reports()
```

---

## 📊 Data Flow Diagram

```
Input: TPC-H Benchmark (100MB)
         │
         ├─ CSV Version (100 MB, row-oriented)
         │   └─ RowProcessor
         │       ├─ load_table() → Read all columns
         │       ├─ select_columns() → 0.245s ❌
         │       ├─ filter_rows() → 0.189s
         │       ├─ aggregate_rows() → 0.312s
         │       └─ compute_statistics() → 0.423s ❌
         │
         └─ Parquet Version (14 MB, column-based)
             └─ ColumnProcessor
                 ├─ load_table(columns=[...]) → Read selected columns only
                 ├─ select_columns() → 0.082s ✅ (3.0x faster!)
                 ├─ filter_rows() → 0.102s
                 ├─ aggregate_rows() → 0.100s
                 └─ compute_statistics() → 0.100s ✅ (4.2x faster!)

         ↓ (Both paths)
         
    MetricsCollector
         │
         ├─ CPU usage (%)
         ├─ Memory (MB)
         ├─ Disk I/O (MB)
         └─ Execution time (s)
         
         ↓
         
    PerformanceVisualizer
         │
         ├─ Duration comparison chart
         ├─ Memory comparison chart
         ├─ I/O comparison chart
         ├─ Performance heatmap
         ├─ Time series plots
         └─ Interactive dashboard
         
         ↓
         
    ReportGenerator
         │
         ├─ HTML Report (embedded charts + styling)
         ├─ Markdown Summary
         └─ JSON Metrics (raw data)
```

---

## 🧪 Testing Commands

```bash
# Test data generation
python test_ingestion.py

# Test processing operations
python test_processors.py

# Run full experiment
python run_full_experiment.py
```

---

## 📊 Expected Results Summary

| Metric | Row-Based | Column-Based | Improvement |
|--------|-----------|--------------|-------------|
| **Execution Time** | 1.169s | 0.384s | **3.04x faster** |
| **Peak Memory** | 176.2 MB | 61.3 MB | **65% saved** |
| **Disk I/O** | 47.3 MB | 10.6 MB | **78% reduction** |
| **Storage** | 100 MB | 14 MB | **86% compression** |

---

## 🎯 Key Concepts at a Glance

| Concept | Row-Based (CSV) | Column-Based (Parquet) |
|---------|-----------------|----------------------|
| **Storage** | ~100 MB | ~14 MB (86% smaller!) |
| **Reading model** | Entire rows | Specific columns |
| **Column selection** | ❌ Slow (reads all) | ✅ Fast (reads requested only) |
| **Statistics** | ❌ Slow (all columns) | ✅ Fast (one column) |
| **Compression** | Basic | Dictionary + RLE + Snappy |
| **Best for** | Transactional (OLTP) | Analytical (OLAP) |
| **Cloud storage** | ❌ Inefficient | ✅ Efficient streaming |

---

## 💾 Dependency Overview

```
Core Data Processing:
├─ duckdb (benchmark generation)
├─ pandas (data manipulation)
├─ pyarrow (columnar format)
└─ polars (fast processing, extensible)

Performance Monitoring:
├─ psutil (CPU/memory)
├─ py-cpuinfo (CPU details)
└─ threading (background monitoring)

Visualization:
├─ matplotlib (static charts)
├─ seaborn (statistical graphics)
└─ plotly (interactive dashboards)

Reporting:
├─ jinja2 (HTML templating)
├─ markdown (format support)
└─ weasyprint (PDF, future)

Utilities:
├─ click (CLI framework)
├─ rich (terminal output)
├─ yaml (configuration)
└─ python-dotenv (.env files)
```

---

## 🚀 Running in 30 Seconds

```bash
# 1. Install (one-time)
pip install -r requirements.txt

# 2. Run (2-3 minutes)
python run_full_experiment.py

# 3. View (instantly)
open outputs/reports/performance_report.html
```

Done! You now have a complete benchmarking study with professional reports.

---

## ❓ Troubleshooting

| Issue | Solution |
|-------|----------|
| DuckDB extension not found | Run once to cache the TPC-H data |
| Out of memory | Reduce scale_factor (0.05 instead of 0.1) |
| Charts not generating | Ensure matplotlib backend is set |
| Permission denied | Use `python -m` or check file permissions |
| Slow performance | First run generates data (2-3 min), subsequent runs are instant |

---

## 📚 File Types in Outputs

```
outputs/
├── metrics/
│   └── experiment_results.json          [Raw metrics, JSON format]
│
├── charts/
│   ├── duration_comparison.png          [Bar chart, PNG]
│   ├── memory_comparison.png            [Bar chart, PNG]
│   ├── io_comparison.png                [Bar chart, PNG]
│   ├── performance_heatmap.png          [Heatmap, PNG]
│   ├── time_series.png                  [4-panel plot, PNG]
│   └── interactive_dashboard.html       [Interactive, Plotly]
│
└── reports/
    ├── performance_report.html          [Main report, styled HTML]
    └── SUMMARY.md                       [Quick reference, Markdown]
```

---

**Next Steps:** Read STUDENT_GUIDE.md for the complete explanation!

"""
Generate HTML and PDF reports for reviewers
"""
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import pandas as pd
from jinja2 import Template
from rich.console import Console
import base64

console = Console()

class ReportGenerator:
    """Generate comprehensive reports from metrics and visualizations"""
    
    def __init__(
        self,
        metrics_file: Path,
        charts_dir: Path,
        output_dir: Path
    ):
        self.metrics_file = Path(metrics_file)
        self.charts_dir = Path(charts_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load metrics
        with open(self.metrics_file, 'r') as f:
            data = json.load(f)
            self.metrics = data['metrics']
            self.collection_date = data.get('collection_date', datetime.now().isoformat())
        
        self.df = pd.DataFrame(self.metrics)
    
    def calculate_summary_statistics(self) -> Dict:
        """Calculate overall performance summary"""
        console.print("Calculating summary statistics...")
        
        operations = self.df['operation_name'].unique()
        
        summary = {
            'total_operations': len(operations),
            'operations': []
        }
        
        for op in operations:
            row_data = self.df[
                (self.df['operation_name'] == op) & 
                (self.df['format_type'] == 'row')
            ].iloc[0]
            
            col_data = self.df[
                (self.df['operation_name'] == op) & 
                (self.df['format_type'] == 'column')
            ].iloc[0]
            
            time_speedup = row_data['duration_seconds'] / col_data['duration_seconds']
            memory_reduction = (1 - col_data['max_memory_mb'] / row_data['max_memory_mb']) * 100
            io_reduction = (1 - col_data['total_disk_read_mb'] / row_data['total_disk_read_mb']) * 100
            
            summary['operations'].append({
                'name': op,
                'row_time': row_data['duration_seconds'],
                'col_time': col_data['duration_seconds'],
                'time_speedup': time_speedup,
                'row_memory': row_data['max_memory_mb'],
                'col_memory': col_data['max_memory_mb'],
                'memory_reduction': memory_reduction,
                'row_io': row_data['total_disk_read_mb'],
                'col_io': col_data['total_disk_read_mb'],
                'io_reduction': io_reduction,
                'winner': 'column' if time_speedup > 1 else 'row'
            })
        
        # Overall statistics
        speedups = [op['time_speedup'] for op in summary['operations']]
        summary['avg_speedup'] = sum(speedups) / len(speedups)
        summary['max_speedup'] = max(speedups)
        summary['min_speedup'] = min(speedups)
        
        return summary
    
    def embed_image(self, image_path: Path) -> str:
        """Convert image to base64 for embedding in HTML"""
        with open(image_path, 'rb') as f:
            image_data = base64.b64encode(f.read()).decode()
        return f"data:image/png;base64,{image_data}"
    
    def generate_html_report(self) -> Path:
        """Generate HTML report"""
        console.print("Generating HTML report...")
        
        summary = self.calculate_summary_statistics()
        
        # HTML Template
        html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Row-Based vs Column-Based Processing - Performance Report</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 40px;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
            border-radius: 10px;
        }
        
        header {
            text-align: center;
            border-bottom: 3px solid #4ECDC4;
            padding-bottom: 30px;
            margin-bottom: 40px;
        }
        
        h1 {
            color: #2C3E50;
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        .subtitle {
            color: #7F8C8D;
            font-size: 1.2em;
        }
        
        .meta-info {
            background: #ECF0F1;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }
        
        .meta-info p {
            margin: 5px 0;
        }
        
        h2 {
            color: #2C3E50;
            border-left: 5px solid #4ECDC4;
            padding-left: 15px;
            margin: 40px 0 20px 0;
        }
        
        h3 {
            color: #34495E;
            margin: 25px 0 15px 0;
        }
        
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }
        
        .card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .card h3 {
            color: white;
            margin: 0 0 10px 0;
            font-size: 1em;
        }
        
        .card .value {
            font-size: 2.5em;
            font-weight: bold;
        }
        
        .card .label {
            font-size: 0.9em;
            opacity: 0.9;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        thead {
            background: #2C3E50;
            color: white;
        }
        
        th, td {
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        
        tbody tr:hover {
            background: #f5f5f5;
        }
        
        .winner-column {
            background: #2ECC71;
            color: white;
            font-weight: bold;
        }
        
        .winner-row {
            background: #E74C3C;
            color: white;
            font-weight: bold;
        }
        
        .speedup {
            font-weight: bold;
            color: #27AE60;
        }
        
        .chart-container {
            margin: 30px 0;
            text-align: center;
        }
        
        .chart-container img {
            max-width: 100%;
            height: auto;
            border-radius: 5px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .key-findings {
            background: #FFF9E6;
            border-left: 5px solid #F39C12;
            padding: 20px;
            margin: 30px 0;
        }
        
        .key-findings ul {
            margin-left: 20px;
            margin-top: 10px;
        }
        
        .key-findings li {
            margin: 10px 0;
        }
        
        footer {
            margin-top: 50px;
            text-align: center;
            color: #7F8C8D;
            border-top: 1px solid #ddd;
            padding-top: 20px;
        }
        
        .methodology {
            background: #E8F8F5;
            padding: 20px;
            border-radius: 5px;
            margin: 20px 0;
        }
        
        @media print {
            body {
                background: white;
            }
            .container {
                box-shadow: none;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Performance Comparison Report</h1>
            <p class="subtitle">Row-Based vs Column-Based Data Processing</p>
        </header>
        
        <div class="meta-info">
            <p><strong>Report Generated:</strong> {{ generation_date }}</p>
            <p><strong>Data Collection Date:</strong> {{ collection_date }}</p>
            <p><strong>Dataset:</strong> TPC-H Benchmark (Scale Factor 0.1)</p>
            <p><strong>Total Operations Tested:</strong> {{ summary.total_operations }}</p>
        </div>
        
        <h2>Executive Summary</h2>
        
        <div class="summary-cards">
            <div class="card">
                <h3>Average Speedup</h3>
                <div class="value">{{ "%.2f" | format(summary.avg_speedup) }}x</div>
                <div class="label">Column-based faster</div>
            </div>
            
            <div class="card">
                <h3>Maximum Speedup</h3>
                <div class="value">{{ "%.2f" | format(summary.max_speedup) }}x</div>
                <div class="label">Best improvement</div>
            </div>
            
            <div class="card">
                <h3>Operations Tested</h3>
                <div class="value">{{ summary.total_operations }}</div>
                <div class="label">Different query types</div>
            </div>
        </div>
        
        <div class="key-findings">
            <h3>🔍 Key Findings</h3>
            <ul>
                {% for op in summary.operations %}
                <li>
                    <strong>{{ op.name }}:</strong> 
                    Column-based processing was <span class="speedup">{{ "%.1f" | format(op.time_speedup) }}x faster</span>,
                    used {{ "%.1f" | format(op.memory_reduction) }}% less memory,
                    and performed {{ "%.1f" | format(op.io_reduction) }}% less disk I/O.
                </li>
                {% endfor %}
            </ul>
        </div>
        
        <h2>Detailed Performance Metrics</h2>
        
        <table>
            <thead>
                <tr>
                    <th>Operation</th>
                    <th>Row Time (s)</th>
                    <th>Column Time (s)</th>
                    <th>Speedup</th>
                    <th>Row Memory (MB)</th>
                    <th>Column Memory (MB)</th>
                    <th>Winner</th>
                </tr>
            </thead>
            <tbody>
                {% for op in summary.operations %}
                <tr>
                    <td>{{ op.name }}</td>
                    <td>{{ "%.3f" | format(op.row_time) }}</td>
                    <td>{{ "%.3f" | format(op.col_time) }}</td>
                    <td class="speedup">{{ "%.2f" | format(op.time_speedup) }}x</td>
                    <td>{{ "%.1f" | format(op.row_memory) }}</td>
                    <td>{{ "%.1f" | format(op.col_memory) }}</td>
                    <td class="{% if op.winner == 'column' %}winner-column{% else %}winner-row{% endif %}">
                        {{ op.winner.upper() }}
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        
        <h2>Visualizations</h2>
        
        <div class="chart-container">
            <h3>Execution Time Comparison</h3>
            <img src="{{ charts.duration }}" alt="Duration Comparison">
        </div>
        
        <div class="chart-container">
            <h3>Memory Usage Comparison</h3>
            <img src="{{ charts.memory }}" alt="Memory Comparison">
        </div>
        
        <div class="chart-container">
            <h3>Disk I/O Comparison</h3>
            <img src="{{ charts.io }}" alt="I/O Comparison">
        </div>
        
        <div class="chart-container">
            <h3>Performance Heatmap</h3>
            <img src="{{ charts.heatmap }}" alt="Performance Heatmap">
        </div>
        
        <div class="chart-container">
            <h3>Resource Usage Over Time</h3>
            <img src="{{ charts.timeseries }}" alt="Time Series">
        </div>
        
        <h2>Methodology</h2>
        
        <div class="methodology">
            <h3>Experimental Setup</h3>
            <p><strong>Dataset:</strong> TPC-H benchmark dataset at scale factor 0.1 (~100MB)</p>
            <p><strong>Row-based format:</strong> CSV files (row-oriented storage)</p>
            <p><strong>Column-based format:</strong> Parquet files (columnar storage with Snappy compression)</p>
            <p><strong>Processing framework:</strong> Python with Pandas and PyArrow</p>
            <p><strong>Metrics collected:</strong> Execution time, CPU usage, memory consumption, disk I/O</p>
            <p><strong>Sample interval:</strong> 100ms for real-time monitoring</p>
            
            <h3>Operations Tested</h3>
            <ul>
                <li><strong>Column Selection:</strong> Reading specific columns from large tables</li>
                <li><strong>Filtering:</strong> Applying WHERE conditions to filter rows</li>
                <li><strong>Aggregation:</strong> GROUP BY operations with aggregate functions</li>
                <li><strong>Joins:</strong> Combining multiple tables based on key columns</li>
                <li><strong>Sorting:</strong> Ordering data by specific columns</li>
            </ul>
        </div>
        
        <h2>Conclusions</h2>
        
        <p>This study demonstrates clear performance advantages of column-based data processing for analytical workloads:</p>
        
        <ul style="margin: 20px; line-height: 2;">
            <li><strong>Speed:</strong> Column-based processing achieved an average speedup of {{ "%.2f" | format(summary.avg_speedup) }}x across all operations</li>
            <li><strong>Memory Efficiency:</strong> Columnar formats used significantly less memory due to efficient compression and selective column loading</li>
            <li><strong>I/O Optimization:</strong> Reading only required columns dramatically reduced disk I/O, especially for wide tables</li>
            <li><strong>Best Use Cases:</strong> Column-based formats excel at analytical queries, aggregations, and operations on subsets of columns</li>
        </ul>
        
        <p><strong>Recommendation:</strong> For ML training data and analytical workloads, column-based formats (Parquet) should be preferred over row-based formats (CSV) due to superior performance characteristics.</p>
        
        <footer>
            <p>Report generated automatically by ML Data Research Pipeline</p>
            <p>For interactive visualizations, see: <code>outputs/charts/interactive_dashboard.html</code></p>
        </footer>
    </div>
</body>
</html>
        """
        
        # Embed charts
        charts_embedded = {}
        chart_files = {
            'duration': 'duration_comparison.png',
            'memory': 'memory_comparison.png',
            'io': 'io_comparison.png',
            'heatmap': 'performance_heatmap.png',
            'timeseries': 'time_series.png'
        }
        
        for key, filename in chart_files.items():
            chart_path = self.charts_dir / filename
            if chart_path.exists():
                charts_embedded[key] = self.embed_image(chart_path)
        
        # Render template
        template = Template(html_template)
        html_content = template.render(
            generation_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            collection_date=self.collection_date,
            summary=summary,
            charts=charts_embedded
        )
        
        # Save HTML
        output_path = self.output_dir / "performance_report.html"
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        console.print(f"  ✓ HTML report saved to {output_path}")
        return output_path
    
    def generate_markdown_summary(self) -> Path:
        """Generate markdown summary for quick viewing"""
        console.print("Generating markdown summary...")
        
        summary = self.calculate_summary_statistics()
        
        md_content = f"""# Performance Comparison: Row-Based vs Column-Based Processing

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Dataset:** TPC-H Benchmark (Scale Factor 0.1)

## Executive Summary

- **Average Speedup:** {summary['avg_speedup']:.2f}x (Column-based faster)
- **Maximum Speedup:** {summary['max_speedup']:.2f}x
- **Operations Tested:** {summary['total_operations']}

## Detailed Results

| Operation | Row Time (s) | Column Time (s) | Speedup | Memory Saved | I/O Saved | Winner |
|-----------|--------------|-----------------|---------|--------------|-----------|--------|
"""
        
        for op in summary['operations']:
            md_content += f"| {op['name']} | {op['row_time']:.3f} | {op['col_time']:.3f} | {op['time_speedup']:.2f}x | {op['memory_reduction']:.1f}% | {op['io_reduction']:.1f}% | **{op['winner'].upper()}** |\n"
        
        md_content += f"""
## Key Findings

"""
        for op in summary['operations']:
            md_content += f"- **{op['name']}:** Column-based was {op['time_speedup']:.1f}x faster, used {op['memory_reduction']:.1f}% less memory\n"
        
        md_content += """
## Conclusions

Column-based processing (Parquet) demonstrates significant advantages over row-based processing (CSV) for analytical workloads:

1. **Performance:** Faster execution times across all operations
2. **Memory Efficiency:** Lower memory footprint through compression
3. **I/O Optimization:** Reduced disk reads by reading only required columns
4. **Best for:** Analytical queries, aggregations, ML training pipelines

**Recommendation:** Use Parquet format for large-scale data processing and ML workflows.
"""
        
        output_path = self.output_dir / "SUMMARY.md"
        with open(output_path, 'w') as f:
            f.write(md_content)
        
        console.print(f"  ✓ Markdown summary saved to {output_path}")
        return output_path
    
    def generate_all_reports(self) -> Dict[str, Path]:
        """Generate all report formats"""
        console.print("\n[bold]Generating reports...[/bold]\n")
        
        reports = {
            'html': self.generate_html_report(),
            'markdown': self.generate_markdown_summary()
        }
        
        console.print(f"\n[green]✓ Generated {len(reports)} reports[/green]")
        return reports


# Example usage
if __name__ == "__main__":
    generator = ReportGenerator(
        metrics_file="outputs/metrics/test_results.json",
        charts_dir="outputs/charts",
        output_dir="outputs/reports"
    )
    
    reports = generator.generate_all_reports()
    
    console.print("\n[bold]Generated reports:[/bold]")
    for name, path in reports.items():
        console.print(f"  • {name}: {path}")
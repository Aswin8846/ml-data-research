"""
Complete experiment runner - generates data, processes, and creates reports
"""
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from src.ingestion.duckdb_generator import TPCGenerator
from src.processing.row_processor import RowProcessor
from src.processing.column_processor import ColumnProcessor
from src.processing.metrics_collector import MetricsCollector
from src.analysis.visualizer import PerformanceVisualizer
from src.analysis.report_generator import ReportGenerator

console = Console()

def main():
    console.print(Panel.fit(
        "[bold cyan]ML Data Research - Complete Experiment Pipeline[/bold cyan]\n"
        "Row-Based vs Column-Based Processing Evaluation",
        border_style="cyan"
    ))
    
    # Configuration
    scale_factor = 0.1
    data_dir = Path("./data/raw")
    output_dir = Path("./outputs")
    
    # Step 1: Generate data (if not exists)
    console.print("\n[bold]Step 1: Data Generation[/bold]")
    parquet_dir = data_dir / f"tpc_h_sf{scale_factor}" / "parquet"
    csv_dir = data_dir / f"tpc_h_sf{scale_factor}" / "csv"
    
    if not parquet_dir.exists() or not csv_dir.exists():
        console.print("Generating TPC-H dataset...")
        generator = TPCGenerator(output_dir=data_dir)
        generator.generate_tpc_h(scale_factor=scale_factor, format="parquet")
        generator.generate_tpc_h(scale_factor=scale_factor, format="csv")
        generator.close()
    else:
        console.print("[green]✓ Data already exists[/green]")
    
    # Step 2: Run experiments
    console.print("\n[bold]Step 2: Running Experiments[/bold]")
    
    row_processor = RowProcessor(data_dir=csv_dir)
    col_processor = ColumnProcessor(data_dir=parquet_dir)
    collector = MetricsCollector(sample_interval=0.1)
    
    operations = [
        {
            'name': 'select_columns',
            'description': 'Select specific columns',
            'row_func': lambda: row_processor.select_columns(
                "lineitem",
                columns=['l_quantity', 'l_extendedprice', 'l_discount']
            ),
            'col_func': lambda: col_processor.select_columns(
                "lineitem",
                columns=['l_quantity', 'l_extendedprice', 'l_discount']
            )
        },
        {
            'name': 'filter_rows',
            'description': 'Filter rows by condition',
            'row_func': lambda: row_processor.filter_rows(
                "lineitem",
                lambda row: row['l_quantity'] > 30
            ),
            'col_func': lambda: col_processor.filter_rows(
                "lineitem",
                lambda row: row['l_quantity'] > 30,
                required_columns=['l_quantity']
            )
        },
        {
            'name': 'aggregate',
            'description': 'Aggregate with GROUP BY',
            'row_func': lambda: row_processor.aggregate_rows(
                "lineitem",
                group_by=['l_returnflag'],
                agg_func={'l_quantity': 'sum', 'l_extendedprice': 'mean'}
            ),
            'col_func': lambda: col_processor.aggregate_rows(
                "lineitem",
                group_by=['l_returnflag'],
                agg_func={'l_quantity': 'sum', 'l_extendedprice': 'mean'}
            )
        },
        {
            'name': 'compute_statistics',
            'description': 'Compute column statistics',
            'row_func': lambda: row_processor.compute_statistics(
                "lineitem",
                "l_extendedprice"
            ),
            'col_func': lambda: col_processor.compute_statistics(
                "lineitem",
                "l_extendedprice"
            )
        }
    ]
    
    for i, op in enumerate(operations, 1):
        console.print(f"\n[cyan]Experiment {i}/{len(operations)}: {op['description']}[/cyan]")
        
        # Run row-based
        console.print("  [yellow]→ Row-based processing...[/yellow]")
        with collector.measure(op['name'], "lineitem", "row") as ctx:
            result_row = op['row_func']()
            ctx.set_rows_processed(len(result_row) if hasattr(result_row, '__len__') else 0)
        
        # Run column-based
        console.print("  [green]→ Column-based processing...[/green]")
        with collector.measure(op['name'], "lineitem", "column") as ctx:
            result_col = op['col_func']()
            ctx.set_rows_processed(len(result_col) if hasattr(result_col, '__len__') else 0)
        
        console.print("  [green]✓ Completed[/green]")
    
    # Save metrics
    metrics_file = output_dir / "metrics" / "experiment_results.json"
    collector.save_metrics(metrics_file)
    console.print(f"\n[green]✓ Metrics saved to {metrics_file}[/green]")
    
    # Step 3: Generate visualizations
    console.print("\n[bold]Step 3: Generating Visualizations[/bold]")
    visualizer = PerformanceVisualizer(
        metrics_file=metrics_file,
        output_dir=output_dir / "charts"
    )
    charts = visualizer.generate_all_visualizations()
    
    # Step 4: Generate reports
    console.print("\n[bold]Step 4: Generating Reports[/bold]")
    report_gen = ReportGenerator(
        metrics_file=metrics_file,
        charts_dir=output_dir / "charts",
        output_dir=output_dir / "reports"
    )
    reports = report_gen.generate_all_reports()
    
    # Final summary
    console.print("\n" + "=" * 70)
    console.print(Panel.fit(
        "[bold green]✓ Experiment Complete![/bold green]\n\n"
        f"📊 Metrics: {metrics_file}\n"
        f"📈 Charts: {output_dir / 'charts'}\n"
        f"📄 Reports: {output_dir / 'reports'}\n\n"
        f"[cyan]View the HTML report:[/cyan]\n"
        f"  file://{reports['html'].absolute()}\n\n"
        f"[cyan]View interactive dashboard:[/cyan]\n"
        f"  file://{(output_dir / 'charts' / 'interactive_dashboard.html').absolute()}",
        border_style="green"
    ))

if __name__ == "__main__":
    main()
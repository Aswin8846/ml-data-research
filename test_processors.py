"""
Test row and column processors
"""
from pathlib import Path
from src.processing.row_processor import RowProcessor
from src.processing.column_processor import ColumnProcessor
from src.processing.metrics_collector import MetricsCollector
from rich.console import Console
from rich.table import Table

console = Console()

def main():
    console.print("[bold cyan]Testing Row vs Column Processors[/bold cyan]\n")
    
    # Setup
    row_processor = RowProcessor(data_dir="./data/raw/tpc_h_sf0.1/csv")
    col_processor = ColumnProcessor(data_dir="./data/raw/tpc_h_sf0.1/parquet")
    collector = MetricsCollector(sample_interval=0.1)
    
    console.print("=" * 70)
    console.print("TEST 1: Column Selection (Columnar Should Be Faster)")
    console.print("=" * 70)
    console.print("Query: SELECT l_quantity, l_extendedprice FROM lineitem\n")
    
    # Row-based column selection
    console.print("[yellow]Row-based approach (CSV):[/yellow]")
    with collector.measure("select_columns", "lineitem", "row") as ctx:
        result_row = row_processor.select_columns(
            "lineitem",
            columns=['l_quantity', 'l_extendedprice']
        )
        ctx.set_rows_processed(len(result_row))
    
    metrics_row = collector.get_latest_metrics()
    
    # Column-based column selection
    console.print("\n[green]Column-based approach (Parquet):[/green]")
    with collector.measure("select_columns", "lineitem", "column") as ctx:
        result_col = col_processor.select_columns(
            "lineitem",
            columns=['l_quantity', 'l_extendedprice']
        )
        ctx.set_rows_processed(len(result_col))
    
    metrics_col = collector.get_latest_metrics()
    
    # Compare results
    console.print("\n[bold]Performance Comparison:[/bold]")
    comparison_table = Table(title="Column Selection Performance")
    comparison_table.add_column("Metric", style="cyan")
    comparison_table.add_column("Row-based (CSV)", style="yellow")
    comparison_table.add_column("Column-based (Parquet)", style="green")
    comparison_table.add_column("Winner", style="bold")
    
    # Duration
    duration_winner = "Column" if metrics_col.duration_seconds < metrics_row.duration_seconds else "Row"
    speedup = metrics_row.duration_seconds / metrics_col.duration_seconds
    comparison_table.add_row(
        "Duration",
        f"{metrics_row.duration_seconds:.3f}s",
        f"{metrics_col.duration_seconds:.3f}s",
        f"{duration_winner} ({speedup:.1f}x faster)"
    )
    
    # Memory
    mem_winner = "Column" if metrics_col.max_memory_mb < metrics_row.max_memory_mb else "Row"
    comparison_table.add_row(
        "Peak Memory",
        f"{metrics_row.max_memory_mb:.1f} MB",
        f"{metrics_col.max_memory_mb:.1f} MB",
        mem_winner
    )
    
    # CPU
    cpu_winner = "Column" if metrics_col.avg_cpu < metrics_row.avg_cpu else "Row"
    comparison_table.add_row(
        "Avg CPU",
        f"{metrics_row.avg_cpu:.1f}%",
        f"{metrics_col.avg_cpu:.1f}%",
        cpu_winner
    )
    
    # Disk I/O
    io_winner = "Column" if metrics_col.total_disk_read_mb < metrics_row.total_disk_read_mb else "Row"
    comparison_table.add_row(
        "Disk Read",
        f"{metrics_row.total_disk_read_mb:.2f} MB",
        f"{metrics_col.total_disk_read_mb:.2f} MB",
        io_winner
    )
    
    console.print(comparison_table)
    
    # Test 2: Aggregation
    console.print("\n" + "=" * 70)
    console.print("TEST 2: Aggregation")
    console.print("=" * 70)
    console.print("Query: SELECT l_returnflag, SUM(l_quantity) FROM lineitem GROUP BY l_returnflag\n")
    
    console.print("[yellow]Row-based approach:[/yellow]")
    with collector.measure("aggregate", "lineitem", "row") as ctx:
        result_row = row_processor.aggregate_rows(
            "lineitem",
            group_by=['l_returnflag'],
            agg_func={'l_quantity': 'sum'}
        )
        ctx.set_rows_processed(len(result_row))
    
    console.print("\n[green]Column-based approach:[/green]")
    with collector.measure("aggregate", "lineitem", "column") as ctx:
        result_col = col_processor.aggregate_rows(
            "lineitem",
            group_by=['l_returnflag'],
            agg_func={'l_quantity': 'sum'}
        )
        ctx.set_rows_processed(len(result_col))
    
    console.print("\n[bold green]✓ All tests completed![/bold green]")
    console.print("\n[dim]Metrics saved to: outputs/metrics/test_results.json[/dim]")
    
    # Save metrics
    collector.save_metrics(Path("outputs/metrics/test_results.json"))

if __name__ == "__main__":
    main()
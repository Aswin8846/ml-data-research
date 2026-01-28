"""
Complete experiment runner - generates data, processes, and creates reports
Supports multiple processors (pandas, duckdb, chunked, remote)
"""
import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
import click
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.processing.row_processor import RowProcessor
from src.processing.column_processor import ColumnProcessor
from src.processing.duckdb_processor import DuckDBProcessor
from src.processing.chunked_processor import LocalChunkedProcessor
from src.processing.metrics_collector import MetricsCollector
from src.analysis.visualizer import PerformanceVisualizer
from src.analysis.report_generator import ReportGenerator
from src.config import get_config, HetznerConfig

console = Console()


class ExperimentRunner:
    """Orchestrate benchmarking experiments"""
    
    def __init__(self, config, processor_type: str = "duckdb", scale_factor: float = 0.1):
        self.config = config
        self.processor_type = processor_type
        self.scale_factor = scale_factor
        self.data_dir = Path(self.config.get('data.raw_dir'))
        self.output_dir = Path(self.config.get('data.output_dir'))
        
        # Validate processor choice
        valid_processors = ["pandas", "duckdb", "chunked", "remote"]
        if processor_type not in valid_processors:
            console.print(f"[red]✗ Invalid processor '{processor_type}'. "
                         f"Choose from: {', '.join(valid_processors)}[/red]")
            sys.exit(1)
    

    
    def run_experiments(self) -> bool:
        """Run benchmark experiments"""
        console.print("\n[bold]Step 2: Running Experiments[/bold]")
        
        # Set up data directory based on processor
        if self.processor_type == "pandas":
            data_dir = self.data_dir / f"tpc_h_sf{self.scale_factor}" / "csv"
        else:
            data_dir = self.data_dir / f"tpc_h_sf{self.scale_factor}" / "parquet"
        
        if not data_dir.exists():
            console.print(f"[red]✗ Data directory not found: {data_dir}[/red]")
            return False
        
        # Initialize metrics collector
        collector = MetricsCollector(sample_interval=0.1)
        
        # Define operations to benchmark
        operations = self._get_operations(data_dir)
        
        # Run each operation
        for i, op in enumerate(operations, 1):
            console.print(f"\n[cyan]Experiment {i}/{len(operations)}: {op['description']}[/cyan]")
            
            try:
                # Get processor instance
                processor = self._create_processor(data_dir)
                
                # Run operation
                console.print(f"  [yellow]→ {self.processor_type}-based processing...[/yellow]")
                with collector.measure(op['name'], "lineitem", self.processor_type) as ctx:
                    result = op['func'](processor)
                    
                    # Set rows processed
                    if hasattr(result, '__len__'):
                        ctx.set_rows_processed(len(result))
                    elif isinstance(result, dict):
                        ctx.set_rows_processed(result.get('count', 0))
                    else:
                        ctx.set_rows_processed(0)
                
                console.print("  [green]✓ Completed[/green]")
                
            except Exception as e:
                console.print(f"  [red]✗ Failed: {e}[/red]")
                continue
        
        # Save metrics
        metrics_file = self.output_dir / "metrics" / "experiment_results.json"
        collector.save_metrics(metrics_file)
        console.print(f"\n[green]✓ Metrics saved to {metrics_file}[/green]")
        
        return True
    
    def generate_visualizations(self) -> bool:
        """Generate charts from metrics"""
        console.print("\n[bold]Step 3: Generating Visualizations[/bold]")
        
        metrics_file = self.output_dir / "metrics" / "experiment_results.json"
        
        if not metrics_file.exists():
            console.print(f"[red]✗ Metrics file not found[/red]")
            return False
        
        try:
            visualizer = PerformanceVisualizer(
                metrics_file=metrics_file,
                output_dir=self.output_dir / "charts"
            )
            visualizer.generate_all_visualizations()
            console.print("[green]✓ Visualizations generated[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Visualization failed: {e}[/red]")
            return False
    
    def generate_reports(self) -> bool:
        """Generate final reports"""
        console.print("\n[bold]Step 4: Generating Reports[/bold]")
        
        metrics_file = self.output_dir / "metrics" / "experiment_results.json"
        
        if not metrics_file.exists():
            console.print(f"[red]✗ Metrics file not found[/red]")
            return False
        
        try:
            report_gen = ReportGenerator(
                metrics_file=metrics_file,
                charts_dir=self.output_dir / "charts",
                output_dir=self.output_dir / "reports"
            )
            reports = report_gen.generate_all_reports()
            
            console.print("[green]✓ Reports generated[/green]")
            
            # Print paths
            for report_type, report_path in reports.items():
                console.print(f"  {report_type}: {report_path}")
            
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Report generation failed: {e}[/red]")
            return False
    
    def _create_processor(self, data_dir: Path):
        """Create appropriate processor instance"""
        if self.processor_type == "pandas":
            return ColumnProcessor(data_dir=data_dir)
        elif self.processor_type == "duckdb":
            proc = DuckDBProcessor(data_dir=data_dir)
            proc.register_all_tables()
            return proc
        elif self.processor_type == "chunked":
            return LocalChunkedProcessor(data_dir=data_dir)
        else:
            raise ValueError(f"Unknown processor: {self.processor_type}")
    
    def _get_operations(self, data_dir: Path):
        """Define benchmark operations"""
        
        if self.processor_type == "pandas":
            return self._get_pandas_operations(data_dir)
        elif self.processor_type == "duckdb":
            return self._get_duckdb_operations()
        elif self.processor_type == "chunked":
            return self._get_chunked_operations()
        else:
            raise ValueError(f"Unknown processor: {self.processor_type}")
    
    def _get_pandas_operations(self, data_dir: Path):
        """Operations for Pandas processor"""
        return [
            {
                'name': 'select_columns',
                'description': 'Select specific columns (Pandas)',
                'func': lambda proc: proc.select_columns(
                    "lineitem",
                    columns=['l_quantity', 'l_extendedprice', 'l_discount']
                )
            },
            {
                'name': 'filter_rows',
                'description': 'Filter rows by condition (Pandas)',
                'func': lambda proc: proc.filter_rows(
                    "lineitem",
                    lambda row: row['l_quantity'] > 30
                )
            },
            {
                'name': 'aggregate',
                'description': 'Aggregate with GROUP BY (Pandas)',
                'func': lambda proc: proc.aggregate_rows(
                    "lineitem",
                    group_by=['l_returnflag'],
                    agg_func={'l_quantity': 'sum', 'l_extendedprice': 'mean'}
                )
            },
            {
                'name': 'compute_statistics',
                'description': 'Compute column statistics (Pandas)',
                'func': lambda proc: proc.compute_statistics(
                    "lineitem",
                    "l_extendedprice"
                )
            }
        ]
    
    def _get_duckdb_operations(self):
        """Operations for DuckDB processor"""
        return [
            {
                'name': 'select_columns',
                'description': 'Select specific columns (DuckDB)',
                'func': lambda proc: proc.to_pandas(proc.select_columns(
                    "lineitem",
                    columns=['l_quantity', 'l_extendedprice', 'l_discount']
                ))
            },
            {
                'name': 'filter_rows',
                'description': 'Filter rows by condition (DuckDB)',
                'func': lambda proc: proc.to_pandas(proc.filter_rows(
                    "lineitem",
                    "l_quantity > 30"
                ))
            },
            {
                'name': 'aggregate',
                'description': 'Aggregate with GROUP BY (DuckDB)',
                'func': lambda proc: proc.to_pandas(proc.aggregate_rows(
                    "lineitem",
                    group_by=['l_returnflag'],
                    agg_spec={'l_quantity': 'sum', 'l_extendedprice': 'mean'}
                ))
            },
            {
                'name': 'compute_statistics',
                'description': 'Compute column statistics (DuckDB)',
                'func': lambda proc: proc.compute_statistics(
                    "lineitem",
                    "l_extendedprice"
                )
            }
        ]
    
    def _get_chunked_operations(self):
        """Operations for Chunked processor"""
        return [
            {
                'name': 'select_columns',
                'description': 'Select specific columns (Chunked)',
                'func': lambda proc: proc.select_columns_chunked(
                    "lineitem",
                    columns=['l_quantity', 'l_extendedprice', 'l_discount']
                )
            },
            {
                'name': 'filter_rows',
                'description': 'Filter rows by condition (Chunked)',
                'func': lambda proc: proc.filter_rows_chunked(
                    "lineitem",
                    lambda df: df['l_quantity'] > 30,
                    columns=['l_quantity']
                )
            },
            {
                'name': 'aggregate',
                'description': 'Aggregate with GROUP BY (Chunked)',
                'func': lambda proc: proc.aggregate_rows_chunked(
                    "lineitem",
                    group_by=['l_returnflag'],
                    agg_spec={'l_quantity': 'sum', 'l_extendedprice': 'mean'}
                )
            },
            {
                'name': 'compute_statistics',
                'description': 'Compute column statistics (Chunked)',
                'func': lambda proc: proc.compute_statistics_chunked(
                    "lineitem",
                    "l_extendedprice"
                )
            }
        ]
    
    def run_all(self) -> bool:
        """Run complete pipeline"""
        start_time = time.time()
        
        console.print(Panel.fit(
            "[bold cyan]ML Data Research - Complete Experiment Pipeline[/bold cyan]\n"
            f"Row-Based vs Column-Based Processing Evaluation\n"
            f"Processor: {self.processor_type.upper()}, Scale: {self.scale_factor}",
            border_style="cyan"
        ))
        
        # Run steps
        success = (
            self.run_experiments() and
            self.generate_visualizations() and
            self.generate_reports()
        )
        
        # Final summary
        elapsed = time.time() - start_time
        
        if success:
            console.print("\n" + "=" * 70)
            console.print(Panel.fit(
                "[bold green]✓ Experiment Complete![/bold green]\n\n"
                f"📊 Processor: {self.processor_type}\n"
                f"📈 Scale Factor: {self.scale_factor}\n"
                f"⏱ Duration: {elapsed:.1f}s\n"
                f"📁 Output: {self.output_dir}\n",
                border_style="green"
            ))
        else:
            console.print(Panel.fit(
                "[bold red]✗ Experiment Failed[/bold red]",
                border_style="red"
            ))
        
        return success


@click.command()
@click.option('--processor', type=click.Choice(['pandas', 'duckdb', 'chunked', 'remote']),
              default='duckdb', help='Processing engine to use')
@click.option('--scale-factor', type=float, default=0.1,
              help='TPC-H scale factor (0.1=100MB, 1=10GB, 10=100GB)')
@click.option('--data-source', type=click.Choice(['local', 'hetzner']),
              default='local', help='Data source')
@click.option('--chunk-size', type=int, default=100_000,
              help='Chunk size for chunked processor')
@click.option('--memory-limit', type=int, default=2048,
              help='Memory limit in MB')
def main(processor, scale_factor, data_source, chunk_size, memory_limit):
    """
    Run ML data research benchmarking experiments
    
    Examples:
        # Run with DuckDB on local SF=0.1 (default)
        python run_full_experiment.py
        
        # Run with local 10GB dataset
        python run_full_experiment.py --scale-factor 1
        
        # Run with chunked processor
        python run_full_experiment.py --processor chunked --scale-factor 1
        
        # Run with Hetzner S3 (when configured)
        python run_full_experiment.py --processor duckdb --data-source hetzner
    """
    # Load configuration
    config = get_config()
    
    # Update config from CLI options
    config.set('processing.processor', processor)
    config.set('processing.scale_factor', scale_factor)
    config.set('processing.chunk_size', chunk_size)
    config.set('processing.max_memory_mb', memory_limit)
    
    # Validate Hetzner if needed
    if data_source == 'hetzner':
        hetzner_config = HetznerConfig(config)
        if not hetzner_config.validate():
            console.print("[red]✗ Hetzner data source selected but not configured[/red]")
            console.print("Set credentials in .env or config/hetzner.yaml")
            sys.exit(1)
    
    # Run experiment
    runner = ExperimentRunner(config, processor_type=processor, scale_factor=scale_factor)
    success = runner.run_all()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

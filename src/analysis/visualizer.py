"""
Create visualizations comparing row vs column performance
"""
import json
from pathlib import Path
from typing import List, Dict
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from rich.console import Console

console = Console()

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

class PerformanceVisualizer:
    """Create visualizations from metrics data"""
    
    def __init__(self, metrics_file: Path, output_dir: Path):
        self.metrics_file = Path(metrics_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load metrics
        with open(self.metrics_file, 'r') as f:
            data = json.load(f)
            self.metrics = data['metrics']
        
        # Convert to DataFrame
        self.df = pd.DataFrame(self.metrics)
        
        console.print(f"[green]✓ Loaded {len(self.metrics)} metric records[/green]")
    
    def create_duration_comparison(self) -> Path:
        """
        Create bar chart comparing execution times
        """
        console.print("Creating duration comparison chart...")
        
        # Group by operation and format
        pivot = self.df.pivot_table(
            values='duration_seconds',
            index='operation_name',
            columns='format_type',
            aggfunc='mean'
        )
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 6))
        
        x = range(len(pivot.index))
        width = 0.35
        
        row_bars = ax.bar(
            [i - width/2 for i in x],
            pivot['row'],
            width,
            label='Row-based (CSV)',
            color='#FF6B6B',
            alpha=0.8
        )
        
        col_bars = ax.bar(
            [i + width/2 for i in x],
            pivot['column'],
            width,
            label='Column-based (Parquet)',
            color='#4ECDC4',
            alpha=0.8
        )
        
        # Add value labels on bars
        for bars in [row_bars, col_bars]:
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2.,
                    height,
                    f'{height:.3f}s',
                    ha='center',
                    va='bottom',
                    fontsize=9
                )
        
        # Calculate speedup and add annotations
        for i, op in enumerate(pivot.index):
            row_time = pivot.loc[op, 'row']
            col_time = pivot.loc[op, 'column']
            speedup = row_time / col_time
            
            # Add speedup annotation
            ax.text(
                i,
                max(row_time, col_time) * 1.15,
                f'{speedup:.1f}x',
                ha='center',
                fontsize=10,
                fontweight='bold',
                color='green' if speedup > 1 else 'red'
            )
        
        ax.set_xlabel('Operation', fontsize=12, fontweight='bold')
        ax.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
        ax.set_title('Execution Time: Row-based vs Column-based Processing', 
                     fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(pivot.index, rotation=45, ha='right')
        ax.legend(fontsize=11)
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        
        output_path = self.output_dir / "duration_comparison.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        console.print(f"  ✓ Saved to {output_path}")
        return output_path
    
    def create_memory_comparison(self) -> Path:
        """
        Create bar chart comparing memory usage
        """
        console.print("Creating memory comparison chart...")
        
        pivot = self.df.pivot_table(
            values='max_memory_mb',
            index='operation_name',
            columns='format_type',
            aggfunc='mean'
        )
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        x = range(len(pivot.index))
        width = 0.35
        
        ax.bar(
            [i - width/2 for i in x],
            pivot['row'],
            width,
            label='Row-based (CSV)',
            color='#FF6B6B',
            alpha=0.8
        )
        
        ax.bar(
            [i + width/2 for i in x],
            pivot['column'],
            width,
            label='Column-based (Parquet)',
            color='#4ECDC4',
            alpha=0.8
        )
        
        ax.set_xlabel('Operation', fontsize=12, fontweight='bold')
        ax.set_ylabel('Peak Memory (MB)', fontsize=12, fontweight='bold')
        ax.set_title('Memory Usage: Row-based vs Column-based Processing',
                     fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(pivot.index, rotation=45, ha='right')
        ax.legend(fontsize=11)
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        
        output_path = self.output_dir / "memory_comparison.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        console.print(f"  ✓ Saved to {output_path}")
        return output_path
    
    def create_io_comparison(self) -> Path:
        """
        Create bar chart comparing disk I/O
        """
        console.print("Creating I/O comparison chart...")
        
        pivot = self.df.pivot_table(
            values='total_disk_read_mb',
            index='operation_name',
            columns='format_type',
            aggfunc='mean'
        )
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        x = range(len(pivot.index))
        width = 0.35
        
        ax.bar(
            [i - width/2 for i in x],
            pivot['row'],
            width,
            label='Row-based (CSV)',
            color='#FF6B6B',
            alpha=0.8
        )
        
        ax.bar(
            [i + width/2 for i in x],
            pivot['column'],
            width,
            label='Column-based (Parquet)',
            color='#4ECDC4',
            alpha=0.8
        )
        
        ax.set_xlabel('Operation', fontsize=12, fontweight='bold')
        ax.set_ylabel('Disk Read (MB)', fontsize=12, fontweight='bold')
        ax.set_title('Disk I/O: Row-based vs Column-based Processing',
                     fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(pivot.index, rotation=45, ha='right')
        ax.legend(fontsize=11)
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        
        output_path = self.output_dir / "io_comparison.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        console.print(f"  ✓ Saved to {output_path}")
        return output_path
    
    def create_performance_heatmap(self) -> Path:
        """
        Create heatmap showing relative performance
        """
        console.print("Creating performance heatmap...")
        
        # Calculate speedup for each metric
        operations = self.df['operation_name'].unique()
        metrics = ['duration_seconds', 'max_memory_mb', 'max_cpu', 'total_disk_read_mb']
        metric_labels = ['Duration', 'Memory', 'CPU', 'Disk I/O']
        
        speedup_matrix = []
        
        for op in operations:
            row_data = self.df[(self.df['operation_name'] == op) & 
                              (self.df['format_type'] == 'row')]
            col_data = self.df[(self.df['operation_name'] == op) & 
                              (self.df['format_type'] == 'column')]
            
            if len(row_data) > 0 and len(col_data) > 0:
                speedups = []
                for metric in metrics:
                    row_val = row_data[metric].iloc[0]
                    col_val = col_data[metric].iloc[0]
                    
                    if col_val > 0:
                        speedup = row_val / col_val
                        speedups.append(speedup)
                    else:
                        speedups.append(1.0)
                
                speedup_matrix.append(speedups)
        
        speedup_matrix = pd.DataFrame(
            speedup_matrix,
            index=operations,
            columns=metric_labels
        )
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        sns.heatmap(
            speedup_matrix,
            annot=True,
            fmt='.2f',
            cmap='RdYlGn',
            center=1.0,
            vmin=0.5,
            vmax=3.0,
            cbar_kws={'label': 'Speedup Factor (Row/Column)'},
            ax=ax
        )
        
        ax.set_title('Performance Heatmap: Speedup Factor (>1 = Column Faster)',
                     fontsize=14, fontweight='bold', pad=20)
        ax.set_xlabel('Metric', fontsize=12, fontweight='bold')
        ax.set_ylabel('Operation', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        
        output_path = self.output_dir / "performance_heatmap.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        console.print(f"  ✓ Saved to {output_path}")
        return output_path
    
    def create_time_series_plot(self) -> Path:
        """
        Create time series showing resource usage over time
        """
        console.print("Creating time series plot...")
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))
        fig.suptitle('Resource Usage Over Time', fontsize=16, fontweight='bold')
        
        # Get first operation of each type for detailed view
        row_metric = next((m for m in self.metrics if m['format_type'] == 'row'), None)
        col_metric = next((m for m in self.metrics if m['format_type'] == 'column'), None)
        
        if row_metric and col_metric and row_metric.get('snapshots') and col_metric.get('snapshots'):
            # CPU Usage
            row_snapshots = pd.DataFrame(row_metric['snapshots'])
            col_snapshots = pd.DataFrame(col_metric['snapshots'])
            
            row_snapshots['time'] = row_snapshots['timestamp'] - row_snapshots['timestamp'].iloc[0]
            col_snapshots['time'] = col_snapshots['timestamp'] - col_snapshots['timestamp'].iloc[0]
            
            # Plot CPU
            axes[0, 0].plot(row_snapshots['time'], row_snapshots['cpu_percent'], 
                           label='Row-based', color='#FF6B6B', linewidth=2)
            axes[0, 0].plot(col_snapshots['time'], col_snapshots['cpu_percent'], 
                           label='Column-based', color='#4ECDC4', linewidth=2)
            axes[0, 0].set_title('CPU Usage', fontweight='bold')
            axes[0, 0].set_xlabel('Time (seconds)')
            axes[0, 0].set_ylabel('CPU %')
            axes[0, 0].legend()
            axes[0, 0].grid(alpha=0.3)
            
            # Plot Memory
            axes[0, 1].plot(row_snapshots['time'], row_snapshots['memory_mb'], 
                           label='Row-based', color='#FF6B6B', linewidth=2)
            axes[0, 1].plot(col_snapshots['time'], col_snapshots['memory_mb'], 
                           label='Column-based', color='#4ECDC4', linewidth=2)
            axes[0, 1].set_title('Memory Usage', fontweight='bold')
            axes[0, 1].set_xlabel('Time (seconds)')
            axes[0, 1].set_ylabel('Memory (MB)')
            axes[0, 1].legend()
            axes[0, 1].grid(alpha=0.3)
            
            # Plot Disk Read
            axes[1, 0].plot(row_snapshots['time'], row_snapshots['disk_read_mb'], 
                           label='Row-based', color='#FF6B6B', linewidth=2)
            axes[1, 0].plot(col_snapshots['time'], col_snapshots['disk_read_mb'], 
                           label='Column-based', color='#4ECDC4', linewidth=2)
            axes[1, 0].set_title('Disk Read', fontweight='bold')
            axes[1, 0].set_xlabel('Time (seconds)')
            axes[1, 0].set_ylabel('Disk Read (MB)')
            axes[1, 0].legend()
            axes[1, 0].grid(alpha=0.3)
            
            # Plot Disk Write
            axes[1, 1].plot(row_snapshots['time'], row_snapshots['disk_write_mb'], 
                           label='Row-based', color='#FF6B6B', linewidth=2)
            axes[1, 1].plot(col_snapshots['time'], col_snapshots['disk_write_mb'], 
                           label='Column-based', color='#4ECDC4', linewidth=2)
            axes[1, 1].set_title('Disk Write', fontweight='bold')
            axes[1, 1].set_xlabel('Time (seconds)')
            axes[1, 1].set_ylabel('Disk Write (MB)')
            axes[1, 1].legend()
            axes[1, 1].grid(alpha=0.3)
        
        plt.tight_layout()
        
        output_path = self.output_dir / "time_series.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        console.print(f"  ✓ Saved to {output_path}")
        return output_path
    
    def create_interactive_dashboard(self) -> Path:
        """
        Create interactive Plotly dashboard
        """
        console.print("Creating interactive dashboard...")
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Execution Time Comparison',
                'Memory Usage Comparison',
                'CPU Usage Comparison',
                'Disk I/O Comparison'
            ),
            specs=[[{'type': 'bar'}, {'type': 'bar'}],
                   [{'type': 'bar'}, {'type': 'bar'}]]
        )
        
        operations = self.df['operation_name'].unique()
        
        # Duration
        row_durations = [
            self.df[(self.df['operation_name'] == op) & 
                   (self.df['format_type'] == 'row')]['duration_seconds'].iloc[0]
            for op in operations
        ]
        col_durations = [
            self.df[(self.df['operation_name'] == op) & 
                   (self.df['format_type'] == 'column')]['duration_seconds'].iloc[0]
            for op in operations
        ]
        
        fig.add_trace(
            go.Bar(name='Row-based', x=operations, y=row_durations, 
                   marker_color='#FF6B6B'),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(name='Column-based', x=operations, y=col_durations,
                   marker_color='#4ECDC4'),
            row=1, col=1
        )
        
        # Memory
        row_memory = [
            self.df[(self.df['operation_name'] == op) & 
                   (self.df['format_type'] == 'row')]['max_memory_mb'].iloc[0]
            for op in operations
        ]
        col_memory = [
            self.df[(self.df['operation_name'] == op) & 
                   (self.df['format_type'] == 'column')]['max_memory_mb'].iloc[0]
            for op in operations
        ]
        
        fig.add_trace(
            go.Bar(name='Row-based', x=operations, y=row_memory,
                   marker_color='#FF6B6B', showlegend=False),
            row=1, col=2
        )
        fig.add_trace(
            go.Bar(name='Column-based', x=operations, y=col_memory,
                   marker_color='#4ECDC4', showlegend=False),
            row=1, col=2
        )
        
        # CPU
        row_cpu = [
            self.df[(self.df['operation_name'] == op) & 
                   (self.df['format_type'] == 'row')]['max_cpu'].iloc[0]
            for op in operations
        ]
        col_cpu = [
            self.df[(self.df['operation_name'] == op) & 
                   (self.df['format_type'] == 'column')]['max_cpu'].iloc[0]
            for op in operations
        ]
        
        fig.add_trace(
            go.Bar(name='Row-based', x=operations, y=row_cpu,
                   marker_color='#FF6B6B', showlegend=False),
            row=2, col=1
        )
        fig.add_trace(
            go.Bar(name='Column-based', x=operations, y=col_cpu,
                   marker_color='#4ECDC4', showlegend=False),
            row=2, col=1
        )
        
        # Disk I/O
        row_io = [
            self.df[(self.df['operation_name'] == op) & 
                   (self.df['format_type'] == 'row')]['total_disk_read_mb'].iloc[0]
            for op in operations
        ]
        col_io = [
            self.df[(self.df['operation_name'] == op) & 
                   (self.df['format_type'] == 'column')]['total_disk_read_mb'].iloc[0]
            for op in operations
        ]
        
        fig.add_trace(
            go.Bar(name='Row-based', x=operations, y=row_io,
                   marker_color='#FF6B6B', showlegend=False),
            row=2, col=2
        )
        fig.add_trace(
            go.Bar(name='Column-based', x=operations, y=col_io,
                   marker_color='#4ECDC4', showlegend=False),
            row=2, col=2
        )
        
        fig.update_layout(
            title_text="Row-based vs Column-based Performance Dashboard",
            title_font_size=20,
            height=800,
            showlegend=True
        )
        
        # Update axes labels
        fig.update_yaxes(title_text="Time (seconds)", row=1, col=1)
        fig.update_yaxes(title_text="Memory (MB)", row=1, col=2)
        fig.update_yaxes(title_text="CPU %", row=2, col=1)
        fig.update_yaxes(title_text="Disk Read (MB)", row=2, col=2)
        
        output_path = self.output_dir / "interactive_dashboard.html"
        fig.write_html(output_path)
        
        console.print(f"  ✓ Saved to {output_path}")
        return output_path
    
    def generate_all_visualizations(self) -> Dict[str, Path]:
        """Generate all visualizations"""
        console.print("\n[bold]Generating all visualizations...[/bold]\n")
        
        charts = {
            'duration': self.create_duration_comparison(),
            'memory': self.create_memory_comparison(),
            'io': self.create_io_comparison(),
            'heatmap': self.create_performance_heatmap(),
            'timeseries': self.create_time_series_plot(),
            'dashboard': self.create_interactive_dashboard()
        }
        
        console.print(f"\n[green]✓ Generated {len(charts)} visualizations[/green]")
        return charts


# Example usage
if __name__ == "__main__":
    visualizer = PerformanceVisualizer(
        metrics_file="outputs/metrics/test_results.json",
        output_dir="outputs/charts"
    )
    
    charts = visualizer.generate_all_visualizations()
    
    console.print("\n[bold]Generated charts:[/bold]")
    for name, path in charts.items():
        console.print(f"  • {name}: {path}")
"""
Collect system metrics during data processing operations
"""
import time
import psutil
import threading
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pathlib import Path
import json
from datetime import datetime

@dataclass
class MetricSnapshot:
    """Single point-in-time metric measurement"""
    timestamp: float
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    disk_read_mb: float
    disk_write_mb: float
    
    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp,
            'cpu_percent': self.cpu_percent,
            'memory_mb': self.memory_mb,
            'memory_percent': self.memory_percent,
            'disk_read_mb': self.disk_read_mb,
            'disk_write_mb': self.disk_write_mb
        }

@dataclass
class OperationMetrics:
    """Complete metrics for a single operation"""
    operation_name: str
    dataset: str
    format_type: str  # "row" or "column"
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration_seconds: Optional[float] = None
    snapshots: List[MetricSnapshot] = field(default_factory=list)
    
    # Summary statistics
    avg_cpu: Optional[float] = None
    max_cpu: Optional[float] = None
    avg_memory_mb: Optional[float] = None
    max_memory_mb: Optional[float] = None
    total_disk_read_mb: Optional[float] = None
    total_disk_write_mb: Optional[float] = None
    rows_processed: Optional[int] = None
    
    def finalize(self):
        """Calculate summary statistics"""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        
        if self.snapshots:
            cpu_values = [s.cpu_percent for s in self.snapshots]
            memory_values = [s.memory_mb for s in self.snapshots]
            
            self.avg_cpu = sum(cpu_values) / len(cpu_values)
            self.max_cpu = max(cpu_values)
            self.avg_memory_mb = sum(memory_values) / len(memory_values)
            self.max_memory_mb = max(memory_values)
            
            if len(self.snapshots) > 1:
                self.total_disk_read_mb = (
                    self.snapshots[-1].disk_read_mb - 
                    self.snapshots[0].disk_read_mb
                )
                self.total_disk_write_mb = (
                    self.snapshots[-1].disk_write_mb - 
                    self.snapshots[0].disk_write_mb
                )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'operation_name': self.operation_name,
            'dataset': self.dataset,
            'format_type': self.format_type,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': self.duration_seconds,
            'avg_cpu': self.avg_cpu,
            'max_cpu': self.max_cpu,
            'avg_memory_mb': self.avg_memory_mb,
            'max_memory_mb': self.max_memory_mb,
            'total_disk_read_mb': self.total_disk_read_mb,
            'total_disk_write_mb': self.total_disk_write_mb,
            'rows_processed': self.rows_processed,
            'snapshots': [s.to_dict() for s in self.snapshots]
        }


class MetricsCollector:
    """
    Collect system metrics during operation execution
    
    Usage:
        collector = MetricsCollector()
        with collector.measure("filter_operation", "tpc_h", "row"):
            # Your processing code here
            result = process_data()
        
        metrics = collector.get_latest_metrics()
    """
    
    def __init__(self, sample_interval: float = 0.1):
        """
        Args:
            sample_interval: How often to sample metrics (seconds)
        """
        self.sample_interval = sample_interval
        self.process = psutil.Process()
        self.metrics_history: List[OperationMetrics] = []
        self._current_metrics: Optional[OperationMetrics] = None
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
    
    def _get_disk_io(self) -> tuple:
        """Get current disk I/O in MB"""
        try:
            io_counters = self.process.io_counters()
            read_mb = io_counters.read_bytes / (1024 * 1024)
            write_mb = io_counters.write_bytes / (1024 * 1024)
            return read_mb, write_mb
        except:
            return 0.0, 0.0
    
    def _collect_snapshot(self) -> MetricSnapshot:
        """Collect a single metric snapshot"""
        memory_info = self.process.memory_info()
        cpu_percent = self.process.cpu_percent()
        disk_read_mb, disk_write_mb = self._get_disk_io()
        
        return MetricSnapshot(
            timestamp=time.time(),
            cpu_percent=cpu_percent,
            memory_mb=memory_info.rss / (1024 * 1024),
            memory_percent=self.process.memory_percent(),
            disk_read_mb=disk_read_mb,
            disk_write_mb=disk_write_mb
        )
    
    def _monitoring_loop(self):
        """Background thread that collects metrics"""
        while self._monitoring:
            if self._current_metrics:
                snapshot = self._collect_snapshot()
                self._current_metrics.snapshots.append(snapshot)
            time.sleep(self.sample_interval)
    
    def start_monitoring(
        self, 
        operation_name: str,
        dataset: str,
        format_type: str
    ):
        """Start collecting metrics"""
        self._current_metrics = OperationMetrics(
            operation_name=operation_name,
            dataset=dataset,
            format_type=format_type
        )
        
        self._monitoring = True
        self._monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True
        )
        self._monitor_thread.start()
    
    def stop_monitoring(self, rows_processed: Optional[int] = None):
        """Stop collecting metrics"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=1.0)
        
        if self._current_metrics:
            self._current_metrics.rows_processed = rows_processed
            self._current_metrics.finalize()
            self.metrics_history.append(self._current_metrics)
            result = self._current_metrics
            self._current_metrics = None
            return result
    
    def measure(self, operation_name: str, dataset: str, format_type: str):
        """
        Context manager for measuring operations
        
        Usage:
            with collector.measure("filter", "tpc_h", "row"):
                result = process_data()
        """
        return MetricsContext(self, operation_name, dataset, format_type)
    
    def get_latest_metrics(self) -> Optional[OperationMetrics]:
        """Get the most recent metrics"""
        return self.metrics_history[-1] if self.metrics_history else None
    
    def get_all_metrics(self) -> List[OperationMetrics]:
        """Get all collected metrics"""
        return self.metrics_history
    
    def save_metrics(self, output_path: Path):
        """Save all metrics to JSON file"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            'collection_date': datetime.now().isoformat(),
            'metrics': [m.to_dict() for m in self.metrics_history]
        }
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def clear_history(self):
        """Clear all collected metrics"""
        self.metrics_history.clear()


class MetricsContext:
    """Context manager for metrics collection"""
    
    def __init__(
        self, 
        collector: MetricsCollector,
        operation_name: str,
        dataset: str,
        format_type: str
    ):
        self.collector = collector
        self.operation_name = operation_name
        self.dataset = dataset
        self.format_type = format_type
        self.rows_processed = None
    
    def __enter__(self):
        self.collector.start_monitoring(
            self.operation_name,
            self.dataset,
            self.format_type
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.collector.stop_monitoring(self.rows_processed)
        return False
    
    def set_rows_processed(self, count: int):
        """Set the number of rows processed"""
        self.rows_processed = count


# Example usage
if __name__ == "__main__":
    from rich.console import Console
    console = Console()
    
    collector = MetricsCollector(sample_interval=0.1)
    
    # Simulate some work
    console.print("[bold]Testing metrics collection...[/bold]\n")
    
    with collector.measure("test_operation", "test_dataset", "row") as ctx:
        # Simulate processing
        data = [i ** 2 for i in range(1000000)]
        ctx.set_rows_processed(1000000)
        time.sleep(1)
    
    metrics = collector.get_latest_metrics()
    
    console.print(f"Operation: {metrics.operation_name}")
    console.print(f"Duration: {metrics.duration_seconds:.3f} seconds")
    console.print(f"Avg CPU: {metrics.avg_cpu:.1f}%")
    console.print(f"Max CPU: {metrics.max_cpu:.1f}%")
    console.print(f"Avg Memory: {metrics.avg_memory_mb:.1f} MB")
    console.print(f"Max Memory: {metrics.max_memory_mb:.1f} MB")
    console.print(f"Disk Read: {metrics.total_disk_read_mb:.2f} MB")
    console.print(f"Disk Write: {metrics.total_disk_write_mb:.2f} MB")
    console.print(f"Snapshots collected: {len(metrics.snapshots)}")
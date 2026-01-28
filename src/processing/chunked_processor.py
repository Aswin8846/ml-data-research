"""
Chunked/streaming processor for large datasets
Reads data in batches instead of loading entire files into memory

Advantages:
- Works with datasets larger than available RAM
- Progressive processing (can start work before all data loads)
- Memory usage stays constant regardless of file size
- Useful for remote data (S3, etc.)
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, List, Optional, Callable, Dict, Any
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
from rich.console import Console
from rich.progress import track
import tempfile

console = Console()


class ChunkedProcessor(ABC):
    """
    Abstract base class for streaming/chunked data processing
    
    Subclasses implement read_batches() for different sources (local, S3, etc.)
    All other operations process data in chunks without full loads
    """
    
    def __init__(
        self,
        chunk_size: int = 100_000,
        max_memory_mb: int = 2048
    ):
        """
        Args:
            chunk_size: Number of rows per batch
            max_memory_mb: Max memory to keep in RAM (soft limit)
        """
        self.chunk_size = chunk_size
        self.max_memory_mb = max_memory_mb
        self.format_type = "chunked"
    
    @abstractmethod
    def read_batches(
        self,
        path: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict] = None
    ) -> Iterator[pd.DataFrame]:
        """
        Read data in batches
        
        Args:
            path: File or path to read
            columns: Specific columns to read (None = all)
            filters: Optional filtering to apply at read time
            
        Yields:
            DataFrames with up to chunk_size rows
        """
        pass
    
    def filter_rows_chunked(
        self,
        path: str,
        condition: Callable[[pd.DataFrame], pd.Series],
        output_path: Optional[Path] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Filter rows across chunks
        
        Args:
            path: File to filter
            condition: Function taking DataFrame, returning boolean Series
            output_path: Optional path to write results (for very large results)
            columns: Columns needed for condition (optimization)
            
        Returns:
            Filtered DataFrame (or writes to disk if output_path provided)
            
        Example:
            result = processor.filter_rows_chunked(
                "lineitem.parquet",
                lambda df: df['l_quantity'] > 30
            )
        """
        console.print(f"[cyan]Filtering chunks from {path}[/cyan]")
        
        result_batches = []
        total_read = 0
        total_matched = 0
        
        for batch_idx, batch in enumerate(self.read_batches(path, columns=columns)):
            # Apply condition
            mask = condition(batch)
            filtered_batch = batch[mask]
            
            total_read += len(batch)
            total_matched += len(filtered_batch)
            
            if len(filtered_batch) > 0:
                result_batches.append(filtered_batch)
                
                # Check memory usage
                memory_mb = sum(
                    b.memory_usage(deep=True).sum() / (1024 * 1024)
                    for b in result_batches
                )
                
                # Write to disk if accumulating too much
                if output_path and memory_mb > self.max_memory_mb:
                    self._write_results(output_path, result_batches, append=batch_idx > 0)
                    result_batches = []
        
        # Handle remaining
        if result_batches:
            result = pd.concat(result_batches, ignore_index=True)
            if output_path:
                self._write_results(output_path, [result], append=False)
            console.print(
                f"[green]✓ Filtered: {total_matched:,} / {total_read:,} rows matched[/green]"
            )
            return result
        
        console.print(
            f"[green]✓ Filtered: {total_matched:,} / {total_read:,} rows matched[/green]"
        )
        return pd.DataFrame()
    
    def aggregate_rows_chunked(
        self,
        path: str,
        group_by: List[str],
        agg_spec: Dict[str, str],
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Aggregate across chunks with GROUP BY
        
        Args:
            path: File to aggregate
            group_by: Columns to group by
            agg_spec: Aggregation spec {"col": "func"}
            columns: Columns to read (optimization)
            
        Returns:
            Aggregated DataFrame
            
        Example:
            result = processor.aggregate_rows_chunked(
                "lineitem.parquet",
                group_by=["l_returnflag"],
                agg_spec={"l_quantity": "sum", "l_extendedprice": "mean"}
            )
        """
        console.print(f"[cyan]Aggregating chunks from {path}[/cyan]")
        
        # Determine needed columns
        if columns is None:
            columns = list(set(group_by + list(agg_spec.keys())))
        
        all_groups = {}
        total_rows = 0
        
        for batch in self.read_batches(path, columns=columns):
            total_rows += len(batch)
            
            # Group this batch
            batch_grouped = batch.groupby(group_by).agg(agg_spec)
            
            # Merge with running totals
            for group_key, group_data in batch_grouped.iterrows():
                if group_key not in all_groups:
                    all_groups[group_key] = group_data.to_dict()
                else:
                    # Merge aggregations (simple approach: sum for sums, keep last for others)
                    for col, func in agg_spec.items():
                        col_name = f"{col}"
                        if func in ["sum", "count"]:
                            all_groups[group_key][col_name] += group_data[col_name]
                        # Note: mean, min, max would need more sophisticated merging
        
        # Convert to DataFrame
        result = pd.DataFrame.from_dict(all_groups, orient='index')
        result = result.reset_index()
        
        console.print(
            f"[green]✓ Aggregated {total_rows:,} rows into {len(result):,} groups[/green]"
        )
        return result
    
    def compute_statistics_chunked(
        self,
        path: str,
        column: str,
        percentiles: Optional[List[float]] = None
    ) -> Dict[str, float]:
        """
        Compute statistics without loading full column
        
        Args:
            path: File to analyze
            column: Column for statistics
            percentiles: Optional percentiles (expensive - uses sampling)
            
        Returns:
            Dictionary with statistics
            
        Note: For exact percentiles on huge files, consider using DuckDB instead
        """
        console.print(f"[cyan]Computing statistics for {column}[/cyan]")
        
        stats = {
            'count': 0,
            'sum': 0.0,
            'sum_sq': 0.0,
            'min': float('inf'),
            'max': float('-inf'),
            'values_sample': []  # For percentiles
        }
        
        for batch in self.read_batches(path, columns=[column]):
            col_data = batch[column].dropna().astype(float)
            
            stats['count'] += len(col_data)
            stats['sum'] += col_data.sum()
            stats['sum_sq'] += (col_data ** 2).sum()
            stats['min'] = min(stats['min'], col_data.min())
            stats['max'] = max(stats['max'], col_data.max())
            
            # Keep sample for percentiles (limit to 10k values)
            if len(stats['values_sample']) < 10000:
                sample_size = min(1000, len(col_data))
                stats['values_sample'].extend(
                    col_data.sample(sample_size).tolist()
                )
        
        # Calculate statistics
        mean = stats['sum'] / stats['count'] if stats['count'] > 0 else 0
        variance = (stats['sum_sq'] / stats['count'] - mean ** 2) if stats['count'] > 0 else 0
        stddev = variance ** 0.5
        
        result = {
            'count': stats['count'],
            'mean': mean,
            'stddev': stddev,
            'min': stats['min'],
            'max': stats['max'],
            'median': pd.Series(stats['values_sample']).median() if stats['values_sample'] else None
        }
        
        # Add percentiles if requested
        if percentiles:
            for p in percentiles:
                result[f'p{int(p*100)}'] = pd.Series(stats['values_sample']).quantile(p)
        
        console.print(f"[green]✓ Statistics computed[/green]")
        return result
    
    def select_columns_chunked(
        self,
        path: str,
        columns: List[str],
        output_path: Optional[Path] = None
    ) -> Optional[pd.DataFrame]:
        """
        Select specific columns, write to output if large
        
        Args:
            path: Source file
            columns: Columns to select
            output_path: Optional output file (for large results)
            
        Returns:
            DataFrame if small, None if written to disk
        """
        console.print(f"[cyan]Selecting {len(columns)} columns from {path}[/cyan]")
        
        result_batches = []
        total_rows = 0
        
        for batch in self.read_batches(path, columns=columns):
            total_rows += len(batch)
            result_batches.append(batch)
            
            # Write if getting too large
            if output_path:
                memory_mb = sum(
                    b.memory_usage(deep=True).sum() / (1024 * 1024)
                    for b in result_batches
                )
                
                if memory_mb > self.max_memory_mb:
                    self._write_results(output_path, result_batches)
                    result_batches = []
        
        if result_batches:
            result = pd.concat(result_batches, ignore_index=True)
            if output_path:
                self._write_results(output_path, [result])
            console.print(f"[green]✓ Selected {len(result):,} rows[/green]")
            return result
        
        return None
    
    def _write_results(
        self,
        path: Path,
        batches: List[pd.DataFrame],
        append: bool = False
    ):
        """Write batches to parquet file"""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        combined = pd.concat(batches, ignore_index=True)
        
        if append and path.exists():
            # Append to existing file
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, combined], ignore_index=True)
        
        combined.to_parquet(path, compression='snappy', index=False)


class LocalChunkedProcessor(ChunkedProcessor):
    """Process local parquet/CSV files in chunks"""
    
    def __init__(
        self,
        data_dir: Path,
        chunk_size: int = 100_000,
        max_memory_mb: int = 2048
    ):
        super().__init__(chunk_size, max_memory_mb)
        self.data_dir = Path(data_dir)
    
    def read_batches(
        self,
        path: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict] = None
    ) -> Iterator[pd.DataFrame]:
        """
        Stream local parquet file in batches
        
        Args:
            path: File name or relative path
            columns: Specific columns to read
            filters: PyArrow filters (not supported in basic implementation)
            
        Yields:
            DataFrames with up to chunk_size rows
        """
        if isinstance(path, str) and not path.endswith('.parquet'):
            path = f"{path}.parquet"
        
        file_path = self.data_dir / path
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        console.print(f"[cyan]Reading {file_path.name} in {self.chunk_size:,}-row batches[/cyan]")
        
        try:
            parquet_file = pq.ParquetFile(file_path)
            
            # Get row groups
            num_row_groups = parquet_file.num_row_groups
            total_rows = parquet_file.metadata.num_rows
            
            console.print(
                f"  Total: {total_rows:,} rows, {num_row_groups} row groups"
            )
            
            batch_count = 0
            for rg in track(
                range(num_row_groups),
                description="Reading batches",
                total=num_row_groups
            ):
                # Read row group
                table = parquet_file.read_row_group(rg, columns=columns)
                df = table.to_pandas()
                
                # Yield batches of chunk_size
                for i in range(0, len(df), self.chunk_size):
                    batch = df.iloc[i:i+self.chunk_size]
                    batch_count += 1
                    yield batch
            
            console.print(f"[green]✓ Read {batch_count} batches[/green]")
            
        except Exception as e:
            console.print(f"[red]✗ Error reading file: {e}[/red]")
            raise


class RemoteChunkedProcessor(ChunkedProcessor):
    """
    Process files from remote storage (S3, etc.)
    
    Supports multiple formats: .parquet, .tbl, .dat, .csv
    Works with any file size via streaming
    """
    
    def __init__(
        self,
        storage,  # HetznerStorage instance
        chunk_size: int = 100_000,
        max_memory_mb: int = 2048
    ):
        super().__init__(chunk_size, max_memory_mb)
        self.storage = storage
    
    def read_batches(
        self,
        path: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict] = None
    ) -> Iterator[pd.DataFrame]:
        """
        Stream remote file in batches from S3
        
        Auto-detects format: .parquet, .tbl, .dat, .csv
        
        Args:
            path: File path in S3 bucket
            columns: Specific columns to read (only for parquet)
            filters: Optional filtering
            
        Yields:
            DataFrames with up to chunk_size rows
            
        Example:
            processor = RemoteChunkedProcessor(storage)
            for batch in processor.read_batches("lineitem.tbl"):
                # Process batch
                pass
        """
        try:
            # Auto-detect format and read appropriately
            if path.endswith('.parquet'):
                for batch in self.storage.read_parquet_batch(
                    path,
                    columns=columns,
                    batch_size=self.chunk_size
                ):
                    yield batch
            elif path.endswith('.tbl'):
                for batch in self.storage.read_tbl_batch(
                    path,
                    batch_size=self.chunk_size
                ):
                    yield batch
            elif path.endswith('.dat') or path.endswith('.csv'):
                for batch in self.storage.read_dat_batch(
                    path,
                    batch_size=self.chunk_size
                ):
                    yield batch
            else:
                # Try auto-detection
                for batch in self.storage.read_auto_format(
                    path,
                    batch_size=self.chunk_size
                ):
                    yield batch
        except Exception as e:
            console.print(f"[red]✗ Error reading remote file: {e}[/red]")
            raise


# Example usage
if __name__ == "__main__":
    from pathlib import Path
    
    data_dir = Path("./data/raw/tpc_h_sf0.1/parquet")
    
    processor = LocalChunkedProcessor(data_dir, chunk_size=50_000)
    
    # Test 1: Read batches
    console.print("\n[bold]Test 1: Reading batches[/bold]")
    batch_count = 0
    total_rows = 0
    for batch in processor.read_batches("lineitem", columns=["l_quantity", "l_extendedprice"]):
        batch_count += 1
        total_rows += len(batch)
        console.print(f"  Batch {batch_count}: {len(batch):,} rows")
        if batch_count >= 2:  # Just show first 2 batches
            break
    
    # Test 2: Filter
    console.print("\n[bold]Test 2: Filter rows[/bold]")
    result = processor.filter_rows_chunked(
        "lineitem",
        lambda df: df['l_quantity'] > 30,
        columns=["l_quantity", "l_extendedprice", "l_discount"]
    )
    console.print(f"Filtered result: {len(result):,} rows")
    
    # Test 3: Statistics
    console.print("\n[bold]Test 3: Statistics[/bold]")
    stats = processor.compute_statistics_chunked("lineitem", "l_extendedprice")
    for key, value in stats.items():
        if isinstance(value, float):
            console.print(f"  {key}: {value:.2f}")
        else:
            console.print(f"  {key}: {value}")
    
    # Test 4: Aggregation
    console.print("\n[bold]Test 4: Aggregation[/bold]")
    result = processor.aggregate_rows_chunked(
        "lineitem",
        group_by=["l_returnflag"],
        agg_spec={"l_quantity": "sum"}
    )
    console.print(result)

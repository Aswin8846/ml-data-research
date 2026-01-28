"""
Hetzner S3 storage integration for reading/writing large datasets
Supports streaming reads and multipart uploads for efficient large-file handling
"""
import s3fs
import pyarrow.parquet as pq
from pathlib import Path
from typing import Iterator, Optional, List
import pandas as pd
from rich.console import Console
from rich.progress import track

console = Console()


class HetznerStorage:
    """
    Interface to Hetzner Object Storage (S3-compatible)
    
    Handles:
    - Connection management
    - File listing and metadata
    - Streaming parquet reads (chunked)
    - Multipart uploads for large files
    - Remote file operations
    
    Example:
        storage = HetznerStorage(
            endpoint="https://fsn1-1.your-hetzner.storage.api/",
            access_key="...",
            secret_key="...",
            bucket="ml-data-research"
        )
        
        # Stream parquet file in batches
        for batch in storage.read_parquet_batch("data/lineitem.parquet", batch_size=100_000):
            print(f"Batch: {len(batch)} rows")
    """
    
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "fsn1"
    ):
        """
        Initialize Hetzner storage connection
        
        Args:
            endpoint: Full endpoint URL (e.g., https://fsn1-1.your-hetzner.storage.api/)
            access_key: S3 access key
            secret_key: S3 secret key
            bucket: Bucket name
            region: Hetzner region (fsn1, etc.)
        """
        self.endpoint = endpoint
        self.bucket = bucket
        self.region = region
        
        console.print(f"[cyan]Initializing Hetzner storage (bucket: {bucket})[/cyan]")
        
        try:
            self.s3 = s3fs.S3FileSystem(
                anon=False,
                use_ssl=True,
                client_kwargs={'endpoint_url': endpoint},
                key=access_key,
                secret=secret_key,
                config_kwargs={'retries': {'max_attempts': 3}}
            )
            
            # Test connection
            self.s3.ls(f"s3://{bucket}")
            console.print(f"[green]✓ Connected to Hetzner storage[/green]")
            
        except Exception as e:
            console.print(f"[red]✗ Failed to connect to Hetzner: {e}[/red]")
            raise
    
    def list_files(self, prefix: str = "", suffix: str = ".parquet") -> List[str]:
        """
        List files in bucket with optional prefix and suffix filters
        
        Args:
            prefix: Path prefix (e.g., "data/tpc_h_sf1/")
            suffix: File extension filter (e.g., ".parquet")
            
        Returns:
            List of file paths relative to bucket root
        """
        try:
            path = f"s3://{self.bucket}/{prefix}*{suffix}"
            files = self.s3.glob(path)
            
            # Remove bucket prefix from paths
            files = [f.replace(f"s3://{self.bucket}/", "") for f in files]
            
            console.print(f"[cyan]Found {len(files)} files with prefix '{prefix}'[/cyan]")
            return sorted(files)
            
        except Exception as e:
            console.print(f"[red]✗ Failed to list files: {e}[/red]")
            return []
    
    def get_file_info(self, path: str) -> dict:
        """
        Get metadata for a file
        
        Args:
            path: File path relative to bucket root
            
        Returns:
            Dictionary with size, modified time, etc.
        """
        try:
            full_path = f"s3://{self.bucket}/{path}"
            info = self.s3.info(full_path)
            
            return {
                'size_bytes': info.get('Size', 0),
                'size_mb': info.get('Size', 0) / (1024 * 1024),
                'modified': info.get('LastModified'),
                'storage_class': info.get('StorageClass', 'STANDARD'),
                'etag': info.get('ETag')
            }
        except Exception as e:
            console.print(f"[red]✗ Failed to get file info for {path}: {e}[/red]")
            return {}
    
    def get_file_size_mb(self, path: str) -> float:
        """Get file size in MB"""
        return self.get_file_info(path).get('size_mb', 0)
    
    def read_parquet_batch(
        self,
        path: str,
        columns: Optional[List[str]] = None,
        batch_size: int = 100_000
    ) -> Iterator[pd.DataFrame]:
        """
        Stream parquet file in batches
        
        This is the key method for reading large files without loading into memory
        
        Args:
            path: File path relative to bucket root
            columns: Specific columns to read (columnar advantage)
            batch_size: Number of rows per batch (100k default)
            
        Yields:
            DataFrames with up to batch_size rows
            
        Example:
            for batch in storage.read_parquet_batch("data/lineitem.parquet", batch_size=50_000):
                print(f"Processing {len(batch)} rows")
                # Process batch without loading entire file
        """
        try:
            full_path = f"s3://{self.bucket}/{path}"
            file_info = self.get_file_info(path)
            
            console.print(
                f"[cyan]Reading {path} ({file_info['size_mb']:.1f} MB) "
                f"in {batch_size:,}-row batches[/cyan]"
            )
            
            with self.s3.open(full_path, 'rb') as f:
                parquet_file = pq.ParquetFile(f)
                
                total_rows = parquet_file.metadata.num_rows
                batches = (total_rows + batch_size - 1) // batch_size
                
                for batch_idx in track(
                    range(batches),
                    description=f"Reading batches",
                    total=batches
                ):
                    # Read batch
                    batch = parquet_file.read(
                        columns=columns,
                        row_groups=[batch_idx]  # Read one row group at a time
                    )
                    
                    # Convert to pandas and yield
                    yield batch.to_pandas()
                    
        except Exception as e:
            console.print(f"[red]✗ Failed to read parquet file {path}: {e}[/red]")
            raise
    
    def read_tbl_batch(
        self,
        path: str,
        delimiter: str = '|',
        batch_size: int = 100_000
    ) -> Iterator[pd.DataFrame]:
        """
        Stream TPC-H .tbl file in batches
        
        Args:
            path: File path relative to bucket root (e.g., "lineitem.tbl")
            delimiter: Field delimiter (TPC-H uses '|')
            batch_size: Rows per batch
            
        Yields:
            DataFrames with up to batch_size rows
            
        Example:
            for batch in storage.read_tbl_batch("lineitem.tbl"):
                print(f"Processing {len(batch)} rows")
        """
        try:
            full_path = f"s3://{self.bucket}/{path}"
            file_info = self.get_file_info(path)
            
            console.print(
                f"[cyan]Reading {path} ({file_info['size_mb']:.1f} MB) "
                f"in {batch_size:,}-row batches[/cyan]"
            )
            
            with self.s3.open(full_path, 'rb') as f:
                # Read TBL file in chunks
                batch_rows = []
                for line in f:
                    line = line.decode('utf-8').rstrip('\n')
                    if line:
                        batch_rows.append(line)
                    
                    if len(batch_rows) >= batch_size:
                        # Convert batch to DataFrame
                        df = self._parse_tbl_batch(batch_rows, delimiter)
                        yield df
                        batch_rows = []
                
                # Yield remaining rows
                if batch_rows:
                    df = self._parse_tbl_batch(batch_rows, delimiter)
                    yield df
                    
        except Exception as e:
            console.print(f"[red]✗ Failed to read TBL file {path}: {e}[/red]")
            raise
    
    def read_dat_batch(
        self,
        path: str,
        delimiter: str = ',',
        batch_size: int = 100_000
    ) -> Iterator[pd.DataFrame]:
        """
        Stream .dat file in batches (similar to CSV)
        
        Args:
            path: File path relative to bucket root
            delimiter: Field delimiter (default comma)
            batch_size: Rows per batch
            
        Yields:
            DataFrames with up to batch_size rows
        """
        try:
            full_path = f"s3://{self.bucket}/{path}"
            file_info = self.get_file_info(path)
            
            console.print(
                f"[cyan]Reading {path} ({file_info['size_mb']:.1f} MB) "
                f"in {batch_size:,}-row batches[/cyan]"
            )
            
            with self.s3.open(full_path, 'rb') as f:
                # Read DAT file in chunks using pandas
                batch_rows = []
                for line in f:
                    line = line.decode('utf-8').rstrip('\n')
                    if line:
                        batch_rows.append(line)
                    
                    if len(batch_rows) >= batch_size:
                        df = pd.read_csv(
                            pd.io.common.StringIO('\n'.join(batch_rows)),
                            delimiter=delimiter
                        )
                        yield df
                        batch_rows = []
                
                # Yield remaining rows
                if batch_rows:
                    df = pd.read_csv(
                        pd.io.common.StringIO('\n'.join(batch_rows)),
                        delimiter=delimiter
                    )
                    yield df
                    
        except Exception as e:
            console.print(f"[red]✗ Failed to read DAT file {path}: {e}[/red]")
            raise
    
    def read_auto_format(
        self,
        path: str,
        batch_size: int = 100_000,
        delimiter: Optional[str] = None
    ) -> Iterator[pd.DataFrame]:
        """
        Automatically detect and read file format
        
        Supports: .parquet, .tbl, .dat, .csv
        
        Args:
            path: File path (extension determines format)
            batch_size: Rows per batch
            delimiter: Optional delimiter override
            
        Yields:
            DataFrames with up to batch_size rows
            
        Example:
            for batch in storage.read_auto_format("lineitem.tbl"):
                # Automatically detects format
                print(f"Processing {len(batch)} rows")
        """
        if path.endswith('.parquet'):
            yield from self.read_parquet_batch(path, batch_size=batch_size)
        elif path.endswith('.tbl'):
            tbl_delimiter = delimiter or '|'
            yield from self.read_tbl_batch(path, delimiter=tbl_delimiter, batch_size=batch_size)
        elif path.endswith('.dat'):
            dat_delimiter = delimiter or ','
            yield from self.read_dat_batch(path, delimiter=dat_delimiter, batch_size=batch_size)
        elif path.endswith('.csv'):
            csv_delimiter = delimiter or ','
            yield from self.read_dat_batch(path, delimiter=csv_delimiter, batch_size=batch_size)
        else:
            raise ValueError(f"Unsupported file format: {path}")
    
    def _parse_tbl_batch(
        self,
        lines: List[str],
        delimiter: str = '|'
    ) -> pd.DataFrame:
        """
        Parse TBL batch lines into DataFrame
        
        TBL files have trailing delimiter on each line that needs removal
        """
        from io import StringIO
        
        # Remove trailing delimiters
        clean_lines = [line.rstrip(delimiter) for line in lines]
        
        # Parse as CSV with delimiter
        csv_data = '\n'.join(clean_lines)
        df = pd.read_csv(StringIO(csv_data), delimiter=delimiter, header=None)
        
        return df
    
    def read_parquet_full(
        self,
        path: str,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Read entire parquet file (use for small files only)
        
        Args:
            path: File path
            columns: Specific columns to read
            
        Returns:
            Full DataFrame
        """
        file_info = self.get_file_info(path)
        
        if file_info['size_mb'] > 2048:
            console.print(
                f"[yellow]⚠ Warning: Loading {file_info['size_mb']:.0f}MB into memory. "
                f"Consider using read_parquet_batch() instead[/yellow]"
            )
        
        full_path = f"s3://{self.bucket}/{path}"
        
        with self.s3.open(full_path, 'rb') as f:
            return pd.read_parquet(f, columns=columns)
    
    def upload_file(
        self,
        local_path: Path,
        remote_path: str,
        multipart: bool = True,
        chunk_size_mb: int = 100
    ) -> bool:
        """
        Upload local file to S3
        
        Uses multipart upload for large files (more efficient, resumable)
        
        Args:
            local_path: Local file path
            remote_path: Destination path (relative to bucket root)
            multipart: Use multipart upload for large files
            chunk_size_mb: Size of each part in multipart upload
            
        Returns:
            True if successful
        """
        local_path = Path(local_path)
        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        
        console.print(
            f"[cyan]Uploading {local_path.name} ({file_size_mb:.1f} MB) → {remote_path}[/cyan]"
        )
        
        try:
            full_path = f"s3://{self.bucket}/{remote_path}"
            
            if multipart and file_size_mb > 100:
                # Multipart upload for large files
                chunk_size_bytes = chunk_size_mb * 1024 * 1024
                chunks = []
                
                with open(local_path, 'rb') as f:
                    while True:
                        chunk = f.read(chunk_size_bytes)
                        if not chunk:
                            break
                        chunks.append(chunk)
                
                # Upload chunks
                for i, chunk in track(
                    enumerate(chunks, 1),
                    description="Uploading chunks",
                    total=len(chunks)
                ):
                    with self.s3.open(full_path, 'ab' if i > 1 else 'wb') as f:
                        f.write(chunk)
            else:
                # Simple upload for small files
                self.s3.put_file(str(local_path), full_path)
            
            console.print(f"[green]✓ Upload complete: {remote_path}[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Upload failed: {e}[/red]")
            return False
    
    def download_file(
        self,
        remote_path: str,
        local_path: Path
    ) -> bool:
        """
        Download file from S3
        
        Args:
            remote_path: File path (relative to bucket)
            local_path: Local destination
            
        Returns:
            True if successful
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_info = self.get_file_info(remote_path)
        
        console.print(
            f"[cyan]Downloading {remote_path} ({file_info['size_mb']:.1f} MB)[/cyan]"
        )
        
        try:
            full_path = f"s3://{self.bucket}/{remote_path}"
            self.s3.get_file(full_path, str(local_path))
            
            console.print(f"[green]✓ Download complete: {local_path}[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Download failed: {e}[/red]")
            return False
    
    def list_datasets(self) -> dict:
        """
        List all datasets in bucket with metadata
        
        Returns:
            Dictionary of datasets with their metadata
        """
        datasets = {}
        
        # Find all parquet files
        files = self.list_files()
        
        for file_path in files:
            # Extract dataset name from path (e.g., "tpc_h_sf1" from "tpc_h_sf1/lineitem.parquet")
            parts = file_path.split('/')
            if len(parts) >= 2:
                dataset_name = parts[0]
                table_name = parts[1].replace('.parquet', '')
                
                if dataset_name not in datasets:
                    datasets[dataset_name] = {'tables': {}, 'total_mb': 0}
                
                info = self.get_file_info(file_path)
                datasets[dataset_name]['tables'][table_name] = info['size_mb']
                datasets[dataset_name]['total_mb'] += info['size_mb']
        
        return datasets
    
    def get_dataset_info(self, dataset_name: str) -> dict:
        """
        Get detailed info for a specific dataset
        
        Args:
            dataset_name: Dataset to inspect
            
        Returns:
            Dictionary with tables and sizes
        """
        files = self.list_files(prefix=f"{dataset_name}/")
        
        info = {
            'name': dataset_name,
            'tables': {},
            'total_mb': 0,
            'row_counts': {}
        }
        
        for file_path in files:
            table_name = file_path.split('/')[-1].replace('.parquet', '')
            file_info = self.get_file_info(file_path)
            
            info['tables'][table_name] = {
                'size_mb': file_info['size_mb'],
                'path': file_path
            }
            info['total_mb'] += file_info['size_mb']
        
        return info


# Example usage
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # Load credentials from .env
    load_dotenv()
    
    storage = HetznerStorage(
        endpoint=os.getenv("HETZNER_ENDPOINT"),
        access_key=os.getenv("HETZNER_ACCESS_KEY"),
        secret_key=os.getenv("HETZNER_SECRET_KEY"),
        bucket=os.getenv("HETZNER_BUCKET", "ml-data-research")
    )
    
    # Test listing datasets
    console.print("\n[bold]Available Datasets:[/bold]")
    datasets = storage.list_datasets()
    for dataset_name, info in datasets.items():
        console.print(f"  {dataset_name}: {info['total_mb']:.0f} MB")
        for table_name, size_mb in info['tables'].items():
            console.print(f"    - {table_name}: {size_mb:.1f} MB")
    
    # Test reading parquet (if dataset exists)
    if datasets:
        dataset_name = list(datasets.keys())[0]
        table_name = list(datasets[dataset_name]['tables'].keys())[0]
        
        console.print(f"\n[bold]Testing parquet read from {dataset_name}/{table_name}[/bold]")
        
        path = f"{dataset_name}/{table_name}.parquet"
        
        try:
            for i, batch in enumerate(storage.read_parquet_batch(path, batch_size=10_000)):
                console.print(f"  Batch {i+1}: {len(batch)} rows")
                if i >= 2:  # Just test first 3 batches
                    break
        except Exception as e:
            console.print(f"[red]Error reading: {e}[/red]")

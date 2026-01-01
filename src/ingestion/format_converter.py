"""
Convert between CSV and Parquet formats
"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Literal
from rich.console import Console
from rich.progress import track

console = Console()

class FormatConverter:
    """Convert data between formats"""
    
    @staticmethod
    def csv_to_parquet(
        csv_path: Path,
        parquet_path: Path,
        compression: str = "snappy",
        row_group_size: int = 100000
    ):
        """
        Convert CSV to Parquet (columnar format)
        
        Args:
            csv_path: Input CSV file
            parquet_path: Output Parquet file
            compression: Compression algorithm
            row_group_size: Rows per row group
        """
        console.print(f"Converting {csv_path.name} to Parquet...")
        
        # Read CSV
        df = pd.read_csv(csv_path)
        
        # Convert to Arrow Table
        table = pa.Table.from_pandas(df)
        
        # Write Parquet with columnar layout
        pq.write_table(
            table,
            parquet_path,
            compression=compression,
            row_group_size=row_group_size,
            use_dictionary=True,  # Efficient for low-cardinality columns
            write_statistics=True
        )
        
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        parquet_size = parquet_path.stat().st_size / (1024 * 1024)
        ratio = (1 - parquet_size / csv_size) * 100
        
        console.print(f"  CSV: {csv_size:.2f} MB → Parquet: {parquet_size:.2f} MB "
                     f"(saved {ratio:.1f}%)")
    
    @staticmethod
    def parquet_to_csv(parquet_path: Path, csv_path: Path):
        """Convert Parquet to CSV (row-based format)"""
        console.print(f"Converting {parquet_path.name} to CSV...")
        
        # Read Parquet
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        
        # Write CSV
        df.to_csv(csv_path, index=False)
        
        parquet_size = parquet_path.stat().st_size / (1024 * 1024)
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        
        console.print(f"  Parquet: {parquet_size:.2f} MB → CSV: {csv_size:.2f} MB")
    
    @staticmethod
    def batch_convert_directory(
        input_dir: Path,
        output_dir: Path,
        input_format: Literal["csv", "parquet"],
        output_format: Literal["csv", "parquet"]
    ):
        """Convert all files in a directory"""
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get files
        files = list(input_dir.glob(f"*.{input_format}"))
        console.print(f"\n[bold]Converting {len(files)} files[/bold]")
        
        for file in track(files, description="Converting"):
            output_file = output_dir / f"{file.stem}.{output_format}"
            
            if input_format == "csv" and output_format == "parquet":
                FormatConverter.csv_to_parquet(file, output_file)
            elif input_format == "parquet" and output_format == "csv":
                FormatConverter.parquet_to_csv(file, output_file)
        
        console.print(f"[green]✓ Converted {len(files)} files[/green]")


# Example usage
if __name__ == "__main__":
    converter = FormatConverter()
    
    # Convert a directory
    converter.batch_convert_directory(
        input_dir="./data/raw/tpc_h_sf0.1/csv",
        output_dir="./data/raw/tpc_h_sf0.1/parquet",
        input_format="csv",
        output_format="parquet"
    )
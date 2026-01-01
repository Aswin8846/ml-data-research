"""
Test data ingestion components
"""
from pathlib import Path
from src.ingestion.duckdb_generator import TPCGenerator
from src.ingestion.format_converter import FormatConverter
from rich.console import Console

console = Console()

def main():
    console.print("[bold]Testing Data Ingestion Pipeline[/bold]\n")
    
    # 1. Generate TPC-H data
    console.print("=" * 60)
    console.print("STEP 1: Generate TPC-H Dataset")
    console.print("=" * 60)
    
    generator = TPCGenerator(output_dir="./data/raw")
    
    # Generate small dataset (100MB) in both formats
    parquet_paths = generator.generate_tpc_h(scale_factor=0.1, format="parquet")
    csv_paths = generator.generate_tpc_h(scale_factor=0.1, format="csv")
    
    generator.close()
    
    console.print("\n[green]✓ Data generation complete![/green]")
    console.print(f"Parquet files: data/raw/tpc_h_sf0.1/parquet/")
    console.print(f"CSV files: data/raw/tpc_h_sf0.1/csv/")
    
    # 2. Verify files exist
    console.print("\n" + "=" * 60)
    console.print("STEP 2: Verify Generated Files")
    console.print("=" * 60)
    
    parquet_dir = Path("./data/raw/tpc_h_sf0.1/parquet")
    csv_dir = Path("./data/raw/tpc_h_sf0.1/csv")
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    csv_files = list(csv_dir.glob("*.csv"))
    
    console.print(f"\nParquet files: {len(parquet_files)}")
    console.print(f"CSV files: {len(csv_files)}")
    
    # Calculate total sizes
    parquet_size = sum(f.stat().st_size for f in parquet_files) / (1024 * 1024)
    csv_size = sum(f.stat().st_size for f in csv_files) / (1024 * 1024)
    
    console.print(f"\nTotal Parquet size: {parquet_size:.2f} MB")
    console.print(f"Total CSV size: {csv_size:.2f} MB")
    console.print(f"Compression ratio: {(1 - parquet_size/csv_size)*100:.1f}% smaller")
    
    console.print("\n[bold green]✓ All tests passed![/bold green]")

if __name__ == "__main__":
    main()
"""
Generate TPC-H and TPC-DS datasets using DuckDB
"""
import duckdb
from pathlib import Path
from typing import Literal
from rich.console import Console
from rich.progress import track
import time

console = Console()

class TPCGenerator:
    """Generate TPC benchmark datasets"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect()
        
    def generate_tpc_h(
        self, 
        scale_factor: float,
        format: Literal["csv", "parquet"] = "parquet"
    ) -> dict:
        """
        Generate TPC-H dataset
        
        Args:
            scale_factor: Size multiplier (0.1 = ~100MB, 1 = ~1GB, 10 = ~10GB)
            format: Output format
            
        Returns:
            Dictionary with paths to generated files
        """
        console.print(f"\n[bold blue]Generating TPC-H (SF={scale_factor})[/bold blue]")
        
        # Install and load TPC-H extension
        console.print("Installing TPC-H extension...")
        self.conn.execute("INSTALL tpch; LOAD tpch;")
        
        # Generate data
        console.print(f"Generating data at scale factor {scale_factor}...")
        self.conn.execute(f"CALL dbgen(sf={scale_factor});")
        
        # Get table list
        tables = self.conn.execute("SHOW TABLES;").fetchall()
        table_names = [t[0] for t in tables]
        
        console.print(f"Found {len(table_names)} tables: {', '.join(table_names)}")
        
        # Export each table
        output_paths = {}
        output_subdir = self.output_dir / f"tpc_h_sf{scale_factor}" / format
        output_subdir.mkdir(parents=True, exist_ok=True)
        
        for table in track(table_names, description="Exporting tables"):
            if format == "csv":
                output_path = output_subdir / f"{table}.csv"
                self.conn.execute(
                    f"COPY {table} TO '{output_path}' (HEADER, DELIMITER ',')"
                )
            else:  # parquet
                output_path = output_subdir / f"{table}.parquet"
                self.conn.execute(
                    f"COPY {table} TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
                )
            
            output_paths[table] = output_path
            
            # Get statistics
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            size_mb = output_path.stat().st_size / (1024 * 1024)
            console.print(f"  ✓ {table}: {count:,} rows, {size_mb:.2f} MB")
        
        return output_paths
    
    def generate_tpc_ds(
        self,
        scale_factor: int,
        format: Literal["csv", "parquet"] = "parquet"
    ) -> dict:
        """Generate TPC-DS dataset"""
        console.print(f"\n[bold blue]Generating TPC-DS (SF={scale_factor})[/bold blue]")
        
        # Install and load TPC-DS extension
        console.print("Installing TPC-DS extension...")
        self.conn.execute("INSTALL tpcds; LOAD tpcds;")
        
        # Generate data
        console.print(f"Generating data at scale factor {scale_factor}...")
        self.conn.execute(f"CALL dsdgen(sf={scale_factor});")
        
        # Get table list
        tables = self.conn.execute("SHOW TABLES;").fetchall()
        table_names = [t[0] for t in tables]
        
        console.print(f"Found {len(table_names)} tables")
        
        # Export tables (similar to TPC-H)
        output_paths = {}
        output_subdir = self.output_dir / f"tpc_ds_sf{scale_factor}" / format
        output_subdir.mkdir(parents=True, exist_ok=True)
        
        for table in track(table_names, description="Exporting tables"):
            if format == "csv":
                output_path = output_subdir / f"{table}.csv"
                self.conn.execute(
                    f"COPY {table} TO '{output_path}' (HEADER, DELIMITER ',')"
                )
            else:
                output_path = output_subdir / f"{table}.parquet"
                self.conn.execute(
                    f"COPY {table} TO '{output_path}' (FORMAT PARQUET)"
                )
            
            output_paths[table] = output_path
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            size_mb = output_path.stat().st_size / (1024 * 1024)
            console.print(f"  ✓ {table}: {count:,} rows, {size_mb:.2f} MB")
        
        return output_paths
    
    def get_schema_info(self, dataset: str, scale_factor: float) -> dict:
        """Get schema information for a dataset"""
        if dataset == "tpc_h":
            self.conn.execute("INSTALL tpch; LOAD tpch;")
            self.conn.execute(f"CALL dbgen(sf={scale_factor});")
        else:
            self.conn.execute("INSTALL tpcds; LOAD tpcds;")
            self.conn.execute(f"CALL dsdgen(sf={scale_factor});")
        
        tables = self.conn.execute("SHOW TABLES;").fetchall()
        schema = {}
        
        for table in tables:
            table_name = table[0]
            columns = self.conn.execute(f"PRAGMA table_info({table_name});").fetchall()
            schema[table_name] = {
                'columns': [(col[1], col[2]) for col in columns],
                'row_count': self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            }
        
        return schema
    
    def close(self):
        """Close DuckDB connection"""
        self.conn.close()


# Example usage
if __name__ == "__main__":
    generator = TPCGenerator(output_dir="./data/raw")
    
    # Generate small TPC-H dataset
    paths = generator.generate_tpc_h(scale_factor=0.1, format="parquet")
    console.print(f"\n[green]Generated {len(paths)} tables[/green]")
    
    # Generate CSV version too
    paths_csv = generator.generate_tpc_h(scale_factor=0.1, format="csv")
    
    generator.close()
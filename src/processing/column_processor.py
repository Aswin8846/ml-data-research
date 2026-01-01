"""
Column-based data processor
Reads and processes data in columnar format (Parquet)
"""
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from typing import List, Dict, Callable, Any
from rich.console import Console

console = Console()

class ColumnProcessor:
    """
    Process data in column-based format
    
    Column-based processing can read specific columns efficiently,
    which is ideal for analytical queries
    """
    
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.format_type = "column"
    
    def load_table(
        self, 
        table_name: str,
        columns: List[str] = None
    ) -> pd.DataFrame:
        """
        Load a table from Parquet format
        
        Args:
            table_name: Table to load
            columns: Optional list of columns (columnar advantage!)
        """
        parquet_path = self.data_dir / f"{table_name}.parquet"
        
        if not parquet_path.exists():
            raise FileNotFoundError(f"Table not found: {parquet_path}")
        
        console.print(f"Loading {table_name} (column-based/Parquet)...")
        
        if columns:
            # Column-based advantage: Only read requested columns
            console.print(f"  Reading only {len(columns)} columns: {columns}")
            df = pd.read_parquet(parquet_path, columns=columns)
        else:
            df = pd.read_parquet(parquet_path)
        
        console.print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
        return df
    
    def filter_rows(
        self, 
        table_name: str,
        condition: Callable[[pd.Series], bool],
        required_columns: List[str] = None
    ) -> pd.DataFrame:
        """
        Filter rows based on condition
        
        Column-based approach: Can read only columns needed for filtering
        
        Args:
            table_name: Name of table to filter
            condition: Function that takes a row and returns bool
            required_columns: Columns needed (optimization hint)
        """
        # If we know which columns are needed, only load those
        if required_columns:
            df = self.load_table(table_name, columns=required_columns)
        else:
            df = self.load_table(table_name)
        
        console.print(f"Filtering {len(df):,} rows...")
        result = df[df.apply(condition, axis=1)]
        
        console.print(f"  Result: {len(result):,} rows match condition")
        return result
    
    def aggregate_rows(
        self,
        table_name: str,
        group_by: List[str],
        agg_func: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Aggregate data with GROUP BY
        
        Column-based approach: Only read grouping and aggregation columns
        """
        # Determine required columns
        required_columns = list(set(group_by + list(agg_func.keys())))
        
        console.print(f"Loading only required columns: {required_columns}")
        df = self.load_table(table_name, columns=required_columns)
        
        console.print(f"Aggregating {len(df):,} rows by {group_by}...")
        result = df.groupby(group_by).agg(agg_func).reset_index()
        
        console.print(f"  Result: {len(result):,} groups")
        return result
    
    def join_tables(
        self,
        left_table: str,
        right_table: str,
        on: str,
        how: str = 'inner',
        left_columns: List[str] = None,
        right_columns: List[str] = None
    ) -> pd.DataFrame:
        """
        Join two tables
        
        Column-based approach: Can load only needed columns from each table
        """
        console.print(f"Joining {left_table} with {right_table}...")
        
        left_df = self.load_table(left_table, columns=left_columns)
        right_df = self.load_table(right_table, columns=right_columns)
        
        result = pd.merge(left_df, right_df, on=on, how=how)
        
        console.print(f"  Result: {len(result):,} rows")
        return result
    
    def sort_table(
        self,
        table_name: str,
        by: List[str],
        ascending: bool = True,
        columns: List[str] = None
    ) -> pd.DataFrame:
        """
        Sort table by columns
        
        Column-based approach: Can load only needed columns
        """
        if columns:
            # Ensure sort columns are included
            columns = list(set(columns + by))
        
        df = self.load_table(table_name, columns=columns)
        
        console.print(f"Sorting {len(df):,} rows by {by}...")
        result = df.sort_values(by=by, ascending=ascending)
        
        console.print(f"  Sorted {len(result):,} rows")
        return result
    
    def select_columns(
        self,
        table_name: str,
        columns: List[str]
    ) -> pd.DataFrame:
        """
        Select specific columns
        
        Column-based approach: MAJOR ADVANTAGE - only read requested columns
        This is where Parquet shines!
        """
        console.print(f"Selecting {len(columns)} columns...")
        result = self.load_table(table_name, columns=columns)
        
        console.print(f"  Selected {len(columns)} columns, {len(result):,} rows")
        return result
    
    def compute_statistics(
        self,
        table_name: str,
        column: str
    ) -> Dict[str, float]:
        """
        Compute statistics for a column
        
        Column-based approach: MAJOR ADVANTAGE - only read one column!
        """
        console.print(f"Computing statistics for {column}...")
        console.print(f"  Reading ONLY {column} column (columnar advantage!)")
        
        # Only read the one column we need
        df = self.load_table(table_name, columns=[column])
        df[column] = pd.to_numeric(df[column],errors='coerce')
        stats = {
            'count': len(df),
            'mean': float(df[column].mean()),
            'std': float(df[column].std()),
            'min': float(df[column].min()),
            'max': float(df[column].max()),
            'median': float(df[column].median())
        }
        
        return stats
    
    def get_parquet_metadata(self, table_name: str) -> Dict:
        """Get Parquet file metadata"""
        parquet_path = self.data_dir / f"{table_name}.parquet"
        
        parquet_file = pq.ParquetFile(parquet_path)
        metadata = parquet_file.metadata
        
        return {
            'num_rows': metadata.num_rows,
            'num_columns': metadata.num_columns,
            'num_row_groups': metadata.num_row_groups,
            'serialized_size': metadata.serialized_size,
            'columns': [
                parquet_file.schema.column(i).name 
                for i in range(metadata.num_columns)
            ]
        }


# Example usage
if __name__ == "__main__":
    processor = ColumnProcessor(data_dir="./data/raw/tpc_h_sf0.1/parquet")
    
    # Test column selection (columnar advantage)
    console.print("\n[bold]Test 1: Column Selection (Columnar Advantage!)[/bold]")
    result = processor.select_columns(
        "lineitem",
        columns=['l_quantity', 'l_extendedprice']
    )
    console.print(f"Selected columns result: {len(result)} rows\n")
    
    # Test statistics (only read one column)
    console.print("[bold]Test 2: Statistics (Only One Column Read!)[/bold]")
    stats = processor.compute_statistics("lineitem", "l_quantity")
    for key, value in stats.items():
        console.print(f"  {key}: {value:.2f}")
    
    # Show metadata
    console.print("\n[bold]Test 3: Parquet Metadata[/bold]")
    metadata = processor.get_parquet_metadata("lineitem")
    console.print(f"  Rows: {metadata['num_rows']:,}")
    console.print(f"  Columns: {metadata['num_columns']}")
    console.print(f"  Row groups: {metadata['num_row_groups']}")
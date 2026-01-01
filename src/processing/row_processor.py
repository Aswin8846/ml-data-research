"""
Row-based data processor
Reads and processes data in row-oriented format (CSV)
"""
import pandas as pd
import csv
from pathlib import Path
from typing import List, Dict, Callable, Any
from rich.console import Console
from rich.progress import track

console = Console()

class RowProcessor:
    """
    Process data in row-based format
    
    Row-based processing reads entire rows at once,
    which is efficient for operations that need all columns
    """
    
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.format_type = "row"
    
    def load_table(self, table_name: str) -> pd.DataFrame:
        """Load a table from CSV format"""
        csv_path = self.data_dir / f"{table_name}.csv"
        
        if not csv_path.exists():
            raise FileNotFoundError(f"Table not found: {csv_path}")
        
        console.print(f"Loading {table_name} (row-based/CSV)...")
        df = pd.read_csv(csv_path)
        
        console.print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
        return df
    
    def filter_rows(
        self, 
        table_name: str,
        condition: Callable[[pd.Series], bool]
    ) -> pd.DataFrame:
        """
        Filter rows based on condition
        
        Row-based approach: Read entire row, check condition
        
        Args:
            table_name: Name of table to filter
            condition: Function that takes a row and returns bool
        
        Example:
            processor.filter_rows(
                "lineitem",
                lambda row: row['l_shipdate'] > '1995-01-01'
            )
        """
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
        
        Row-based approach: Group entire rows
        
        Args:
            table_name: Table to aggregate
            group_by: Columns to group by
            agg_func: Aggregation functions {'column': 'func'}
        
        Example:
            processor.aggregate_rows(
                "lineitem",
                group_by=['l_returnflag'],
                agg_func={'l_quantity': 'sum', 'l_extendedprice': 'mean'}
            )
        """
        df = self.load_table(table_name)
        
        console.print(f"Aggregating {len(df):,} rows by {group_by}...")
        result = df.groupby(group_by).agg(agg_func).reset_index()
        
        console.print(f"  Result: {len(result):,} groups")
        return result
    
    def join_tables(
        self,
        left_table: str,
        right_table: str,
        on: str,
        how: str = 'inner'
    ) -> pd.DataFrame:
        """
        Join two tables
        
        Row-based approach: Join entire rows from both tables
        
        Args:
            left_table: First table name
            right_table: Second table name
            on: Column to join on
            how: Join type ('inner', 'left', 'right', 'outer')
        """
        console.print(f"Joining {left_table} with {right_table}...")
        
        left_df = self.load_table(left_table)
        right_df = self.load_table(right_table)
        
        result = pd.merge(left_df, right_df, on=on, how=how)
        
        console.print(f"  Result: {len(result):,} rows")
        return result
    
    def sort_table(
        self,
        table_name: str,
        by: List[str],
        ascending: bool = True
    ) -> pd.DataFrame:
        """
        Sort table by columns
        
        Row-based approach: Sort entire rows based on column values
        """
        df = self.load_table(table_name)
        
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
        
        Row-based approach: Must still read entire rows,
        then extract columns (inefficient for CSV)
        """
        df = self.load_table(table_name)
        
        console.print(f"Selecting {len(columns)} columns from {len(df):,} rows...")
        result = df[columns]
        
        console.print(f"  Selected {len(columns)} columns")
        return result
    
    def compute_statistics(
        self,
        table_name: str,
        column: str
    ) -> Dict[str, float]:
        """
        Compute statistics for a column
        
        Row-based approach: Must read all rows to access one column
        """
        df = self.load_table(table_name)
        
        console.print(f"Computing statistics for {column}...")

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


# Example usage
if __name__ == "__main__":
    processor = RowProcessor(data_dir="./data/raw/tpc_h_sf0.1/csv")
    
    # Test filter
    console.print("\n[bold]Test 1: Filter Operation[/bold]")
    result = processor.filter_rows(
        "lineitem",
        lambda row: row['l_quantity'] > 30
    )
    console.print(f"Filtered result: {len(result)} rows\n")
    
    # Test aggregation
    console.print("[bold]Test 2: Aggregation Operation[/bold]")
    result = processor.aggregate_rows(
        "lineitem",
        group_by=['l_returnflag'],
        agg_func={'l_quantity': 'sum', 'l_extendedprice': 'mean'}
    )
    console.print(result)
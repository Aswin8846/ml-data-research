import duckdb
from pathlib import Path
from typing import List, Dict, Optional, Any
import tempfile
import shutil
from rich.console import Console

console = Console()


class DuckDBProcessor:
    
    def __init__(
        self,
        data_dir: Path,
        temp_dir: Optional[Path] = None,
        in_memory: bool = True,
        memory_limit_gb: int = 16
    ):
        """
        Initialize DuckDB processor
        
        Args:
            data_dir: Directory with parquet files
            temp_dir: Temp directory for query files (None = system temp)
            in_memory: Use in-memory DB (fast) vs file-based (persistent)
            memory_limit_gb: Max memory DuckDB can use
        """
        self.data_dir = Path(data_dir)
        self.temp_dir = Path(temp_dir) if temp_dir else Path(tempfile.gettempdir())
        self.in_memory = in_memory
        self.memory_limit_gb = memory_limit_gb
        self.registered_tables = set()
        
        console.print(f"[cyan]Initializing DuckDB processor[/cyan]")
        console.print(f"  Data dir: {self.data_dir}")
        console.print(f"  Memory limit: {memory_limit_gb}GB")
        console.print(f"  Mode: {'in-memory' if in_memory else 'file-based'}")
        
        try:
            if in_memory:
                # In-memory database (fast, but data lost on exit)
                self.conn = duckdb.connect(":memory:")
            else:
                # File-based database
                db_path = self.temp_dir / "duckdb_query.db"
                self.conn = duckdb.connect(str(db_path))
            
            # Set memory limit
            self.conn.execute(f"SET memory_limit='{memory_limit_gb}GB'")
            
            # Enable other optimizations
            self.conn.execute("PRAGMA threads=6")  # Use 4 threads
            
            console.print(f"[green]✓ DuckDB initialized[/green]")
            
        except Exception as e:
            console.print(f"[red]✗ Failed to initialize DuckDB: {e}[/red]")
            raise
    
    def register_parquet_table(self, table_name: str, path: Optional[str] = None) -> bool:
        """
        Register a parquet file as a table
        
        Args:
            table_name: Name to use in queries
            path: Optional specific path (defaults to {data_dir}/{table_name}.parquet)
            
        Returns:
            True if successful
            
        Example:
            processor.register_parquet_table("lineitem")  # Reads lineitem.parquet
        """
        if path is None:
            path = self.data_dir / f"{table_name}.parquet"
        else:
            path = Path(path)
        
        if not path.exists():
            console.print(f"[red]✗ File not found: {path}[/red]")
            return False
        
        try:
            # Read parquet and create view
            console.print(f"[cyan]Registering {table_name} from {path.name}[/cyan]")
            
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM read_parquet('{path}')
            """)
            
            # Get row count for confirmation
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            col_count = len(self.conn.execute(f"SELECT * FROM {table_name} LIMIT 0").description)
            
            self.registered_tables.add(table_name)
            console.print(f"[green]✓ {table_name}: {row_count:,} rows, {col_count} columns[/green]")
            
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Failed to register {table_name}: {e}[/red]")
            return False
    
    def register_all_tables(self, suffix: str = ".parquet") -> int:
        """
        Register all parquet files in data directory
        
        Args:
            suffix: File extension to match
            
        Returns:
            Number of tables registered
        """
        parquet_files = self.data_dir.glob(f"*{suffix}")
        count = 0
        
        for parquet_file in parquet_files:
            table_name = parquet_file.stem
            if self.register_parquet_table(table_name, str(parquet_file)):
                count += 1
        
        console.print(f"[green]Registered {count} tables[/green]")
        return count
    
    def select_columns(
        self,
        table_name: str,
        columns: List[str]
    ) -> Any:
        """
        Select specific columns (columnar advantage)
        
        Args:
            table_name: Table to select from
            columns: Column names
            
        Returns:
            DuckDB relation (can call .df() for pandas, .pl() for polars, etc.)
        """
        self._ensure_registered(table_name)
        
        cols = ", ".join(columns)
        console.print(f"[cyan]Selecting {len(columns)} columns from {table_name}[/cyan]")
        
        result = self.conn.execute(f"SELECT {cols} FROM {table_name}")
        
        console.print(f"[green]✓ {result.count:,} rows selected[/green]")
        return result
    
    def filter_rows(
        self,
        table_name: str,
        where_clause: str,
        columns: Optional[List[str]] = None
    ) -> Any:
        """
        Filter rows with WHERE clause (vectorized, fast)
        
        Args:
            table_name: Table to filter
            where_clause: SQL WHERE clause (e.g., "l_quantity > 30")
            columns: Optional columns to return (all by default)
            
        Returns:
            DuckDB relation
            
        Example:
            result = processor.filter_rows(
                "lineitem",
                "l_quantity > 30 AND l_discount > 0.05"
            )
        """
        self._ensure_registered(table_name)
        
        cols = "*" if columns is None else ", ".join(columns)
        
        console.print(f"[cyan]Filtering {table_name} WHERE {where_clause}[/cyan]")
        
        result = self.conn.execute(f"""
            SELECT {cols} FROM {table_name}
            WHERE {where_clause}
        """)
        
        console.print(f"[green]✓ {result.count:,} rows match filter[/green]")
        return result
    
    def aggregate_rows(
        self,
        table_name: str,
        group_by: List[str],
        agg_spec: Dict[str, str],
        having: Optional[str] = None
    ) -> Any:
        """
        Aggregate with GROUP BY (optimized)
        
        Args:
            table_name: Table to aggregate
            group_by: Columns to group by
            agg_spec: Aggregations as {"column": "function"} (e.g., {"qty": "sum"})
            having: Optional HAVING clause
            
        Returns:
            DuckDB relation
            
        Example:
            result = processor.aggregate_rows(
                "lineitem",
                group_by=["l_returnflag"],
                agg_spec={"l_quantity": "sum", "l_extendedprice": "mean"}
            )
        """
        self._ensure_registered(table_name)
        
        group_cols = ", ".join(group_by)
        agg_cols = ", ".join([
            f"{func}({col}) as {col}_{func}"
            for col, func in agg_spec.items()
        ])
        
        query = f"""
            SELECT {group_cols}, {agg_cols}
            FROM {table_name}
            GROUP BY {group_cols}
        """
        
        if having:
            query += f"\nHAVING {having}"
        
        console.print(f"[cyan]Aggregating {table_name} by {group_by}[/cyan]")
        
        result = self.conn.execute(query)
        
        console.print(f"[green]✓ {result.count:,} groups created[/green]")
        return result
    
    def compute_statistics(
        self,
        table_name: str,
        column: str,
        percentiles: Optional[List[float]] = None
    ) -> Dict[str, float]:
        """
        Compute statistics for a column (very fast with DuckDB)
        
        This is where DuckDB shines - 10-100x faster than Pandas
        
        Args:
            table_name: Table name
            column: Column to analyze
            percentiles: Optional percentiles to compute (e.g., [0.25, 0.5, 0.75])
            
        Returns:
            Dictionary with statistics
            
        Example:
            stats = processor.compute_statistics("lineitem", "l_extendedprice")
            # Returns: {count, mean, stddev, min, max, median, ...}
        """
        self._ensure_registered(table_name)
        
        console.print(f"[cyan]Computing statistics for {column} in {table_name}[/cyan]")
        
        # Build query
        agg_functions = [
            f"COUNT(*) as count",
            f"COUNT(DISTINCT {column}) as distinct_count",
            f"AVG({column}) as mean",
            f"STDDEV_POP({column}) as stddev",
            f"MIN({column}) as min",
            f"MAX({column}) as max",
            f"QUANTILE_CONT({column}, 0.5) as median",
            f"QUANTILE_CONT({column}, 0.25) as q1",
            f"QUANTILE_CONT({column}, 0.75) as q3",
        ]
        
        if percentiles:
            for p in percentiles:
                agg_functions.append(f"QUANTILE_CONT({column}, {p}) as p{int(p*100)}")
        
        query = f"SELECT {', '.join(agg_functions)} FROM {table_name}"
        
        result = self.conn.execute(query).fetchone()
        result_dict = dict(zip([desc[0] for desc in self.conn.description], result))
        
        console.print(f"[green]✓ Statistics computed[/green]")
        return result_dict
    
    def join_tables(
        self,
        left_table: str,
        right_table: str,
        on: str,
        how: str = "inner",
        left_columns: Optional[List[str]] = None,
        right_columns: Optional[List[str]] = None
    ) -> Any:
        """
        Join two tables
        
        Args:
            left_table: Left table
            right_table: Right table
            on: Join column(s) (e.g., "id" or "left.id = right.id")
            how: Join type (inner, left, right, full)
            left_columns: Columns from left table
            right_columns: Columns from right table
            
        Returns:
            DuckDB relation
        """
        self._ensure_registered(left_table)
        self._ensure_registered(right_table)
        
        left_cols = "*" if left_columns is None else ", ".join([f"{left_table}.{c}" for c in left_columns])
        right_cols = "*" if right_columns is None else ", ".join([f"{right_table}.{c}" for c in right_columns])
        
        console.print(f"[cyan]Joining {left_table} {how} {right_table} on {on}[/cyan]")
        
        result = self.conn.execute(f"""
            SELECT {left_cols}, {right_cols}
            FROM {left_table}
            {how.upper()} JOIN {right_table} ON {on}
        """)
        
        console.print(f"[green]✓ {result.count:,} rows in join result[/green]")
        return result
    
    def execute_query(self, query: str) -> Any:
        """
        Execute raw SQL query
        
        Args:
            query: SQL query string
            
        Returns:
            DuckDB relation
        """
        console.print(f"[cyan]Executing query[/cyan]")
        result = self.conn.execute(query)
        console.print(f"[green]✓ Query completed: {result.count:,} rows[/green]")
        return result
    
    def to_pandas(self, result: Any) -> Any:
        """Convert DuckDB result to pandas DataFrame"""
        return result.df()
    
    def to_polars(self, result: Any) -> Any:
        """Convert DuckDB result to Polars DataFrame"""
        return result.pl()
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get metadata about a registered table"""
        self._ensure_registered(table_name)
        
        info = self.conn.execute(f"""
            SELECT
                COUNT(*) as row_count,
                COUNT(DISTINCT *) as unique_rows
            FROM {table_name}
        """).fetchone()
        
        columns = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        
        return {
            'row_count': info[0],
            'columns': len(columns),
            'column_details': [
                {'name': col[1], 'type': col[2]}
                for col in columns
            ]
        }
    
    def _ensure_registered(self, table_name: str):
        """Check if table is registered, raise error if not"""
        if table_name not in self.registered_tables:
            raise ValueError(
                f"Table '{table_name}' not registered. "
                f"Call register_parquet_table('{table_name}') first."
            )
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            console.print("[cyan]DuckDB connection closed[/cyan]")
    
    def __enter__(self):
        """Context manager support"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()


# Example usage
if __name__ == "__main__":
    from pathlib import Path
    
    data_dir = Path("./data/raw/tpc_h_sf0.1/parquet")
    
    with DuckDBProcessor(data_dir=data_dir) as processor:
        # Register tables
        processor.register_all_tables()
        
        # Test: Column selection
        console.print("\n[bold]Test 1: Column Selection[/bold]")
        result = processor.select_columns(
            "lineitem",
            ["l_quantity", "l_extendedprice", "l_discount"]
        )
        df = processor.to_pandas(result)
        console.print(f"Result shape: {df.shape}")
        
        # Test: Statistics (DuckDB advantage)
        console.print("\n[bold]Test 2: Statistics Computation (FAST!)[/bold]")
        stats = processor.compute_statistics("lineitem", "l_extendedprice")
        for key, value in stats.items():
            if isinstance(value, float):
                console.print(f"  {key}: {value:.2f}")
            else:
                console.print(f"  {key}: {value}")
        
        # Test: Filtering
        console.print("\n[bold]Test 3: Row Filtering[/bold]")
        result = processor.filter_rows(
            "lineitem",
            "l_quantity > 30 AND l_discount > 0.05"
        )
        
        # Test: Aggregation
        console.print("\n[bold]Test 4: Aggregation[/bold]")
        result = processor.aggregate_rows(
            "lineitem",
            group_by=["l_returnflag"],
            agg_spec={"l_quantity": "sum", "l_extendedprice": "mean"}
        )
        df = processor.to_pandas(result)
        console.print(df)

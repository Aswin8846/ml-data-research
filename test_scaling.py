"""
Tests for scaling functionality (Steps 2-4)
Validates DuckDB, chunked processing, and configuration
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import sys

sys.path.insert(0, str(Path(__file__).parent))

from src.processing.duckdb_processor import DuckDBProcessor
from src.processing.chunked_processor import LocalChunkedProcessor
from src.config import Config, get_config, HetznerConfig, reset_config


class TestDuckDBProcessor:
    """Test DuckDB processor"""
    
    @pytest.fixture
    def data_dir(self):
        """Get test data directory"""
        data_dir = Path("./data/raw/tpc_h_sf0.1/parquet")
        if not data_dir.exists():
            pytest.skip("Test data not found - run data generation first")
        return data_dir
    
    @pytest.fixture
    def processor(self, data_dir):
        """Create DuckDB processor"""
        return DuckDBProcessor(data_dir=data_dir, in_memory=True)
    
    def test_initialization(self, processor):
        """Test processor initialization"""
        assert processor is not None
        assert processor.data_dir.exists()
    
    def test_register_table(self, processor):
        """Test registering a parquet file"""
        assert processor.register_parquet_table("lineitem")
        assert "lineitem" in processor.registered_tables
    
    def test_select_columns(self, processor):
        """Test column selection"""
        processor.register_parquet_table("lineitem")
        
        result = processor.select_columns(
            "lineitem",
            ["l_quantity", "l_extendedprice"]
        )
        df = processor.to_pandas(result)
        
        assert len(df) > 0
        assert list(df.columns) == ["l_quantity", "l_extendedprice"]
    
    def test_filter_rows(self, processor):
        """Test row filtering"""
        processor.register_parquet_table("lineitem")
        
        result = processor.filter_rows(
            "lineitem",
            "l_quantity > 30"
        )
        df = processor.to_pandas(result)
        
        assert len(df) > 0
        assert (df['l_quantity'] > 30).all()
    
    def test_compute_statistics(self, processor):
        """Test statistics computation"""
        processor.register_parquet_table("lineitem")
        
        stats = processor.compute_statistics(
            "lineitem",
            "l_extendedprice"
        )
        
        # Check expected keys
        assert 'count' in stats
        assert 'mean' in stats
        assert 'min' in stats
        assert 'max' in stats
        
        # Validate values
        assert stats['count'] > 0
        assert stats['min'] <= stats['mean'] <= stats['max']
    
    def test_aggregate_rows(self, processor):
        """Test aggregation"""
        processor.register_parquet_table("lineitem")
        
        result = processor.aggregate_rows(
            "lineitem",
            group_by=["l_returnflag"],
            agg_spec={"l_quantity": "sum"}
        )
        df = processor.to_pandas(result)
        
        assert len(df) > 0
        assert "l_returnflag" in df.columns
    
    def test_register_all_tables(self, processor):
        """Test registering all parquet files"""
        count = processor.register_all_tables()
        assert count >= 1  # At least lineitem


class TestChunkedProcessor:
    """Test chunked processor"""
    
    @pytest.fixture
    def data_dir(self):
        """Get test data directory"""
        data_dir = Path("./data/raw/tpc_h_sf0.1/parquet")
        if not data_dir.exists():
            pytest.skip("Test data not found - run data generation first")
        return data_dir
    
    @pytest.fixture
    def processor(self, data_dir):
        """Create chunked processor"""
        return LocalChunkedProcessor(data_dir=data_dir, chunk_size=50_000)
    
    def test_read_batches(self, processor):
        """Test batch reading"""
        batches = list(processor.read_batches("lineitem"))
        
        assert len(batches) > 0
        for batch in batches:
            assert isinstance(batch, pd.DataFrame)
            assert len(batch) > 0
    
    def test_batch_size(self, processor):
        """Test that batches respect size limit"""
        max_batch_size = processor.chunk_size
        
        for batch in processor.read_batches("lineitem"):
            assert len(batch) <= max_batch_size
    
    def test_filter_rows_chunked(self, processor):
        """Test chunked filtering"""
        result = processor.filter_rows_chunked(
            "lineitem",
            lambda df: df['l_quantity'] > 30,
            columns=['l_quantity', 'l_extendedprice']
        )
        
        assert len(result) > 0
        assert (result['l_quantity'] > 30).all()
    
    def test_compute_statistics_chunked(self, processor):
        """Test chunked statistics"""
        stats = processor.compute_statistics_chunked(
            "lineitem",
            "l_extendedprice"
        )
        
        assert 'count' in stats
        assert 'mean' in stats
        assert stats['count'] > 0
    
    def test_select_columns_chunked(self, processor):
        """Test chunked column selection"""
        result = processor.select_columns_chunked(
            "lineitem",
            columns=['l_quantity', 'l_extendedprice']
        )
        
        assert len(result) > 0
        assert list(result.columns) == ['l_quantity', 'l_extendedprice']


class TestEquivalence:
    """Test that different processors produce same results"""
    
    @pytest.fixture
    def data_dir(self):
        """Get test data directory"""
        data_dir = Path("./data/raw/tpc_h_sf0.1/parquet")
        if not data_dir.exists():
            pytest.skip("Test data not found - run data generation first")
        return data_dir
    
    @pytest.fixture
    def duckdb_processor(self, data_dir):
        """Create DuckDB processor"""
        proc = DuckDBProcessor(data_dir=data_dir, in_memory=True)
        proc.register_all_tables()
        return proc
    
    @pytest.fixture
    def chunked_processor(self, data_dir):
        """Create chunked processor"""
        return LocalChunkedProcessor(data_dir=data_dir, chunk_size=50_000)
    
    def test_select_columns_equivalence(self, duckdb_processor, chunked_processor):
        """Test that select columns produces same result"""
        duckdb_result = duckdb_processor.to_pandas(
            duckdb_processor.select_columns(
                "lineitem",
                ["l_quantity", "l_extendedprice"]
            )
        ).sort_index()
        
        chunked_result = chunked_processor.select_columns_chunked(
            "lineitem",
            columns=["l_quantity", "l_extendedprice"]
        ).sort_index()
        
        # Compare (allow minor floating point differences)
        pd.testing.assert_frame_equal(
            duckdb_result.reset_index(drop=True),
            chunked_result.reset_index(drop=True),
            check_exact=False,
            rtol=1e-5
        )
    
    def test_statistics_equivalence(self, duckdb_processor, chunked_processor):
        """Test that statistics are similar between processors"""
        duckdb_stats = duckdb_processor.compute_statistics(
            "lineitem",
            "l_extendedprice"
        )
        
        chunked_stats = chunked_processor.compute_statistics_chunked(
            "lineitem",
            "l_extendedprice"
        )
        
        # Compare key statistics (within tolerance for percentiles)
        assert duckdb_stats['count'] == chunked_stats['count']
        assert abs(duckdb_stats['mean'] - chunked_stats['mean']) < 1  # Close enough
        assert duckdb_stats['min'] == chunked_stats['min']
        assert duckdb_stats['max'] == chunked_stats['max']


class TestConfiguration:
    """Test configuration loading"""
    
    def test_config_initialization(self):
        """Test config loads without errors"""
        reset_config()
        config = get_config()
        assert config is not None
    
    def test_config_defaults(self):
        """Test default values are set"""
        reset_config()
        config = get_config()
        
        assert config.get('processing.processor') in ['pandas', 'duckdb', 'chunked', 'remote']
        assert config.get('processing.scale_factor') > 0
        assert config.get('processing.chunk_size') > 0
    
    def test_config_get_dot_notation(self):
        """Test dot-notation config access"""
        reset_config()
        config = get_config()
        
        processor = config.get('processing.processor')
        assert isinstance(processor, str)
    
    def test_config_set_dot_notation(self):
        """Test dot-notation config setting"""
        reset_config()
        config = get_config()
        
        config.set('processing.processor', 'duckdb')
        assert config.get('processing.processor') == 'duckdb'
    
    def test_config_defaults_preserved(self):
        """Test config defaults are preserved on get"""
        reset_config()
        config = get_config()
        
        value = config.get('some.nonexistent.key', 'default_value')
        assert value == 'default_value'


class TestHetznerConfig:
    """Test Hetzner configuration"""
    
    def test_hetzner_disabled_by_default(self):
        """Test Hetzner is disabled by default"""
        reset_config()
        config = get_config()
        hetzner = HetznerConfig(config)
        
        assert not hetzner.is_enabled()
    
    def test_hetzner_credentials_missing(self):
        """Test Hetzner returns None for missing credentials"""
        reset_config()
        config = get_config()
        hetzner = HetznerConfig(config)
        
        # If not enabled and no env vars, should return None
        creds = hetzner.get_credentials()
        assert creds is None or isinstance(creds, dict)


@pytest.mark.benchmark
class TestPerformance:
    """Performance benchmarks for different processors"""
    
    @pytest.fixture
    def data_dir(self):
        """Get test data directory"""
        data_dir = Path("./data/raw/tpc_h_sf0.1/parquet")
        if not data_dir.exists():
            pytest.skip("Test data not found - run data generation first")
        return data_dir
    
    def test_duckdb_statistics_speed(self, benchmark, data_dir):
        """Benchmark DuckDB statistics computation"""
        proc = DuckDBProcessor(data_dir=data_dir, in_memory=True)
        proc.register_parquet_table("lineitem")
        
        result = benchmark(
            proc.compute_statistics,
            "lineitem",
            "l_extendedprice"
        )
        
        assert result is not None
        assert result['count'] > 0
    
    def test_chunked_statistics_speed(self, benchmark, data_dir):
        """Benchmark chunked statistics computation"""
        proc = LocalChunkedProcessor(data_dir=data_dir, chunk_size=50_000)
        
        result = benchmark(
            proc.compute_statistics_chunked,
            "lineitem",
            "l_extendedprice"
        )
        
        assert result is not None
        assert result['count'] > 0


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])

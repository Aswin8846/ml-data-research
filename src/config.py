"""
Configuration management for ML Data Research
Loads settings from YAML files and environment variables
"""
import os
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
from dotenv import load_dotenv
from rich.console import Console

console = Console()


class Config:
    """
    Unified configuration management
    
    Priority order (highest to lowest):
    1. Environment variables
    2. .env file
    3. YAML config files
    4. Hardcoded defaults
    """
    
    # Defaults
    DEFAULTS = {
        'processing': {
            'processor': 'duckdb',  # pandas, duckdb, chunked, remote
            'scale_factor': 0.1,
            'chunk_size': 100_000,
            'max_memory_mb': 2048,
            'in_memory_db': True,
            'memory_limit_gb': 16
        },
        'data': {
            'base_dir': './data',
            'raw_dir': './data/raw',
            'output_dir': './outputs'
        },
        'hetzner': {
            'enabled': False,
            'bucket': 'ml-data-research'
        },
        'metrics': {
            'sample_interval': 0.1,
            'track_io': True,
            'track_memory': True
        }
    }
    
    def __init__(self, config_dir: Path = None):
        """
        Initialize configuration
        
        Args:
            config_dir: Directory containing config files (defaults to ./config)
        """
        # Load environment variables from .env
        load_dotenv()
        
        # Set config directory
        self.config_dir = Path(config_dir) if config_dir else Path("./config")
        
        # Initialize with defaults
        self.config = self._deep_copy(self.DEFAULTS)
        
        # Load YAML files
        self._load_yaml_files()
        
        # Override with environment variables
        self._override_from_env()
        
        console.print(f"[cyan]Configuration loaded[/cyan]")
        console.print(f"  Processor: {self.get('processing.processor')}")
        console.print(f"  Scale factor: {self.get('processing.scale_factor')}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation key
        
        Args:
            key: Dot-separated path (e.g., "processing.chunk_size")
            default: Default value if key not found
            
        Returns:
            Configuration value
            
        Example:
            processor = config.get('processing.processor')  # 'duckdb'
            chunk_size = config.get('processing.chunk_size')  # 100_000
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """
        Set configuration value by dot-notation key
        
        Args:
            key: Dot-separated path
            value: Value to set
        """
        keys = key.split('.')
        target = self.config
        
        for k in keys[:-1]:
            if k not in target:
                target[k] = {}
            target = target[k]
        
        target[keys[-1]] = value
    
    def _load_yaml_files(self):
        """Load all YAML files from config directory"""
        if not self.config_dir.exists():
            console.print(f"[yellow]Config directory not found: {self.config_dir}[/yellow]")
            return
        
        yaml_files = sorted(self.config_dir.glob("*.yaml"))
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file) as f:
                    yaml_config = yaml.safe_load(f) or {}
                
                # Merge YAML config into main config
                self._merge_dicts(self.config, yaml_config)
                console.print(f"[green]✓ Loaded {yaml_file.name}[/green]")
                
            except Exception as e:
                console.print(f"[yellow]⚠ Failed to load {yaml_file.name}: {e}[/yellow]")
    
    def _override_from_env(self):
        """
        Override config from environment variables
        
        Environment variable format: APP_CONFIG_SECTION_KEY
        Example: APP_PROCESSING_PROCESSOR=duckdb
        """
        prefix = "APP_CONFIG_"
        
        for env_var, value in os.environ.items():
            if env_var.startswith(prefix):
                # Convert APP_CONFIG_PROCESSING_PROCESSOR → processing.processor
                key = env_var[len(prefix):].lower()
                key = key.replace('_', '.')
                
                # Try to parse as YAML (handles booleans, numbers)
                try:
                    parsed_value = yaml.safe_load(value)
                except:
                    parsed_value = value
                
                self.set(key, parsed_value)
                console.print(f"[cyan]  {key} = {parsed_value} (from env)[/cyan]")
    
    def _merge_dicts(self, base: Dict, update: Dict):
        """Recursively merge update dict into base dict"""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_dicts(base[key], value)
            else:
                base[key] = value
    
    def _deep_copy(self, d: Dict) -> Dict:
        """Deep copy dictionary"""
        import copy
        return copy.deepcopy(d)
    
    def to_dict(self) -> Dict:
        """Get entire configuration as dictionary"""
        return self._deep_copy(self.config)
    
    def __repr__(self) -> str:
        """Pretty print configuration"""
        import json
        return json.dumps(self.config, indent=2, default=str)


class HetznerConfig:
    """Hetzner S3 configuration helper"""
    
    def __init__(self, config: Config):
        self.config = config
        self.enabled = config.get('hetzner.enabled', False)
    
    def is_enabled(self) -> bool:
        """Check if Hetzner is configured"""
        return self.enabled
    
    def get_credentials(self) -> Optional[Dict[str, str]]:
        """
        Get Hetzner credentials from config/env
        
        Returns:
            Dictionary with endpoint, access_key, secret_key, bucket
            or None if not configured
        """
        endpoint = os.getenv(
            'HETZNER_ENDPOINT',
            self.config.get('hetzner.endpoint')
        )
        access_key = os.getenv(
            'HETZNER_ACCESS_KEY',
            self.config.get('hetzner.access_key')
        )
        secret_key = os.getenv(
            'HETZNER_SECRET_KEY',
            self.config.get('hetzner.secret_key')
        )
        bucket = os.getenv(
            'HETZNER_BUCKET',
            self.config.get('hetzner.bucket', 'ml-data-research')
        )
        
        if not all([endpoint, access_key, secret_key]):
            return None
        
        return {
            'endpoint': endpoint,
            'access_key': access_key,
            'secret_key': secret_key,
            'bucket': bucket
        }
    
    def validate(self) -> bool:
        """Validate Hetzner configuration"""
        if not self.is_enabled():
            return True  # Not enabled, skip validation
        
        creds = self.get_credentials()
        if not creds:
            console.print("[red]✗ Hetzner enabled but credentials missing[/red]")
            return False
        
        # Try to connect
        try:
            from src.ingestion.remote_storage import HetznerStorage
            
            storage = HetznerStorage(**creds)
            storage.s3.ls(f"s3://{creds['bucket']}")
            console.print("[green]✓ Hetzner connection validated[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Hetzner validation failed: {e}[/red]")
            return False


# Global config instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get or create global config instance"""
    global _config
    if _config is None:
        _config = Config()
    return _config


def reset_config():
    """Reset global config (for testing)"""
    global _config
    _config = None


# Example usage
if __name__ == "__main__":
    config = Config()
    
    console.print("\n[bold]Configuration Summary:[/bold]")
    console.print(f"  Processor: {config.get('processing.processor')}")
    console.print(f"  Scale factor: {config.get('processing.scale_factor')}")
    console.print(f"  Chunk size: {config.get('processing.chunk_size'):,}")
    console.print(f"  Max memory: {config.get('processing.max_memory_mb')} MB")
    console.print(f"  Hetzner enabled: {config.get('hetzner.enabled')}")
    
    # Test Hetzner config
    hetzner_config = HetznerConfig(config)
    if hetzner_config.is_enabled():
        console.print(f"\n[bold]Hetzner Status:[/bold]")
        if hetzner_config.validate():
            console.print("[green]✓ Configured and working[/green]")
        else:
            console.print("[red]✗ Configured but not accessible[/red]")
    else:
        console.print(f"\n[yellow]Hetzner not enabled (set hetzner.enabled=true to use)[/yellow]")

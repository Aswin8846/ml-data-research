import click
from pathlib import Path

@click.group()
def cli():
    """ML Data Research - Data Ingestion Tool"""
    pass

@cli.command()
@click.option('--source', required=True, help='Public S3 path')
@click.option('--dataset', required=True, help='tpc-h-1gb, tpc-ds-10gb, etc.')
@click.option('--dry-run', is_flag=True, help='Show plan without executing')
def from_s3(source, dataset, dry_run):
    """Copy data from public S3 to your bucket"""
    # Implementation next

@cli.command()
@click.option('--path', required=True, type=click.Path(exists=True))
@click.option('--dataset', required=True)
@click.option('--dry-run', is_flag=True)
def from_local(path, dataset, dry_run):
    """Upload local files to S3"""
    # Implementation next

@cli.command()
@click.option('--dataset', required=True, type=click.Choice(['tpc-h', 'tpc-ds']))
@click.option('--size-gb', required=True, type=int)
@click.option('--dry-run', is_flag=True)
def from_duckdb(dataset, size_gb, dry_run):
    """Generate TPC data using DuckDB"""
    # Implementation next

@cli.command()
@click.option('--competition', required=True)
@click.option('--dry-run', is_flag=True)
def from_kaggle(competition, dry_run):
    """Download Kaggle dataset"""
    # Implementation next

if __name__ == '__main__':
    cli()
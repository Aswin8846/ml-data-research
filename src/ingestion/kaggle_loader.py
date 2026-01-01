"""
Download datasets from Kaggle
"""
import os
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
from rich.console import Console
from rich.progress import Progress
import zipfile

console = Console()

class KaggleLoader:
    """Load datasets from Kaggle"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.api = KaggleApi()
        
        # Check for credentials
        try:
            self.api.authenticate()
            console.print("[green]✓ Kaggle API authenticated[/green]")
        except Exception as e:
            console.print(f"[red]✗ Kaggle authentication failed: {e}[/red]")
            console.print("\n[yellow]Setup instructions:[/yellow]")
            console.print("1. Go to https://www.kaggle.com/account")
            console.print("2. Click 'Create New API Token'")
            console.print("3. Place kaggle.json in ~/.kaggle/")
            console.print("4. Run: chmod 600 ~/.kaggle/kaggle.json")
            raise
    
    def download_competition(self, competition: str) -> Path:
        """
        Download competition dataset
        
        Args:
            competition: Competition slug (e.g., 'titanic')
            
        Returns:
            Path to downloaded data
        """
        console.print(f"\n[bold blue]Downloading {competition}[/bold blue]")
        
        output_path = self.output_dir / competition
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Download
        with console.status(f"Downloading {competition}..."):
            self.api.competition_download_files(
                competition,
                path=output_path
            )
        
        # Unzip
        zip_file = list(output_path.glob("*.zip"))[0]
        console.print(f"Extracting {zip_file.name}...")
        
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(output_path)
        
        zip_file.unlink()  # Remove zip file
        
        # List files
        files = list(output_path.glob("*"))
        console.print(f"[green]✓ Downloaded {len(files)} files[/green]")
        for f in files:
            size_mb = f.stat().st_size / (1024 * 1024)
            console.print(f"  - {f.name} ({size_mb:.2f} MB)")
        
        return output_path
    
    def download_dataset(self, dataset: str) -> Path:
        """
        Download regular dataset (not competition)
        
        Args:
            dataset: Dataset slug (e.g., 'user/dataset-name')
        """
        console.print(f"\n[bold blue]Downloading {dataset}[/bold blue]")
        
        output_path = self.output_dir / dataset.replace('/', '_')
        output_path.mkdir(parents=True, exist_ok=True)
        
        with console.status(f"Downloading {dataset}..."):
            self.api.dataset_download_files(
                dataset,
                path=output_path,
                unzip=True
            )
        
        files = list(output_path.glob("*"))
        console.print(f"[green]✓ Downloaded {len(files)} files[/green]")
        
        return output_path


# Example usage
if __name__ == "__main__":
    loader = KaggleLoader(output_dir="./data/raw/kaggle")
    
    # Download Titanic dataset
    try:
        path = loader.download_competition("titanic")
        console.print(f"\n[green]Data saved to: {path}[/green]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
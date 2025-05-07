# modules/csv_exporter.py

import pandas as pd
from pathlib import Path

class CSVExporter:
    @staticmethod
    def export(df: pd.DataFrame, filename: str, output_dir: str = "data"):
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)

        full_path = output_dir_path / filename
        df.to_csv(full_path, index=False)
        print(f"✅ Saved: {full_path}")
from __future__ import annotations
import pandas as pd
from pathlib import Path
from transform import TRANSFORMS

RAW_DIR   = Path(r"Z:/TJ_Capital/TJ_Capital_database")
CLEAN_DIR = RAW_DIR / "updated"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)


def transform_export_csv(table: str) -> None:
    """
    1. Read raw CSV
    2. Apply the correct transform
    3. Write the cleaned CSV
    """
    raw_path   = RAW_DIR   / f"{table}.csv"
    clean_path = CLEAN_DIR / f"{table}_upd.csv"

    if not raw_path.exists():
        raise FileNotFoundError(f"Input file not found: {raw_path}")

    df_raw   = pd.read_csv(raw_path)
    df_clean = TRANSFORMS[table](df_raw)        # ← chooses the right function
    df_clean.to_csv(clean_path, index=False)
    print(f"✅ {table}: wrote {clean_path}")


def main(tables: list[str] | None = None) -> None:
    """
    Run the pipeline:
      • tables omitted  → process all six
      • tables provided → process just those
    """
    for table in tables or TRANSFORMS.keys():
        transform_export_csv(table)


if __name__ == "__main__":
    main()









from pathlib import Path
from typing import Any

import pyarrow as pa


def write_parquet(
    path_to_folder: str, table_name: str, table_data: pa.Table, **kwargs: Any
) -> None:
    path = Path(path_to_folder)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exists: {path_to_folder}")

    path_to_write: Path = path / table_name

    pa.parquet.write_table(table_data, path_to_write.with_suffix(".parquet"), **kwargs)

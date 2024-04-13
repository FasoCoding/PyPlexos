from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(
    path_to_folder: str, table_dict: dict[str, pa.Table], **kwargs: Any
) -> None:
    path = Path(path_to_folder)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exists: {path_to_folder}")

    for table_name, table_data in table_dict.items():
        path_to_write: Path = path / table_name
        pq.write_table(table_data, path_to_write.with_suffix(".parquet"), **kwargs)

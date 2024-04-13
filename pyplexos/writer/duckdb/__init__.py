from pathlib import Path
from typing import Any

import duckdb as duck
import pyarrow as pa


def write_duckdb(
    path_to_db: str, table_dict: dict[str, pa.Table], **kwargs: Any
) -> None:
    path = Path(path_to_db)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exists: {path_to_db}")

    conn = duck.connect(path.as_posix())

    for name, data in table_dict.items():
        conn.from_arrow(data).insert_into(table_name=name)

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

    db_path = path / "prg.duckdb"
    
    conn = duck.connect(db_path.as_posix())

    for name, data in table_dict.items():
        if data.num_columns == 0:
            continue 
        conn.from_arrow(data).create(table_name=name)
    
    conn.close()

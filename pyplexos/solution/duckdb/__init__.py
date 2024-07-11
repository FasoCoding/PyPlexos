from pathlib import Path
from typing import Any

import duckdb as duck

from pyplexos.solution.schema import SolutionProtocol

def write_duckdb(
    path_to_db: str,
    solution: SolutionProtocol,
    **kwargs: Any
) -> None:
    path = Path(path_to_db)
    db_name = "raw.duck"

    if "db_name" in kwargs:
        db_name: str = kwargs["db_name"]

    if not path.exists() and not path.is_dir():
        raise FileNotFoundError(f"Path does not exists: {path_to_db}")

    
    db_path = path / db_name
    
    conn = duck.connect(db_path.as_posix())

    for table_name, table_data in solution.items():
        if table_data.num_columns == 0:
            continue 
        conn.from_arrow(table_data).create(table_name=table_name)
    
    conn.close()

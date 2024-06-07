from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from pyplexos.solution.schema import SolutionProtocol


def write_parquet(
    path_to_folder: str,
    solution: SolutionProtocol,
    # table_data: pa.Table,
    **kwargs: Any,
) -> None:
    path = Path(path_to_folder)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exists: {path_to_folder}")

    for table_name, table_data in solution.items():
        path_to_write: Path = path / table_name
        pq.write_table(table_data, path_to_write.with_suffix(".parquet"), **kwargs)

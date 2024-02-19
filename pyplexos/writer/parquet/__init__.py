from pathlib import Path
from dataclasses import dataclass
from typing import Self

from pyplexos.protocols import PlexosReaderProtocol

import polars as pl


@dataclass
class ParquetWriter:
    path_to_dir: Path

    @classmethod
    def parquet_writer(cls, path_to_dir: str) -> Self:
        temp_path = Path(path_to_dir)
        if not temp_path.exists():
            raise ValueError(f"Path: {path_to_dir} does not exists.")
        return cls(path_to_dir=temp_path)

    def write(self, solution: PlexosReaderProtocol) -> None:
        for table_name, table_data in solution.get_solution_model.items():
            if table_data is None:
                continue
            file = self.path_to_dir / table_name
            pl.from_dicts(table_data).write_parquet(file.with_suffix(".parquet"))

        for table_name, table_data in solution.get_solution_data.items():
            if table_data is None:
                continue
            file = self.path_to_dir / table_name
            pl.from_dict(table_data).write_parquet(file.with_suffix(".parquet"))

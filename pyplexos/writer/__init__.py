from pathlib import Path
from dataclasses import dataclass
from typing import Protocol, Self

import polars as pl

from pyplexos.writer.schema.duck_model import DuckModel

class PlexosReaderProtocol(Protocol):
    def get_solution_model(self) -> dict[str, list[dict]]:
        pass
    def get_solution_data(self) -> dict[str, dict[str, list]]:
        pass

@dataclass
class PlexosWriter:

    output_model: DuckModel = None

    @classmethod
    def duck_writer(
        cls,
        path_to_dir: str,
        db_name: str = "pcp.ddb",
        mode: str = "replace"
    ) -> Self:
        temp_path = Path(path_to_dir)
        if not temp_path.exists():
            raise ValueError(f"Path: {path_to_dir} does not exists.")
        
        return cls(
            output_model=DuckModel.create_medallion_duck(temp_path / db_name, mode=mode)
        )
        

    def to_parquet(plexos_solution, path_to_dir: str, period_to_extract: str = "interval") -> None:
        """
        Transform a plexos zip solution to parquet files, given a path.

        Args:
            path_to_dir (str): path to directory to save parquets.
            period_to_extract (str): time period to extract.

        Returns:
            None.
        """

        path = Path(path_to_dir)
        if not path.exists():
            raise ValueError(f"Path: {path_to_dir} does not exists.")
        print(f"Escribiendo parquets en {path}")

        for table_name, table_data in plexos_solution.solution_model.model_dump(
            by_alias=True
        ).items():
            if table_data is None:
                continue
            file = path / table_name
            pl.from_dicts(table_data).write_parquet(file.with_suffix(".parquet"))

        for table_name, table_data in plexos_solution.solution_data.model_dump(
            by_alias=True
        ).items():
            if table_data is None:
                continue
            file = path / table_name
            pl.from_dict(table_data).write_parquet(file.with_suffix(".parquet"))


    def to_duck(self, solution: PlexosReaderProtocol) -> None:
        """
        Transform a plexos zip solution to duckdb, given a path.

        Args:
            path_to_dir (str): path to directory to save parquets.

        Returns:
            None.
        """

        self.output_model.duck_bronze_schema()
        self.output_model.duck_silver_schema()

        for table_name, table_data in solution.get_solution_model.items():
            if table_data is None:
                continue
            self.output_model.conn.from_arrow(pl.from_dicts(table_data).to_arrow()).insert_into(
                f"bronze.{table_name}"
            )

        for table_name, table_data in solution.get_solution_data.items():
            if table_data is None:
                continue
            self.output_model.conn.from_arrow(
                pl.from_dict(table_data).to_arrow()
                ).insert_into(f"bronze.{table_name}")

        self.output_model.conn.close()

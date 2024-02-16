import importlib.resources as sql_resources

from pathlib import Path
from dataclasses import dataclass

from pyplexos.Protocols import PlexosReaderProtocol

import duckdb as duck
import polars as pl


@dataclass
class DuckWriter:
    conn: duck.DuckDBPyConnection

    @classmethod
    def create_medallion_duck(cls, path_to_db: Path, db_name: str, mode: str = "replace") -> duck.DuckDBPyConnection:
        """Create a duckdb connection from plexos solution.

        Returns:
            duck.DuckDBPyConnection: Connection to duckdb.
        """
        temp_path = Path(path_to_db)
        if not temp_path.exists():
            raise ValueError(f"Path: {path_to_db} does not exists.")
        path = temp_path / db_name

        if path.exists() and mode == "replace":
            path.unlink()
        return cls(duck.connect(path.as_posix()))

    def create_schema(self) -> None:
        sql = sql_resources.files("pyplexos.writer.model.sql")
        bronze_schema = (sql / "bronze.sql").read_text()
        siler_schema = (sql / "silver.sql").read_text()

        self.conn.sql(bronze_schema)
        self.conn.sql(siler_schema)
        

    def write(self, solution: PlexosReaderProtocol) -> None:
        self.create_schema()
        for table_name, table_data in solution.get_solution_model.items():
            if table_data is None:
                continue
            self.conn.from_arrow(
                pl.from_dicts(table_data).to_arrow()
            ).insert_into(f"bronze.{table_name}")

        for table_name, table_data in solution.get_solution_data.items():
            if table_data is None:
                continue
            self.conn.from_arrow(
                pl.from_dict(table_data).to_arrow()
            ).insert_into(f"bronze.{table_name}")
        self.conn.close()

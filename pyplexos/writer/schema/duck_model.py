import importlib.resources as sql_resources

from pathlib import Path
from dataclasses import dataclass

import duckdb as duck

@dataclass
class DuckModel:
    conn: duck.DuckDBPyConnection

    @classmethod
    def create_medallion_duck(cls, path: Path, mode: str = "replace") -> duck.DuckDBPyConnection:
        """Create a duckdb connection from plexos solution.

        Returns:
            duck.DuckDBPyConnection: Connection to duckdb.
        """
        if path.exists() and mode == "replace":
            path.unlink()
        return cls(duck.connect(path.as_posix()))

    def duck_bronze_schema(self) -> None:
        sql = sql_resources.files("pyplexos.writer.schema.sql")
        sql_str = (sql / "bronze.sql").read_text()
        self.conn.sql(sql_str)

    def duck_silver_schema(self) -> None:
        sql = sql_resources.files("pyplexos.writer.schema.sql")
        sql_str = (sql / "silver.sql").read_text()
        self.conn.sql(sql_str)

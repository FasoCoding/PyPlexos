from functools import partial
from typing import Callable

import pyarrow as pa

from pyplexos.writer.duckdb import DuckWriter
from pyplexos.writer.parquet import write_parquet

WriterFunction = Callable[[str, pa.Table], None]


def writer(writer_type: str, path: str) -> WriterFunction:
    """
    Returns a partially applied writer function based on the specified writer type.
    This function is designed to dynamically select and configure a writer function that
    is specialized for different types of data target (e.g., Duckdb files, ACCDB files, parquet files).

    Parameters:
    - writer_type (str): The type of reader to return. Current supported types include "duckdb" and "parquet".
                         More types like "accdb" can be added later.
    - path (str): The file path or connection string to be used by the writer function.

    Returns:
    - WriterFunction: A callable that takes a path string, table name string, Arrow table, **kwargs specific
                      for each functino and returns a None.
                      This callable is a partially applied function that is ready to be executed
                      with its required arguments.

    Raises:
    - ValueError: If no writer function is available for the specified `writer_type`.

    Example:
    - To get a reader function for ZIP files:
        parquet_writer = writer("parquet", "path/to/folder")
        parquet_writer("t_data_0", PlexosSolution.t_data_0)

    Note:
    - This function uses `functools.partial` to pre-configure the reader functions with the
      necessary `path`, making the returned function ready to be called without any parameters.
    """
    match writer_type.lower():
        case "duckdb":
            return partial(write_duckdb, path)
        case "parquet":
            return partial(write_parquet, path)
        case "accdb":
            return partial(write_accdb, path)
        case _:
            raise ValueError(f"No writer for {writer_type}")

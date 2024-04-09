from functools import partial
from typing import Callable

from pyplexos.schema import PlexosSolution
from pyplexos.reader.zip import read_zip
from pyplexos.reader.accdb import read_accdb

ReaderFunction = Callable[[], PlexosSolution]


def reader(reader_type: str, path: str) -> ReaderFunction:
    """
    Returns a partially applied reader function based on the specified reader type.
    This function is designed to dynamically select and configure a reader function that
    is specialized for different types of data sources (e.g., ZIP files, ACCDB files).

    Parameters:
    - reader_type (str): The type of reader to return. Current supported types include "zip".
                         More types like "accdb" can be added later.
    - path (str): The file path or connection string to be used by the reader function.

    Returns:
    - ReaderFunction: A callable that takes a path string and returns a PlexosSolution object.
                      This callable is a partially applied function that is ready to be executed
                      with its required arguments.

    Raises:
    - ValueError: If no reader function is available for the specified `reader_type`.

    Example:
    - To get a reader function for ZIP files:
        zip_reader = reader("zip", "path/to/file.zip")
        solution = zip_reader()

    Note:
    - This function uses `functools.partial` to pre-configure the reader functions with the
      necessary `path`, making the returned function ready to be called without any parameters.
    """
    match reader_type.lower():
        case "zip":
            return partial(read_zip, path)
        case "accdb":
            return partial(read_accdb, path)
        case _:
            raise ValueError(f"No reader for {reader_type}")

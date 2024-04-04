from typing import Self
from dataclasses import dataclass

from pyplexos.reader.zip import PlexosZipReader
from pyplexos.protocols import PlexosReaderProtocol


@dataclass
class PlexosReader:
    """
    A class for reading plexos data from a ZIP solution file containing XML
    and binary files. On creation checks for existing files.
    """

    solution_reader: PlexosReaderProtocol

    @classmethod
    def zip_reader(cls, path: str) -> Self:
        """
        Initialize a PlexosZipReader object from a zip file.

        Args:
            zip_file_path (str): The path to the ZIP file.
        """
        return cls(solution_reader=PlexosZipReader.read(zip_path=path))

    # TODO. check si un yield se puede usar para bajar computo.
    #@property
    #def get_solution_model(self) -> dict[str, list[dict[str, Any]]]:
    #    return self.solution_reader.get_solution_model

    # TODO. check si un yield se puede usar para bajar computo.
    #@property
    #def get_solution_data(self) -> dict[str, dict[str, list[Any]]]:
    #    return self.solution_reader.get_solution_data

    #@property
    #def get_initial_datetime(self) -> datetime:
    #    return self.solution_reader.get_initial_datetime

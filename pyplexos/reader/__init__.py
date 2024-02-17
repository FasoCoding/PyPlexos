from typing import Self
from dataclasses import dataclass
from datetime import datetime

from pyplexos.reader.zip.zip_reader import PlexosZipReader
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
        return cls(solution_reader=PlexosZipReader.from_zip(zip_path=path))

    # TODO. check si un yield se puede usar para bajar computo.
    @property
    def get_solution_model(self) -> dict[str, list[dict]]:
        return self.solution_model.model_dump(by_alias=True)

    # TODO. check si un yield se puede usar para bajar computo.
    @property
    def get_solution_data(self) -> dict[str, dict[str, list]]:
        return self.solution_data.model_dump(by_alias=True)

    @property
    def get_min_datetime(self) -> datetime:
        return self.solution_model.t_period_0[0].datetime

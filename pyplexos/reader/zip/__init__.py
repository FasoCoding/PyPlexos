from pathlib import Path
from zipfile import ZipFile
from typing import Self
from dataclasses import dataclass
from datetime import datetime

from pyplexos.reader.zip.xml_model import SolutionModel
from pyplexos.reader.zip.bin_model import SolutionData


@dataclass
class PlexosZipReader:
    """
    A class for reading plexos data from a ZIP solution file containing XML
    and binary files. On creation checks for existing files.
    """
    solution_model: SolutionModel = None
    solution_data: SolutionData = None

    @classmethod
    def from_zip(cls, zip_path: str) -> Self:
        """
        Initialize a PlexosZipReader object from a zip file.

        Args:
            zip_file_path (str): The path to the ZIP file.
        """

        # Check if the file exists
        zip_file_path = Path(zip_path)
        if not zip_file_path.exists():
            raise ValueError("The specified ZIP file path does not exist.")

        # get the xml and bin files names
        with ZipFile(zip_file_path, "r") as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.startswith("Model") and file_name.endswith(".xml"):
                    xml_file_name = file_name
                if file_name == "t_data_0.BIN":
                    bin_file_name = file_name

        # check if the files were found or raise error.
        if xml_file_name is None:
            raise ValueError("No model file on zip solution.")
        if bin_file_name is None:
            raise ValueError("No BIN files on zip solution.")

        # extract the model and binary data
        with ZipFile(zip_file_path, "r") as zip_ref:
            with zip_ref.open(xml_file_name) as xml_file:
                content = xml_file.read().decode("utf-8")
                solution_model = SolutionModel.from_xml(content)
            with zip_ref.open(bin_file_name) as bin_file:
                binary_data = bin_file.read()
                solution_data = SolutionData.from_binary(
                    solution_model.t_key_index, binary_data
                )

        return cls(
            solution_model=solution_model,
            solution_data=solution_data,
        )

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

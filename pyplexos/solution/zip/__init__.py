from pathlib import Path
from typing import Any
from zipfile import ZipFile

from pyplexos.solution.zip.xml import SolutionModel
from pyplexos.solution.zip.bin import SolutionData


def extract_zip_data(path: Path) -> dict[str, Any]:
    """
    Extracts and parses data from the specified ZIP file path. This function looks for specific XML and binary
    files within the ZIP archive, parses them, and consolidates the data into a dictionary.

    Parameters:
    - path (Path): A pathlib.Path object pointing to the ZIP file to be processed.

    Returns:
    - dict[str, Any]: A dictionary containing the parsed data from the XML and binary files.
      This includes model and solution data structured according to their respective schemas.

    Raises:
    - XMLFileError: If the required XML file is not found in the ZIP archive.
    - BinFileError: If the required binary data file (t_data_0.BIN) is not found in the ZIP archive.

    The function first identifies the XML and binary files required for the solution data.
    If either file is missing, it raises an error. After successfully locating the files, it reads and
    parses the XML to a solution model and the binary data to solution data, which are then merged into
    a single dictionary that gets returned.
    """
    xml_file_name: str = ""
    bin_file_name: str = ""

    # get the xml and bin files names
    with ZipFile(path, "r") as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.startswith("Model") and file_name.endswith(".xml"):
                xml_file_name = file_name
            if file_name == "t_data_0.BIN":
                bin_file_name = file_name

        # check if the files were found or raise error.
        if xml_file_name == "":
            raise FileNotFoundError("no existe archivo .xml")
        if bin_file_name == "":
            raise FileNotFoundError("no existe archivo .bin")

        # Open XML
        with zip_ref.open(xml_file_name) as xml_file:
            solution_model = SolutionModel.from_xml(xml_file)

        # Open BIN
        with zip_ref.open(bin_file_name) as bin_file:
            binary_data = bin_file.read()
            solution_data = SolutionData.from_binary(
                solution_model.t_key_index, binary_data
            )

    return solution_model.model_dump(by_alias=True) | solution_data.model_dump(
        by_alias=True
    )

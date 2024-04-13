from pathlib import Path
from typing import Any
from zipfile import ZipFile

import pyarrow as pa

from pyplexos.model import PlexosSolution, SolutionSchema
from pyplexos.reader.zip.bin_model import SolutionData
from pyplexos.reader.zip.errors import (
    BinFileError,
    XMLFileError,
)
from pyplexos.reader.zip.xml_model import SolutionModel


def read_zip(zip_file_path: str) -> PlexosSolution:
    """
    Reads data from a ZIP file specified by `zip_file_path` and constructs a PlexosSolution object.

    This function extracts various tables (e.g., t_unit, t_band) from the ZIP file, parses them,
    and constructs a PlexosSolution data structure using these tables.

    Parameters:
    - zip_file_path (str): The file path of the ZIP file containing the Plexos solution data.

    Returns:
    - PlexosSolution: An object populated with data tables extracted and parsed from the ZIP file.

    Raises:
    - FileNotFoundError: If the ZIP file specified does not exist.
    - XMLFileError: If the XML file required for extraction is not found in the ZIP file.
    - BinFileError: If the binary data file required is not found in the ZIP file.
    """
    path = Path(zip_file_path)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exists: {zip_file_path}")

    data = extract_zip_data(path=path)

    return PlexosSolution(
        t_unit=pa.Table.from_pylist(data[SolutionSchema.t_unit.value]),
        t_band=pa.Table.from_pylist(data[SolutionSchema.t_band.value]),
        t_category=pa.Table.from_pylist(data[SolutionSchema.t_category.value]),
        t_class=pa.Table.from_pylist(data[SolutionSchema.t_class.value]),
        t_class_group=pa.Table.from_pylist(data[SolutionSchema.t_class_group.value]),
        t_collection=pa.Table.from_pylist(data[SolutionSchema.t_collection.value]),
        t_config=pa.Table.from_pylist(data[SolutionSchema.t_config.value]),
        t_key=pa.Table.from_pylist(data[SolutionSchema.t_key.value]),
        t_membership=pa.Table.from_pylist(data[SolutionSchema.t_membership.value]),
        t_model=pa.Table.from_pylist(data[SolutionSchema.t_model.value]),
        t_object=pa.Table.from_pylist(data[SolutionSchema.t_object.value]),
        t_period_0=pa.Table.from_pylist(data[SolutionSchema.t_period_0.value]),
        t_period_1=pa.Table.from_pylist(data[SolutionSchema.t_period_1.value]),
        t_period_2=pa.Table.from_pylist(data[SolutionSchema.t_period_2.value]),
        t_period_3=pa.Table.from_pylist(data[SolutionSchema.t_period_3.value]),
        t_period_4=pa.Table.from_pylist(data[SolutionSchema.t_period_4.value]),
        t_period_6=pa.Table.from_pylist(data[SolutionSchema.t_period_6.value]),
        t_period_7=pa.Table.from_pylist(data[SolutionSchema.t_period_7.value]),
        t_phase_1=pa.Table.from_pylist(data[SolutionSchema.t_phase_1.value]),
        t_phase_2=pa.Table.from_pylist(data[SolutionSchema.t_phase_2.value]),
        t_phase_3=pa.Table.from_pylist(data[SolutionSchema.t_phase_3.value]),
        t_phase_4=pa.Table.from_pylist(data[SolutionSchema.t_phase_4.value]),
        t_sample=pa.Table.from_pylist(data[SolutionSchema.t_sample.value]),
        t_timeslice=pa.Table.from_pylist(data[SolutionSchema.t_timeslice.value]),
        t_key_index=pa.Table.from_pylist(data[SolutionSchema.t_key_index.value]),
        t_property=pa.Table.from_pylist(data[SolutionSchema.t_property.value]),
        t_attribute_data=pa.Table.from_pylist(
            data[SolutionSchema.t_attribute_data.value]
        ),
        t_attribute=pa.Table.from_pylist(data[SolutionSchema.t_attribute.value]),
        t_sample_weight=pa.Table.from_pylist(
            data[SolutionSchema.t_sample_weight.value]
        ),
        t_custom_column=pa.Table.from_pylist(
            data[SolutionSchema.t_custom_column.value]
        ),
        t_memo_object=pa.Table.from_pylist(data[SolutionSchema.t_memo_object.value]),
        t_object_meta=pa.Table.from_pylist(data[SolutionSchema.t_object_meta.value]),
        t_data_0=pa.Table.from_pydict(data[SolutionSchema.t_data_0.value]),
    )


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
            raise XMLFileError
        if bin_file_name == "":
            raise BinFileError

        # Open XML
        with zip_ref.open(xml_file_name) as xml_file:
            content: str = xml_file.read().decode("utf-8")
            solution_model = SolutionModel.from_xml(content)
        # Open BIN
        with zip_ref.open(bin_file_name) as bin_file:
            binary_data = bin_file.read()
            solution_data = SolutionData.from_binary(
                solution_model.t_key_index, binary_data
            )
    return solution_model.model_dump(by_alias=True) | solution_data.model_dump(
        by_alias=True
    )

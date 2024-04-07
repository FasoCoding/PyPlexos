from pathlib import Path
from zipfile import ZipFile
from datetime import datetime
from enum import Enum

import pyarrow as pa

from pyplexos.reader.zip.xml_model import SolutionModel
from pyplexos.reader.zip.bin_model import SolutionData
from pyplexos.reader.zip.errors import (
    XMLFileError,
    BinFileError,
    ZipFileError,
    TableNotFoundError,
)

class SolutionSchema(Enum):
    t_unit: str = "t_unit"
    t_band: str = "t_band"
    t_category: str = "t_category"
    t_class: str = "t_class"
    t_class_group: str = "t_class_group"
    t_collection: str = "t_collection"
    t_config: str = "t_config"
    t_key: str = "t_key"
    t_membership: str = "t_membership"
    t_model: str = "t_model"
    t_object: str = "t_object"
    t_period_0: str = "t_period_0"
    t_period_1: str = "t_period_1"
    t_period_2: str = "t_period_2"
    t_period_3: str = "t_period_3"
    t_period_4: str = "t_period_4"
    t_period_6: str = "t_period_6"
    t_period_7: str = "t_period_7"
    t_phase_1: str = "t_phase_1"
    t_phase_2: str = "t_phase_2"
    t_phase_3: str = "t_phase_3"
    t_phase_4: str = "t_phase_4"
    t_sample: str = "t_sample"
    t_timeslice: str = "t_timeslice"
    t_key_index: str = "t_key_index"
    t_property: str = "t_property"
    t_attribute_data: str = "t_attribute_data"
    t_attribute: str = "t_attribute"
    t_sample_weight: str = "t_sample_weight"
    t_custom_column: str = "t_custom_column"
    t_memo_object: str = "t_memo_object"
    t_object_meta: str = "t_object_meta"
    t_data_0: str = "t_data_0"


class PlexosZipReader:
    """
    A class for reading plexos data from a ZIP solution file containing XML
    and binary files. On creation checks for existing files.
    """

    def __init__(self, path: str) -> None:
        """
        Initializes an instance of PlexosZipReader, preparing it to read Plexos data
        from a ZIP file containing XML and binary files. Upon creation, it checks
        for the existence of the necessary files within the ZIP file.

        The initialization process involves:
        - Verifying that the ZIP file exists at the specified path.
        - Searching within the ZIP file for a valid XML file that starts with "Model" and ends in ".xml",
        and a binary file specifically named "t_data_0.BIN".
        - Preparing the instance for reading these files, facilitating the extraction and
        manipulation of Plexos solution data.

        Args:
            path (str): The path to the ZIP file containing the Plexos solution to be read.
                        Must be a valid path to an existing .zip file.

        Raises:
            ZipFileError: Thrown if the ZIP file specified in `path` does not exist or cannot be opened.
            XMLFileError: Thrown if a valid XML file (starting with "Model" and ending in ".xml") is not found
                        within the ZIP file.
            BinFileError: Thrown if the binary file "t_data_0.BIN" is not found within the ZIP file.

        Example:
            To read a Plexos solution from a ZIP file:

                reader = PlexosZipReader("/path/to/plexos_solution.zip")

            Make sure to handle exceptions properly to avoid runtime errors.
        """
        zip_file_path: Path = Path(path)
        xml_file_name: str = ""
        bin_file_name: str = ""

        if not zip_file_path.exists():
            raise ZipFileError

        # get the xml and bin files names
        with ZipFile(zip_file_path, "r") as zip_ref:
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
        
        self._zip_file_path: Path = zip_file_path
        self._xml_file_name: str = xml_file_name
        self._bin_file_name: str = bin_file_name


    def read(self) -> None:
        """
        Reads the Plexos solution data from the previously validated ZIP file, extracting and
        processing both XML and binary files contained within. This method populates the instance
        with various data structures representing the solution model and data extracted from
        these files, enabling further manipulation and analysis.

        The read process includes:
        - Opening and reading the XML file identified during initialization to construct a
        SolutionModel object, which represents the solution's model structure.
        - Opening and reading the binary file "t_data_0.BIN" to construct a SolutionData object,
        which contains the solution's time-series and other data, linked to the model via
        keys and indices.
        - Populating the instance with Apache Arrow Tables constructed from the solution model
        and data for efficient data manipulation and analysis.

        This method is intended to be called after the instance has been initialized and before
        any specific data extraction or manipulation tasks are performed. It prepares the
        instance by loading all relevant data into memory.

        Side Effects:
        - Populates the instance with numerous properties representing different aspects of the
        solution, such as units, categories, classes, configurations, and time periods, among
        others, all stored as Apache Arrow Tables for efficient processing.
        - Sets internal metadata properties such as the minimum datetime of the solution and
        the server name extracted from the solution model.

        Raises:
            ValueError: If there is an issue with reading or processing the files, such as missing
                        data or unexpected file formats.

        Example:
            Assuming `reader` is an instance of PlexosZipReader initialized with a valid ZIP file path:

                reader.read()

            After calling `read`, the instance is populated with the solution data, ready for
            analysis and manipulation.
        """
        with ZipFile(self._zip_file_path, "r") as zip_ref:
            with zip_ref.open(self._xml_file_name) as xml_file:
                content: str = xml_file.read().decode("utf-8")
                solution_model = SolutionModel.from_xml(content)
            with zip_ref.open(self._bin_file_name) as bin_file:
                binary_data = bin_file.read()
                solution_data = SolutionData.from_binary(
                    solution_model.t_key_index, binary_data
                )

        self._prg_datetime: datetime = self._get_min_datetime(solution_model=solution_model)
        self._prg_server: str = self._get_server_name(solution_model=solution_model)

        # generate the dict equivalent por transformations
        solution_model_dict = solution_model.model_dump(by_alias=True)
        solution_data_dict = solution_data.model_dump(by_alias=True)

        # List of tables for plexos solution.
        self._t_unit: pa.Table = pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_unit.value])
        self._t_band= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_band.value])
        self._t_category= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_category.value])
        self._t_class= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_class.value])
        self._t_class_group= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_class_group.value])
        self._t_collection= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_collection.value])
        self._t_config= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_config.value])
        self._t_key= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_key.value])
        self._t_membership= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_membership.value])
        self._t_model= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_model.value])
        self._t_object= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_object.value])
        self._t_period_0= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_period_0.value])
        self._t_period_1 = pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_period_1.value])
        self._t_period_2= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_period_2.value])
        self._t_period_3= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_period_3.value])
        self._t_period_4= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_period_4.value])
        self._t_period_6= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_period_6.value])
        self._t_period_7= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_period_7.value])
        self._t_phase_1= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_phase_1.value])
        self._t_phase_2= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_phase_2.value])
        self._t_phase_3= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_phase_3.value])
        self._t_phase_4= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_phase_4.value])
        self._t_sample= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_sample.value])
        self._t_timeslice= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_timeslice.value])
        self._t_key_index= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_key_index.value])
        self._t_property= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_property.value])
        self._t_attribute_data= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_attribute_data.value])
        self._t_attribute= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_attribute.value])
        self._t_sample_weight= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_sample_weight.value])
        self._t_custom_column= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_custom_column.value])
        self._t_memo_object= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_memo_object.value])
        self._t_object_meta= pa.Table.from_pylist(solution_model_dict[SolutionSchema.t_object_meta.value])
        self._t_data_0= pa.Table.from_pydict(solution_data_dict[SolutionSchema.t_data_0.value])


    def _get_min_datetime(self, solution_model: SolutionModel) -> datetime:
        """
        Determines the minimum datetime present in the solution model, which represents
        the starting point of the simulation. This method is essential for understanding
        the temporal scope of the Plexos solution data.

        The method iterates through various phases of the solution model, identifying the
        earliest interval (phase) based on available data. It leverages the hierarchical
        structure of time-related data within the solution model to find the minimum datetime.

        Args:
            solution_model (SolutionModel): An object representing the solution's model structure,
                                            which contains detailed information about different
                                            phases and periods of the simulation.

        Returns:
            datetime: The earliest datetime found within the solution model's periods and phases,
                    indicating the start of the simulation period. This datetime is critical
                    for aligning the solution data with real-world timeframes for analysis.

        Notes:
            - The method assumes that the solution model has a proper temporal hierarchy and
            structure as expected from a Plexos solution file. The structure is check with Pydantic.
            - This is a private method intended for internal use by the PlexosZipReader class to
            facilitate data preparation and analysis tasks. It should not be called directly
            from outside the class.

        Example:
            This method is not intended to be called directly by users. It is used internally
            within the `PlexosZipReader` class, as part of the data preparation process:

                min_datetime = self._get_min_datetime(solution_model)

            Here, `solution_model` is an instance of `SolutionModel`, and `min_datetime` will
            hold the earliest datetime value based on the model's data.
        """
        # Check the minimum interval to get datetime for solution.
        if solution_model.t_phase_4 is not None:
            min_interval = solution_model.t_phase_4[0].interval_id - 1
        elif solution_model.t_phase_3 is not None:
            min_interval = solution_model.t_phase_3[0].interval_id - 1
        elif solution_model.t_phase_2 is not None:
            min_interval = solution_model.t_phase_2[0].interval_id - 1
        elif solution_model.t_phase_1 is not None:
            min_interval = solution_model.t_phase_1[0].interval_id - 1
        else:
            min_interval = 0 # TODO. make something better XD.

        return solution_model.t_period_0[min_interval].datetime
    
    # TODO. Check should be on XML validation.
    def _get_server_name(self, solution_model: SolutionModel) -> str:
        """
        Extracts the server name from the solution model where the Plexos simulation was executed.
        This information is crucial for tracking and documenting the computational environment
        used for the simulation, which can be valuable for replication or auditing purposes.

        The method searches through the configuration settings within the solution model for
        an entry corresponding to the computer or server name. If found, it returns the name
        as a string. If no such entry is found, a ValueError is raised indicating the absence
        of server name information.

        Args:
            solution_model (SolutionModel): An object representing the solution's model structure,
                                            specifically containing configuration settings that
                                            include the server name.

        Returns:
            str: The name of the server on which the Plexos simulation was executed.

        Raises:
            ValueError: If the solution model does not contain an entry for the server name within
                        the configuration settings, indicating either a missing or malformed
                        configuration.

        Notes:
            - The server name extraction is based on the assumption that the configuration settings
            within the solution model are properly structured and contain an entry named 'Computer'
            that holds the server name.
            - This is a private method intended for internal use by the PlexosZipReader class to
            enhance understanding of the simulation's execution environment. It should not be
            called directly from outside the class.

        Example:
            This method is not intended to be called directly by users. It is used internally
            within the `PlexosZipReader` class, for example:

                server_name = self._get_server_name(solution_model)

            Here, `solution_model` is an instance of `SolutionModel`, and `server_name` will
            contain the name of the server as extracted from the model's configuration settings.
        """
        prg_server = next((obj.value for obj in solution_model.t_config if obj.element_ == 'Computer'), None)
        if prg_server is None:
            raise ValueError("Error on t_config table. No Computer data")
        return prg_server


    def list_tables(self) -> list[str]:
        return [table.value for table in SolutionSchema]
    
    def table(self, table_name: str) -> pa.Table:

        if table_name in self.list_tables():
            table = SolutionSchema[table_name]
        else:
            raise KeyError(f"Table {table_name} not in schema")

        match table:
            case SolutionSchema.t_unit: 
                return self._t_unit
            case SolutionSchema.t_band: 
                return self._t_band
            case SolutionSchema.t_category: 
                return self._t_category
            case SolutionSchema.t_class: 
                return self._t_class
            case SolutionSchema.t_class_group: 
                return self._t_class_group
            case SolutionSchema.t_collection: 
                return self._t_collection
            case SolutionSchema.t_config: 
                return self._t_config
            case SolutionSchema.t_key: 
                return self._t_key
            case SolutionSchema.t_membership: 
                return self._t_membership
            case SolutionSchema.t_model: 
                return self._t_model
            case SolutionSchema.t_object: 
                return self._t_object
            case SolutionSchema.t_period_0: 
                return self._t_period_0
            case SolutionSchema.t_period_1: 
                return self._t_period_1
            case SolutionSchema.t_period_2: 
                return self._t_period_2
            case SolutionSchema.t_period_3: 
                return self._t_period_3
            case SolutionSchema.t_period_4: 
                return self._t_period_4
            case SolutionSchema.t_period_6: 
                return self._t_period_6
            case SolutionSchema.t_period_7: 
                return self._t_period_7
            case SolutionSchema.t_phase_1: 
                return self._t_phase_1
            case SolutionSchema.t_phase_2: 
                return self._t_phase_2
            case SolutionSchema.t_phase_3: 
                return self._t_phase_3
            case SolutionSchema.t_phase_4: 
                return self._t_phase_4
            case SolutionSchema.t_sample: 
                return self._t_sample
            case SolutionSchema.t_timeslice: 
                return self._t_timeslice
            case SolutionSchema.t_key_index: 
                return self._t_key_index
            case SolutionSchema.t_property: 
                return self._t_property
            case SolutionSchema.t_attribute_data: 
                return self._t_attribute_data
            case SolutionSchema.t_attribute: 
                return self._t_attribute
            case SolutionSchema.t_sample_weight: 
                return self._t_sample_weight
            case SolutionSchema.t_custom_column: 
                return self._t_custom_column
            case SolutionSchema.t_memo_object: 
                return self._t_memo_object
            case SolutionSchema.t_object_meta: 
                return self._t_object_meta
            case SolutionSchema.t_data_0: 
                return self._t_data_0
        #return getattr(self, "_" + table.value)

#def read_zip(path_to_zip: str) -> None:
#    
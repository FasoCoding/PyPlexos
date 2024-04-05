from pathlib import Path
from zipfile import ZipFile
from datetime import datetime

import pyarrow as pa

from pyplexos.reader.zip.xml_model import SolutionModel
from pyplexos.reader.zip.bin_model import SolutionData
from pyplexos.reader.zip.errors import XMLFileError, BinFileError, ZipFileError

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
        self._t_unit: pa.Table = pa.Table.from_pylist(solution_model_dict["t_unit"])
        self._t_band= pa.Table.from_pylist(solution_model_dict["t_band"])
        self._t_category= pa.Table.from_pylist(solution_model_dict["t_category"])
        self._t_class= pa.Table.from_pylist(solution_model_dict["t_class"])
        self._t_class_group= pa.Table.from_pylist(solution_model_dict["t_class_group"])
        self._t_collection= pa.Table.from_pylist(solution_model_dict["t_collection"])
        self._t_config= pa.Table.from_pylist(solution_model_dict["t_config"])
        self._t_key= pa.Table.from_pylist(solution_model_dict["t_key"])
        self._t_membership= pa.Table.from_pylist(solution_model_dict["t_membership"])
        self._t_model= pa.Table.from_pylist(solution_model_dict["t_model"])
        self._t_object= pa.Table.from_pylist(solution_model_dict["t_object"])
        self._t_period_0= pa.Table.from_pylist(solution_model_dict["t_period_0"])
        self._t_period_1 = pa.Table.from_pylist(solution_model_dict["t_period_1"])
        self._t_period_2= pa.Table.from_pylist(solution_model_dict["t_period_2"])
        self._t_period_3= pa.Table.from_pylist(solution_model_dict["t_period_3"])
        self._t_period_4= pa.Table.from_pylist(solution_model_dict["t_period_4"])
        self._t_period_6= pa.Table.from_pylist(solution_model_dict["t_period_6"])
        self._t_period_7= pa.Table.from_pylist(solution_model_dict["t_period_7"])
        self._t_phase_1= pa.Table.from_pylist(solution_model_dict["t_phase_1"])
        self._t_phase_2= pa.Table.from_pylist(solution_model_dict["t_phase_2"])
        self._t_phase_3= pa.Table.from_pylist(solution_model_dict["t_phase_3"])
        self._t_phase_4= pa.Table.from_pylist(solution_model_dict["t_phase_4"])
        self._t_sample= pa.Table.from_pylist(solution_model_dict["t_sample"])
        self._t_timeslice= pa.Table.from_pylist(solution_model_dict["t_timeslice"])
        self._t_key_index= pa.Table.from_pylist(solution_model_dict["t_key_index"])
        self._t_property= pa.Table.from_pylist(solution_model_dict["t_property"])
        self._t_attribute_data= pa.Table.from_pylist(solution_model_dict["t_attribute_data"])
        self._t_attribute= pa.Table.from_pylist(solution_model_dict["t_attribute"])
        self._t_sample_weight= pa.Table.from_pylist(solution_model_dict["t_sample_weight"])
        self._t_custom_column= pa.Table.from_pylist(solution_model_dict["t_custom_column"])
        self._t_memo_object= pa.Table.from_pylist(solution_model_dict["t_memo_object"])
        self._t_object_meta= pa.Table.from_pylist(solution_model_dict["t_object_meta"])
        self._t_data_0= pa.Table.from_pydict(solution_data_dict["t_data_0"])


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


    #def list_tables(self) -> list[str]:
    #    ...
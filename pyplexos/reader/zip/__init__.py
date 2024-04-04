from pathlib import Path
from zipfile import ZipFile
#from typing import Self, Any, Optional
from datetime import datetime

import pyarrow as pa

from pyplexos.reader.zip.xml_model import SolutionModel
from pyplexos.reader.zip.bin_model import SolutionData


class PlexosZipReader:
    """
    A class for reading plexos data from a ZIP solution file containing XML
    and binary files. On creation checks for existing files.
    """

    def __init__(self, path: str) -> None:
        zip_file_path = Path(path)
        xml_file_name: str = ""
        bin_file_name: str = ""

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
        if xml_file_name == "":
            raise ValueError("No model file on zip solution.")
        if bin_file_name == "":
            raise ValueError("No BIN files on zip solution.")
        
        self._zip_file_path: Path = zip_file_path
        self._xml_file_name: str = xml_file_name
        self._bin_file_name: str = bin_file_name


    def read(self) -> None:
        with ZipFile(self._zip_file_path, "r") as zip_ref:
            with zip_ref.open(self._xml_file_name) as xml_file:
                content: str = xml_file.read().decode("utf-8")
                solution_model = SolutionModel.from_xml(content)
            with zip_ref.open(self._bin_file_name) as bin_file:
                binary_data = bin_file.read()
                solution_data = SolutionData.from_binary(
                    solution_model.t_key_index, binary_data
                )
        
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

        # generate the dict equivalent por transformations
        solution_model_dict = solution_model.model_dump(by_alias=True)
        solution_data_dict = solution_data.model_dump(by_alias=True)

        self._sol_model_dict = solution_model_dict
        self._sol_model = solution_model

        prg_server = next((obj.value for obj in solution_model.t_config if obj.element_ == 'Computer'), None)
        if prg_server is not None:
            self._prg_server: str = prg_server
        else:
            raise ValueError("Error on t_config table. No Computer data")

        self._prg_datetime: datetime = solution_model.t_period_0[min_interval].datetime

        # TODO. Value check on pydantic deserialization so, try to think in a better way to do this. Generator could get errors.
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

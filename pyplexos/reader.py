"""
**plexos_reader.py**
Clase para manejar e interactuar con salidas de plexos
"""

from pathlib import Path
from zipfile import ZipFile
from typing import Self
from dataclasses import dataclass

import polars as pl
#import duckdb as duck

from pyplexos.model.xml_model import SolutionModel
from pyplexos.model.bin_model import SolutionData
from pyplexos.model.duck_model import duck_model


@dataclass
class PlexosZipReader:
    """
    A class for reading plexos data from a ZIP solution file containing XML
    and binary files. On creation checks for existing files.
    """

    xml_file_name: str | None = None
    bin_file_name: str | None = None

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
            xml_file_name=xml_file_name,
            bin_file_name=bin_file_name,
            solution_model=solution_model,
            solution_data=solution_data,
        )

    def to_parquet(self, path_to_dir: str, period_to_extract: str = "interval") -> None:
        """
        Transform a plexos zip solution to parquet files, given a path.

        Args:
            path_to_dir (str): path to directory to save parquets.
            period_to_extract (str): time period to extract.

        Returns:
            None.
        """

        path = Path(path_to_dir)
        if not path.exists():
            raise ValueError(f"Path: {path_to_dir} does not exists.")
        print(f"Escribiendo parquets en {path}")

        for table_name, table_data in self.solution_model.model_dump(
            by_alias=True
        ).items():
            if table_data is None:
                continue
            file = path / table_name
            pl.from_dicts(table_data).write_parquet(file.with_suffix(".parquet"))

        for table_name, table_data in self.solution_data.model_dump(
            by_alias=True
        ).items():
            if table_data is None:
                continue
            file = path / table_name
            pl.from_dict(table_data).write_parquet(file.with_suffix(".parquet"))

    def to_duck(self, path_to_dir: str, name_db: str | None = None) -> None:
        """
        Transform a plexos zip solution to duckdb, given a path.

        Args:
            path_to_dir (str): path to directory to save parquets.

        Returns:
            None.
        """

        path = Path(path_to_dir)
        if not path.exists():
            raise ValueError(f"Path: {path_to_dir} does not exists.")
        print(f"Escribiendo duck en {path}")
        
        if name_db is None:
            name_db = self.solution_model.t_period_0[0].datetime.date().strftime("%Y%m%d")
            name_db += ".db"
        
        path_duck = path / name_db

        if path_duck.exists():
            path_duck.unlink()
        
        duck = duck_model.create_medallion_duck(path_duck)
        duck.duck_bronze_schema()
        duck.duck_silver_schema()

        for table_name, table_data in self.solution_model.model_dump(
            by_alias=True
        ).items():
            if table_data is None:
                continue
            duck.conn.from_arrow(pl.from_dicts(table_data).to_arrow()).insert_into(
                f"bronze.{table_name}"
            )

        for table_name, table_data in self.solution_data.model_dump(
            by_alias=True
        ).items():
            if table_data is None:
                continue
            duck.conn.from_arrow(pl.from_dict(table_data).to_arrow()).insert_into(
                f"bronze.{table_name}"
            )

        duck.conn.close()

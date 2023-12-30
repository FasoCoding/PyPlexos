"""
**plexos_reader.py**
Clase para manejar e interactuar con salidas de plexos
"""

from pathlib import Path
from zipfile import ZipFile

#import polars

from .model import ModelPRG
from .binary import process_binary_data


class PlexosZipReader:
    """
    A class for reading plexos data from a ZIP solution file containing XML
    and binary files. On creation checks for existing files.
    """

    __PATTERN_MAP: dict = {
        "interval": "t_data_0.BIN",
        "hour": "t_data_1.BIN",
        "day": "t_data_2.BIN",
        "week": "t_data_3.BIN",
        "month": "t_data_4.BIN",
        "year": "t_data_5.BIN"
    }

    def __init__(self, zip_file_path: str) -> None:
        """
        Initialize a PlexosZipReader object.

        Args:
            zip_file_path (str): The path to the ZIP file.
        """
        self.zip_file_path = Path(zip_file_path)
        if not self.zip_file_path.exists():
            raise ValueError("The specified ZIP file path does not exist.")

        self.xml_file_name = self._extract_xml_file_name()
        self.bin_file_name = self._extract_bin_file_name()


    def _extract_xml_file_name(self) -> str | None:
        """
        Internal function.
        Extracts name of an XML file that matches the
        pattern "Model*.xml" from a ZIP file.

        Returns:
            str: Name of the XML file if found and matches the pattern.
        
        Raises:
            ValueError: if no Model*.XML found.
        """
        with ZipFile(self.zip_file_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.startswith('Model') and file_name.endswith('.xml'):
                    return file_name
        raise ValueError("No model file on zip solution.")

    def _extract_bin_file_name(self) -> list[str]:
        """
        Internal function.
        Extracts the name of a BIN file that matches the
        pattern "*.BIN" from a ZIP file.

        Returns:
            list[str]: Name of the BIN files if found and matches the pattern.
        
        Raises:
            ValueError: If no BIN files found.
        """
        with ZipFile(self.zip_file_path, 'r') as zip_ref:
            bin_files = [file_name for file_name in zip_ref.namelist() if file_name.endswith("BIN")]

        if len(bin_files) != 0:
            return bin_files

        raise ValueError("No BIN files on zip solution")

    def _extract_solution_model(self) -> ModelPRG:
        """
        Internal function.
        Extracts and reads the XML model from a zip file.

        Returns:
            ModelPRG: Model of XML for PRG dataset.
        """
        with ZipFile(self.zip_file_path, 'r') as zip_ref:
            with zip_ref.open(self.xml_file_name) as xml_file:
                content = xml_file.read().decode('utf-8')
                return ModelPRG.from_xml(content)

    def _extract_solution_binary(self, period: str) -> bytes:
        """
        Internal function.
        Extracts and reads the binary data from a zip file.

        Returns:
            bytes: binary data.
        """
        with ZipFile(self.zip_file_path, 'r') as zip_ref:
            with zip_ref.open(self.__PATTERN_MAP.get(period)) as bin_file:
                return bin_file.read()

    def solution_to_parquet(self, path_to_dir: str, period_to_extract: str = "interval") -> None:
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

        solution_model = self._extract_solution_model()
        solution_model.to_parquet(path)
        solution_data = process_binary_data(solution_model.key_index_tables,
                                            self._extract_solution_binary(period_to_extract))
        file_name = path / self.__PATTERN_MAP.get(period_to_extract)[:-4]
        solution_data.write_parquet(file_name.with_suffix(".parquet"))

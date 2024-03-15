from pydantic import BaseModel
from typing import Optional, Self
from struct import unpack

from pyplexos.reader.zip.xml_model import KeyIndexTable


class DataTable(BaseModel):
    key_id: list[int]
    period_id: list[int]
    value: list[float]


class SolutionData(BaseModel):
    t_data_0: DataTable
    t_data_1: Optional[DataTable] = None
    t_data_2: Optional[DataTable] = None
    t_data_3: Optional[DataTable] = None
    t_data_4: Optional[DataTable] = None
    t_data_6: Optional[DataTable] = None
    t_data_7: Optional[DataTable] = None

    @classmethod
    def from_binary(cls, t_key_index: list[KeyIndexTable], binary_data: bytes) -> Self:
        """
        Process binary data with the t_key_index table. If the t_key_index table is filtered,
        te resulting data will correspond to it.

        Args:
            t_key_index (List[KeyIndexTable]): The list containing t_key_index data.
            binary_data (bytes): The binary data to read.

        Returns:
            SolutionData: The SolutionData with processed t_key_index data.

        """
        key_ids:list[int] = []
        period_ids: list[int] = []
        values: list[float] = []

        for data in t_key_index:
            if data.period_type_id != 0:
                continue
            binary_value = binary_data[data.position : data.position + data.length * 8]
            values.extend(read_double_values(binary_value))
            period_ids.extend(
                range(1 + data.period_offset, 1 + data.length + data.period_offset)
            )
            key_ids.extend([data.key_id] * data.length)

        return cls(
            t_data_0=DataTable(key_id=key_ids, period_id=period_ids, value=values)
        )


def read_double_values(binary_data: bytes) -> list[float]:
    """
    Read double values from binary data.

    Args:
        binary_data (bytes): The binary data to read.

    Returns:
        List[float]: The list of double values.

    """
    num_values: int = len(binary_data) // 8
    format_string: str = f"{num_values}d"
    double_values: list[float] = list(unpack(format_string, binary_data))
    return double_values

"""
Lectura de binario de plexos
"""

from struct import unpack

import polars as pl


def process_binary_data(t_key_index: pl.DataFrame, binary_data: bytes) -> pl.DataFrame:
    """
    Process binary data with the t_key_index table. If the t_key_index table is filtered,
    te resulting data will correspond to it.

    Args:
        table_data (Dict): The dictionary containing t_key_index data.

    Returns:
        pl.DataFrame: The Polars DataFrame with processed t_key_index data.

    """
    positions = t_key_index["position"].to_list()
    lengths = t_key_index["length"].to_list()
    keys = t_key_index["key_id"].to_list()

    key_ids = []
    period_ids = []
    values = []

    for position, length, key_id in zip(positions, lengths, keys):
        binary_value = binary_data[position : position + length * 8]
        values.extend(read_double_values(binary_value))
        period_ids.extend(range(1, length + 1))
        key_ids.extend([key_id] * length)

    key_index_df = pl.DataFrame(
        {
            "key_id": key_ids,
            "period_id": period_ids,
            "value": values,
        },
        schema={"key_id": pl.Int64, "period_id": pl.Int64, "value": pl.Float64},
    )

    return key_index_df


def read_double_values(binary_data: bytes) -> list[float]:
    """
    Read double values from binary data.

    Args:
        binary_data (bytes): The binary data to read.

    Returns:
        List[float]: The list of double values.

    """
    num_values = len(binary_data) // 8
    format_string = f"{num_values}d"
    double_values = list(unpack(format_string, binary_data))
    return double_values

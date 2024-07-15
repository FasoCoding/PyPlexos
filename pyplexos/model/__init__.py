from dataclasses import dataclass
from typing import Self
from pathlib import Path

from pyplexos.model.xml import MasterDataSet

import pyarrow as pa

@dataclass
class PlexosModel:
    t_attribute: pa.Table
    t_attribute_data: pa.Table
    t_band: pa.Table
    t_category: pa.Table
    t_class: pa.Table
    t_class_group: pa.Table
    t_collection: pa.Table
    t_collection_report: pa.Table
    t_config: pa.Table
    t_data: pa.Table
    t_date_from: pa.Table
    t_date_to: pa.Table
    t_membership: pa.Table
    t_object: pa.Table
    t_property: pa.Table
    t_property_group: pa.Table
    t_tag: pa.Table
    t_text: pa.Table
    t_unit: pa.Table
    t_memo_data: pa.Table
    t_property_report: pa.Table
    t_report: pa.Table
    t_action: pa.Table
    t_message: pa.Table
    t_property_tag: pa.Table

    @classmethod
    def from_xml(cls, xml_file_path: str) -> Self:
        path = Path(xml_file_path)

        if not path.exists():
            raise FileNotFoundError(f"Path does not exists: {xml_file_path}")

        with open(path, "rb") as xml_file:
            plexos_model = MasterDataSet.from_xml(xml_file).model_dump(by_alias=True, exclude_none=True)
        
        #transformed_model = map(pa.Table.from_pylist, plexos_model)
        #transformed_2 = {table_name: pa.Table.from_pylist(table_data) for table_name, table_data in plexos_model}
        return cls(**{table_name: pa.Table.from_pylist(table_data) for table_name, table_data in plexos_model.items()})
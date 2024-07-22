from dataclasses import dataclass, fields
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

    def items(self):
        for field in fields(self):
            yield field.name, getattr(self, field.name)

    @classmethod
    def from_xml(cls, xml_file_path: str) -> Self:
        path = Path(xml_file_path)

        if not path.exists():
            raise FileNotFoundError(f"Path does not exists: {xml_file_path}")

        with open(path, "rb") as xml_file:
            plexos_model = MasterDataSet.from_xml(xml_file).model_dump(by_alias=True)

        return cls(**{table_name: pa.Table.from_pylist(table_data) for table_name, table_data in plexos_model.items()})

    def to_xml(self, xml_file_path: str | None) -> str:
        """Transform Plexos Model to xml format, if file_path is define then it will write the xml file.

        Args:
            xml_file_path (str | None): path to write (no filename), it has an standart name "DBSEN_PRGDIARIO.xml".

        Returns:
            str: xml representation of the Plexos Model
        """
        plexos_model = MasterDataSet(**{table_name: table_data.to_pylist() for table_name, table_data in self.items()})
        xml = plexos_model.to_xml(xml_path=xml_file_path)

        return xml

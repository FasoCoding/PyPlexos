"""
**plexos_model.py**
Recopilation of classes to interpret a plexos xml solution.
"""

from pathlib import Path
from typing import Optional
from pydantic_xml import BaseXmlModel, element

import polars as pl

NSMAP = {'': "http://tempuri.org/SolutionDataset.xsd"}


class UnitTable(BaseXmlModel, tag="t_unit", nsmap=NSMAP):
    """
    Class to represent the table: t_unit
    """
    unit_id: int = element()
    value: str = element()
    lang_id: int = element()


class BandTable(BaseXmlModel, tag="t_band", nsmap=NSMAP):
    """
    Class to represent the table: t_band
    """
    band_id: int = element()


class CategoryTable(BaseXmlModel, tag="t_category", nsmap=NSMAP):
    """
    Class to represent the table: t_category
    """
    category_id: int = element()
    class_id: int = element()
    rank: int = element()
    name: str = element()


class ClassTable(BaseXmlModel, tag="t_class", nsmap=NSMAP):
    """
    Class to represent the table: t_class
    """
    class_id: int = element()
    name: str = element()
    class_group_id: int = element()
    lang_id: int = element()
    state: int = element()


class ClassGroupTable(BaseXmlModel, tag="t_class_group", nsmap=NSMAP):
    """
    Class to represent the table: t_class_group
    """
    class_group_id: int = element()
    name: str = element()
    lang_id: int = element()


class CollectionTable(BaseXmlModel, tag="t_collection", nsmap=NSMAP):
    """
    Class to represent the table: t_collection
    """
    collection_id: int = element()
    parent_class_id: int = element()
    child_class_id: int = element()
    name: str = element()
    complement_name: Optional[str] = element()
    lang_id: int = element()


class ConfigTable(BaseXmlModel, tag="t_config", nsmap=NSMAP):
    """
    Class to represent the table: t_config
    """
    element_: str = element(tag="element")
    value: str | None = element()


class KeyTable(BaseXmlModel, tag="t_key", nsmap=NSMAP):
    """
    Class to represent the table: t_key
    """
    key_id: int = element()
    membership_id: int = element()
    model_id: int = element()
    phase_id: int = element()
    property_id: int = element()
    period_type_id: int = element()
    band_id: int = element()
    sample_id: int = element()
    timeslice_id: int = element()


class MembershipTable(BaseXmlModel, tag="t_membership", nsmap=NSMAP):
    """
    Class to represent the table: t_membership
    """
    membership_id: int = element()
    parent_class_id: int = element()
    child_class_id: int = element()
    collection_id: int = element()
    parent_object_id: int = element()
    child_object_id: int = element()


class ModelTable(BaseXmlModel, tag="t_model", nsmap=NSMAP):
    """
    Class to represent the table: t_model
    """
    model_id: int = element()
    name: str = element()


class ObjectTable(BaseXmlModel, tag="t_object", nsmap=NSMAP):
    """
    Class to represent the table: t_object
    """
    class_id: int = element()
    name: str = element()
    category_id: int = element()
    index: int = element()
    object_id: int = element()
    # show: bool = element() No es necesario, pero existe.
    # GUID: Optional[str] = element() No es necesario, pero existe.


class Period0Table(BaseXmlModel, tag="t_period_0", nsmap=NSMAP):
    """
    Class to represent the table: t_period_0
    """
    interval_id: int = element()
    hour_id: int = element()
    day_id: int = element()
    week_id: int = element()
    month_id: int = element()
    quarter_id: int = element()
    fiscal_year_id: int = element()
    datetime: str = element()
    period_of_day: int = element()


class Phase3Table(BaseXmlModel, tag="t_phase_3", nsmap=NSMAP):
    """
    Class to represent the table: t_pahse_3
    """
    interval_id: int = element()
    period_id: int = element()


class SampleTable(BaseXmlModel, tag="t_sample", nsmap=NSMAP):
    """
    Class to represent the table: t_sample
    """
    sample_id: int = element()
    sample_name: str = element()


class TimesliceTable(BaseXmlModel, tag="t_timeslice", nsmap=NSMAP):
    """
    Class to represent the table: t_timeslice
    """
    timeslice_id: int = element()
    name: str = element()


class KeyIndexTable(BaseXmlModel, tag="t_key_index", nsmap=NSMAP):
    """
    Class to represent the table: t_ley_index
    """
    key_id: int = element()
    period_type_id: int = element()
    position: int = element()
    length: int = element()
    period_offset: int = element()


class PropertyTable(BaseXmlModel, tag="t_property", nsmap=NSMAP):
    """
    Class to represent the table: t_property
    """
    property_id: int = element()
    collection_id: int = element()
    enum_id: int = element()
    name: str = element()
    summary_name: str = element()
    unit_id: int = element()
    summary_unit_id: int = element()
    is_multi_band: bool = element()
    is_period: bool = element()
    is_summary: bool = element()
    lang_id: int = element()


class AttributeDataTable(BaseXmlModel, tag="t_attribute_data", nsmap=NSMAP):
    """
    Class to represent the table: t_attribute_data
    """
    object_id: int = element()
    attribute_id: int = element()
    value: float = element()


class AttributeTable(BaseXmlModel, tag="t_attribute", nsmap=NSMAP):
    """
    Class to represent the table: t_attribute
    """
    attribute_id: int = element()
    class_id: int = element()
    enum_id: int = element()
    name: str = element()
    description: str = element()
    lang_id: int | None = element()


class SampleWeightTable(BaseXmlModel, tag="t_sample_weight", nsmap=NSMAP):
    """
    Class to represent the table: t_sample_weight
    """
    sample_id: int = element()
    phase_id: int = element()
    value: int = element()


class ModelPRG(BaseXmlModel, tag="SolutionDataset", nsmap=NSMAP):
    """
    Class to represent a complete solution of plexos .xml file.
    
    This class only work for a PCP solution.
    """
    t_unit: list[UnitTable]
    t_band: list[BandTable]
    t_category: list[CategoryTable]
    t_class: list[ClassTable]
    t_class_group: list[ClassGroupTable]
    t_collection: list[CollectionTable]
    t_config: list[ConfigTable]
    t_key: list[KeyTable]
    t_membership: list[MembershipTable]
    t_model: list[ModelTable]
    t_object: list[ObjectTable]
    t_period_0: list[Period0Table]
    t_phase_3: list[Phase3Table]
    t_sample: list[SampleTable]
    t_timeslice: list[TimesliceTable]
    t_key_index: list[KeyIndexTable]
    t_property: list[PropertyTable]
    t_attribute_data: list[AttributeDataTable]
    t_attribute: list[AttributeTable]
    t_sample_weight: list[SampleWeightTable]

    def to_parquet(self, path_to_dir: str) -> None:
        """
        Converts the Plexos Model to parquet tables.
        
        Args:
            path_to_dir (str): path to save the tables.
        
        Returns:
            None
        """
        path = Path(path_to_dir)
        if not path.exists():
            raise ValueError(f"Path: {path_to_dir} does not exist.")

        for table_name, table_data in self.dict().items():
            file = path / table_name
            pl.from_dicts(table_data).write_parquet(file.with_suffix(".parquet"))

    @property
    def key_index_tables(self) -> pl.DataFrame:
        """retorna un dataframe con la tabla "t_key_index". Utilizada para
        decodificar los datos binarios de la salida plexos.

        Returns:
            pl.DataFrame: Tabla "t_key_index".
        """
        return pl.from_dicts(self.dict(include={"t_key_index"})["t_key_index"])

def main() -> None:
    """Función para poder usar el código de manera independiente.

    Raises:
        ValueError: Si no existe la ruta del archivo.
    """

    print("Script para extraer modelo de plexos")
    path_to_xml = input("Enter path to xml:\n")
    path = Path(path_to_xml)
    print("") # espacio para saltar linea XD
    if not path.exists():
        raise ValueError("No existe el path.")
    xml_file = path.read_text(encoding='utf-8')
    solution = ModelPRG.from_xml(xml_file)
    path_to_folder = input("Enter path to save model:\n")
    solution.to_parquet(path_to_folder)
    print("Listo!")

if __name__ == "__main__":
    main()

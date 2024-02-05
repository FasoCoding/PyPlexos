"""
**plexos_model.py**
Recopilation of classes to interpret a plexos xml solution.
"""
from typing import Optional
from pydantic_xml import BaseXmlModel, element

NSMAP = {"": "http://tempuri.org/SolutionDataset.xsd"}


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


class CollectionTable(BaseXmlModel, tag="t_collection", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_collection
    """

    collection_id: int = element()
    parent_class_id: int = element()
    child_class_id: int = element()
    name: str = element()
    complement_name: Optional[str] = element(default=None)
    lang_id: int = element()


class ConfigTable(BaseXmlModel, tag="t_config", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_config
    """

    element_: str = element(tag="element", serialization_alias="element")
    value: Optional[str] = element(default=None)


class KeyTable(BaseXmlModel, tag="t_key", nsmap=NSMAP):
    """
    Class to represent the table: t_key
    attribute model_id is set to id_model for conflicts on pydantic
    """

    key_id: int = element()
    membership_id: int = element()
    id_model: int = element(tag="model_id", serialization_alias="model_id")
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
    attribute model_id is set to id_model for conflicts on pydantic
    """

    id_model: int = element(tag="model_id", serialization_alias="model_id")
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
    show: bool = element()
    guid: Optional[str] = element(tag="GUID", serialization_alias="GUID", default=None)


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


class AttributeTable(BaseXmlModel, tag="t_attribute", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_attribute
    """

    attribute_id: int = element()
    class_id: int = element()
    enum_id: int = element()
    name: str = element()
    description: str = element()
    lang_id: Optional[int] = element(default=None)


class SampleWeightTable(BaseXmlModel, tag="t_sample_weight", nsmap=NSMAP):
    """
    Class to represent the table: t_sample_weight
    """

    sample_id: int = element()
    phase_id: int = element()
    value: int = element()


class CustomCoumnTable(
    BaseXmlModel, tag="t_custom_column", nsmap=NSMAP, skip_empty=True
):
    """
    Class to represent the table: t_custom_column
    """

    column_id: int = element(default=None)
    class_id: int = element(default=None)
    name: str = element(default=None)
    position: int = element(default=None)
    guid: int = element(default=None)


class MemoObjectTable(BaseXmlModel, tag="t_memo_object", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_memo_object
    """

    object_id: int = element(default=None)
    column_id: int = element(default=None)
    value: str = element(default=None)


class ObjectMetaTable(BaseXmlModel, tag="t_object_meta", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_object_meta
    """

    object_id: int = element(default=None)
    class_name: str = element(tag="class", serialization_alias="class", default=None)
    property_name: str = element(
        tag="property", serialization_alias="property", default=None
    )
    value: str = element(default=None)
    state: str = element(default=None)


class Period1Table(BaseXmlModel, tag="t_period_1", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_period_1
    """

    day_id: int = element(default=None)
    date: str = element(default=None)
    week_id: int = element(default=None)
    month_id: int = element(default=None)
    quarter_id: int = element(default=None)
    fiscal_year_id: int = element(default=None)


class Period2Table(BaseXmlModel, tag="t_period_2", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_period_2
    """

    week_id: int = element(default=None)
    week_ending: str = element(default=None)


class Period3Table(BaseXmlModel, tag="t_period_3", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_period_3
    """

    month_id: int = element(default=None)
    month_beginning: str = element(default=None)


class Period4Table(BaseXmlModel, tag="t_period_4", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_period_4
    """

    fiscal_year_id: int = element(default=None)
    year_ending: str = element(default=None)


class Period6Table(BaseXmlModel, tag="t_period_6", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_period_6
    """

    hour_id: int = element(default=None)
    day_id: int = element(default=None)
    datetime: str = element(default=None)


class Period7Table(BaseXmlModel, tag="t_period_7", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_period_7
    """

    quarter_id: int = element(default=None)
    quarter_beginning: str = element(default=None)


class Phase1Table(BaseXmlModel, tag="t_phase_1", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_phase_1
    """

    interval_id: int = element(default=None)
    period_id: int = element(default=None)


class Phase2Table(BaseXmlModel, tag="t_phase_2", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_phase_2
    """

    interval_id: int = element(default=None)
    period_id: int = element(default=None)


class Phase4Table(BaseXmlModel, tag="t_phase_4", nsmap=NSMAP, skip_empty=True):
    """
    Class to represent the table: t_phase_4
    """

    interval_id: int = element(default=None)
    period_id: int = element(default=None)


class SolutionModel(BaseXmlModel, tag="SolutionDataset", nsmap=NSMAP):
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
    t_custom_column: Optional[list[CustomCoumnTable]] = None
    t_memo_object: Optional[list[MemoObjectTable]] = None
    t_object_meta: Optional[list[ObjectMetaTable]] = None
    t_period_1: Optional[list[Period1Table]] = None
    t_period_2: Optional[list[Period2Table]] = None
    t_period_3: Optional[list[Period3Table]] = None
    t_period_4: Optional[list[Period4Table]] = None
    t_period_6: Optional[list[Period6Table]] = None
    t_period_7: Optional[list[Period7Table]] = None
    t_phase_1: Optional[list[Phase1Table]] = None
    t_phase_2: Optional[list[Phase2Table]] = None
    t_phase_4: Optional[list[Phase4Table]] = None

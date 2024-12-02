import datetime as dt
import xmltodict

# from io import BytesIO, StringIO
from pydantic import BaseModel, field_validator, Field
from typing import Optional, OrderedDict, Any, IO
from pyplexos.solution.schema import SolutionSchema


class UnitTable(BaseModel):
    """
    Class to represent the table: t_unit
    """

    unit_id: int
    value: str
    lang_id: int


class BandTable(BaseModel):
    """
    Class to represent the table: t_band
    """

    band_id: int


class CategoryTable(BaseModel):
    """
    Class to represent the table: t_category
    """

    category_id: int
    class_id: int
    rank: int
    name: str


class ClassTable(BaseModel):
    """
    Class to represent the table: t_class
    """

    class_id: int
    name: str
    class_group_id: int
    lang_id: int
    state: int


class ClassGroupTable(BaseModel):
    """
    Class to represent the table: t_class_group
    """

    class_group_id: int
    name: str
    lang_id: int


class CollectionTable(BaseModel):
    """
    Class to represent the table: t_collection
    """

    collection_id: int
    parent_class_id: int
    child_class_id: int
    name: str
    complement_name: str | None
    lang_id: int


class ConfigTable(BaseModel):
    """
    Class to represent the table: t_config
    """

    element: str
    value: str | None


class KeyTable(BaseModel):
    """
    Class to represent the table: t_key
    attribute model_id is set to id_model for conflicts on pydantic
    """

    key_id: int
    membership_id: int
    id_model: int = Field(alias="model_id")
    phase_id: int
    property_id: int
    period_type_id: int
    band_id: int
    sample_id: int
    timeslice_id: int


class MembershipTable(BaseModel):
    """
    Class to represent the table: t_membership
    """

    membership_id: int
    parent_class_id: int
    child_class_id: int
    collection_id: int
    parent_object_id: int
    child_object_id: int


class ModelTable(BaseModel):
    """
    Class to represent the table: t_model
    attribute model_id is set to id_model for conflicts on pydantic
    """

    id_model: int = Field(alias="model_id")
    name: str


class ObjectTable(BaseModel):
    """
    Class to represent the table: t_object
    """

    class_id: int
    name: str
    category_id: int
    index: int
    object_id: int
    show: bool
    guid: str | None = Field(alias="GUID", default=None)


class Period0Table(BaseModel):
    """
    Class to represent the table: t_period_0
    """

    interval_id: int
    hour_id: int
    day_id: int
    week_id: int
    month_id: int
    quarter_id: int
    fiscal_year_id: int
    datetime: dt.datetime
    period_of_day: int

    @field_validator("datetime", mode="before")
    @classmethod
    def decode_datetime(cls, value: str) -> dt.datetime:
        return dt.datetime.strptime(value, r"%d/%m/%Y %H:%M:%S")


class Phase3Table(BaseModel):
    """
    Class to represent the table: t_pahse_3
    """

    interval_id: int
    period_id: int


class SampleTable(BaseModel):
    """
    Class to represent the table: t_sample
    """

    sample_id: int
    sample_name: str


class TimesliceTable(BaseModel):
    """
    Class to represent the table: t_timeslice
    """

    timeslice_id: int
    name: str


class KeyIndexTable(BaseModel):
    """
    Class to represent the table: t_ley_index
    """

    key_id: int
    period_type_id: int
    position: int
    length: int
    period_offset: int


class PropertyTable(BaseModel):
    """
    Class to represent the table: t_property
    """

    property_id: int
    collection_id: int
    enum_id: int
    name: str
    summary_name: str
    unit_id: int
    summary_unit_id: int
    is_multi_band: bool
    is_period: bool
    is_summary: bool
    lang_id: int


class AttributeDataTable(
    BaseModel,
):
    """
    Class to represent the table: t_attribute_data
    """

    object_id: int
    attribute_id: int
    value: float


class AttributeTable(BaseModel):
    """
    Class to represent the table: t_attribute
    """

    attribute_id: int
    class_id: int
    enum_id: int
    name: str
    description: str
    lang_id: Optional[int]


class SampleWeightTable(BaseModel):
    """
    Class to represent the table: t_sample_weight
    """

    sample_id: int
    phase_id: int
    value: int


class CustomCoumnTable(BaseModel):
    """
    Class to represent the table: t_custom_column
    """

    column_id: int
    class_id: int
    name: str
    position: int
    guid: int


class MemoObjectTable(BaseModel):
    """
    Class to represent the table: t_memo_object
    """

    object_id: int
    column_id: int
    value: str


class ObjectMetaTable(BaseModel):
    """
    Class to represent the table: t_object_meta
    """

    object_id: int
    class_name: str = Field(alias="class")
    property_name: str = Field(alias="property")
    value: str
    state: str


class Period1Table(BaseModel):
    """
    Class to represent the table: t_period_1
    """

    day_id: int
    date: dt.date
    week_id: int
    month_id: int
    quarter_id: int
    fiscal_year_id: int

    @field_validator("date", mode="before")
    @classmethod
    def decode_datetime(cls, value: str) -> dt.date:
        return dt.datetime.strptime(value, r"%Y-%m-%dT%H:%M:%S").date()


class Period2Table(BaseModel):
    """
    Class to represent the table: t_period_2
    """

    week_id: int
    week_ending: dt.date

    @field_validator("week_ending", mode="before")
    @classmethod
    def decode_datetime(cls, value: str) -> dt.date:
        return dt.datetime.strptime(value, r"%Y-%m-%dT%H:%M:%S").date()


class Period3Table(BaseModel):
    """
    Class to represent the table: t_period_3
    """

    month_id: int
    month_beginning: dt.date

    @field_validator("month_beginning", mode="before")
    @classmethod
    def decode_datetime(cls, value: str) -> dt.date:
        return dt.datetime.strptime(value, r"%Y-%m-%dT%H:%M:%S").date()


class Period4Table(BaseModel):
    """
    Class to represent the table: t_period_4
    """

    fiscal_year_id: int
    year_ending: dt.date

    @field_validator("year_ending", mode="before")
    @classmethod
    def decode_datetime(cls, value: str) -> dt.date:
        return dt.datetime.strptime(value, r"%Y-%m-%dT%H:%M:%S").date()


class Period6Table(BaseModel):
    """
    Class to represent the table: t_period_6
    """

    hour_id: int
    day_id: int
    datetime: dt.datetime

    @field_validator("datetime", mode="before")
    @classmethod
    def decode_datetime(cls, value: str) -> dt.datetime:
        return dt.datetime.strptime(value, r"%Y-%m-%dT%H:%M:%S")


class Period7Table(BaseModel):
    """
    Class to represent the table: t_period_7
    """

    quarter_id: int
    quarter_beginning: dt.date

    @field_validator("quarter_beginning", mode="before")
    @classmethod
    def decode_datetime(cls, value: str) -> dt.date:
        return dt.datetime.strptime(value, r"%Y-%m-%dT%H:%M:%S").date()


class Phase1Table(BaseModel):
    """
    Class to represent the table: t_phase_1
    """

    interval_id: int
    period_id: int


class Phase2Table(BaseModel):
    """
    Class to represent the table: t_phase_2
    """

    interval_id: int
    period_id: int


class Phase4Table(BaseModel):
    """
    Class to represent the table: t_phase_4
    """

    interval_id: int
    period_id: int


class SolutionModel(BaseModel):
    """
    Class to represent a complete solution of plexos .xml file.
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
    t_period_1: Optional[list[Period1Table]] = None
    t_period_2: Optional[list[Period2Table]] = None
    t_period_3: Optional[list[Period3Table]] = None
    t_period_4: Optional[list[Period4Table]] = None
    t_period_6: Optional[list[Period6Table]] = None
    t_period_7: Optional[list[Period7Table]] = None
    t_phase_1: Optional[list[Phase1Table]] = None
    t_phase_2: Optional[list[Phase2Table]] = None
    t_phase_3: Optional[list[Phase3Table]] = None
    t_phase_4: Optional[list[Phase4Table]] = None
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

    @classmethod
    def from_xml(cls, xml_file: str | IO[bytes]):
        namespace = {"http://tempuri.org/SolutionDataset.xsd": None}
        force_list_data = [value.name for value in SolutionSchema]
        content: OrderedDict[str, Any] = xmltodict.parse(
            xml_input=xml_file,
            force_list=force_list_data,
            process_namespaces=True,
            namespaces=namespace,
        )
        return cls(**content["SolutionDataset"])

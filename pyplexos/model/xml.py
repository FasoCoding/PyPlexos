import datetime as dt
import xmltodict

#from io import BytesIO, StringIO
from pydantic import BaseModel, Field, field_serializer
from typing import Optional, OrderedDict, Any, IO
from pathlib import Path


class AttributeTable(BaseModel):
    attribute_id: int
    class_id: int
    enum_id: int
    name: str
    unit_id: int
    default_value: float | int
    validation_rule: Optional[str] = None
    input_mask: Optional[str] = None
    is_enabled: bool
    is_integer: bool
    lang_id: int
    description: str
    tag: Optional[int] = None
    is_visible: bool

class AttributeDataTable(BaseModel):
    object_id: int
    attribute_id: int
    value: float

class BandTable(BaseModel):
    data_id: int
    band_id: int

class CategoryTable(BaseModel):
    category_id: int
    class_id: int
    rank: int
    name: str

class ClassTable(BaseModel):
    class_id: int
    name: str
    class_group_id: int
    is_enabled: bool
    lang_id: int
    description: str

class ClassGroupTable(BaseModel):
    class_group_id: int
    name: str
    lang_id: int

class CollectionTable(BaseModel):
    collection_id: int
    parent_class_id: int
    child_class_id: int
    name: str
    min_count: int
    max_count: int
    is_enabled: bool
    is_one_to_many: bool
    lang_id: int
    description: Optional[str] = None
    complement_name: Optional[str] = None
    complement_min_count: Optional[int] = None
    complement_max_count: Optional[int] = None
    complement_description: Optional[str] = None

class CollectionReportTable(BaseModel):
    collection_id: int
    left_collection_id: int
    right_collection_id: int
    rule_left_collection_id: Optional[int] = None
    rule_right_collection_id: Optional[int] = None
    rule_id: Optional[int] = None

class ConfigTable(BaseModel):
    element_: str = Field(alias="element")
    value_: str = Field(alias="value")

class DataTable(BaseModel):
    data_id: int
    membership_id: int
    property_id: int
    value: float | int
    uid: int

class DateFromTable(BaseModel):
    data_id: int
    date: dt.datetime

    @field_serializer("date")
    def ser_date(self, date: dt.datetime):
        return date.strftime(r"%Y-%m-%dT%H:%M:%S")

class DateToTable(BaseModel):
    data_id: int
    date: dt.datetime

    @field_serializer("date")
    def ser_date(self, date: dt.datetime):
        return date.strftime(r"%Y-%m-%dT%H:%M:%S")

class MembershipTable(BaseModel):
    membership_id: int
    parent_class_id: int
    parent_object_id: int
    collection_id: int
    child_class_id: int
    child_object_id: int

class ObjectTable(BaseModel):
    object_id: int
    class_id: int
    name: str
    category_id: int
    description: Optional[str] = None
    guid: str = Field(alias="GUID")

class PropertyTable(BaseModel):
    property_id: int
    collection_id: int
    property_group_id: int
    enum_id: int
    name: str
    unit_id: int
    default_value: float | int
    validation_rule: Optional[str] = None
    input_mask: Optional[str] = None
    upscaling_method: int
    downscaling_method: int
    property_type: int
    period_type_id: int
    is_key: bool
    is_enabled: bool
    is_dynamic: bool
    is_multi_band: bool
    max_band_id: int
    lang_id: int
    description: Optional[str] = None
    tag: Optional[str] = None
    is_visible: bool

class PropertyGroupTable(BaseModel):
    property_group_id: int
    name: str
    lang_id: int

class TagTable(BaseModel):
    data_id: int
    object_id: int
    action_id: Optional[int] = None

class TextTable(BaseModel):
    data_id: int
    class_id: int
    value: str

    @field_serializer("value")
    def ser_value(self, value: str):
        return rf'{value}'

class UnitTable(BaseModel):
    unit_id: int
    value: str
    default: str
    lang_id: int
    imperial_energy: Optional[str] = None
    metric_level: Optional[str] = None
    imperial_level: Optional[str] = None
    metric_volume: Optional[str] = None
    imperial_volume: Optional[str] = None
    description: Optional[str] = None

class MemoDataTable(BaseModel):
    data_id: int
    value: str

class PropertyReportTable(BaseModel):
    property_id: int
    collection_id: int
    property_group_id: int
    enum_id: int
    name: str
    summary_name: str
    unit_id: int
    summary_unit_id: int
    is_period: bool
    is_summary: bool
    is_multi_band: bool
    is_quantity: bool
    is_LT: bool
    is_PA: bool
    is_MT: bool
    is_ST: bool
    lang_id: int
    summary_lang_id: int
    description: str
    is_visible: bool

class ReportTable(BaseModel):
    object_id: int
    property_id: int
    phase_id: int
    report_period: bool
    report_summary: bool
    report_statistics: bool
    report_samples: bool
    write_flat_files: bool

class ActionTable(BaseModel):
    action_id: int
    action_symbol: str

class MessageTable(BaseModel):
    number: int
    severity: Optional[int] = None
    default_action: int
    action: int
    description: Optional[str] = None

class PropertyTagTable(BaseModel):
    tag_id: int
    name: str

class MasterDataSet(BaseModel):
    t_attribute: list[AttributeTable]
    t_attribute_data: list[AttributeDataTable]
    t_band: list[BandTable]
    t_category: list[CategoryTable]
    t_class: list[ClassTable]
    t_class_group: list[ClassGroupTable]
    t_collection: list[CollectionTable]
    t_collection_report: list[CollectionReportTable]
    t_config: list[ConfigTable]
    t_data: list[DataTable]
    t_date_from: list[DateFromTable]
    t_date_to: list[DateToTable]
    t_membership: list[MembershipTable]
    t_object: list[ObjectTable]
    t_property: list[PropertyTable]
    t_property_group: list[PropertyGroupTable]
    t_tag: list[TagTable]
    t_text: list[TextTable]
    t_unit: list[UnitTable]
    t_memo_data: list[MemoDataTable]
    t_property_report: list[PropertyReportTable]
    t_report: list[ReportTable]
    t_action: list[ActionTable]
    t_message: list[MessageTable]
    t_property_tag: list[PropertyTagTable]

    @classmethod
    def from_xml(cls, xml_file: str | IO[bytes]):
        namespace = {"http://tempuri.org/MasterDataSet.xsd": None}
        force_list_data = [name for name in cls.model_fields.keys()]
        content: OrderedDict[str, Any] = xmltodict.parse(
            xml_input=xml_file,
            force_list=force_list_data,
            process_namespaces=True,
            namespaces=namespace
        )
        return cls(**content['MasterDataSet'])

    def to_xml(self, xml_path: Optional[str] = None):
        namespace = {"@xmlns": "http://tempuri.org/MasterDataSet.xsd"}
        xml_data = xmltodict.unparse(
            {'MasterDataSet': self.model_dump(exclude_none=True, by_alias=True) | namespace},
            full_document=False,
            pretty=True,
            indent="  "
        )

        if xml_path:
            path = Path(xml_path)
            with open( path / "DBSEN_PRGDIARIO.xml", "w", encoding="utf-8") as file:
                file.write(xml_data)
        
        return xml_data

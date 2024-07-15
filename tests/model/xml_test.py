import xmltodict
import pytest

from typing import Any
import pyplexos.model.xml as xml_model
import datetime as dt

@pytest.fixture()
def xml_data() -> dict[str,Any]:
    xml_file = r"""
    <MasterDataSet xmlns="http://tempuri.org/MasterDataSet.xsd">
      <t_attribute>
        <attribute_id>1</attribute_id>
        <class_id>2</class_id>
        <enum_id>1</enum_id>
        <name>Latitude</name>
        <unit_id>11</unit_id>
        <default_value>0</default_value>
        <is_enabled>true</is_enabled>
        <is_integer>false</is_integer>
        <lang_id>343</lang_id>
        <description>Latitude</description>
        <tag>10000</tag>
        <is_visible>true</is_visible>
      </t_attribute>
      <t_attribute_data>
        <object_id>2</object_id>
        <attribute_id>1</attribute_id>
        <value>-37.36327240989711</value>
      </t_attribute_data>
      <t_band>
        <data_id>30572</data_id>
        <band_id>3</band_id>
      </t_band>
      <t_category>
        <category_id>3</category_id>
        <class_id>3</class_id>
        <rank>0</rank>
        <name>-</name>
      </t_category>
      <t_class>
        <class_id>1</class_id>
        <name>System</name>
        <class_group_id>1</class_group_id>
        <is_enabled>true</is_enabled>
        <lang_id>1</lang_id>
        <description>The integrated energy system</description>
      </t_class>
      <t_class_group>
        <class_group_id>1</class_group_id>
        <name>-</name>
        <lang_id>0</lang_id>
      </t_class_group>
      <t_collection>
        <collection_id>1</collection_id>
        <parent_class_id>1</parent_class_id>
        <child_class_id>2</child_class_id>
        <name>Generators</name>
        <min_count>0</min_count>
        <max_count>-1</max_count>
        <is_enabled>true</is_enabled>
        <is_one_to_many>true</is_one_to_many>
        <lang_id>36</lang_id>
        <description>Generator objects</description>
      </t_collection>
      <t_collection_report>
        <collection_id>58</collection_id>
        <left_collection_id>6</left_collection_id>
        <right_collection_id>59</right_collection_id>
      </t_collection_report>
      <t_config>
        <element>Dynamic</element>
        <value>0</value>
      </t_config>
      <t_data>
        <data_id>1</data_id>
        <membership_id>1</membership_id>
        <property_id>347</property_id>
        <value>1</value>
        <uid>2067428595</uid>
      </t_data>
      <t_date_from>
        <data_id>27316</data_id>
        <date>2020-06-05T08:00:00</date>
      </t_date_from>
      <t_date_to>
        <data_id>27316</data_id>
        <date>2020-06-05T18:00:00</date>
      </t_date_to>
      <t_membership>
        <membership_id>1</membership_id>
        <parent_class_id>2</parent_class_id>
        <parent_object_id>31</parent_object_id>
        <collection_id>29</collection_id>
        <child_class_id>69</child_class_id>
        <child_object_id>2128</child_object_id>
      </t_membership>
      <t_object>
        <object_id>2</object_id>
        <class_id>2</class_id>
        <name>ABANICO</name>
        <category_id>96</category_id>
        <description />
        <GUID>127a2012-f638-4f28-af07-b326a3cbb405</GUID>
      </t_object>
      <t_property>
        <property_id>1</property_id>
        <collection_id>1</collection_id>
        <property_group_id>2</property_group_id>
        <enum_id>1</enum_id>
        <name>Must Report</name>
        <unit_id>13</unit_id>
        <default_value>0</default_value>
        <validation_rule>In (0,-1)</validation_rule>
        <input_mask>-1;"Yes";0;"No"</input_mask>
        <upscaling_method>0</upscaling_method>
        <downscaling_method>2</downscaling_method>
        <property_type>0</property_type>
        <period_type_id>0</period_type_id>
        <is_key>false</is_key>
        <is_enabled>false</is_enabled>
        <is_dynamic>false</is_dynamic>
        <is_multi_band>false</is_multi_band>
        <max_band_id>1</max_band_id>
        <lang_id>525</lang_id>
        <description>If the generator must be reported even if it is out-of-service</description>
        <tag>800000</tag>
        <is_visible>true</is_visible>
      </t_property>
      <t_property_group>
        <property_group_id>1</property_group_id>
        <name>-</name>
        <lang_id>1</lang_id>
      </t_property_group>
      <t_tag>
        <data_id>651</data_id>
        <object_id>3164</object_id>
      </t_tag>
      <t_text>
        <data_id>1637</data_id>
        <class_id>73</class_id>
        <value>CFdata\CTF\CTFOFF_CELCO_BL2_MR.csv</value>
      </t_text>
      <t_unit>
        <unit_id>0</unit_id>
        <value>-</value>
        <default>-</default>
        <lang_id>0</lang_id>
      </t_unit>
      <t_memo_data>
        <data_id>27462</data_id>
        <value>250 MVA</value>
      </t_memo_data>
      <t_property_report>
        <property_id>1</property_id>
        <collection_id>1</collection_id>
        <property_group_id>3</property_group_id>
        <enum_id>1</enum_id>
        <name>Generation</name>
        <summary_name>Generation</summary_name>
        <unit_id>1</unit_id>
        <summary_unit_id>2</summary_unit_id>
        <is_period>true</is_period>
        <is_summary>true</is_summary>
        <is_multi_band>false</is_multi_band>
        <is_quantity>false</is_quantity>
        <is_LT>true</is_LT>
        <is_PA>false</is_PA>
        <is_MT>true</is_MT>
        <is_ST>true</is_ST>
        <lang_id>227</lang_id>
        <summary_lang_id>227</summary_lang_id>
        <description>Generation</description>
        <is_visible>true</is_visible>
      </t_property_report>
      <t_report>
        <object_id>3184</object_id>
        <property_id>1</property_id>
        <phase_id>1</phase_id>
        <report_period>true</report_period>
        <report_summary>true</report_summary>
        <report_statistics>false</report_statistics>
        <report_samples>false</report_samples>
        <write_flat_files>false</write_flat_files>
      </t_report>
      <t_action>
        <action_id>0</action_id>
        <action_symbol>=</action_symbol>
      </t_action>
      <t_message>
        <number>1</number>
        <severity>2</severity>
        <default_action>1</default_action>
        <action>1</action>
        <description>Running in Diagnostic Mode! Execution may be slow and/or excessive disk space may be consumed.</description>
      </t_message>
      <t_property_tag>
        <tag_id>1</tag_id>
        <name>Popular</name>
      </t_property_tag>
    </MasterDataSet>
    """
    namespace = {"http://tempuri.org/MasterDataSet.xsd": None}
    force_list_data = [name for name in xml_model.MasterDataSet.model_fields.keys()]
    return xmltodict.parse(
        xml_input=xml_file,
        force_list=force_list_data,
        process_namespaces=True,
        namespaces=namespace
    )['MasterDataSet']


def test_attribute_table():
    xml = (
        "<t_attribute>\n"
            "  <attribute_id>1</attribute_id>\n"
            "  <class_id>2</class_id>\n"
            "  <enum_id>1</enum_id>\n"
            "  <name>Latitude</name>\n"
            "  <unit_id>11</unit_id>\n"
            "  <default_value>0</default_value>\n"
            "  <is_enabled>true</is_enabled>\n"
            "  <is_integer>false</is_integer>\n"
            "  <lang_id>343</lang_id>\n"
            "  <description>Latitude</description>\n"
            "  <tag>10000</tag>\n"
            "  <is_visible>true</is_visible>\n"
        "</t_attribute>"
    )
    table = xml_model.AttributeTable(
        attribute_id=1,
        class_id=2,
        enum_id=1,
        name="Latitude",
        unit_id=11,
        default_value=0,
        validation_rule=None,
        input_mask=None,
        is_enabled=True,
        is_integer=False,
        lang_id=343,
        description="Latitude",
        tag=10000,
        is_visible=True,
    )
    # Prueba de lectura de xml
    assert table == xml_model.AttributeTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_attribute']
        )['t_attribute'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_attribute': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml
    

def test_attribute_data_table():
    xml = (
        "<t_attribute_data>\n"
            "  <object_id>2</object_id>\n"
            "  <attribute_id>1</attribute_id>\n"
            "  <value>-37.36327240989711</value>\n"
        "</t_attribute_data>"
    )
    table = xml_model.AttributeDataTable(
        object_id=2,
        attribute_id=1,
        value=-37.36327240989711
    )
    # Prueba de lectura de xml
    assert table == xml_model.AttributeDataTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_attribute_data']
        )['t_attribute_data'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_attribute_data': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_band_table():
    xml = (
        "<t_band>\n"
            "  <data_id>30572</data_id>\n"
            "  <band_id>3</band_id>\n"
        "</t_band>"
    )
    table = xml_model.BandTable(
        data_id=30572,
        band_id=3
    )
    # Prueba de lectura de xml
    assert table == xml_model.BandTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_band']
        )['t_band'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_band': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_category_table():
    xml = (
        "<t_category>\n"
            "  <category_id>3</category_id>\n"
            "  <class_id>3</class_id>\n"
            "  <rank>0</rank>\n"
            "  <name>-</name>\n"
        "</t_category>"
    )
    table = xml_model.CategoryTable(
        category_id=3,
        class_id=3,
        rank=0,
        name="-"
    )
    # Prueba de lectura de xml
    assert table == xml_model.CategoryTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_category']
        )['t_category'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_category': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_class_table():
    xml = (
        "<t_class>\n"
            "  <class_id>1</class_id>\n"
            "  <name>System</name>\n"
            "  <class_group_id>1</class_group_id>\n"
            "  <is_enabled>true</is_enabled>\n"
            "  <lang_id>1</lang_id>\n"
            "  <description>The integrated energy system</description>\n"
        "</t_class>"
    )
    table = xml_model.ClassTable(
        class_id=1,
        name="System",
        class_group_id=1,
        is_enabled=True,
        lang_id=1,
        description="The integrated energy system"
    )
    # Prueba de lectura de xml
    assert table == xml_model.ClassTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_class']
        )['t_class'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_class': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_class_group_table():
    xml = (
        "<t_class_group>\n"
            "  <class_group_id>1</class_group_id>\n"
            "  <name>-</name>\n"
            "  <lang_id>0</lang_id>\n"
        "</t_class_group>"
    )
    table = xml_model.ClassGroupTable(
        class_group_id=1,
        name="-",
        lang_id=0
    )
    # Prueba de lectura de xml
    assert table == xml_model.ClassGroupTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_class_group']
        )['t_class_group'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_class_group': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_collection_table():
    xml = (
        "<t_collection>\n"
            "  <collection_id>1</collection_id>\n"
            "  <parent_class_id>1</parent_class_id>\n"
            "  <child_class_id>2</child_class_id>\n"
            "  <name>Generators</name>\n"
            "  <min_count>0</min_count>\n"
            "  <max_count>-1</max_count>\n"
            "  <is_enabled>true</is_enabled>\n"
            "  <is_one_to_many>true</is_one_to_many>\n"
            "  <lang_id>36</lang_id>\n"
            "  <description>Generator objects</description>\n"
        "</t_collection>"
    )
    table = xml_model.CollectionTable(
        collection_id=1,
        parent_class_id=1,
        child_class_id=2,
        name="Generators",
        min_count=0,
        max_count=-1,
        is_enabled=True,
        is_one_to_many=True,
        lang_id=36,
        description="Generator objects"
    )
    # Prueba de lectura de xml
    assert table == xml_model.CollectionTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_collection']
        )['t_collection'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_collection': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_collection_report_table():
    xml = (
        "<t_collection_report>\n"
            "  <collection_id>58</collection_id>\n"
            "  <left_collection_id>6</left_collection_id>\n"
            "  <right_collection_id>59</right_collection_id>\n"
        "</t_collection_report>"
    )
    table = xml_model.CollectionReportTable(
        collection_id=58,
        left_collection_id=6,
        right_collection_id=59
    )
    # Prueba de lectura de xml
    assert table == xml_model.CollectionReportTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_collection_report']
        )['t_collection_report'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_collection_report': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_config_table():
    xml = (
        "<t_config>\n"
            "  <element>Dynamic</element>\n"
            "  <value>0</value>\n"
        "</t_config>"
    )
    table = xml_model.ConfigTable(
        element="Dynamic",
        value="0"
    )
    # Prueba de lectura de xml
    assert table == xml_model.ConfigTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_config']
        )['t_config'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_config': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_data_table():
    xml = (
        "<t_data>\n"
            "  <data_id>1</data_id>\n"
            "  <membership_id>1</membership_id>\n"
            "  <property_id>347</property_id>\n"
            "  <value>1</value>\n"
            "  <uid>2067428595</uid>\n"
        "</t_data>"
    )
    table = xml_model.DataTable(
        data_id=1,
        membership_id=1,
        property_id=347,
        value=1,
        uid=2067428595
    )
    # Prueba de lectura de xml
    assert table == xml_model.DataTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_data']
        )['t_data'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_data': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_date_from_table():
    xml = (
        "<t_date_from>\n"
            "  <data_id>27316</data_id>\n"
            "  <date>2020-06-05T08:00:00</date>\n"
        "</t_date_from>"
    )
    table = xml_model.DateFromTable(
        data_id=27316,
        date=dt.datetime(2020,6,5,8)
    )
    # Prueba de lectura de xml
    assert table == xml_model.DateFromTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_date_from']
        )['t_date_from'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_date_from': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_date_to_table():
    xml = (
        "<t_date_to>\n"
            "  <data_id>27316</data_id>\n"
            "  <date>2020-06-05T18:00:00</date>\n"
        "</t_date_to>"
    )
    table = xml_model.DateToTable(
        data_id=27316,
        date=dt.datetime(2020,6,5,18)
    )
    # Prueba de lectura de xml
    assert table == xml_model.DateToTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_date_to']
        )['t_date_to'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_date_to': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_membership_table():
    xml = (
        "<t_membership>\n"
            "  <membership_id>1</membership_id>\n"
            "  <parent_class_id>2</parent_class_id>\n"
            "  <parent_object_id>31</parent_object_id>\n"
            "  <collection_id>29</collection_id>\n"
            "  <child_class_id>69</child_class_id>\n"
            "  <child_object_id>2128</child_object_id>\n"
        "</t_membership>"
    )
    table = xml_model.MembershipTable(
        membership_id=1,
        parent_class_id=2,
        parent_object_id=31,
        collection_id=29,
        child_class_id=69,
        child_object_id=2128
    )
    # Prueba de lectura de xml
    assert table == xml_model.MembershipTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_membership']
        )['t_membership'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_membership': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_object_table():
    xml = (
        "<t_object>\n"
            "  <object_id>2</object_id>\n"
            "  <class_id>2</class_id>\n"
            "  <name>ABANICO</name>\n"
            "  <category_id>96</category_id>\n"
            "  <GUID>127a2012-f638-4f28-af07-b326a3cbb405</GUID>\n"
        "</t_object>"
    )
    table = xml_model.ObjectTable(
        object_id=2,
        class_id=2,
        name="ABANICO",
        category_id=96,
        GUID="127a2012-f638-4f28-af07-b326a3cbb405"
    )
    # Prueba de lectura de xml
    assert table == xml_model.ObjectTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_object']
        )['t_object'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_object': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_property_table():
    xml = (
        "<t_property>\n"
            '  <property_id>1</property_id>\n'
            '  <collection_id>1</collection_id>\n'
            '  <property_group_id>2</property_group_id>\n'
            '  <enum_id>1</enum_id>\n'
            '  <name>Must Report</name>\n'
            '  <unit_id>13</unit_id>\n'
            '  <default_value>0</default_value>\n'
            '  <validation_rule>In (0,-1)</validation_rule>\n'
            '  <input_mask>-1;"Yes";0;"No"</input_mask>\n'
            '  <upscaling_method>0</upscaling_method>\n'
            '  <downscaling_method>2</downscaling_method>\n'
            '  <property_type>0</property_type>\n'
            '  <period_type_id>0</period_type_id>\n'
            '  <is_key>false</is_key>\n'
            '  <is_enabled>false</is_enabled>\n'
            '  <is_dynamic>false</is_dynamic>\n'
            '  <is_multi_band>false</is_multi_band>\n'
            '  <max_band_id>1</max_band_id>\n'
            '  <lang_id>525</lang_id>\n'
            '  <description>If the generator must be reported even if it is out-of-service</description>\n'
            '  <tag>800000</tag>\n'
            '  <is_visible>true</is_visible>\n'
        "</t_property>"
    )
    table = xml_model.PropertyTable(
        property_id=1,
        collection_id=1,
        property_group_id=2,
        enum_id=1,
        name="Must Report",
        unit_id=13,
        default_value=0,
        validation_rule="In (0,-1)",
        input_mask='-1;"Yes";0;"No"',
        upscaling_method=0,
        downscaling_method=2,
        property_type=0,
        period_type_id=0,
        is_key=False,
        is_enabled=False,
        is_dynamic=False,
        is_multi_band=False,
        max_band_id=1,
        lang_id=525,
        description="If the generator must be reported even if it is out-of-service",
        tag="800000",
        is_visible=True
    )
    # Prueba de lectura de xml
    assert table == xml_model.PropertyTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_property']
        )['t_property'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_property': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_property_group_table():
    xml = (
        "<t_property_group>\n"
            "  <property_group_id>1</property_group_id>\n"
            "  <name>-</name>\n"
            "  <lang_id>1</lang_id>\n"
        "</t_property_group>"
    )
    table = xml_model.PropertyGroupTable(
        property_group_id=1,
        name="-",
        lang_id=1
    )
    # Prueba de lectura de xml
    assert table == xml_model.PropertyGroupTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_property_group']
        )['t_property_group'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_property_group': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_tag_table():
    xml = (
        "<t_tag>\n"
            "  <data_id>651</data_id>\n"
            "  <object_id>3164</object_id>\n"
        "</t_tag>"
    )
    table = xml_model.TagTable(
        data_id=651,
        object_id=3164
    )
    # Prueba de lectura de xml
    assert table == xml_model.TagTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_tag']
        )['t_tag'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_tag': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_text_table():
    xml = (
        "<t_text>\n"
            "  <data_id>1637</data_id>\n"
            "  <class_id>73</class_id>\n"
            r"  <value>CFdata\CTF\CTFOFF_CELCO_BL2_MR.csv</value>"+"\n"
        "</t_text>"
    )
    table = xml_model.TextTable(
        data_id=1637,
        class_id=73,
        value=r"CFdata\CTF\CTFOFF_CELCO_BL2_MR.csv"
    )
    # Prueba de lectura de xml
    assert table == xml_model.TextTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_text']
        )['t_text'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_text': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_unit_table():
    xml = (
        "<t_unit>\n"
            "  <unit_id>0</unit_id>\n"
            "  <value>-</value>\n"
            "  <default>-</default>\n"
            "  <lang_id>0</lang_id>\n"
        "</t_unit>"
    )
    table = xml_model.UnitTable(
        unit_id=0,
        value="-",
        default="-",
        lang_id=0
    )
    # Prueba de lectura de xml
    assert table == xml_model.UnitTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_unit']
        )['t_unit'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_unit': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_memo_data_table():
    xml = (
        "<t_memo_data>\n"
            "  <data_id>27462</data_id>\n"
            "  <value>250 MVA</value>\n"
        "</t_memo_data>"
    )
    table = xml_model.MemoDataTable(
        data_id=27462,
        value="250 MVA"
    )
    # Prueba de lectura de xml
    assert table == xml_model.MemoDataTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_memo_data']
        )['t_memo_data'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_memo_data': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_property_report_table():
    xml = (
        "<t_property_report>\n"
            "  <property_id>1</property_id>\n"
            "  <collection_id>1</collection_id>\n"
            "  <property_group_id>3</property_group_id>\n"
            "  <enum_id>1</enum_id>\n"
            "  <name>Generation</name>\n"
            "  <summary_name>Generation</summary_name>\n"
            "  <unit_id>1</unit_id>\n"
            "  <summary_unit_id>2</summary_unit_id>\n"
            "  <is_period>true</is_period>\n"
            "  <is_summary>true</is_summary>\n"
            "  <is_multi_band>false</is_multi_band>\n"
            "  <is_quantity>false</is_quantity>\n"
            "  <is_LT>true</is_LT>\n"
            "  <is_PA>false</is_PA>\n"
            "  <is_MT>true</is_MT>\n"
            "  <is_ST>true</is_ST>\n"
            "  <lang_id>227</lang_id>\n"
            "  <summary_lang_id>227</summary_lang_id>\n"
            "  <description>Generation</description>\n"
            "  <is_visible>true</is_visible>\n"
        "</t_property_report>"
    )
    table = xml_model.PropertyReportTable(
        property_id=1,
        collection_id=1,
        property_group_id=3,
        enum_id=1,
        name="Generation",
        summary_name="Generation",
        unit_id=1,
        summary_unit_id=2,
        is_period=True,
        is_summary=True,
        is_multi_band=False,
        is_quantity=False,
        is_LT=True,
        is_PA=False,
        is_MT=True,
        is_ST=True,
        lang_id=227,
        summary_lang_id=227,
        description="Generation",
        is_visible=True
    )
    # Prueba de lectura de xml
    assert table == xml_model.PropertyReportTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_property_report']
        )['t_property_report'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_property_report': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_report_table():
    xml = (
        "<t_report>\n"
            "  <object_id>3184</object_id>\n"
            "  <property_id>1</property_id>\n"
            "  <phase_id>1</phase_id>\n"
            "  <report_period>true</report_period>\n"
            "  <report_summary>true</report_summary>\n"
            "  <report_statistics>false</report_statistics>\n"
            "  <report_samples>false</report_samples>\n"
            "  <write_flat_files>false</write_flat_files>\n"
        "</t_report>"
    )
    table = xml_model.ReportTable(
        object_id=3184,
        property_id=1,
        phase_id=1,
        report_period=True,
        report_summary=True,
        report_statistics=False,
        report_samples=False,
        write_flat_files=False
    )
    # Prueba de lectura de xml
    assert table == xml_model.ReportTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_report']
        )['t_report'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_report': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_action_table():
    xml = (
        "<t_action>\n"
            "  <action_id>0</action_id>\n"
            "  <action_symbol>=</action_symbol>\n"
        "</t_action>"
    )
    table = xml_model.ActionTable(
        action_id=0,
        action_symbol="="
    )
    # Prueba de lectura de xml
    assert table == xml_model.ActionTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_action']
        )['t_action'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_action': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_message_table():
    xml = (
        "<t_message>\n"
            "  <number>1</number>\n"
            "  <severity>2</severity>\n"
            "  <default_action>1</default_action>\n"
            "  <action>1</action>\n"
            "  <description>Running in Diagnostic Mode! Execution may be slow and/or excessive disk space may be consumed.</description>\n"
        "</t_message>"
    )
    table = xml_model.MessageTable(
        number=1,
        severity=2,
        default_action=1,
        action=1,
        description="Running in Diagnostic Mode! Execution may be slow and/or excessive disk space may be consumed."
    )
    # Prueba de lectura de xml
    assert table == xml_model.MessageTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_message']
        )['t_message'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_message': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_property_tag_table():
    xml = (
        "<t_property_tag>\n"
            "  <tag_id>1</tag_id>\n"
            "  <name>Popular</name>\n"
        "</t_property_tag>"
    )
    table = xml_model.PropertyTagTable(
        tag_id=1,
        name="Popular"
    )
    # Prueba de lectura de xml
    assert table == xml_model.PropertyTagTable(
        **xmltodict.parse(
            xml_input=xml,
            force_list=['t_property_tag']
        )['t_property_tag'][0]
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'t_property_tag': table.model_dump(by_alias=True, exclude_none=True)},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml

def test_master_dataset_table():
    xml = (
        '<MasterDataSet xmlns="http://tempuri.org/MasterDataSet.xsd">\n'
            '  <t_attribute>\n'
                '    <attribute_id>1</attribute_id>\n'
                '    <class_id>2</class_id>\n'
                '    <enum_id>1</enum_id>\n'
                '    <name>Latitude</name>\n'
                '    <unit_id>11</unit_id>\n'
                '    <default_value>0</default_value>\n'
                '    <is_enabled>true</is_enabled>\n'
                '    <is_integer>false</is_integer>\n'
                '    <lang_id>343</lang_id>\n'
                '    <description>Latitude</description>\n'
                '    <tag>10000</tag>\n'
                '    <is_visible>true</is_visible>\n'
            '  </t_attribute>\n'
            '  <t_attribute_data>\n'
                '    <object_id>2</object_id>\n'
                '    <attribute_id>1</attribute_id>\n'
                '    <value>-37.36327240989711</value>\n'
            '  </t_attribute_data>\n'
            '  <t_band>\n'
                '    <data_id>30572</data_id>\n'
                '    <band_id>3</band_id>\n'
            '  </t_band>\n'
            '  <t_category>\n'
                '    <category_id>3</category_id>\n'
                '    <class_id>3</class_id>\n'
                '    <rank>0</rank>\n'
                '    <name>-</name>\n'
            '  </t_category>\n'
            '  <t_class>\n'
                '    <class_id>1</class_id>\n'
                '    <name>System</name>\n'
                '    <class_group_id>1</class_group_id>\n'
                '    <is_enabled>true</is_enabled>\n'
                '    <lang_id>1</lang_id>\n'
                '    <description>The integrated energy system</description>\n'
            '  </t_class>\n'
            '  <t_class_group>\n'
                '    <class_group_id>1</class_group_id>\n'
                '    <name>-</name>\n'
                '    <lang_id>0</lang_id>\n'
            '  </t_class_group>\n'
            '  <t_collection>\n'
                '    <collection_id>1</collection_id>\n'
                '    <parent_class_id>1</parent_class_id>\n'
                '    <child_class_id>2</child_class_id>\n'
                '    <name>Generators</name>\n'
                '    <min_count>0</min_count>\n'
                '    <max_count>-1</max_count>\n'
                '    <is_enabled>true</is_enabled>\n'
                '    <is_one_to_many>true</is_one_to_many>\n'
                '    <lang_id>36</lang_id>\n'
                '    <description>Generator objects</description>\n'
            '  </t_collection>\n'
            '  <t_collection_report>\n'
                '    <collection_id>58</collection_id>\n'
                '    <left_collection_id>6</left_collection_id>\n'
                '    <right_collection_id>59</right_collection_id>\n'
            '  </t_collection_report>\n'
            '  <t_config>\n'
                '    <element>Dynamic</element>\n'
                '    <value>0</value>\n'
            '  </t_config>\n'
            '  <t_data>\n'
                '    <data_id>1</data_id>\n'
                '    <membership_id>1</membership_id>\n'
                '    <property_id>347</property_id>\n'
                '    <value>1</value>\n'
                '    <uid>2067428595</uid>\n'
            '  </t_data>\n'
            '  <t_date_from>\n'
                '    <data_id>27316</data_id>\n'
                '    <date>2020-06-05T08:00:00</date>\n'
            '  </t_date_from>\n'
            '  <t_date_to>\n'
                '    <data_id>27316</data_id>\n'
                '    <date>2020-06-05T18:00:00</date>\n'
            '  </t_date_to>\n'
            '  <t_membership>\n'
                '    <membership_id>1</membership_id>\n'
                '    <parent_class_id>2</parent_class_id>\n'
                '    <parent_object_id>31</parent_object_id>\n'
                '    <collection_id>29</collection_id>\n'
                '    <child_class_id>69</child_class_id>\n'
                '    <child_object_id>2128</child_object_id>\n'
            '  </t_membership>\n'
            '  <t_object>\n'
                '    <object_id>2</object_id>\n'
                '    <class_id>2</class_id>\n'
                '    <name>ABANICO</name>\n'
                '    <category_id>96</category_id>\n'
                '    <GUID>127a2012-f638-4f28-af07-b326a3cbb405</GUID>\n'
            '  </t_object>\n'
            '  <t_property>\n'
                '    <property_id>1</property_id>\n'
                '    <collection_id>1</collection_id>\n'
                '    <property_group_id>2</property_group_id>\n'
                '    <enum_id>1</enum_id>\n'
                '    <name>Must Report</name>\n'
                '    <unit_id>13</unit_id>\n'
                '    <default_value>0</default_value>\n'
                '    <validation_rule>In (0,-1)</validation_rule>\n'
                '    <input_mask>-1;"Yes";0;"No"</input_mask>\n'
                '    <upscaling_method>0</upscaling_method>\n'
                '    <downscaling_method>2</downscaling_method>\n'
                '    <property_type>0</property_type>\n'
                '    <period_type_id>0</period_type_id>\n'
                '    <is_key>false</is_key>\n'
                '    <is_enabled>false</is_enabled>\n'
                '    <is_dynamic>false</is_dynamic>\n'
                '    <is_multi_band>false</is_multi_band>\n'
                '    <max_band_id>1</max_band_id>\n'
                '    <lang_id>525</lang_id>\n'
                '    <description>If the generator must be reported even if it is out-of-service</description>\n'
                '    <tag>800000</tag>\n'
                '    <is_visible>true</is_visible>\n'
            '  </t_property>\n'
            '  <t_property_group>\n'
                '    <property_group_id>1</property_group_id>\n'
                '    <name>-</name>\n'
                '    <lang_id>1</lang_id>\n'
            '  </t_property_group>\n'
            '  <t_tag>\n'
                '    <data_id>651</data_id>\n'
                '    <object_id>3164</object_id>\n'
            '  </t_tag>\n'
            '  <t_text>\n'
                '    <data_id>1637</data_id>\n'
                '    <class_id>73</class_id>\n'
                r'    <value>CFdata\CTF\CTFOFF_CELCO_BL2_MR.csv</value>'+'\n'
            '  </t_text>\n'
            '  <t_unit>\n'
                '    <unit_id>0</unit_id>\n'
                '    <value>-</value>\n'
                '    <default>-</default>\n'
                '    <lang_id>0</lang_id>\n'
            '  </t_unit>\n'
            '  <t_memo_data>\n'
                '    <data_id>27462</data_id>\n'
                '    <value>250 MVA</value>\n'
            '  </t_memo_data>\n'
            '  <t_property_report>\n'
                '    <property_id>1</property_id>\n'
                '    <collection_id>1</collection_id>\n'
                '    <property_group_id>3</property_group_id>\n'
                '    <enum_id>1</enum_id>\n'
                '    <name>Generation</name>\n'
                '    <summary_name>Generation</summary_name>\n'
                '    <unit_id>1</unit_id>\n'
                '    <summary_unit_id>2</summary_unit_id>\n'
                '    <is_period>true</is_period>\n'
                '    <is_summary>true</is_summary>\n'
                '    <is_multi_band>false</is_multi_band>\n'
                '    <is_quantity>false</is_quantity>\n'
                '    <is_LT>true</is_LT>\n'
                '    <is_PA>false</is_PA>\n'
                '    <is_MT>true</is_MT>\n'
                '    <is_ST>true</is_ST>\n'
                '    <lang_id>227</lang_id>\n'
                '    <summary_lang_id>227</summary_lang_id>\n'
                '    <description>Generation</description>\n'
                '    <is_visible>true</is_visible>\n'
            '  </t_property_report>\n'
            '  <t_report>\n'
                '    <object_id>3184</object_id>\n'
                '    <property_id>1</property_id>\n'
                '    <phase_id>1</phase_id>\n'
                '    <report_period>true</report_period>\n'
                '    <report_summary>true</report_summary>\n'
                '    <report_statistics>false</report_statistics>\n'
                '    <report_samples>false</report_samples>\n'
                '    <write_flat_files>false</write_flat_files>\n'
            '  </t_report>\n'
            '  <t_action>\n'
                '    <action_id>0</action_id>\n'
                '    <action_symbol>=</action_symbol>\n'
            '  </t_action>\n'
            '  <t_message>\n'
                '    <number>1</number>\n'
                '    <severity>2</severity>\n'
                '    <default_action>1</default_action>\n'
                '    <action>1</action>\n'
                '    <description>Running in Diagnostic Mode! Execution may be slow and/or excessive disk space may be consumed.</description>\n'
            '  </t_message>\n'
            '  <t_property_tag>\n'
                '    <tag_id>1</tag_id>\n'
                '    <name>Popular</name>\n'
            '  </t_property_tag>\n'
        '</MasterDataSet>'
    )
    table = xml_model.MasterDataSet(
        t_attribute=[xml_model.AttributeTable(
            attribute_id=1,
            class_id=2,
            enum_id=1,
            name="Latitude",
            unit_id=11,
            default_value=0,
            validation_rule=None,
            input_mask=None,
            is_enabled=True,
            is_integer=False,
            lang_id=343,
            description="Latitude",
            tag=10000,
            is_visible=True,)],
        t_attribute_data=[xml_model.AttributeDataTable(
            object_id=2,
            attribute_id=1,
            value=-37.36327240989711)],
        t_band=[xml_model.BandTable(
            data_id=30572,
            band_id=3)],
        t_category=[xml_model.CategoryTable(
            category_id=3,
            class_id=3,
            rank=0,
            name="-")],
        t_class=[xml_model.ClassTable(
            class_id=1,
            name="System",
            class_group_id=1,
            is_enabled=True,
            lang_id=1,
            description="The integrated energy system")],
        t_class_group=[xml_model.ClassGroupTable(
            class_group_id=1,
            name="-",
            lang_id=0)],
        t_collection=[xml_model.CollectionTable(
            collection_id=1,
            parent_class_id=1,
            child_class_id=2,
            name="Generators",
            min_count=0,
            max_count=-1,
            is_enabled=True,
            is_one_to_many=True,
            lang_id=36,
            description="Generator objects")],
        t_collection_report=[xml_model.CollectionReportTable(
            collection_id=58,
            left_collection_id=6,
            right_collection_id=59)],
        t_config=[xml_model.ConfigTable(
            element="Dynamic",
            value="0")],
        t_data=[xml_model.DataTable(
            data_id=1,
            membership_id=1,
            property_id=347,
            value=1,
            uid=2067428595)],
        t_date_from=[xml_model.DateFromTable(
            data_id=27316,
            date=dt.datetime(2020,6,5,8))],
        t_date_to=[xml_model.DateToTable(
            data_id=27316,
            date=dt.datetime(2020,6,5,18))],
        t_membership=[xml_model.MembershipTable(
            membership_id=1,
            parent_class_id=2,
            parent_object_id=31,
            collection_id=29,
            child_class_id=69,
            child_object_id=2128)],
        t_object=[xml_model.ObjectTable(
            object_id=2,
            class_id=2,
            name="ABANICO",
            category_id=96,
            GUID="127a2012-f638-4f28-af07-b326a3cbb405")],
        t_property=[xml_model.PropertyTable(
            property_id=1,
            collection_id=1,
            property_group_id=2,
            enum_id=1,
            name="Must Report",
            unit_id=13,
            default_value=0,
            validation_rule="In (0,-1)",
            input_mask='-1;"Yes";0;"No"',
            upscaling_method=0,
            downscaling_method=2,
            property_type=0,
            period_type_id=0,
            is_key=False,
            is_enabled=False,
            is_dynamic=False,
            is_multi_band=False,
            max_band_id=1,
            lang_id=525,
            description="If the generator must be reported even if it is out-of-service",
            tag="800000",
            is_visible=True)],
        t_property_group=[xml_model.PropertyGroupTable(
            property_group_id=1,
            name="-",
            lang_id=1)],
        t_tag=[xml_model.TagTable(
            data_id=651,
            object_id=3164)],
        t_text=[xml_model.TextTable(
            data_id=1637,
            class_id=73,
            value=r"CFdata\CTF\CTFOFF_CELCO_BL2_MR.csv")],
        t_unit=[xml_model.UnitTable(
            unit_id=0,
            value="-",
            default="-",
            lang_id=0)],
        t_memo_data=[xml_model.MemoDataTable(
            data_id=27462,
            value="250 MVA")],
        t_property_report=[xml_model.PropertyReportTable(
            property_id=1,
            collection_id=1,
            property_group_id=3,
            enum_id=1,
            name="Generation",
            summary_name="Generation",
            unit_id=1,
            summary_unit_id=2,
            is_period=True,
            is_summary=True,
            is_multi_band=False,
            is_quantity=False,
            is_LT=True,
            is_PA=False,
            is_MT=True,
            is_ST=True,
            lang_id=227,
            summary_lang_id=227,
            description="Generation",
            is_visible=True)],
        t_report=[xml_model.ReportTable(
            object_id=3184,
            property_id=1,
            phase_id=1,
            report_period=True,
            report_summary=True,
            report_statistics=False,
            report_samples=False,
            write_flat_files=False)],
        t_action=[xml_model.ActionTable(
            action_id=0,
            action_symbol="=")],
        t_message=[xml_model.MessageTable(
            number=1,
            severity=2,
            default_action=1,
            action=1,
            description="Running in Diagnostic Mode! Execution may be slow and/or excessive disk space may be consumed.")],
        t_property_tag=[xml_model.PropertyTagTable(
            tag_id=1,
            name="Popular")]
    )
    namespace_in = {"http://tempuri.org/MasterDataSet.xsd": None}
    namespace_out = {"@xmlns": "http://tempuri.org/MasterDataSet.xsd"}
    force_list_data = [name for name in xml_model.MasterDataSet.model_fields.keys()]
    # Prueba de lectura de xml
    assert table == xml_model.MasterDataSet(
        **xmltodict.parse(
            xml_input=xml,
            force_list=force_list_data,
            process_namespaces=True,
            namespaces=namespace_in
        )['MasterDataSet']
    )
    # Prueba de escritura de xml
    assert xmltodict.unparse(
        {'MasterDataSet': table.model_dump(by_alias=True, exclude_none=True) | namespace_out},
        full_document=False,
        pretty=True,
        indent="  "
    ) == xml
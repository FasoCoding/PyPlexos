CREATE SCHEMA IF NOT EXISTS pcp_model;

CREATE TABLE IF NOT EXISTS pcp_model.t_attribute(
    attribute_id INTEGER,
    class_id INTEGER,
    enum_id INTEGER,
    name VARCHAR,
    unit_id INTEGER,
    default_value DOUBLE,
    validation_rule VARCHAR,
    input_mask VARCHAR,
    is_enabled BOOLEAN,
    is_integer BOOLEAN,
    lang_id INTEGER,
    description VARCHAR,
    tag INTEGER,
    is_visible BOOLEAN,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_attribute_data(
    object_id INTEGER,
    attribute_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_band(
    data_id INTEGER,
    band_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_category(
    category_id INTEGER,
    class_id INTEGER,
    rank INTEGER,
    name VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_class(
    class_id INTEGER,
    name VARCHAR,
    class_group_id INTEGER,
    is_enabled BOOLEAN,
    lang_id INTEGER,
    description VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_class(
    class_group_id INTEGER,
    name VARCHAR,
    lang_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_collection(
    collection_id INTEGER,
    parent_class_id INTEGER,
    child_class_id INTEGER,
    name VARCHAR,
    min_count INTEGER,
    max_count INTEGER,
    is_enabled BOOLEAN,
    is_one_to_many BOOLEAN,
    lang_id INTEGER,
    description VARCHAR,
    complement_name VARCHAR,
    complement_min_count INTEGER,
    complement_max_count INTEGER,
    complement_description VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_collection_report(
    collection_id INTEGER,
    left_collection_id INTEGER,
    right_collection_id INTEGER,
    rule_left_collection_id INTEGER,
    rule_right_collection_id INTEGER,
    rule_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_config(
    element VARCHAR,
    value VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_data(
    data_id INTEGER,
    membership_id INTEGER,
    property_id INTEGER,
    value DOUBLE,
    uid INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_date_from(
    data_id INTEGER,
    date TIMESTAMP,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_date_to(
    data_id INTEGER,
    date TIMESTAMP,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_membership(
    membership_id INTEGER,
    parent_class_id INTEGER,
    parent_object_id INTEGER,
    collection_id INTEGER,
    child_class_id INTEGER,
    child_object_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_object(
    object_id INTEGER,
    class_id INTEGER,
    name VARCHAR,
    category_id INTEGER,
    description VARCHAR,
    guid VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_property(
    property_id INTEGER,
    collection_id INTEGER,
    property_group_id INTEGER,
    enum_id INTEGER,
    name VARCHAR,
    unit_id INTEGER,
    default_value DOUBLE,
    validation_rule VARCHAR,
    input_mask VARCHAR,
    upscaling_method INTEGER,
    downscaling_method INTEGER,
    property_type INTEGER,
    period_type_id INTEGER,
    is_key BOOLEAN,
    is_enabled BOOLEAN,
    is_dynamic BOOLEAN,
    is_multi_band BOOLEAN,
    max_band_id INTEGER,
    lang_id INTEGER,
    description VARCHAR,
    tag VARCHAR,
    is_visible BOOLEAN,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_property_group(
    property_group_id INTEGER,
    name VARCHAR,
    lang_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_tag(
    data_id INTEGER,
    object_id INTEGER,
    action_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_text(
    data_id INTEGER,
    class_id INTEGER,
    value VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_unit(
    unit_id INTEGER,
    value VARCHAR,
    "default" VARCHAR,
    lang_id INTEGER,
    imperial_energy VARCHAR,
    metric_level VARCHAR,
    imperial_level VARCHAR,
    metric_volume VARCHAR,
    imperial_volume VARCHAR,
    description VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_memo_data(
    data_id INTEGER,
    value VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_property_report(
    property_id INTEGER,
    collection_id INTEGER,
    property_group_id INTEGER,
    enum_id INTEGER,
    name VARCHAR,
    summary_name VARCHAR,
    unit_id INTEGER,
    summary_unit_id INTEGER,
    is_period BOOLEAN,
    is_summary BOOLEAN,
    is_multi_band BOOLEAN,
    is_quantity BOOLEAN,
    is_LT BOOLEAN,
    is_PA BOOLEAN,
    is_MT BOOLEAN,
    is_ST BOOLEAN,
    lang_id INTEGER,
    summary_lang_id INTEGER,
    description VARCHAR,
    is_visible BOOLEAN,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_report(
    object_id INTEGER,
    property_id INTEGER,
    phase_id INTEGER,
    report_period BOOLEAN,
    report_summary BOOLEAN,
    report_statistics BOOLEAN,
    report_samples BOOLEAN,
    write_flat_files BOOLEAN,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_action(
    action_id INTEGER,
    action_symbol VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_message(
    number INTEGER,
    severity INTEGER,
    default_action INTEGER,
    action INTEGER,
    description VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_model.t_property_tag(
    tag_id INTEGER,
    name VARCHAR,
    prg_date TIMESTAMP,
);

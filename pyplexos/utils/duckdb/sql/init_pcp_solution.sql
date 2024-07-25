CREATE SCHEMA IF NOT EXISTS pcp_solution;

CREATE TABLE IF NOT EXISTS pcp_solution.t_attribute (
    attribute_id INTEGER,
    class_id INTEGER,
    enum_id INTEGER,
    name VARCHAR,
    description VARCHAR,
    lang_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_attribute_data (
    object_id INTEGER,
    attribute_id INTEGER,
    value INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_band (
    band_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_category (
    category_id INTEGER,
    class_id INTEGER,
    rank INTEGER,
    name VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_class (
    class_id INTEGER,
    name VARCHAR,
    class_group_id INTEGER,
    lang_id INTEGER,
    state INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_class_group (
    class_group_id INTEGER,
    name VARCHAR,
    lang_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_collection (
    collection_id INTEGER,
    parent_class_id INTEGER,
    child_class_id INTEGER,
    name VARCHAR,
    complement_name VARCHAR,
    lang_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_config (
    element VARCHAR,
    value VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_custom_column (
    column_id INTEGER,
    class_id INTEGER,
    name VARCHAR,
    position INTEGER,
    GUID INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_data_0 (
    key_id INTEGER,
    period_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_data_1 (
    key_id INTEGER,
    period_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_data_2 (
    key_id INTEGER,
    period_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_data_3 (
    key_id INTEGER,
    period_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_data_4 (
    key_id INTEGER,
    period_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_data_6 (
    key_id INTEGER,
    period_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_data_7 (
    key_id INTEGER,
    period_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_data_current (
    key_id INTEGER,
    period_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_key (
    key_id INTEGER,
    membership_id INTEGER,
    model_id INTEGER,
    phase_id INTEGER,
    property_id INTEGER,
    period_type_id INTEGER,
    band_id INTEGER,
    sample_id INTEGER,
    timeslice_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_key_index (
    key_id INTEGER,
    period_type_id INTEGER,
    position BIGINT,
    length INTEGER,
    period_offset INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_membership (
    membership_id INTEGER,
    parent_class_id INTEGER,
    child_class_id INTEGER,
    collection_id INTEGER,
    parent_object_id INTEGER,
    child_object_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_memo_object (
    object_id INTEGER,
    column_id INTEGER,
    value VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_model (
    model_id INTEGER,
    name VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_object (
    class_id INTEGER,
    name VARCHAR,
    category_id INTEGER,
    index INTEGER,
    object_id INTEGER,
    show INTEGER,
    GUID VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_object_meta (
    object_id INTEGER,
    class VARCHAR,
    property VARCHAR,
    value VARCHAR,
    state INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_period_0 (
    interval_id INTEGER,
    hour_id INTEGER,
    day_id INTEGER,
    week_id INTEGER,
    month_id INTEGER,
    quarter_id INTEGER,
    fiscal_year_id INTEGER,
    datetime TIMESTAMP,
    period_of_day INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_period_1 (
    day_id INTEGER,
    date TIMESTAMP,
    week_id INTEGER,
    month_id INTEGER,
    quarter_id INTEGER,
    fiscal_year_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_period_2 (
    week_id INTEGER,
    week_ending TIMESTAMP,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_period_3 (
    month_id INTEGER,
    month_beginning TIMESTAMP,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_period_4 (
    fiscal_year_id INTEGER,
    year_ending TIMESTAMP,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_period_6 (
    hour_id INTEGER,
    day_id INTEGER,
    datetime TIMESTAMP,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_period_7 (
    quarter_id INTEGER,
    quarter_beginning TIMESTAMP,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_phase_1 (
    interval_id INTEGER,
    period_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_phase_2 (
    interval_id INTEGER,
    period_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_phase_3 (
    interval_id INTEGER,
    period_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_phase_4 (
    interval_id INTEGER,
    period_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_property (
    property_id INTEGER,
    collection_id INTEGER,
    enum_id INTEGER,
    name VARCHAR,
    summary_name VARCHAR,
    unit_id INTEGER,
    summary_unit_id INTEGER,
    is_multi_band INTEGER,
    is_period INTEGER,
    is_summary INTEGER,
    lang_id INTEGER,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_sample (
    sample_id INTEGER,
    sample_name VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_sample_weight (
    sample_id INTEGER,
    phase_id INTEGER,
    value DOUBLE,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_timeslice (
    timeslice_id INTEGER,
    timeslice_name VARCHAR,
    prg_date TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS pcp_solution.t_unit (
    unit_id INTEGER,
    value VARCHAR,
    lang_id INTEGER,
    prg_date TIMESTAMP,
);

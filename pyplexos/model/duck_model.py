from pathlib import Path
from dataclasses import dataclass

import duckdb as duck

@dataclass
class duck_model:
    conn: duck.DuckDBPyConnection

    @classmethod
    def create_medallion_duck(cls, path: Path) -> duck.DuckDBPyConnection:
        """Create a duckdb connection from plexos solution.

        Returns:
            duck.DuckDBPyConnection: Connection to duckdb.
        """
        return cls(duck.connect(path.as_posix()))

    def duck_bronze_schema(self) -> None:
        """Create bronze layer to duckdb dataset. This layer is a replica of the plexos solution.

        Args:
            conn (duck.DuckDBPyConnection): Duckdb connection object.
        """
        self.conn.sql("CREATE SCHEMA IF NOT EXISTS bronze")
        self.conn.sql(
            """
            CREATE TABLE bronze.t_attribute (
                attribute_id INTEGER,
                class_id INTEGER,
                enum_id INTEGER,
                name VARCHAR,
                description VARCHAR,
                lang_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_attribute_data (
                object_id INTEGER,
                attribute_id INTEGER,
                value INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_band (
                band_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_category (
                category_id INTEGER,
                class_id INTEGER,
                rank INTEGER,
                name VARCHAR,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_class (
                class_id INTEGER,
                name VARCHAR,
                class_group_id INTEGER,
                lang_id INTEGER,
                state INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_class_group (
                class_group_id INTEGER,
                name VARCHAR,
                lang_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_collection (
                collection_id INTEGER,
                parent_class_id INTEGER,
                child_class_id INTEGER,
                name VARCHAR,
                complement_name VARCHAR,
                lang_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_config (
                element VARCHAR,
                value VARCHAR,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_custom_column (
                column_id INTEGER,
                class_id INTEGER,
                name VARCHAR,
                position INTEGER,
                GUID INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_data_0 (
                key_id INTEGER,
                period_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_data_1 (
                key_id INTEGER,
                period_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_data_2 (
                key_id INTEGER,
                period_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_data_3 (
                key_id INTEGER,
                period_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_data_4 (
                key_id INTEGER,
                period_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_data_6 (
                key_id INTEGER,
                period_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_data_7 (
                key_id INTEGER,
                period_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_data_current (
                key_id INTEGER,
                period_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_key (
                key_id INTEGER,
                membership_id INTEGER,
                model_id INTEGER,
                phase_id INTEGER,
                property_id INTEGER,
                period_type_id INTEGER,
                band_id INTEGER,
                sample_id INTEGER,
                timeslice_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_key_index (
                key_id INTEGER,
                period_type_id INTEGER,
                position BIGINT,
                length INTEGER,
                period_offset INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_membership (
                membership_id INTEGER,
                parent_class_id INTEGER,
                child_class_id INTEGER,
                collection_id INTEGER,
                parent_object_id INTEGER,
                child_object_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_memo_object (
                object_id INTEGER,
                column_id INTEGER,
                value VARCHAR,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_model (
                model_id INTEGER,
                name VARCHAR,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_object (
                class_id INTEGER,
                name VARCHAR,
                category_id INTEGER,
                index INTEGER,
                object_id INTEGER,
                show INTEGER,
                GUID VARCHAR,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_object_meta (
                object_id INTEGER,
                class VARCHAR,
                property VARCHAR,
                value VARCHAR,
                state INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_period_0 (
                interval_id INTEGER,
                hour_id INTEGER,
                day_id INTEGER,
                week_id INTEGER,
                month_id INTEGER,
                quarter_id INTEGER,
                fiscal_year_id INTEGER,
                datetime TIMESTAMP,
                period_of_day INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_period_1 (
                day_id INTEGER,
                date TIMESTAMP,
                week_id INTEGER,
                month_id INTEGER,
                quarter_id INTEGER,
                fiscal_year_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_period_2 (
                week_id INTEGER,
                week_ending TIMESTAMP,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_period_3 (
                month_id INTEGER,
                month_beginning TIMESTAMP,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_period_4 (
                fiscal_year_id INTEGER,
                year_ending TIMESTAMP,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_period_6 (
                hour_id INTEGER,
                day_id INTEGER,
                datetime TIMESTAMP,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_period_7 (
                quarter_id INTEGER,
                quarter_beginning TIMESTAMP,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_phase_1 (
                interval_id INTEGER,
                period_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_phase_2 (
                interval_id INTEGER,
                period_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_phase_3 (
                interval_id INTEGER,
                period_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_phase_4 (
                interval_id INTEGER,
                period_id INTEGER,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_property (
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
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_sample (
                sample_id INTEGER,
                sample_name VARCHAR,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_sample_weight (
                sample_id INTEGER,
                phase_id INTEGER,
                value DOUBLE,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_timeslice (
                timeslice_id INTEGER,
                timeslice_name VARCHAR,
            )
            """
        )
        self.conn.sql(
            """
            CREATE TABLE bronze.t_unit (
                unit_id INTEGER,
                value VARCHAR,
                lang_id INTEGER,
            )
            """
        )

    def duck_silver_schema(self) -> None:
        """Create silver layer to duckdb dataset. This layer is a replica of the plexos solution.

        Args:
            conn (duck.DuckDBPyConnection): Duckdb connection object.
        """
        self.conn.sql("CREATE SCHEMA IF NOT EXISTS silver")
        self.conn.sql(
            """
            CREATE VIEW silver.dim_centrales AS (
                select  
                    t_membership.membership_id as id_central,
                    t_object.name as central
                from bronze.t_membership
                inner join bronze.t_object ON t_object.object_id = t_membership.child_object_id
                where t_membership.collection_id == 1
                order by t_membership.membership_id
            )
            """
        )
        self.conn.sql(
            """
            CREATE VIEW silver.dim_fechas AS (
                select 
                    t_phase_3.interval_id as id_intervalo,
                    t_phase_3.period_id as id_periodo,
                    t_period_0.datetime as fecha_hora,
                    t_period_0.datetime::date as fecha,
                    extract('hour' from t_period_0.datetime::time) as hora
                from bronze.t_phase_3
                inner join bronze.t_period_0 on t_period_0.interval_id = t_phase_3.interval_id
                order by t_phase_3.interval_id
            )
            """
        )
        self.conn.sql(
            """
            CREATE VIEW silver.fct_generacion AS (
                select 
                    t_key.membership_id as id_central,
                    t_data_0.period_id as id_periodo,
                    t_data_0.value as generacion
                from bronze.t_data_0
                inner join bronze.t_key on t_key.key_id = t_data_0.key_id
                inner join bronze.t_property on t_property.property_id = t_key.property_id
                where t_property.collection_id = 1 and t_property.name = 'Generation'
                order by t_data_0.value, t_key.membership_id
            )
            """
        )
        #self.conn.sql(
        #    """
        #    with filter_keys as (
        #        select
        #            t_key.key_id,
        #            t_key.membership_id as id_central
        #        from bronze.t_key
        #        inner join bronze.t_property on t_property.property_id = t_key.property_id
        #        where t_property.collection_id = 1 and t_property.name = 'Generation'
        #        order by id_central
        #    )

        #    create view silver.fct_gen_2 as (
        #        select
        #            filter_keys.id_central as id_central,
        #            t_data_0.period_id as id_periodo,
        #            t_data_0.value as available_capacity
        #        from bronze.t_data_0
        #        inner join filter_keys on filter_keys.key_id = t_data_0.key_id;
        #        order by id_central, id_periodo
        #    )
        #    """
        #)

def duck_silver_schema_materialize(conn: duck.DuckDBPyConnection) -> None:
    """
    Add siler layer to duckdb dataset.

    Args:
        path_to_dir (str): path to directory to save parquets.

    Returns:
        None.
    """
    
    # inicia creacion de capa silver
    conn.sql("CREATE SCHEMA IF NOT EXISTS silver")
    conn.sql("""
        CREATE TABLE silver.dim_centrales (
            id_central INTEGER PRIMARY KEY,
            central STRING UNIQUE
        )
        """
    )
    conn.sql(
        """
        INSERT INTO silver.dim_centrales
        select  
            t_membership.membership_id as id_central,
            t_object.name as central
        from bronze.t_membership
        inner join bronze.t_object ON t_object.object_id = t_membership.child_object_id
        where t_membership.collection_id == 1
        order by t_membership.membership_id
        """
    )
    conn.sql("""
        CREATE TABLE silver.dim_fechas (
            id_fecha INTEGER PRIMARY KEY,
            id_periodo INTEGER,
            fecha_hora TIMESTAMP UNIQUE,
            fecha DATE,
            hora INTEGER CHECK(hora >= 0 and hora <= 24)
        )
        """
    )
    conn.sql(
        """
        INSERT INTO silver.dim_fechas
        select 
            t_phase_3.interval_id as id_fecha,
            t_phase_3.period_id as id_periodo,
            t_period_0.datetime as fecha,
            t_period_0.datetime::date as fecha,
            extract('hour' from t_period_0.datetime::time) as hora
        from bronze.t_phase_3
        inner join bronze.t_period_0 on t_period_0.interval_id = t_phase_3.interval_id
        order by t_phase_3.interval_id
        """
    )
    conn.sql(
        """
        CREATE TABLE silver.fct_generacion (
            id_central INTEGER NOT NULL,
            id_fecha INTEGER NOT NULL,
            generacion DOUBLE NOT NULL,
            PRIMARY KEY (id_central, id_fecha)
        )
        """
    )
    conn.sql(
        """
        INSERT INTO silver.fct_generacion
        select 
            t_key.membership_id as id_central,
            t_data_0.period_id as id_fecha,
            t_data_0.value as generacion
        from bronze.t_data_0
        inner join bronze.t_key on t_key.key_id = t_data_0.key_id
        inner join bronze.t_property on t_property.property_id = t_key.property_id
        where t_property.property_id == 1
        order by t_data_0.value, t_key.membership_id
        """
    )
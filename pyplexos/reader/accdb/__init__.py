# TODO. Class for accdb.
from pathlib import Path
from typing import Any
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import (
    engine,
    create_engine,
)

import pyarrow as pa
import polars as pl

from pyplexos.schema import PlexosSolution, SolutionSchema

def read_accdb(accdb_file_path: str) -> PlexosSolution:
    path = Path(accdb_file_path)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exists: {accdb_file_path}")
    
    accdb_engine = create_accdb_engine(path)
    
    try:
        with accdb_engine.connect() as conn:
            data: PlexosSolution = PlexosSolution(
                    t_unit= get_access_data(SolutionSchema.t_unit.value,conn),
                    t_band= get_access_data(SolutionSchema.t_band.value,conn),
                    t_category= get_access_data(SolutionSchema.t_category.value,conn),
                    t_class= get_access_data(SolutionSchema.t_class.value,conn),
                    t_class_group= get_access_data(SolutionSchema.t_class_group.value,conn),
                    t_collection= get_access_data(SolutionSchema.t_collection.value,conn),
                    t_config= get_access_data(SolutionSchema.t_config.value,conn),
                    t_key= get_access_data(SolutionSchema.t_key.value,conn),
                    t_membership= get_access_data(SolutionSchema.t_membership.value,conn),
                    t_model= get_access_data(SolutionSchema.t_model.value,conn),
                    t_object= get_access_data(SolutionSchema.t_object.value,conn),
                    t_period_0= get_access_data(SolutionSchema.t_period_0.value,conn),
                    t_period_1= get_access_data(SolutionSchema.t_period_1.value,conn),
                    t_period_2= get_access_data(SolutionSchema.t_period_2.value,conn),
                    t_period_3= get_access_data(SolutionSchema.t_period_3.value,conn),
                    t_period_4= get_access_data(SolutionSchema.t_period_4.value,conn),
                    t_period_6= get_access_data(SolutionSchema.t_period_6.value,conn),
                    t_period_7= get_access_data(SolutionSchema.t_period_7.value,conn),
                    t_phase_1= get_access_data(SolutionSchema.t_phase_1.value,conn),
                    t_phase_2= get_access_data(SolutionSchema.t_phase_2.value,conn),
                    t_phase_3= get_access_data(SolutionSchema.t_phase_3.value,conn),
                    t_phase_4= get_access_data(SolutionSchema.t_phase_4.value,conn),
                    t_sample= get_access_data(SolutionSchema.t_sample.value,conn),
                    t_timeslice= get_access_data(SolutionSchema.t_timeslice.value,conn),
                    t_key_index= get_access_data(SolutionSchema.t_key_index.value,conn),
                    t_property= get_access_data(SolutionSchema.t_property.value,conn),
                    t_attribute_data= get_access_data(SolutionSchema.t_attribute_data.value,conn),
                    t_attribute= get_access_data(SolutionSchema.t_attribute.value,conn),
                    t_sample_weight= get_access_data(SolutionSchema.t_sample_weight.value,conn),
                    t_custom_column= get_access_data(SolutionSchema.t_custom_column.value,conn),
                    t_memo_object= get_access_data(SolutionSchema.t_memo_object.value,conn),
                    t_object_meta= get_access_data(SolutionSchema.t_object_meta.value,conn),
                    t_data_0= get_access_data(SolutionSchema.t_data_0.value,conn),
                )
    except SQLAlchemyError as e:
        print(f"Error: {e}")
    
    finally:
        conn.close()
    
    return data
    

def create_accdb_engine(path_prg: Path) -> engine.Engine:
    """Creates a SQLAlchemy engine for a Microsoft Access database.

    This function takes a path to a Microsoft Access database file and returns a SQLAlchemy engine
    that can be used to interact with the database.

    Args:
        path_prg (Path): A pathlib.Path object representing the path to the .mdb or .accdb file.

    Raises:
        ValueError: If the provided path does not exist.

    Returns:
        engine.Engine: A SQLAlchemy engine object.
    """
    connection_string = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={path_prg.as_posix()};"
        r"ExtendedAnsiSQL=1;"
    )
    connection_url = engine.URL.create(
        "access+pyodbc", query={"odbc_connect": connection_string}
    )

    return create_engine(connection_url)


def get_access_data(table_name: str, conn: Any) -> pa.Table:
    """Wrapper function to read data from a Microsoft Access database.

    Args:
        sql_str (str): sql query to be executed.
        prg_engine (engine.Engine): SQLAlchemy engine object.

    Raises:
        f: SQLAlchemyError if connection to database fails.

    Returns:
        pl.DataFrame: A polars DataFrame with the results of the query.
    """
    try:
        return pl.read_database(query=f"select * from {table_name}", connection=conn).to_arrow()
    except SQLAlchemyError as e:
        print(f"Error: {e}")
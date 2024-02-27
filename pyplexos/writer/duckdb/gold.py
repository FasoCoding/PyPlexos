import duckdb as duck
import polars as pl


def get_gen_by_tecnology(conn: duck.DuckDBPyConnection) -> pl.LazyFrame:
    fct_generator: pl.LazyFrame = conn.table("silver.fct_generator").pl().lazy()
    dim_generator: pl.LazyFrame = conn.table("silver.dim_generator").pl().lazy()
    dim_datetime: pl.LazyFrame = conn.table("silver.dim_datetime").pl().lazy()

    return (
        fct_generator
        .select(
            pl.col("id_generator"),
            pl.col("id_period"),
            pl.col("generation")
        )
        .join(
            dim_generator.select(
                pl.col("id_generator"),
                #pl.col("generator_name"),
                pl.col("category_name"),
            ),
            on="id_generator",
            how="inner"
        )
        .join(
            dim_datetime.select(
                pl.col("id_period"),
                pl.col("datetime")
            ),
            on="id_period",
            how="inner"
        )
        .groupby(by=["category_name", "datetime"])
        .agg(pl.sum("generation").alias("generation"))
    )

def set_gold_schema(conn: duck.DuckDBPyConnection) -> None:
    conn.sql("CREATE SCHEMA IF NOT EXISTS gold")

    technology_gen = get_gen_by_tecnology(conn)
    conn.from_arrow(technology_gen.collect().to_arrow()).to_table("gold.technology_generation")
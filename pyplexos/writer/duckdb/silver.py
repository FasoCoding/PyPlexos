import duckdb as duck
import polars as pl

object_map: dict = {
    "generator": 1,
    "fuel": 33,
    "storage": 80,
    "waterway": 88,
    "reserve": 115,
    "node": 245,
    "line": 264,
    "constrain": 635,
    "variable": 642,
}

def get_dim_factory(conn: duck.DuckDBPyConnection) -> pl.LazyFrame:
    t_membership: pl.LazyFrame = conn.table("bronze.t_membership").pl().lazy()
    t_object: pl.LazyFrame = conn.table("bronze.t_object").pl().lazy()
    t_category: pl.LazyFrame = conn.table("bronze.t_category").pl().lazy()

    return (
        t_membership
        .filter(pl.col("parent_object_id").eq(1))
        .select(
            pl.col("membership_id"),
            pl.col("collection_id"),
            pl.col("child_object_id").alias("object_id")
        )
        .join(
            t_object.select(
                pl.col("object_id"),
                pl.col("name").alias("object_name"),
                pl.col("category_id")
            ),
            on="object_id",
            how="inner"
        )
        .join(
            t_category.select(
                pl.col("category_id"),
                pl.col("name").alias("category_name")
            ),
            on="category_id",
            how="left"
        )
        .select(
            pl.col("membership_id"),
            pl.col("collection_id"),
            pl.col("object_name"),
            pl.col("category_name")
        )
        .sort(by=["category_name","membership_id"])
    )


def get_fct_factory(conn: duck.DuckDBPyConnection) -> pl.LazyFrame:
    t_data: pl.LazyFrame = conn.execute("select * from bronze.t_data_0").pl().lazy()
    t_key: pl.LazyFrame = conn.execute("select * from bronze.t_key").pl().lazy()
    t_property: pl.LazyFrame = conn.execute("select * from bronze.t_property").pl().lazy()

    return (
        t_data
        .join(
            t_key.select(
                pl.col("key_id"),
                pl.col("membership_id"),
                pl.col("property_id")
            ),
            on="key_id",
            how="inner"
        )
        .join(
            t_property.select(
                pl.col("property_id"),
                pl.col("collection_id"),
                pl.col("name")
            ),
            on="property_id",
            how="inner"
        )
        .with_columns(
                (pl.col("name")
                 .str.replace_all(" ","_",literal=True)
                 .str.to_lowercase()
                 .alias("property"))
        )
    )
    

def set_silver_schema(conn: duck.DuckDBPyConnection) -> None:
    conn.sql("CREATE SCHEMA IF NOT EXISTS silver")

    lazy_fct = get_fct_factory(conn)
    lazy_dim = get_dim_factory(conn)

    for name, collection_value in object_map.items():
        fct = (
            lazy_fct
            .filter(pl.col("collection_id").eq(collection_value))
            .sort(by="property")
            .collect()
            .pivot(
                values="value",
                columns="property",
                index=["membership_id","period_id"]
            )
            .sort(by=["membership_id","period_id"])
            .rename({"membership_id": f"id_{name}","period_id": "id_period"})
        ).to_arrow()
        conn.from_arrow(fct).to_table(f"silver.fct_{name}")

        dim = (
            lazy_dim
            .filter(pl.col("collection_id").eq(collection_value))
            .select(pl.exclude("collection_id"))
            .collect()
            .rename({"membership_id": f"id_{name}",
                     "object_name": f"{name}_name"})
        ).to_arrow()
        conn.from_arrow(dim).to_table(f"silver.dim_{name}")

    
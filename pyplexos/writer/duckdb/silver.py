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
    "constraint": 635,
    "variable": 642,
}

link_map: dict = {
    "nodes_generator": 12,
    "fuels_generator": 6,
    "in_storage_generator": 8,
    "out_storage_generator": 9,
    "constraint_generator": 29,
    #"variables_generator": 31, no está en la lista de membership
    "constraint_fuels": 44,
    "constraint_storage": 84,
    "storage_from_waterway": 91,
    "storage_to_waterway": 92,
    "constraint_waterway": 93,
    "generator_reserve": 118,
    "constraint_reserve": 128,
    "line_from_node": 267,
    "line_to_node": 268,
    "constraint_line": 271,
    "constraint_variable": 645,
}

def get_link_factory(conn: duck.DuckDBPyConnection, collection_id: int, parent_id: int, child_id: int) -> pl.LazyFrame:
    t_membership: pl.LazyFrame = conn.table("bronze.t_membership").pl().lazy()
    t_child: pl.LazyFrame = conn.table("bronze.t_object").pl().lazy()
    t_parent: pl.LazyFrame = conn.table("bronze.t_object").pl().lazy()

    return (
        t_membership
        .select(
            pl.col("collection_id"),
            pl.col("parent_object_id"),
            pl.col("child_object_id"),
        )
        .filter(pl.col("collection_id").eq(collection_id))
        .join(
            (
                t_child
                .filter(pl.col("class_id").eq(child_id))
                .select(
                    pl.col("object_id").alias("child_object_id"),
                    pl.col("name").alias("child_name")
                )
            ),
            on="child_object_id",
            how="inner"
        )
        .join(
            (
                t_parent
                .filter(pl.col("class_id").eq(parent_id))
                .select(
                    pl.col("object_id").alias("parent_object_id"),
                    pl.col("name").alias("parent_name")
                )
            ),
            on="parent_object_id",
            how="inner"
        )
        .select(

            pl.col("parent_object_id"),
            pl.col("child_object_id"),
            pl.col("parent_name"),
            pl.col("child_name"),
        )
    )

def get_dim_time(conn: duck.DuckDBPyConnection) -> pl.LazyFrame:
    t_key: pl.LazyFrame = conn.table("bronze.t_key").pl().lazy()
    phase_id: int = t_key.select(pl.col("phase_id").first()).collect().item()
    t_phase: pl.LazyFrame = conn.table(f"bronze.t_phase_{phase_id}").pl().lazy()
    t_period: pl.LazyFrame = conn.table("bronze.t_period_0").pl().lazy()

    return (
        t_phase
        .join(
            t_period,
            on="interval_id",
            how="inner"
        )
        .with_columns(
            pl.col("datetime").dt.date().alias("date"),
            pl.col("datetime").dt.hour().alias("time")
        )
        .rename({"interval_id": "id_interval",
                 "period_id": "id_period"})
        .sort(by="id_interval")
    )


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
    
    lazy_date = get_dim_time(conn)
    conn.from_arrow(lazy_date.collect().to_arrow()).to_table("silver.dim_datetime")

    t_collection: pl.LazyFrame = conn.table("bronze.t_collection").pl().lazy()
    for name, collection_value in link_map.items():
        filter_collection: pl.LazyFrame = t_collection.filter(pl.col("collection_id").eq(collection_value))
        child_id: int = filter_collection.select(pl.col("child_class_id")).collect().item()
        parent_id: int = filter_collection.select(pl.col("parent_class_id")).collect().item()
        parent_name: str = filter_collection.select(pl.col("complement_name")).collect().item().lower()
        child_name: str = filter_collection.select(pl.col("name")).collect().item().lower()
        lazy_link: pl.LazyFrame = get_link_factory(conn, collection_value, parent_id, child_id)
        lazy_link = lazy_link.rename(
            {
                "parent_object_id": f"id_{parent_name}",
                "child_object_id": f"id_{child_name}",
                "parent_name": f"{parent_name}_name",
                "child_name": f"{child_name}_name",
            }
        )
        conn.from_arrow(lazy_link.collect().to_arrow()).to_table(f"silver.lnk_{name}")

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

    
from typing import Callable, Optional
from enum import Enum

import pyarrow as pa
import polars as pl

from pyplexos.model import PlexosSolution
from pyplexos.model.query import QueryCollection

ReaderFunction = Callable[[], PlexosSolution]
WriterFunction = Callable[[dict[str, pa.Table]], None]
ProcessFunction = Callable[[PlexosSolution], dict[str, pa.Table]]

QueryFunction = Callable[[QueryCollection], pa.Table]


def pipeline(
    reader: ReaderFunction, writer: WriterFunction, process: ProcessFunction
) -> None:
    writer(process(reader()))


def query(solution_data: PlexosSolution, type: str) -> pa.Table:
    ...


def query_datetime_setup(
        t_phase: pa.Table, t_period: pa.Table
) -> Callable[[], pa.Table]:
    pl_phase: pl.LazyFrame = pl.from_arrow(t_phase).lazy() # type: ignore
    pl_period: pl.LazyFrame = pl.from_arrow(t_period).lazy() # type: ignore

    def query_dim_time() -> pa.Table:
        return (
            pl_phase
            .join(
                pl_period,
                on="interval_id",
                how="inner"
            )
            .with_columns(
                pl.col("datetime").dt.date().alias("date"),
                pl.col("datetime").dt.hour().alias("time")
            )
            .sort(by="interval_id")
            .collect()
            .to_arrow()
        )
    return query_dim_time

def query_dim_setup(
    t_membership: pa.Table, t_object: pa.Table, t_category: pa.Table
) -> pa.Table:
    pl_membership: pl.LazyFrame = pl.from_arrow(t_membership).lazy() # type: ignore
    pl_object: pl.LazyFrame = pl.from_arrow(t_object).lazy() # type: ignore
    pl_category: pl.LazyFrame = pl.from_arrow(t_category).lazy() # type: ignore

    lazy_dim = (
        pl_membership
        .filter(pl.col("parent_object_id").eq(1))
        .select(
            pl.col("membership_id"),
            pl.col("collection_id"),
            pl.col("child_object_id").alias("object_id")
        )
        .join(
            pl_object.select(
                pl.col("object_id"),
                pl.col("name").alias("object_name"),
                pl.col("category_id")
            ),
            on="object_id",
            how="inner"
        )
        .join(
            pl_category.select(
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

    def query_dim(collection: QueryCollection) -> QueryFunction:
        return (
            lazy_dim
            .filter(pl.col("collection_id").eq(collection.value))
            .select(pl.exclude("collection_id"))
            .rename({"membership_id": f"{collection.name.lower()}_id",
                     "object_name": f"{collection.name.lower()}_name"})
            .collect()
        ).to_arrow()
    
    return query_dim

def query_fct_setup(
    t_data: pa.Table, t_key: pa.Table, t_property: pa.Table
) -> pa.Table:
    pl_data: pl.LazyFrame = pl.from_arrow(t_data).lazy() # type: ignore
    pl_key: pl.LazyFrame = pl.from_arrow(t_key).lazy() # type: ignore
    pl_property: pl.LazyFrame = pl.from_arrow(t_property).lazy() # type: ignore

    lazy_fct = (
        pl_data
        .join(
            pl_key.select(
                pl.col("key_id"),
                pl.col("membership_id"),
                pl.col("property_id")
            ),
            on="key_id",
            how="inner"
        )
        .join(
            pl_property.select(
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

    def query_fct_collection(collection: QueryCollection) -> Callable[[Optional[Enum]],pa.Table]:
        lazy_fct_collection = (
            lazy_fct
            .filter(pl.col("collection_id").eq(collection.value))
            .select(pl.exclude(["collection_id","name"]))
            .rename({"membership_id": f"{collection.name.lower()}_id",})
        )

        def query_fct(property: Optional[Enum] = None) -> QueryFunction:
            if property is None:
                return (
                    lazy_fct_collection
                    .select(pl.exclude("property"))
                    .collect()
                    .to_arrow()
                )
            
            return (
                lazy_fct_collection
                .filter(
                    pl.col("property").eq(property.name.lower())
                )
                .select(pl.exclude("property"))
                .collect()
                .to_arrow()
            )
        
        return query_fct
    
    return query_fct_collection

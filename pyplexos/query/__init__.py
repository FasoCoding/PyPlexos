from dataclasses import dataclass
from typing import Self

#import pyarrow as pa
import polars as pl

from pyplexos.solution import PlexosSolution
from pyplexos.model.query import QueryCollection, QueryProperty

@dataclass
class QuerySolution:
    fact_table: pl.LazyFrame
    dim_table: pl.LazyFrame
    datetime_table: pl.LazyFrame

    @classmethod
    def from_solution(cls, solution: PlexosSolution) -> Self:
        
        return cls(
            fact_table = lazy_fact(solution),
            dim_table = lazy_dim(solution),
            datetime_table = lazy_datetime(solution),
        )

    def obt(self, collection: int, property: int) -> pl.LazyFrame:
        return (
            self.fact_table
            .join(
                self.datetime_table,
                on="period_id",
                how="left",
            )
            .join(
                self.dim_table,
                on=["membership_id", "collection_id"],
                how="left"
            )
            .filter(
                pl.col("collection_id").eq(collection),
                pl.col("property_id").eq(property)
            )
            .select(
                pl.col("datetime"),
                pl.col("name").alias("property"),
                pl.col("object_name"),#.alias("category"),
                pl.col("category_name"),
                pl.col("value")
            )
        )
    
    @property
    def gen(self) -> pl.DataFrame:
        return (
            self.obt(
                QueryCollection.GENERATOR.value,
                QueryProperty.GENERATOR.GENERATION.value
            )
            .rename({"object_name":"central"})
            .collect()
        )

    @property
    def cmg(self) -> pl.DataFrame:
        return (
            self.obt(
                QueryCollection.NODE.value,
                QueryProperty.NODE.PRICE.value
            )
            .rename({"object_name":"node"})
            .collect()
        )

def lazy_datetime(solution: PlexosSolution) -> pl.LazyFrame:
    phase = pl.from_arrow(solution.t_key).select("phase_id").item(0,0) # type: ignore
    match phase:
        case 1:
            pl_phase: pl.LazyFrame = pl.from_arrow(solution.t_phase_1) # type: ignore
        case 2:
            pl_phase: pl.LazyFrame = pl.from_arrow(solution.t_phase_2) # type: ignore
        case 3:
            pl_phase: pl.LazyFrame = pl.from_arrow(solution.t_phase_3) # type: ignore
        case 4:
            pl_phase: pl.LazyFrame = pl.from_arrow(solution.t_phase_4) # type: ignore
    
    return (
        pl_phase
        .join(
            pl.from_arrow(solution.t_period_0), # type: ignore
            on="interval_id",
            how="inner"
        )
        .with_columns(
            pl.col("datetime").dt.date().alias("date"),
            pl.col("datetime").dt.hour().alias("time")
        )
        .sort(by="interval_id")
        .lazy()
    )

def lazy_dim(solution: PlexosSolution) -> pl.LazyFrame:
    pl_membership: pl.LazyFrame = pl.from_arrow(solution.t_membership).lazy() # type: ignore
    pl_object: pl.LazyFrame = pl.from_arrow(solution.t_object).lazy() # type: ignore
    pl_category: pl.LazyFrame = pl.from_arrow(solution.t_category).lazy() # type: ignore

    return (
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
        .lazy()
    )

def lazy_fact(solution: PlexosSolution) -> pl.LazyFrame:
    pl_data: pl.LazyFrame = pl.from_arrow(solution.t_data_0).lazy() # type: ignore
    pl_key: pl.LazyFrame = pl.from_arrow(solution.t_key).lazy() # type: ignore
    pl_property: pl.LazyFrame = pl.from_arrow(solution.t_property).lazy() # type: ignore

    return (
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
        .lazy()
    )
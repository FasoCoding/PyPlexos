from dataclasses import dataclass, fields
from enum import Enum
from pathlib import Path
from typing import Any, Self

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from pyplexos.solution.accdb import create_accdb_engine, get_data
from pyplexos.solution.zip import extract_zip_data

from pyplexos.solution.schema import QuerySchema
from pyplexos.solution.schema import SolutionSchema as SolSch
from pyplexos.solution.duckdb import write_duckdb


@dataclass
class PlexosSolution:
    t_unit: pa.Table
    t_band: pa.Table
    t_category: pa.Table
    t_class: pa.Table
    t_class_group: pa.Table
    t_collection: pa.Table
    t_config: pa.Table
    t_key: pa.Table
    t_membership: pa.Table
    t_model: pa.Table
    t_object: pa.Table
    t_period_0: pa.Table
    t_period_1: pa.Table
    t_period_2: pa.Table
    t_period_3: pa.Table
    t_period_4: pa.Table
    t_period_6: pa.Table
    t_period_7: pa.Table
    t_phase_1: pa.Table
    t_phase_2: pa.Table
    t_phase_3: pa.Table
    t_phase_4: pa.Table
    t_sample: pa.Table
    t_timeslice: pa.Table
    t_key_index: pa.Table
    t_property: pa.Table
    t_attribute_data: pa.Table
    t_attribute: pa.Table
    t_sample_weight: pa.Table
    t_custom_column: pa.Table
    t_memo_object: pa.Table
    t_object_meta: pa.Table
    t_data_0: pa.Table

    def items(self):
        for field in fields(self):
            yield field.name, getattr(self, field.name)

    def __getitem__(self, item):
        return getattr(self, item)

    @classmethod
    def from_zip(cls, zip_file_path: str) -> Self:
        path = Path(zip_file_path)

        if not path.exists():
            raise FileNotFoundError(f"Path does not exists: {zip_file_path}")

        data = extract_zip_data(path=path)

        return cls(
            t_unit=pa.Table.from_pylist(data[SolSch.t_unit.value]),
            t_band=pa.Table.from_pylist(data[SolSch.t_band.value]),
            t_category=pa.Table.from_pylist(data[SolSch.t_category.value]),
            t_class=pa.Table.from_pylist(data[SolSch.t_class.value]),
            t_class_group=pa.Table.from_pylist(data[SolSch.t_class_group.value]),
            t_collection=pa.Table.from_pylist(data[SolSch.t_collection.value]),
            t_config=pa.Table.from_pylist(data[SolSch.t_config.value]),
            t_key=pa.Table.from_pylist(data[SolSch.t_key.value]),
            t_membership=pa.Table.from_pylist(data[SolSch.t_membership.value]),
            t_model=pa.Table.from_pylist(data[SolSch.t_model.value]),
            t_object=pa.Table.from_pylist(data[SolSch.t_object.value]),
            t_period_0=pa.Table.from_pylist(data[SolSch.t_period_0.value]),
            t_period_1=pa.Table.from_pylist(data[SolSch.t_period_1.value]),
            t_period_2=pa.Table.from_pylist(data[SolSch.t_period_2.value]),
            t_period_3=pa.Table.from_pylist(data[SolSch.t_period_3.value]),
            t_period_4=pa.Table.from_pylist(data[SolSch.t_period_4.value]),
            t_period_6=pa.Table.from_pylist(data[SolSch.t_period_6.value]),
            t_period_7=pa.Table.from_pylist(data[SolSch.t_period_7.value]),
            t_phase_1=pa.Table.from_pylist(data[SolSch.t_phase_1.value]),
            t_phase_2=pa.Table.from_pylist(data[SolSch.t_phase_2.value]),
            t_phase_3=pa.Table.from_pylist(data[SolSch.t_phase_3.value]),
            t_phase_4=pa.Table.from_pylist(data[SolSch.t_phase_4.value]),
            t_sample=pa.Table.from_pylist(data[SolSch.t_sample.value]),
            t_timeslice=pa.Table.from_pylist(data[SolSch.t_timeslice.value]),
            t_key_index=pa.Table.from_pylist(data[SolSch.t_key_index.value]),
            t_property=pa.Table.from_pylist(data[SolSch.t_property.value]),
            t_attribute_data=pa.Table.from_pylist(data[SolSch.t_attribute_data.value]),
            t_attribute=pa.Table.from_pylist(data[SolSch.t_attribute.value]),
            t_sample_weight=pa.Table.from_pylist(data[SolSch.t_sample_weight.value]),
            t_custom_column=pa.Table.from_pylist(data[SolSch.t_custom_column.value]),
            t_memo_object=pa.Table.from_pylist(data[SolSch.t_memo_object.value]),
            t_object_meta=pa.Table.from_pylist(data[SolSch.t_object_meta.value]),
            t_data_0=pa.Table.from_pydict(data[SolSch.t_data_0.value]),
        )

    @classmethod
    def from_accdb(cls, accdb_file_path: str) -> Self:
        path = Path(accdb_file_path)

        if not path.exists():
            raise FileNotFoundError(f"Path does not exists: {accdb_file_path}")

        accdb_engine = create_accdb_engine(path)

        with accdb_engine.connect() as conn:
            data = {
                SolSch.t_unit.value: get_data(SolSch.t_unit.value, conn),
                SolSch.t_band.value: get_data(SolSch.t_band.value, conn),
                SolSch.t_category.value: get_data(SolSch.t_category.value, conn),
                SolSch.t_class.value: get_data(SolSch.t_class.value, conn),
                SolSch.t_class_group.value: get_data(SolSch.t_class_group.value, conn),
                SolSch.t_collection.value: get_data(SolSch.t_collection.value, conn),
                SolSch.t_config.value: get_data(SolSch.t_config.value, conn),
                SolSch.t_key.value: get_data(SolSch.t_key.value, conn),
                SolSch.t_membership.value: get_data(SolSch.t_membership.value, conn),
                SolSch.t_model.value: get_data(SolSch.t_model.value, conn),
                SolSch.t_object.value: get_data(SolSch.t_object.value, conn),
                SolSch.t_period_0.value: get_data(SolSch.t_period_0.value, conn),
                SolSch.t_period_1.value: get_data(SolSch.t_period_1.value, conn),
                SolSch.t_period_2.value: get_data(SolSch.t_period_2.value, conn),
                SolSch.t_period_3.value: get_data(SolSch.t_period_3.value, conn),
                SolSch.t_period_4.value: get_data(SolSch.t_period_4.value, conn),
                SolSch.t_period_6.value: get_data(SolSch.t_period_6.value, conn),
                SolSch.t_period_7.value: get_data(SolSch.t_period_7.value, conn),
                SolSch.t_phase_1.value: get_data(SolSch.t_phase_1.value, conn),
                SolSch.t_phase_2.value: get_data(SolSch.t_phase_2.value, conn),
                SolSch.t_phase_3.value: get_data(SolSch.t_phase_3.value, conn),
                SolSch.t_phase_4.value: get_data(SolSch.t_phase_4.value, conn),
                SolSch.t_sample.value: get_data(SolSch.t_sample.value, conn),
                SolSch.t_timeslice.value: get_data(SolSch.t_timeslice.value, conn),
                SolSch.t_key_index.value: get_data(SolSch.t_key_index.value, conn),
                SolSch.t_property.value: get_data(SolSch.t_property.value, conn),
                SolSch.t_attribute_data.value: get_data(
                    SolSch.t_attribute_data.value, conn
                ),
                SolSch.t_attribute.value: get_data(SolSch.t_attribute.value, conn),
                SolSch.t_sample_weight.value: get_data(
                    SolSch.t_sample_weight.value, conn
                ),
                SolSch.t_custom_column.value: get_data(
                    SolSch.t_custom_column.value, conn
                ),
                SolSch.t_memo_object.value: get_data(SolSch.t_memo_object.value, conn),
                SolSch.t_object_meta.value: get_data(SolSch.t_object_meta.value, conn),
                SolSch.t_data_0.value: get_data(SolSch.t_data_0.value, conn),
            }

        return cls(**data)

    def to_parquet(self, path_to_folder: str, **kwargs: Any) -> None:
        path = Path(path_to_folder)

        if not path.exists():
            raise FileNotFoundError(f"Path does not exists: {path_to_folder}")

        for table_name, table_data in self.items():
            path_to_write: Path = path / table_name
            pq.write_table(table_data, path_to_write.with_suffix(".parquet"), **kwargs)

    def to_duck(self, path: str, **kwargs: Any) -> None:
        write_duckdb(path_to_db=path, solution=self, **kwargs)

    def query(self, query_enum: Enum) -> pl.DataFrame:
        t_membership: pl.LazyFrame = pl.from_arrow(self.t_membership).lazy()  # type: ignore
        t_parent_object: pl.LazyFrame = pl.from_arrow(self.t_object).lazy()  # type: ignore
        t_child_object: pl.LazyFrame = pl.from_arrow(self.t_object).lazy()  # type: ignore
        t_property: pl.LazyFrame = pl.from_arrow(self.t_property).lazy()  # type: ignore
        t_collection: pl.LazyFrame = pl.from_arrow(self.t_collection).lazy()  # type: ignore
        t_category: pl.LazyFrame = pl.from_arrow(self.t_category).lazy()  # type: ignore
        t_key: pl.LazyFrame = pl.from_arrow(self.t_key).lazy()  # type: ignore
        t_data: pl.LazyFrame = pl.from_arrow(self.t_data_0).lazy()  # type: ignore
        t_period: pl.LazyFrame = pl.from_arrow(self.t_period_0).lazy()  # type: ignore
        phase = t_key.select("phase_id").collect().item(0, 0)  # type: ignore
        match phase:
            case 1:
                t_phase: pl.LazyFrame = pl.from_arrow(self.t_phase_1).lazy()  # type: ignore
            case 2:
                t_phase: pl.LazyFrame = pl.from_arrow(self.t_phase_2).lazy()  # type: ignore
            case 3:
                t_phase: pl.LazyFrame = pl.from_arrow(self.t_phase_3).lazy()  # type: ignore
            case 4:
                t_phase: pl.LazyFrame = pl.from_arrow(self.t_phase_4).lazy()  # type: ignore

        data = (
            t_membership.join(
                t_collection,
                on=["collection_id", "parent_class_id", "child_class_id"],
            )
            .select(pl.exclude(["complement_name", "lang_id"]))
            .join(
                t_parent_object,
                left_on="parent_object_id",
                right_on="object_id",
                suffix="_parent",
            )
            .select(pl.exclude(["class_id", "category_id", "index", "show", "GUID"]))
            .join(
                t_child_object,
                left_on="child_object_id",
                right_on="object_id",
                suffix="_child",
            )
            .select(pl.exclude(["class_id", "index", "show", "GUID"]))
            .join(t_property, on="collection_id", suffix="_property")
            .select(
                pl.exclude(
                    [
                        "enum_id",
                        "summary_name",
                        "summary_unit_id",
                        "is_multi_band",
                        "is_period",
                        "is_summary",
                        "lang_id",
                        "unit_id",
                    ]
                )
            )
            .join(t_key, on=["membership_id", "property_id"])
            .select(
                pl.exclude(
                    [
                        "model_id",
                        "period_type_id",
                        "band_id",
                        "sample_id",
                        "timeslice_id",
                    ]
                )
            )
            .join(t_data, on="key_id")
            .join(t_phase, on="period_id")
            .join(
                t_period.select(["interval_id", "day_id", "period_of_day", "datetime"]),
                on="interval_id",
            )
            .join(
                t_category.select(["category_id", "name"]),
                on="category_id",
                suffix="_category",
            )
            .rename(
                {
                    "name": "collection_name",
                    "name_parent": "parent_name",
                    "name_child": "child_name",
                    "name_property": "property_name",
                    "name_category": "category_name",
                    "day_id": "day",
                    "period_of_day": "hour",
                }
            )
        )
        collection_value, _, property_name = query_enum.value
        return (
            data.filter(
                pl.col("collection_id").eq(collection_value),
                pl.col("property_name").eq(property_name),
            )
            .select(
                [
                    "collection_name",
                    "category_name",
                    "parent_name",
                    "child_name",
                    "property_name",
                    "datetime",
                    "day",
                    "hour",
                    "value",
                ]
            )
            .collect()
        )

    @property
    def gen(self) -> pl.DataFrame:
        return self.query(QuerySchema.GENERATOR.GENERATION)

    @property
    def cmg(self) -> pl.DataFrame:
        return self.query(QuerySchema.NODE.PRICE)

    @property
    def flow(self) -> pl.DataFrame:
        return self.query(QuerySchema.LINE.FLOW)

    @property
    def load(self) -> pl.DataFrame:
        return self.query(QuerySchema.NODE.LOAD)

    @property
    def reserve(self) -> pl.DataFrame:
        return self.query(QuerySchema.RESERVE.PROVISION)

    @property
    def va(self) -> pl.DataFrame:
        return self.query(QuerySchema.STORAGE.SHADOW_PRICE).with_columns(
            (pl.col("value") * 1000 / 86400).alias("value")
        )

""" PLEXOS QUERY
Còdigo que toma las salidas (en parquet) de un zip plexos
y permite hacer queries.
"""

from pathlib import Path, PurePath

import datetime
import polars as pl


class PlexosQuery:
    """Clase para hacer queries de un PRG en parquet."""

    def __init__(self, path_to_dir: str | Path) -> None:
        """
        Initialize a PlexosQuery object.
        """
        if isinstance(path_to_dir, PurePath):
            self.data_path = path_to_dir
        else:
            self.data_path = Path(path_to_dir)

        # self.data_path = "/Users/faso/Documents/1_Proyectos/ET_PLEXOS/PRG_dashboard/data/"
        self._scan_data()
        self.data = self._join_all()

    def _scan_data(self) -> None:
        self.t_membership = pl.scan_parquet(self.data_path / "t_membership.parquet")
        self.t_collection = pl.scan_parquet(self.data_path / "t_collection.parquet")
        self.t_parent_object = pl.scan_parquet(self.data_path / "t_object.parquet")
        self.t_child_object = pl.scan_parquet(self.data_path / "t_object.parquet")
        self.t_property = pl.scan_parquet(self.data_path / "t_property.parquet")
        self.t_unit = pl.scan_parquet(self.data_path / "t_unit.parquet")
        self.t_key = pl.scan_parquet(self.data_path / "t_key.parquet")
        self.t_model = pl.scan_parquet(self.data_path / "t_model.parquet")
        self.t_timeslice = pl.scan_parquet(self.data_path / "t_timeslice.parquet")
        self.t_sample = pl.scan_parquet(self.data_path / "t_sample.parquet")
        self.t_data_0 = pl.scan_parquet(self.data_path / "t_data_0.parquet")
        self.t_phase_3 = pl.scan_parquet(self.data_path / "t_phase_3.parquet")
        self.t_period_0 = pl.scan_parquet(
            self.data_path / "t_period_0.parquet"
        ).with_columns(pl.col("datetime").str.to_datetime())
        self.t_category = pl.scan_parquet(self.data_path / "t_category.parquet")

    def _join_all(self) -> pl.LazyFrame:
        return (
            self.t_membership.join(
                self.t_collection,
                on=["collection_id", "parent_class_id", "child_class_id"],
            )
            .select(pl.exclude(["complement_name", "lang_id"]))
            .join(
                self.t_parent_object,
                left_on="parent_object_id",
                right_on="object_id",
                suffix="_parent",
            )
            .select(pl.exclude(["class_id", "category_id", "index", "show", "GUID"]))
            .join(
                self.t_child_object,
                left_on="child_object_id",
                right_on="object_id",
                suffix="_child",
            )
            .select(pl.exclude(["class_id", "index", "show", "GUID"]))
            .join(self.t_property, on="collection_id", suffix="_property")
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
            .join(self.t_key, on=["membership_id", "property_id"])
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
            .join(self.t_data_0, on="key_id")
            .join(self.t_phase_3, on="period_id")
            .join(
                self.t_period_0.select(
                    ["interval_id", "day_id", "period_of_day", "datetime"]
                ),
                on="interval_id",
            )
            .join(
                self.t_category.select(["category_id", "name"]),
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

    def query_lazy_data(
        self, phase_id: int, collection_id: int, property_id: int
    ) -> pl.LazyFrame:
        return self.data.filter(
            (pl.col("phase_id") == phase_id)
            & (pl.col("collection_id") == collection_id)
            & (pl.col("property_id") == property_id)
        ).select(
            [
                "collection_name",
                "parent_name",
                "child_name",
                "property_name",
                "value",
                "period_id",
                "datetime",
                "day",
                "hour",
                "category_name",
            ]
        )

    def query_data(
        self, phase_id: int, collection_id: int, property_id: int
    ) -> pl.DataFrame:
        return (
            self.data.filter(
                (pl.col("phase_id") == phase_id)
                & (pl.col("collection_id") == collection_id)
                & (pl.col("property_id") == property_id)
            ).select(
                [
                    "collection_name",
                    "parent_name",
                    "child_name",
                    "property_name",
                    "value",
                    "period_id",
                    "datetime",
                    "day",
                    "hour",
                    "category_name",
                ]
            )
        ).collect()

    @property
    def generation(self) -> pl.LazyFrame:
        return self.query_lazy_data(3, 1, 1)

    @property
    def marginal_cost(self) -> pl.LazyFrame:
        return self.query_lazy_data(3, 245, 1233)

    @property
    def load(self) -> pl.LazyFrame:
        return self.query_lazy_data(3, 245, 1200)

    @property
    def lines_flow(self) -> pl.LazyFrame:
        return self.query_lazy_data(3, 264, 1274)

    @property
    def reserves(self) -> pl.LazyFrame:
        return self.query_lazy_data(3, 118, 601)

    @property
    def first_day(self) -> datetime.date:
        return self.t_period_0.select("datetime").min().collect().item()

    @property
    def last_day(self) -> datetime.date:
        return self.t_period_0.select("datetime").max().collect().item()

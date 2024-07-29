from pathlib import Path
from typing import Any

import polars as pl
import polars.selectors as cs
from pydantic import BaseModel, Field


class ActivationPriceNoOfferTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class ActivationPriceNoOfferOFFTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class BidPriceTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class BidPriceOFFTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class BidQuantityTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class BidQuantityOFFTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class DefaultPriceOFFTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class DefaultPriceOfferTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class BESSIniTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class FuelMaxOffTakeTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class FuelPriceTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class GenAuxUseTable(BaseModel):
    name: str = Field(alias="Name")
    value: float = Field(alias="Value")


class GenCommitTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenFixedLoadTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenFuelTransportChargeTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class GenHeatRateTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class GenIniGenerationTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenIniHoursDownTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenIniHoursUpTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenIniUnitsTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenMinStableLevelTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenRatingTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenShutDownCostTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class GenStartCostTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class GenUnitsOutsTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class GenVOMChargeTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class HydroAntucoBoundsTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="Value")


class HydroEfficiencyIncrTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class HydroInitialVolumeTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class HydromaxRampDayTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


# Clase con formato transformado
class HydroMaxVolumeTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


# Clase con formato transformado
class HydroMinVolumeTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class HydroStoWaterValuesTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


# Clase con formato transformado
class HydroWaterFlowsTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class LinMaxRatingTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


class LinMinRatingTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


# Clase con formato transformado
class LinUnitsTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    band: int = Field(alias="BAND")
    value: float = Field(alias="VALUE")


# Clase con formato transformado
class NodLoadTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    period: int = Field(alias="PERIOD")
    value: float = Field(alias="VALUE")


class ResRequirementTable(BaseModel):
    name: str = Field(alias="NAME")
    pattern: str = Field(alias="PATTERN")
    value: float = Field(alias="VALUE")


# Clase con formato transformado
class ResTimesliceTable(BaseModel):
    name: str = Field(alias="NAME")
    year: int = Field(alias="YEAR")
    month: int = Field(alias="MONTH")
    day: int = Field(alias="DAY")
    value: float = Field(alias="VALUE")


class InputCSV(BaseModel):
    Activation_PriceNoOffer: list[ActivationPriceNoOfferTable]
    Activation_PriceNoOfferOFF: list[ActivationPriceNoOfferOFFTable]
    Bid_Price: list[BidPriceTable]
    Bid_PriceOFF: list[BidPriceOFFTable]
    Bid_Quantity: list[BidQuantityTable]
    Bid_QuantityOFF: list[BidPriceOFFTable]
    Default_PriceOFF: list[DefaultPriceOFFTable]
    Default_PriceOffer: list[DefaultPriceOfferTable]
    BESS_IniValue: list[BESSIniTable]
    Fuel_MaxOfftakeWeek: list[FuelMaxOffTakeTable]
    Fuel_Price: list[FuelPriceTable]
    Gen_AuxUse: list[GenAuxUseTable]
    Gen_Commit: list[GenCommitTable]
    Gen_FixedLoad: list[GenFixedLoadTable]
    Gen_FuelTransportCharge: list[GenFuelTransportChargeTable]
    Gen_HeatRate: list[GenHeatRateTable]
    Gen_IniGeneration: list[GenIniGenerationTable]
    Gen_IniHoursDown: list[GenIniHoursDownTable]
    Gen_IniHoursUp: list[GenIniHoursUpTable]
    Gen_IniUnits: list[GenIniUnitsTable]
    Gen_MinStableLevel: list[GenMinStableLevelTable]
    Gen_Rating: list[GenRatingTable]
    Gen_ShutDownCost: list[GenShutDownCostTable]
    Gen_StartCost: list[GenShutDownCostTable]
    Gen_UnitsOut: list[GenUnitsOutsTable]
    Gen_VOMCharge: list[GenVOMChargeTable]
    Hydro_AntucoBounds: list[HydroAntucoBoundsTable]
    Hydro_EfficiencyIncr: list[HydroEfficiencyIncrTable]
    Hydro_InitialVolume: list[HydroInitialVolumeTable]
    Hydro_MaxRampDay: list[HydromaxRampDayTable]
    Hydro_MaxVolume: list[HydroMaxVolumeTable]
    Hydro_MinVolume: list[HydroMinVolumeTable]
    Hydro_StoWaterValues: list[HydroStoWaterValuesTable]
    Hydro_WaterFlows: list[HydroWaterFlowsTable]
    Lin_MaxRating: list[LinMaxRatingTable]
    Lin_MinRating: list[LinMinRatingTable]
    Lin_Units: list[LinUnitsTable]
    Nod_Load: list[NodLoadTable]
    Res_Requirement: list[ResRequirementTable]

    @classmethod
    def from_csv(cls, path: Path):
        csv_names = [name for name in cls.model_fields.keys()]
        csv_change_schema = (
            "Res_Timeslice",
            "Nod_Load",
            "Lin_Units",
            "Hydro_WaterFlows",
            "Hydro_MinVolume",
            "Hydro_MaxVolume",
        )

        # Data con esquema fijo
        data: dict[str, list[Any]] = {
            table_name: pl.read_csv(
                path.joinpath(table_name).with_suffix(".csv"), infer_schema_length=0
            )
            .filter(cs.first().is_not_null())
            .to_dicts()
            for table_name in csv_names
            if table_name not in csv_change_schema
        }

        # Data con esquema trasnformado, la tabla en el archivo viene pivoteada.
        # Además, algunos archivos tiene comas extras al final.
        data_change_schema: dict[str, list[Any]] = {
            table_name: pl.read_csv(
                path.joinpath(table_name).with_suffix(".csv"), infer_schema_length=0
            )
            .filter(cs.first().is_not_null())
            .melt(
                cs.matches("YEAR|MONTH|DAY|PERIOD|BAND"),
                variable_name="NAME",
                value_name="VALUE",
            )
            .filter(~(pl.col("NAME").eq("") | pl.col("NAME").str.contains("_dup")))
            .to_dicts()
            for table_name in csv_change_schema
        }

        return cls(**(data | data_change_schema))

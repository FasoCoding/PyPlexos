from enum import Enum
from typing import Protocol, Generator
from dataclasses import dataclass

import pyarrow as pa


class SolutionSchema(Enum):
    t_unit = "t_unit"
    t_band = "t_band"
    t_category = "t_category"
    t_class = "t_class"
    t_class_group = "t_class_group"
    t_collection = "t_collection"
    t_config = "t_config"
    t_key = "t_key"
    t_membership = "t_membership"
    t_model = "t_model"
    t_object = "t_object"
    t_period_0 = "t_period_0"
    t_period_1 = "t_period_1"
    t_period_2 = "t_period_2"
    t_period_3 = "t_period_3"
    t_period_4 = "t_period_4"
    t_period_6 = "t_period_6"
    t_period_7 = "t_period_7"
    t_phase_1 = "t_phase_1"
    t_phase_2 = "t_phase_2"
    t_phase_3 = "t_phase_3"
    t_phase_4 = "t_phase_4"
    t_sample = "t_sample"
    t_timeslice = "t_timeslice"
    t_key_index = "t_key_index"
    t_property = "t_property"
    t_attribute_data = "t_attribute_data"
    t_attribute = "t_attribute"
    t_sample_weight = "t_sample_weight"
    t_custom_column = "t_custom_column"
    t_memo_object = "t_memo_object"
    t_object_meta = "t_object_meta"
    t_data_0 = "t_data_0"


class SolutionProtocol(Protocol):
    def items(self) -> Generator[tuple[str, pa.Table], None, None]: ...


class GeneratorProperty(Enum):
    GENERATION = 1, 2, "Generation"
    UNITS_GENERATING = 1, 7, "Units Generating"
    DISPATCHABLE_CAPACITY = 1, 11, "Dispatchable Capacity"
    UNDISPATCHABLE_CAPACITY = 1, 12, "Undispatched Capacity"
    CAPACITY_CURTAILED = 1, 33, "Capacity Curtailed"
    AUXILIARY_USE = 1, 81, "Auxiliary Use"
    PUMP_LOAD = 1, 90, "Pump Load"
    GENERATION_COST = 1, 119, "Generation Cost"
    START_STOP_COST = 1, 120, "Start & Shutdown Cost"
    TOTAL_GENERATION_COST = 1, 127, "Total Generation Cost"
    SRMC = 1, 137, "SRMC"
    MAX_CAPACITY = 1, 212, "Max Capacity"
    UNITS_OUT = 1, 223, "Units Out"
    AVAILABLE_CAPACITY = 1, 236, "Available Capacity"
    AVAILABLE_RESPONSE = 159, 864, "Available Response"
    PROVISION = 159, 865, "Provision"
    SPINNING_RESERVE_PROVISION = 159, 866, "Spinning Reserve Provision"
    NON_SPINNING_RESERVE_PROVISION = 159, 869, "Non-spinning Reserve Provision"
    OFFTAKE = 7, 259, "Offtake"


class FuelProperty(Enum):
    OFFTAKE = 40, 376, "Offtake"
    SHADOW_PRICE = 40, 394, "Shadow Price"


class BatteriesProperty(Enum):
    SOC = 80, 518, "SoC"
    AVAILABLE_SOC = 80, 519, "Available SoC"
    GENERATION = 80, 520, "Generation"
    LOAD = 80, 521, "Load"
    LOSSES = 80, 525, "Losses"
    SHADOW_PRICE = 80, 563, "Shadow Price"
    SHADOW_GENERATION = 80, 564, "Shadow Generation"
    SHADOW_LOAD = 80, 565, "Shadow Load"


class StorageProperty(Enum):
    MAX_VOLUME = 93, 641, "Max Volume"
    MIN_VOLUME = 93, 642, "Min Volume"
    INITIAL_VOLUME = 93, 643, "Initial Volume"
    END_VOLUME = 93, 644, "End Volume"
    NATURAL_INFLOW = 93, 655, "Natural Inflow"
    SHADOW_PRICE = 93, 678, "Shadow Price"
    NON_PHYSICAL_INFLOW = 93, 701, "Non-physical Inflow"


class WaterwayProperty(Enum):
    FLOW = 101, 706, "Flow"
    MAX_FLOW = 101, 707, "Max Flow"
    MIN_FLOW = 101, 708, "Min Flow"
    SHADOW_PRICE = 101, 710, "Shadow Price"


class ReserveProperty(Enum):
    PROVISION = 156, 849, "Provision"
    SHORTAGE = 156, 852, "Shortage"
    CLEARED_OFFER_PRICE = 156, 855, "Cleared Offer Price"
    PRICE = 156, 858, "Price"
    AVAILABLE_RESPONSE = 156, 860, "Available Response"


class NodeProperty(Enum):
    LOAD = 281, 1371, "Load"
    GENERATION = 281, 1375, "Generation"
    UNSERVED_ENERGY = 281, 1406, "Unserved Energy"
    LOSSES = 281, 1408, "Losses"
    PRICE = 281, 1410, "Price"
    MARGINAL_LOSS_FACTOR = 281, 1414, "Marginal Loss Factor"
    PHASE_ANGLE = 281, 1416, "Phase Angle"


class LineProperty(Enum):
    FLOW = 303, 1460, "Flow"
    EXPORT_LIMIT = 303, 1463, "Export Limit"
    IMPORT_LIMIT = 303, 1464, "Import Limit"
    HOURS_CONGESTED = 303, 1469, "Hours Congested"
    RENTAL = 303, 1493, "Rental"
    LOSS = 303, 1498, "Loss"
    UNITS_OUT = 303, 1523, "Units Out"


class ConstraintProperty(Enum):
    ACTIVITY = 700, 3067, "Activity"
    SLACK = 700, 3068, "Slack"
    HOURS_BINDING = 700, 3070, "Hours Binding"
    RHS = 700, 3071, "RHS"
    PRICE = 700, 3072, "Price"


class DecisionVariableProperty(Enum):
    Value = 707, 3085, "value"


@dataclass
class QuerySchema:
    GENERATOR = GeneratorProperty
    FUEL = FuelProperty
    BATTERY = BatteriesProperty
    STORAGE = StorageProperty
    WATERWAY = WaterwayProperty
    RESERVE = ReserveProperty
    NODE = NodeProperty
    LINE = LineProperty
    CONSTRAINT = ConstraintProperty
    DECISION_VARIABLE = DecisionVariableProperty

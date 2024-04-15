from enum import Enum
from dataclasses import dataclass

class QueryCollection(Enum):
    GENERATOR = 1
    FUEL = 33
    STORAGE = 80
    WATERWAY = 88
    RESERVE = 115
    NODE = 245
    LINE = 264
    CONSTRAINT = 635
    VARIABLE = 642

class GeneratorProperty(Enum):
    GENERATION = 1
    UNITS_GENERATING = 6
    DISPATCHABLE_CAPACITY = 9
    UNDISPATCHABLE_CAPACITY = 10
    CAPACITY_CURTAILED = 28
    AUXILIARY_USE = 74
    PUMP_LOAD = 78
    GENERATION_COST = 104
    START_STOP_COST = 105
    TOTAL_GENERATION_COST = 112
    SRMC = 123
    MAX_CAPACITY = 200
    UNITS_OUT = 209
    AVAILABLE_CAPACITY = 219

class FuelProperty(Enum):
    OFFTAKE = 344
    SHADOW_PRICE = 361

class StorageProperty(Enum):
    MAX_VOLUME = 478
    MIN_VOLUME = 479
    INITIAL_VOLUME = 480
    END_VOLUME = 481
    NATURAL_INFLOW = 492
    SHADOW_PRICE = 505
    NON_PHYSICAL_INFLOW = 524

class WaterwayProperty(Enum):
    FLOW = 529
    MAX_FLOW = 530
    MIN_FLOW = 531
    SHADOW_PRICE = 533

class ReserveProperty(Enum):
    PROVISION = 586
    SHORTAGE = 588
    CLEARED_OFFER_PRICE = 591
    PRICE = 594
    AVAILABLE_RESPONSE = 596

class NodeProperty(Enum):
    LOAD = 1200
    GENERATION = 1204
    UNSERVED_ENERGY = 1229
    LOSSES = 1231
    PRICE = 1233
    MARGINAL_LOSS_FACTOR = 1237
    PHASE_ANGLE = 1239

class LineProperty(Enum):
    FLOW = 1274
    EXPORT_LIMIT = 1277
    IMPORT_LIMIT = 1278
    HOURS_CONGESTED = 1281
    RENTAL = 1303
    LOSS = 1307
    UNITS_OUT = 1328

class ConstraintProperty(Enum):
    ACTIVITY = 2642
    SLACK = 2643
    HOURS_BINDING = 2645
    RHS = 2646
    PRICE = 2647

class DecisionVariableProperty(Enum):
    Value = 2660

@dataclass
class QueryProperty:
    GENERATOR = GeneratorProperty
    FUEL = FuelProperty
    STORAGE = StorageProperty
    WATERWAY = WaterwayProperty
    RESERVE = ReserveProperty
    NODE = NodeProperty
    LINE = LineProperty
    CONSTRAINT = ConstraintProperty
    DECISION_VARIABLE = DecisionVariableProperty
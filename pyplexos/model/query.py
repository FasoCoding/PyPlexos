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
    COLLECTION = 1
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
    COLLECTION = 33
    OFFTAKE = 344
    SHADOW_PRICE = 361

class StorageProperty(Enum):
    COLLECTION = 80
    MAX_VOLUME = 478
    MIN_VOLUME = 479
    INITIAL_VOLUME = 480
    END_VOLUME = 481
    NATURAL_INFLOW = 492
    SHADOW_PRICE = 505
    NON_PHYSICAL_INFLOW = 524

class WaterwayProperty(Enum):
    COLLECTION = 88
    FLOW = 529
    MAX_FLOW = 530
    MIN_FLOW = 531
    SHADOW_PRICE = 533

class ReserveProperty(Enum):
    COLLECTION = 115
    PROVISION = 586
    SHORTAGE = 588
    CLEARED_OFFER_PRICE = 591
    PRICE = 594
    AVAILABLE_RESPONSE = 596

class NodeProperty(Enum):
    COLLECTION = 245
    LOAD = 1200
    GENERATION = 1204
    UNSERVED_ENERGY = 1229
    LOSSES = 1231
    PRICE = 1233
    MARGINAL_LOSS_FACTOR = 1237
    PHASE_ANGLE = 1239

class LineProperty(Enum):
    COLLECTION = 264
    FLOW = 1274
    EXPORT_LIMIT = 1277
    IMPORT_LIMIT = 1278
    HOURS_CONGESTED = 1281
    RENTAL = 1303
    LOSS = 1307
    UNITS_OUT = 1328

class ConstraintProperty(Enum):
    COLECTION = 635
    ACTIVITY = 2642
    SLACK = 2643
    HOURS_BINDING = 2645
    RHS = 2646
    PRICE = 2647

class DecisionVariableProperty(Enum):
    COLLECTION = 642
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

@dataclass
class QuerySchema:
    GENERATOR = GeneratorProperty
    FUEL = FuelProperty
    STORAGE = StorageProperty
    WATERWAY = WaterwayProperty
    RESERVE = ReserveProperty
    NODE = NodeProperty
    LINE = LineProperty
    CONSTRAINT = ConstraintProperty
    DECISION_VARIABLE = DecisionVariableProperty
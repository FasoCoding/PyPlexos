from dataclasses import dataclass
from enum import Enum

import pyarrow as pa


class SolutionSchema(Enum):
    t_unit: str = "t_unit"
    t_band: str = "t_band"
    t_category: str = "t_category"
    t_class: str = "t_class"
    t_class_group: str = "t_class_group"
    t_collection: str = "t_collection"
    t_config: str = "t_config"
    t_key: str = "t_key"
    t_membership: str = "t_membership"
    t_model: str = "t_model"
    t_object: str = "t_object"
    t_period_0: str = "t_period_0"
    t_period_1: str = "t_period_1"
    t_period_2: str = "t_period_2"
    t_period_3: str = "t_period_3"
    t_period_4: str = "t_period_4"
    t_period_6: str = "t_period_6"
    t_period_7: str = "t_period_7"
    t_phase_1: str = "t_phase_1"
    t_phase_2: str = "t_phase_2"
    t_phase_3: str = "t_phase_3"
    t_phase_4: str = "t_phase_4"
    t_sample: str = "t_sample"
    t_timeslice: str = "t_timeslice"
    t_key_index: str = "t_key_index"
    t_property: str = "t_property"
    t_attribute_data: str = "t_attribute_data"
    t_attribute: str = "t_attribute"
    t_sample_weight: str = "t_sample_weight"
    t_custom_column: str = "t_custom_column"
    t_memo_object: str = "t_memo_object"
    t_object_meta: str = "t_object_meta"
    t_data_0: str = "t_data_0"


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

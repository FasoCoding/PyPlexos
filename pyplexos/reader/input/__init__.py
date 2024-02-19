from pathlib import Path
from typing import Self
from dataclasses import dataclass
from datetime import datetime

from pyplexos.reader.input.csv import CSVModel

@dataclass
class PlexosInputReader:
    input_csv_model: CSVModel
from typing import Protocol
from datetime import datetime


class PlexosReaderProtocol(Protocol):
    @property
    def get_solution_model(self) -> dict[str, list[dict]]:
        pass

    @property
    def get_solution_data(self) -> dict[str, dict[str, list]]:
        pass

    @property
    def get_initial_datetime(self) -> datetime:
        pass


class WriterModelProtocol(Protocol):
    def write(self, data: PlexosReaderProtocol) -> None:
        pass

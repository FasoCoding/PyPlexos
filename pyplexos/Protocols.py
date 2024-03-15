from typing import Protocol, List, Dict, Any
from datetime import datetime


class PlexosReaderProtocol(Protocol):
    @property
    def get_solution_model(self) -> dict[str, list[dict[str, str]]]:
        ...

    @property
    def get_solution_data(self) -> dict[str, dict[str, List[Any]]]:
        ...

    @property
    def get_initial_datetime(self) -> datetime:
        ...


class WriterModelProtocol(Protocol):
    def write(self, data: PlexosReaderProtocol) -> None:
        ...

class SolutionReaderModelProtocol(Protocol):
    @property
    def get_solution_model(self) -> dict[str, list[Dict[str, Any]]]:
        ...

    @property
    def get_solution_data(self) -> dict[str, dict[str, List[Any]]]:
        ...

    @property
    def get_initial_datetime(self) -> datetime:
        ...

class InputReaderModelProtocol(Protocol):
    def get_data(self) -> dict[str, dict[str, List[Any]]]:
        ...

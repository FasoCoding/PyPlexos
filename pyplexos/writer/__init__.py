from dataclasses import dataclass
from typing import Self

from pyplexos.writer.model.duck_writer import DuckWriter
from pyplexos.writer.model.parquet_writer import ParquetWriter
from pyplexos.Protocols import PlexosReaderProtocol, WriterModelProtocol


@dataclass
class PlexosWriter:
    writer_model: WriterModelProtocol = None

    @classmethod
    def duck_writer(
        cls,
        path_to_dir: str,
        db_name: str = "pcp.ddb",
        mode: str = "replace"
    ) -> Self:
        return cls(
            writer_model=DuckWriter.create_medallion_duck(
                path_to_db=path_to_dir,
                db_name=db_name,
                mode=mode
            )
        )

    @classmethod
    def parquet_writer(cls, path_to_dir: str) -> Self:
        return cls(
            writer_model=ParquetWriter.parquet_writer(path_to_dir)
        )

    def write(self, solution: PlexosReaderProtocol) -> None:
        self.writer_model.write(solution)

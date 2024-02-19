from pydantic import BaseModel

class GenAuxUseTable(BaseModel):
    name: str
    value: float

class InputData(BaseModel):
    gen_auxuse: list[GenAuxUseTable]
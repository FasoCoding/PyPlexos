from typing import Optional, List

from sqlmodel import Field, SQLModel, create_engine, Session
from sqlalchemy import Column, Sequence, Integer

unit_id_seq = Sequence("unit_id_seq")
unit = Column(
    "unit_id",
    Integer,
    unit_id_seq,
    primary_key=True,
    server_default=unit_id_seq.next_value()
)

class UnitTable(SQLModel, table=True):
    __tablename__ = "t_unit"
    unit_id: Optional[int] = Field(
        #primary_key=True,
        default=None,
        sa_column=unit)
    value: str
    lang_id: int

#class BandTable(SQLModel, table=True):
#    __tablename__ = "t_band"
#
#    band_id: Optional[int]# = Field(primary_key=True, default=None)
#    extra_data: Optional[str] = None


#class CategoryTable(SQLModel, table=True):
#    __tablename__ = "t_category"
#
#    category_id: Optional[int]# = Field(primary_key=True, default=None)
#    class_id: int
#    rank: int
#    name: str

if __name__ == "__main__":
    engine = create_engine("duckdb:///example.db")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                UnitTable(unit_id=1, value="MW", lang_id=1),
                UnitTable(unit_id=2, value="MWh", lang_id=1),
                #BandTable(band_id=1, extra_data="Extra data"),
                #CategoryTable(category_id=1, class_id=1, rank=1, name="Category 1"),
                #CategoryTable(category_id=2, class_id=1, rank=2, name="Category 2"),
            ]
        )
        session.commit()
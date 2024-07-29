from io import BytesIO
import polars as pl

import pyplexos.inputs.csv as csv_model

def test_CSVModel():
    csv_input = BytesIO(
        b"NAME,YEAR,MONTH,DAY,PERIOD,BAND,VALUE\n"
        b"ANGOSTURA,2024,1,2,1,1,0.5\n"
        b"Quillota220,2021,10,4,4,1,100\n"
    )

    model = [
        csv_model.CSVModel(
            NAME="ANGOSTURA",
            YEAR=2024,
            MONTH=1,
            DAY=2,
            PERIOD=1,
            BAND=1,
            VALUE=0.5
        ),
        csv_model.CSVModel(
            NAME="Quillota220",
            YEAR=2021,
            MONTH=10,
            DAY=4,
            PERIOD=4,
            BAND=1,
            VALUE=100
        ),
    ]

    # TEST: lectura correcta del CSV.
    assert model == [csv_model.CSVModel(**row) for row in pl.read_csv(csv_input).to_dicts()]
    # TEST: escritura correcta del CSV.
    assert pl.from_dicts(
        [row.model_dump(by_alias=True) for row in model]
    ).write_csv() == pl.read_csv(csv_input).write_csv()

def test_BESS_IniModel():
    csv_input = BytesIO(
        b"NAME,YEAR,MONTH,DAY,PERIOD,VALUE\n"
        b"BAT_ALFALFAL,2024,1,2,1,0.5\n"
        b"BAT_SALVADOR_FV,2021,10,4,4,100\n"
    )

    model = [
        csv_model.CSVModel_v2(
            NAME="BAT_ALFALFAL",
            YEAR=2024,
            MONTH=1,
            DAY=2,
            PERIOD=1,
            VALUE=0.5
        ),
        csv_model.CSVModel_v2(
            NAME="BAT_SALVADOR_FV",
            YEAR=2021,
            MONTH=10,
            DAY=4,
            PERIOD=4,
            VALUE=100
        ),
    ]

    # TEST: lectura correcta del CSV.
    assert model == [csv_model.CSVModel_v2(**row) for row in pl.read_csv(csv_input).to_dicts()]
    # TEST: escritura correcta del CSV.
    assert pl.from_dicts(
        [row.model_dump(by_alias=True) for row in model]
    ).write_csv() == pl.read_csv(csv_input).write_csv()
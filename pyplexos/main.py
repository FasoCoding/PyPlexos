from pyplexos.reader import PlexosZipReader
from pyplexos.writer import PlexosWriter

import time

if __name__ == "__main__":
    print("Probando app")
    start = time.time()
    reader = PlexosZipReader.from_zip("data\Model PRGdia_Full_Definitivo Solution.zip")
    #reader = PlexosZipReader.from_zip("data\Model Test15d Solution.zip")
    end = time.time()
    print(
        f"Modelo extraido en {end - start:.2f} segundos \nIniciando guardado en parquet..."
    )

    parquet_writer = PlexosWriter.parquet_writer(path_to_dir=r"data\Parquet")
    parquet_writer.write(reader)
    end_parquet = time.time()
    print(
        f"termino! guardado en {end_parquet - end:.2f} segundos \nTiempo total: {end_parquet - start:.2f} segundos"
    )

    duck_writer = PlexosWriter.duck_writer(path_to_dir=r"data\DuckDB", db_name="test_pcp.ddb")
    duck_writer.write(reader)
    end_duck = time.time()
    print(
        f"termino! guardado en {end_duck - end_parquet:.2f} segundos \nTiempo total: {end_duck - start:.2f} segundos"
    )

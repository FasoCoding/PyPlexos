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
    #reader.to_parquet(path_to_dir=r"data\Parquet")
    end_parquet = time.time()
    print(
        f"termino! guardado en {end_parquet - end:.2f} segundos \nTiempo total: {end_parquet - start:.2f} segundos"
    )
    writer = PlexosWriter.duck_writer(path_to_dir=r"data\DuckDB", db_name="test_pcp.ddb")
    writer.to_duck(reader)
    end_duck = time.time()
    print(
        f"termino! guardado en {end_duck - end_parquet:.2f} segundos \nTiempo total: {end_duck - start:.2f} segundos"
    )

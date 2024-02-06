from reader import PlexosZipReader

import time

if __name__ == "__main__":
    print("Probando app")
    start = time.time()
    reader = PlexosZipReader.from_zip("data\Model PRGdia_Full_Definitivo Solution.zip")
    end = time.time()
    print(
        f"Modelo extraido en {end - start:.2f} segundos \nIniciando guardado en parquet..."
    )
    #reader.to_parquet(path_to_dir=r"data\Parquet")
    end_parquet = time.time()
    print(
        f"termino! guardado en {end_parquet - end:.2f} segundos \nTiempo total: {end_parquet - start:.2f} segundos"
    )
    reader.to_duck(path_to_dir=r"data\DuckDB")
    end_duck = time.time()
    print(
        f"termino! guardado en {end_duck - end_parquet:.2f} segundos \nTiempo total: {end_duck - start:.2f} segundos"
    )

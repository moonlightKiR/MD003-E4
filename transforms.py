import polars as pl
import pandas as pd

def convert_to_pandas(df):
    print("Iniciando conversión de Polars a Pandas...")
    try:
        df_pandas = df.to_pandas()
        print(f"Conversión exitosa. DataFrame de Pandas listo con {df_pandas.shape[0]} registros.")
        return df_pandas
    except Exception as e:
        print(f"Error crítico durante la conversión a Pandas: {e}")
        return None

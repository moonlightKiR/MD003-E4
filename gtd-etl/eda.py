import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mondongo
import os

def perform_eda():

    try:
        
        # 1. Conexión y carga de datos
        collection = mondongo.get_collection()
        
        print("Cargando datos desde MongoDB para análisis EDA...")
        data = list(collection.find())
        df = pd.DataFrame(data)
        
        if df.empty:
            print("La base de datos está vacía. No se puede realizar el EDA.")
            return

        # 2. Análisis de Nulos
        print("Analizando valores nulos...")
        null_percentage = (df.isnull().sum() / len(df)) * 100
        
        # Columnas con más del 50% de nulos
        high_null_cols = null_percentage[null_percentage > 50]
        print(f"Columnas con más del 50% de nulos:\n{high_null_cols}")

        # 3. Imputación de valores específicos (ej. nkill)
        if 'nkill' in df.columns:
            df['nkill'] = df['nkill'].fillna(0)
            print("Valores nulos en 'nkill' imputados a 0.")

        # 4. Duplicados
        if 'eventid' in df.columns:
            duplicates = df.duplicated(subset=['eventid']).sum()
            print(f"Registros duplicados encontrados por 'eventid': {duplicates}")

        # 5. Visualización: Porcentaje de datos faltantes
        print("Generando visualización de datos faltantes...")
        plt.figure(figsize=(12, 8))
        # Seleccionamos solo columnas con nulos para el gráfico si son muchas
        missing_data = null_percentage[null_percentage > 0].sort_values(ascending=False)
        
        if not missing_data.empty:
            sns.barplot(x=missing_data.values, y=missing_data.index, palette="viridis")
            plt.title("Porcentaje de Datos Faltantes por Variable")
            plt.xlabel("% de Nulos")
            plt.ylabel("Variables")
            
            # Guardamos el gráfico en lugar de mostrarlo (entorno Docker)
            plt.tight_layout()
            plt.savefig("missing_data.png")
            print("Gráfico 'missing_data.png' guardado con éxito.")
        else:
            print("No se encontraron valores nulos para visualizar.")

        client.close()

    except Exception as e:
        print(f"Error durante el proceso EDA: {e}")

if __name__ == "__main__":
    perform_eda()

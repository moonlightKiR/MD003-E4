import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import mondongo

def perform_eda():
    try:
        # 1. Obtener la coleccion de MongoDB
        client = mondongo.MongoClient("mongodb://localhost:27017/")
        collection = client["gtd_database"]["incidents"]
        
        print("Cargando datos desde MongoDB con Polars (la opcion mas eficiente)...")
        # Extraemos los datos. Para 180k registros, Polars es rapidisimo y usa muy poca RAM.
        documents = list(collection.find())
        if not documents:
            print("No hay datos en MongoDB.")
            return

        df = pl.from_dicts(documents)
        print(f"Datos cargados: {df.height} filas y {df.width} columnas.")

        # 2. Analisis de Nulos (Calidad del Dato)
        # Calculamos el porcentaje de nulos por columna
        null_counts = df.null_count()
        total_rows = df.height
        
        # Transformamos a formato largo para analizar porcentajes
        null_percentages = []
        for col_name in df.columns:
            count = null_counts[col_name][0]
            percentage = (count / total_rows) * 100
            if percentage > 0:
                null_percentages.append({"Variable": col_name, "Percentage": percentage})
        
        null_df = pl.DataFrame(null_percentages).sort("Percentage", descending=True)

        # Identificar columnas con > 50% nulos
        high_null_cols = null_df.filter(pl.col("Percentage") > 50)
        print(f"Variables con >50% de nulos: {high_null_cols.height}")
        if high_null_cols.height > 0:
            print(high_null_cols)

        # 3. Imputacion (ej. nkill)
        if "nkill" in df.columns:
            # En Polars las columnas suelen venir como string si no se especifica, 
            # pero al venir de Mongo/JSON suelen mantener tipo. Aseguramos tipo numerico.
            df = df.with_columns(
                pl.col("nkill").cast(pl.Float64, strict=False).fill_null(0.0)
            )
            print("Nulos en 'nkill' imputados a 0.")

        # 4. Duplicados por eventid
        if "eventid" in df.columns:
            duplicates_count = df.height - df.unique(subset=["eventid"]).height
            print(f"Registros duplicados por 'eventid': {duplicates_count}")

        # 5. Visualizacion
        print("Generando graficos de calidad de datos...")
        plt.style.use('dark_background') # Un toque premium
        plt.figure(figsize=(12, 10))
        
        # Usamos los 40 con mas nulos para que el grafico sea legible
        plot_data = null_df.head(40).to_pandas() 
        
        sns.barplot(
            data=plot_data, 
            x="Percentage", 
            y="Variable", 
            palette="viridis"
        )
        
        plt.title("Calidad del Dato: % de Valores Faltantes por Variable", fontsize=15, pad=20)
        plt.xlabel("% de Nulos", fontsize=12)
        plt.ylabel("Variables", fontsize=12)
        plt.grid(axis='x', linestyle='--', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig("missing_data_report.png", dpi=300)
        print("Grafico 'missing_data_report.png' guardado correctamente.")

    except Exception as e:
        print(f"Error en el proceso EDA: {e}")
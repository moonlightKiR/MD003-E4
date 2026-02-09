from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import mondongo
import os

def perform_eda():
    # 1. Obtener la configuracion desde mondongo
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "gtd_database")
    COLLECTION_NAME = "incidents"
    
    print(f"Iniciando PySpark con conexion a MongoDB: {DATABASE_NAME}.{COLLECTION_NAME}")
    
    # 2. Iniciar Spark con el conector de MongoDB
    # Usamos la version 10.x que es la mas moderna para Spark 3.x
    spark = SparkSession.builder \
        .appName("GTD_EDA_Mongo") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .config("spark.mongodb.read.database", DATABASE_NAME) \
        .config("spark.mongodb.read.collection", COLLECTION_NAME) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    try:
        # 3. Cargar datos desde MongoDB directamente a Spark
        print("Cargando datos desde MongoDB...")
        df = spark.read.format("mongodb").load()
        
        total_rows = df.count()
        if total_rows == 0:
            print("No se encontraron registros en la coleccion de MongoDB.")
            return
            
        print(f"Registros cargados exitosamente: {total_rows}")

        # 4. Analisis de Nulos (Calidad del Dato)
        print("Analizando calidad del dato (nulos)...")
        # Calculamos nulos por columna
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
        null_percentages = {k: (v / total_rows) * 100 for k, v in null_counts.items()}

        # Identificar columnas con > 50% nulos
        high_null_cols = {k: v for k, v in null_percentages.items() if v > 50}
        print(f"Variables con >50% de nulos detectadas: {len(high_null_cols)}")
        for col_name, perc in high_null_cols.items():
            print(f"   - {col_name}: {perc:.2f}%")

        # 5. Imputacion (ej. nkill)
        if "nkill" in df.columns:
            df = df.fillna({"nkill": 0})
            print("Imputacion completada: nulos en 'nkill' reemplazados por 0.")

        # 6. Duplicados
        if "eventid" in df.columns:
            distinct_count = df.select("eventid").distinct().count()
            duplicates = total_rows - distinct_count
            print(f"Analisis de duplicados en 'eventid': {duplicates} encontrados.")

        # 7. Visualizacion: % de datos faltantes
        print("Generando visualizacion de calidad del dato...")
        missing_df = pd.DataFrame(list(null_percentages.items()), columns=['Variable', 'Percentage'])
        missing_df = missing_df[missing_df['Percentage'] > 0].sort_values(by='Percentage', ascending=False)

        if not missing_df.empty:
            plt.figure(figsize=(12, 12))
            sns.barplot(data=missing_df.head(40), x='Percentage', y='Variable', palette='flare')
            plt.title("Calidad del Dato: % de Nulos por Variable (MongoDB Source)")
            plt.xlabel("% de Faltantes")
            plt.tight_layout()
            plt.savefig("missing_data_mongo.png")
            print("Visualizacion guardada como 'missing_data_mongo.png'.")

    except Exception as e:
        print(f"Error durante el analisis EDA con Spark: {e}")
    finally:
        print("Cerrando sesion de Spark...")
        spark.stop()

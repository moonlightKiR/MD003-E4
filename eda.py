import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import mondongo
import os
import pandas as pd

def get_dataframe():
    """Obtiene la coleccion de MongoDB y la convierte en un DataFrame de Polars."""
    try:
        collection = mondongo.get_collection()
        print("Conectando a MongoDB para extraer datos...")
        
        documents = list(collection.find())
        if not documents:
            print("No se encontraron datos en la coleccion.")
            return None

        # Procesamiento necesario para compatibilidad con Polars
        for doc in documents:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

        df = pl.from_dicts(documents)
        print(f"DataFrame creado con exito: {df.height} filas y {df.width} columnas.")
        return df
    except Exception as e:
        print(f"Error al obtener el DataFrame: {e}")
        return None

def select_star_schema_variables(df):
    """
    Selecciona las variables necesarias para construir el modelo de estrella
    según la tabla de Hechos y las Dimensiones especificadas.
    """
    print("Seleccionando variables para el modelo")
    
    # Lista de columnas requeridas basada en tu especificación
    required_columns = [
        # 1. ATAQUE (Hechos)
        "eventid", # Usaremos eventid como ID_ataque
        "nkill", 
        "nwound", 
        "success", 
        "propvalue",
        
        # 2. TIEMPO
        "iyear", 
        "imonth", 
        "iday",
        
        # 3. UBICACION
        "country_txt", 
        "region_txt", 
        "provstate", 
        "city", 
        "latitude", 
        "longitude",
        
        # 4. GRUPO
        "gname", 
        "gsubname", 
        
        # 5. METODO
        "attacktype1_txt", 
        "suicide",
        
        # 6. OBJETIVO
        "targtype1_txt", 
        "corp1", 
        "target1",
        
        # 7. ARMA
        "weaptype1_txt", 
        "weapsubtype1_txt"
    ]
    
    # Verificamos qué columnas existen realmente en el DF para evitar errores
    available_columns = [col for col in required_columns if col in df.columns]
    
    if len(available_columns) < len(required_columns):
        missing = set(required_columns) - set(available_columns)
        print(f"Aviso: Faltan algunas columnas en el dataset: {missing}")
    
    # Realizamos el select con Polars
    df_star = df.select(available_columns)
    
    print(f"Modelo de estrella filtrado: {df_star.width} columnas seleccionadas.")
    return df_star

def save_quality_chart(report_df, title="Top 10 Variables con mas Nulos/Vacios"):
    if report_df is None or report_df.is_empty():
        return
        
    print(f"Generando grafico...")
    plt.style.use('ggplot')
    plt.figure(figsize=(10, 6))
    
    # Tomamos el Top 10
    plot_data = report_df.head(10).to_pandas()
    sns.barplot(data=plot_data, x="Percentage", y="Variable")
    
    plt.title(title, fontsize=14, pad=15)
    plt.xlabel("% de Faltantes (Nulos + Vacios)")
    plt.ylabel("Variables")
    plt.tight_layout()
    plt.show()

def analyze_data_quality(df):
    """Calcula, reporta y visualiza el numero exacto de nulos y vacios por columna."""
    print("Analizando calidad del dato (Nulos y Vacios)...")
    total_rows = df.height
    missing_stats = []
    
    for col_name in df.columns:
        n_null = df[col_name].null_count()
        
        n_empty = 0
        if df[col_name].dtype in [pl.Utf8, pl.String]:
            n_empty = (df[col_name] == "").sum()
        
        total_missing = n_null + n_empty
        if total_missing > 0:
            percentage = (total_missing / total_rows) * 100
            missing_stats.append({
                "Variable": col_name,
                "Nulos": n_null,
                "Vacios": n_empty,
                "Total_Missing": total_missing,
                "Percentage": percentage
            })
    
    if not missing_stats:
        print("Excelente: No se han detectado valores nulos ni vacios.")
        return []
    
    report_df = pl.DataFrame(missing_stats).sort("Total_Missing", descending=True)
    
    print(f"\nSe han detectado {len(missing_stats)} columnas con datos faltantes.")
    print("Top 10 variables con mas nulos/vacios:")
    top_10 = report_df.head(10)
    for row in top_10.rows(named=True):
        print(f" - {row['Variable']}: {row['Total_Missing']} faltantes ({row['Percentage']:.2f}%)")
    print("")

    save_quality_chart(report_df)

    return report_df["Variable"].to_list()

def check_duplicates(df, key_col="eventid"):
    """Analiza duplicados basados en una columna clave."""
    if key_col in df.columns:
        total_unique = df.select(key_col).unique().height
        duplicates = df.height - total_unique
        print(f"Analisis de duplicados en '{key_col}': {duplicates} encontrados.")
        return duplicates
    return 0

def clean_data(df):
    """Realiza imputaciones y limpiezas basicas."""
    print("Iniciando limpieza e imputacion...")
    if "nkill" in df.columns:
        df = df.with_columns(
            pl.col("nkill").cast(pl.Float64, strict=False).fill_null(0.0)
        )
        print("Imputacion en 'nkill' completada.")
    return df
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

def analyze_data_quality(df):
    """Calcula el porcentaje de nulos y vacios por columna."""
    print("Analizando calidad del dato...")
    total_rows = df.height
    missing_stats = []
    
    for col_name in df.columns:
        n_null = df[col_name].null_count()
        
        n_empty = 0
        if df[col_name].dtype in [pl.Utf8, pl.String]:
            n_empty = (df[col_name] == "").sum()
        
        total_missing = n_null + n_empty
        if total_missing > 0:
            missing_stats.append({
                "Variable": col_name,
                "Percentage": (total_missing / total_rows) * 100
            })
    
    if not missing_stats:
        print("No se han detectado valores nulos ni vacios.")
        return None
    
    report_df = pl.DataFrame({
        "Variable": [item["Variable"] for item in missing_stats],
        "Percentage": [item["Percentage"] for item in missing_stats]
    }).sort("Percentage", descending=True)
    
    return report_df

def save_quality_chart(report_df, filename="missing_data_report.png"):
    """Genera y guarda un grafico de barras con el porcentaje de nulos."""
    if report_df is None:
        return
        
    print(f"Generando reporte grafico: {filename}...")
    plt.style.use('ggplot')
    plt.figure(figsize=(12, 10))
    
    # Tomamos el Top 40 para que sea legible
    plot_data = report_df.head(40).to_pandas()
    sns.barplot(data=plot_data, x="Percentage", y="Variable", palette="viridis")
    
    plt.title("Calidad del Dato: % de Valores Faltantes o Vacíos", fontsize=15, pad=20)
    plt.xlabel("% de Faltantes/Vacíos")
    plt.ylabel("Variables")
    plt.tight_layout()
    plt.savefig(filename, dpi=300)
    print("Grafico guardado con exito.")

def check_duplicates(df, key_col="eventid"):
    """Analiza duplicados basados en una columna clave."""
    if key_col in df.columns:
        total_rows = df.height
        total_unique = df.select(key_col).unique().height
        duplicates = total_rows - total_unique
        print(f"Analisis de duplicados en '{key_col}': {duplicates} encontrados de {total_rows} registros.")
        return duplicates
    return 0

def clean_data(df):
    """Realiza imputaciones y limpiezas basicas."""
    print("Iniciando limpieza e imputacion...")
    if "nkill" in df.columns:
        df = df.with_columns(
            pl.col("nkill").cast(pl.Float64, strict=False).fill_null(0.0)
        )
        print("Imputacion en 'nkill' completada (rellenado con 0.0).")
    return df

def perform_full_eda():
    """Ejecuta el flujo completo de EDA por partes."""
    try:
        # Parte 1: Carga
        df = get_dataframe()
        if df is None: return

        # Parte 2: Calidad
        report_df = analyze_data_quality(df)
        
        # Parte 3: Visualización
        save_quality_chart(report_df)
        
        # Parte 4: Duplicados
        check_duplicates(df)
        
        # Parte 5: Limpieza
        df = clean_data(df)
        
        print("Proceso EDA completo finalizado.")
        return df

    except Exception as e:
        print(f"Error en perform_full_eda: {e}")
        return None
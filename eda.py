import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import mondongo

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
    
    print("Seleccionando variables para el modelo")
    
    # Lista de columnas requeridas basada en tu especificación
    required_columns = [
        # 1. ATAQUE (Hechos)
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
    
    available_columns = [col for col in required_columns if col in df.columns]
    
    if len(available_columns) < len(required_columns):
        missing = set(required_columns) - set(available_columns)
        print(f"Aviso: Faltan algunas columnas en el dataset: {missing}")
    
    df_star = df.select(available_columns)
    
    print(f"Modelo de estrella filtrado: {df_star.width} columnas seleccionadas.")
    return df_star

def save_quality_chart(report_df, title="Top 10 Variables con mas Nulos/Vacios"):
    if report_df is None or report_df.is_empty():
        return
        
    print(f"Generando grafico...")
    plt.style.use('ggplot')
    plt.figure(figsize=(10, 6))
    
    plot_data = report_df.head(10).to_pandas()
    sns.barplot(data=plot_data, x="Percentage", y="Variable")
    
    plt.title(title, fontsize=14, pad=15)
    plt.xlabel("% de Faltantes (Nulos + Vacios)")
    plt.ylabel("Variables")
    plt.tight_layout()
    plt.show()

def analyze_data_quality(df):

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
    print("Top 100 variables con mas nulos/vacios:")
    top_100 = report_df.head(100)
    for row in top_100.rows(named=True):
        print(f" - {row['Variable']}: {row['Total_Missing']} faltantes ({row['Percentage']:.2f}%)")
    print("")

    save_quality_chart(report_df)

    return report_df["Variable"].to_list()

def check_duplicates(df, key_col="eventid"):
    
    print("Analizando duplicados...")
    
    if key_col in df.columns:
        total_unique = df.select(key_col).unique().height
        duplicates = df.height - total_unique
        print(f"Analisis de duplicados en '{key_col}': {duplicates} encontrados.")
        return duplicates
    return 0

def cast_numeric_columns(df):
    print("Corrigiendo tipos de datos numéricos...")
    
    # Columnas para convertir a Entero
    cols_int = ["nkill", "nwound", "propvalue", "iyear", "imonth", "iday", "success"]
    for col in cols_int:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .cast(pl.Float64, strict=False) 
                .fill_null(0)
                .cast(pl.Int64)
            )
            print(f" - Columna '{col}' convertida a Int64.")

    # Columnas para convertir a Decimal (Float64)
    cols_float = ["latitude", "longitude"]
    for col in cols_float:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .cast(pl.Float64, strict=False) 
                .fill_null(0.0)
            )
            print(f" - Columna '{col}' convertida a Float64.")
            
    return df

def list_categorical_uniques(df):
   
    print("Buscando valores únicos en columnas de texto...")
    
    cat_cols = [col for col in df.columns if df[col].dtype in [pl.String]]
    
    if not cat_cols:
        print("No se han encontrado columnas de tipo string.")
        return {}

    uniques_map = {}
    for col in cat_cols:
        unique_values = df[col].unique().to_list()
        # Quitamos vacíos para limpiar el reporte
        unique_values = [v for v in unique_values if v not in ["", None]]
        uniques_map[col] = unique_values
        
        print(f"\nColumna String: '{col}'")
        print(f"  Total únicos: {len(unique_values)}")
        if len(unique_values) <= 20:
            print(f"  Valores: {unique_values}")
        else:
            print(f"  Valores (Top 15): {unique_values[:15]}...")
            
    return uniques_map

def encode_categorical_columns(df):
    """
    Identifica automáticamente las columnas de tipo texto y las convierte
    a numérico conservando el mapeo.
    Retorna (df_transformado, dict_mapeos).
    """
    # Identificamos columnas de tipo String/Utf8
    cat_cols = [col for col in df.columns if df[col].dtype in [pl.String, pl.Utf8]]
    
    print(f"Codificando columnas detectadas como texto: {cat_cols}...")
    mappings = {}
    
    for col in cat_cols:
        # Obtenemos valores únicos y creamos diccionario {texto: numero}
        unique_vals = df[col].unique().to_list()
        # Filtramos None o vacíos para que el mapeo sea limpio
        mapping = {val: i for i, val in enumerate(unique_vals) if val not in [None, ""]}
        mappings[col] = mapping
        
        # Aplicamos el mapeo usando una expresión de Polars
        df = df.with_columns(
            pl.col(col).replace(mapping, default=None).cast(pl.Int64)
        )
        print(f" - '{col}' codificado ({len(unique_vals)} categorías).")
            
    return df, mappings

def decode_categorical_columns(df, mappings):
    """
    Realiza el proceso inverso: de numérico a los textos originales usando los mapeos.
    """
    print("Decodificando columnas a texto original...")
    for col, mapping in mappings.items():
        if col in df.columns:
            # Invertimos el diccionario: {numero: texto}
            inverse_mapping = {v: k for k, v in mapping.items()}
            df = df.with_columns(
                pl.col(col).replace(inverse_mapping, default="Unknown").cast(pl.String)
            )
    return df

def run_lazy_pipeline(df):
    """
    Ejemplo de como usar Lazy en Polars para optimizar el proceso.
    Combina la seleccion de variables, el casteo y el filtrado en un solo plan.
    """
    print("Iniciando Pipeline Lazy (Optimización de Polars)...")
    
    # 1. Iniciamos el modo Lazy con .lazy()
    lf = df.lazy()
    
    # 2. Definimos las columnas del modelo estrella
    star_columns = [
        "eventid", # Incluimos el ID para evitar ShapeErrors después
        "nkill", "nwound", "success", "propvalue", "iyear", "imonth", "iday", 
        "country_txt", "region_txt", "provstate", "city", "latitude", "longitude", 
        "gname", "gsubname", "attacktype1_txt", "suicide", "targtype1_txt", 
        "corp1", "target1", "weaptype1_txt", "weapsubtype1_txt"
    ]
    available = [c for c in star_columns if c in df.columns]

    # 3. Encadenamos operaciones (todavia no se ejecutan)
    lf = (
        lf.select(available)
        # Primero casteamos a numerico para tratar los ceros correctamente
        .with_columns([
            pl.col(c).cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int64) 
            for c in ["nkill", "nwound", "iyear", "imonth", "iday", "success"] 
            if c in available
        ])
        .with_columns([
            pl.col(c).cast(pl.Float64, strict=False).fill_null(0.0)
            for c in ["latitude", "longitude", "propvalue"]
            if c in available
        ])
        # FILTRO: Eliminamos los registros donde el mes o el día son desconocidos (valor 0)
        .filter(
            (pl.col("imonth") != 0) & (pl.col("iday") != 0)
        )
    )
    
    # 4. Ejecutamos el plan
    print("Ejecutando plan optimizado con .collect()...")
    df_final = lf.collect()
    
    print(f"Procesamiento Lazy finalizado: {df_final.height} registros válidos conservados.")
    return df_final
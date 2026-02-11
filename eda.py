import mlxtend.preprocessing.shuffle
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import mondongo
import numpy as np
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
    
    print("Seleccionando variables para el modelo")
    
    # Lista de columnas requeridas basada en tu especificación
    required_columns = [
        "eventid",
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
    
    print("Iniciando Pipeline Lazy (Optimización de Polars)...")
    
    lf = df.lazy()
    
    star_columns = [
        "eventid", 
        "nkill", "nwound", "success", "propvalue", "iyear", "imonth", "iday", 
        "country_txt", "region_txt", "provstate", "city", "latitude", "longitude", 
        "gname", "gsubname", "attacktype1_txt", "suicide", "targtype1_txt", 
        "corp1", "target1", "weaptype1_txt", "weapsubtype1_txt"
    ]
    available = [c for c in star_columns if c in df.columns]

    lf = (
        lf.select(available)
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
        .filter(
            (pl.col("imonth") != 0) & (pl.col("iday") != 0)
        )
    )
    
    print("Ejecutando plan optimizado con .collect()...")
    df_final = lf.collect()
    
    print(f"Procesamiento Lazy finalizado: {df_final.height} registros válidos conservados.")
    return df_final

def show_specific_mapping(mappings, column_name):
    """
    Imprime de forma legible el mapeo almacenado para una columna específica.
    Muestra qué número corresponde a qué texto original.
    """
    if column_name not in mappings:
        print(f"La columna '{column_name}' no se encuentra en el diccionario de mapeos.")
        return
        
    print(f"\n--- Mapeo para la columna: {column_name} ---")
    mapping_dict = mappings[column_name]
    
    # Invertimos y ordenamos para mostrarlo del 0 al final
    # mappings[col] es {texto: id}, queremos mostrarlo ordenado por id
    sorted_mapping = sorted(mapping_dict.items(), key=lambda item: item[1])
    
    for text, idx in sorted_mapping:
        print(f" ID {idx:2} -> {text}")
    print("------------------------------------------\n")

def plot_top_countries(df, top_n=15):
    """Visualiza los países con mayor número de incidentes divididos por éxito/fallo."""
    print(f"Graficando Top {top_n} países con estado de éxito...")
    
    # 1. Obtenemos los nombres de los top N países
    top_country_names = df.group_by("country_txt").count().sort("count", descending=True).head(top_n).select("country_txt")
    
    # 2. Filtramos y agrupamos por país y éxito
    country_counts = (
        df.filter(pl.col("country_txt").is_in(top_country_names["country_txt"]))
        .group_by(["country_txt", "success"])
        .count()
        .to_pandas()
    )
    
    # Mapeo para la leyenda
    country_counts["success"] = country_counts["success"].map({1: "Exitoso", 0: "Fallido"})
    
    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")
    sns.barplot(
        data=country_counts, 
        x="count", 
        y="country_txt", 
        hue="success", 
        order=top_country_names["country_txt"].to_list(), # Forzamos el orden correcto
        palette={"Exitoso": "#2ecc71", "Fallido": "#e74c3c"}
    )
    
    plt.title(f"Top {top_n} Países: Ataques Exitosos vs. Fallidos", fontsize=15, pad=20)
    plt.xlabel("Cantidad de ataques", fontsize=12)
    plt.ylabel("País", fontsize=12)
    plt.legend(title="Resultado")
    plt.tight_layout()
    plt.show()

def plot_attacks_by_weapon(df):
    """Muestra la distribución de ataques según el tipo de arma, agrupando minoritarios (<5%) en 'Otros'."""
    print("Graficando distribución por tipo de arma...")
    
    # 1. Agrupar y calcular porcentajes
    weapon_counts = (
        df.group_by("weaptype1_txt")
        .count()
        .sort("count", descending=True)
    )
    
    total = weapon_counts["count"].sum()
    weapon_counts = weapon_counts.with_columns(
        (pl.col("count") / total * 100).alias("percentage")
    ).to_pandas()
    
    # 2. Agrupar los menores de 5% en 'Otros'
    mask = weapon_counts["percentage"] >= 5
    others_count = weapon_counts.loc[~mask, "count"].sum()
    
    plot_data = weapon_counts[mask].copy()
    if others_count > 0:
        new_row = pd.DataFrame([{"weaptype1_txt": "Otros", "count": others_count, "percentage": (others_count/total*100)}])
        plot_data = pd.concat([plot_data, new_row], ignore_index=True)
    
    # 3. Graficar
    plt.figure(figsize=(10, 7))
    colors = sns.color_palette("pastel", len(plot_data))
    
    plt.pie(plot_data["count"], 
            labels=plot_data["weaptype1_txt"], 
            autopct='%1.1f%%', 
            startangle=140, 
            colors=colors,
            pctdistance=0.85,
            explode=[0.05] * len(plot_data)) # Un pequeño espacio entre trozos
    
    # Círculo blanco central para convertirlo en Donut (opcional, queda más premium)
    centre_circle = plt.Circle((0,0), 0.70, fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    
    plt.title("Distribución de Armas Utilizadas (Categorías > 5%)", fontsize=15, pad=20)
    plt.axis('equal') 
    plt.tight_layout()
    plt.show()

def plot_historical_evolution(df):
    """Muestra la tendencia de ataques a lo largo de los años dividida por éxito."""
    print("Graficando evolución histórica por éxito...")
    
    yearly_trend = (
        df.group_by(["iyear", "success"])
        .count()
        .sort("iyear")
        .to_pandas()
    )
    
    # Mapeo para leyenda
    yearly_trend["success"] = yearly_trend["success"].map({1: "Exitoso", 0: "Fallido"})
    
    plt.figure(figsize=(14, 6))
    sns.lineplot(data=yearly_trend, x="iyear", y="count", hue="success", palette={"Exitoso": "#2ecc71", "Fallido": "#e74c3c"}, linewidth=2)
    
    plt.title("Evolución de Ataques: Exitosos vs Fallidos por Año", fontsize=16, pad=20)
    plt.xlabel("Año", fontsize=12)
    plt.ylabel("Número de Ataques", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.show()

def plot_top_groups(df, top_n=10, exclude_unknown=True):
    """Visualiza los grupos terroristas más activos divididos por éxito/fallo."""
    print(f"Graficando Top {top_n} grupos con estado de éxito...")
    
    # 1. Filtramos y buscamos top grupos
    groups_base = df
    if exclude_unknown:
        groups_base = groups_base.filter(pl.col("gname") != "Unknown")
        
    top_groups = groups_base.group_by("gname").count().sort("count", descending=True).head(top_n).select("gname")
    
    # 2. Agrupamos por grupo y éxito
    plot_data = (
        groups_base.filter(pl.col("gname").is_in(top_groups["gname"]))
        .group_by(["gname", "success"])
        .count()
        .to_pandas()
    )
    
    plot_data["success"] = plot_data["success"].map({1: "Exitoso", 0: "Fallido"})
    
    plt.figure(figsize=(12, 8))
    sns.set_style("darkgrid")
    sns.barplot(
        data=plot_data, 
        x="count", 
        y="gname", 
        hue="success", 
        order=top_groups["gname"].to_list(), # Forzamos el orden correcto
        palette={"Exitoso": "#27ae60", "Fallido": "#c0392b"}
    )
    
    plt.title(f"Top {top_n} Grupos: Distribución de Éxito", fontsize=15, pad=20)
    plt.xlabel("Número de Ataques", fontsize=12)
    plt.ylabel("Nombre del Grupo", fontsize=12)
    plt.legend(title="Resultado")
    plt.tight_layout()
    plt.show()

def show_correlation_analysis(df, threshold=0.6):
    """
    Calcula la matriz de correlación, muestra un heatmap triangular
    y lista las variables con una correlación mayor al umbral especificado.
    """
    print(f"Iniciando análisis de correlación (Umbral > {threshold})...")
    
    # Seleccionamos numéricas y quitamos IDs
    exclude_list = ["eventid", "id_ataque", "id_tiempo", "id_ubicacion", "latitude", "longitude"]
    numeric_cols = [
        col for col in df.columns 
        if df[col].dtype.is_numeric() and col not in exclude_list
    ]
    
    if not numeric_cols:
        print("No hay variables numéricas.")
        return

    # Calculamos correlación con Pandas
    corr_matrix = df.select(numeric_cols).to_pandas().corr()
    
    # 1. Lista de correlaciones altas (evitando duplicados y la diagonal)
    print(f"\n--- Variables con Correlación >= {threshold} ---")
    high_corr_found = False
    affected_vars = set()
    
    # Obtenemos el triángulo superior
    sol = (corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
                  .stack()
                  .sort_values(ascending=False))
    
    for (val1, val2), value in sol.items():
        if abs(value) >= threshold:
            print(f" * {val1} <-> {val2}: {value:.4f}")
            affected_vars.add(val1)
            affected_vars.add(val2)
            high_corr_found = True
            
    if not high_corr_found:
        print(f"No se han encontrado pares con correlación superior o igual a {threshold}.")
    else:
        print(f"\nLista de variables altamente correlacionadas: {list(affected_vars)}")

    # 2. Mapa de Calor Triangular
    plt.figure(figsize=(12, 10))
    
    # Creamos la mascara para el triangulo inferior (para mostrar el superior) o viceversa
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
    
    sns.heatmap(corr_matrix, 
                mask=mask, 
                annot=True, 
                fmt=".2f", 
                cmap="coolwarm", 
                center=0,
                linewidths=.5)
    
    plt.title(f"Matriz de Correlación Triangular (Thresh: {threshold})", fontsize=15)
    plt.tight_layout()
    plt.show()

    return corr_matrix


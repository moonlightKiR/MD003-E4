import sqlite3
from sqlite3 import Error
import os
import polars as pl

def create_connection(db_file):
    conn = None
    try:
        directory = os.path.dirname(db_file)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directorio creado: {directory}")

        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(f"Error crítico DB: {e}")
    return conn

def limpiar_tablas(conn):
    tablas = ["PUENTE_USA", "FACT_ATAQUES", "ARMA", "OBJETIVO", "METODO", "GRUPO", "UBICACION", "TIEMPO"]
    try:
        c = conn.cursor()
        for tabla in tablas:
            c.execute(f"DELETE FROM {tabla};")
            c.execute(f"DELETE FROM sqlite_sequence WHERE name='{tabla}';")
        conn.commit()
        print("Base de datos limpia.")
    except Error as e:
        print(f"Error limpiando: {e}")

def execute_sql(conn, sql_statement):
    try:
        c = conn.cursor()
        c.execute(sql_statement)
    except Error as e:
        print(f"Error SQL: {e}")

def crear_esquema(conn):
    sql_tiempo = "CREATE TABLE IF NOT EXISTS TIEMPO (id_tiempo INTEGER PRIMARY KEY AUTOINCREMENT, iyear INTEGER, imonth INTEGER, iday INTEGER);"
    sql_ubicacion = "CREATE TABLE IF NOT EXISTS UBICACION (id_ubicacion INTEGER PRIMARY KEY AUTOINCREMENT, country_txt VARCHAR(100), region_txt VARCHAR(100), provstate VARCHAR(100), city VARCHAR(100), latitude REAL, longitude REAL);"
    sql_grupo = "CREATE TABLE IF NOT EXISTS GRUPO (id_grupo INTEGER PRIMARY KEY AUTOINCREMENT, gname VARCHAR(255), subgname VARCHAR(255));"
    sql_metodo = "CREATE TABLE IF NOT EXISTS METODO (id_metodo INTEGER PRIMARY KEY AUTOINCREMENT, attacktype1_txt VARCHAR(150), suicide INTEGER);"
    sql_objetivo = "CREATE TABLE IF NOT EXISTS OBJETIVO (id_objetivo INTEGER PRIMARY KEY AUTOINCREMENT, targtype1_txt VARCHAR(150), corp1 VARCHAR(255), target1 VARCHAR(255));"
    sql_arma = "CREATE TABLE IF NOT EXISTS ARMA (id_arma INTEGER PRIMARY KEY AUTOINCREMENT, weaptype1_txt VARCHAR(150), weapsubtype1_txt VARCHAR(150));"

    sql_fact = """CREATE TABLE IF NOT EXISTS FACT_ATAQUES (
        id_ataque INTEGER PRIMARY KEY, nkill INTEGER, nwound INTEGER, success INTEGER, propvalue REAL,
        id_tiempo INTEGER, id_ubicacion INTEGER, id_grupo INTEGER, id_metodo INTEGER, id_objetivo INTEGER,
        FOREIGN KEY (id_tiempo) REFERENCES TIEMPO (id_tiempo),
        FOREIGN KEY (id_ubicacion) REFERENCES UBICACION (id_ubicacion),
        FOREIGN KEY (id_grupo) REFERENCES GRUPO (id_grupo),
        FOREIGN KEY (id_metodo) REFERENCES METODO (id_metodo),
        FOREIGN KEY (id_objetivo) REFERENCES OBJETIVO (id_objetivo));"""

    sql_puente = """CREATE TABLE IF NOT EXISTS PUENTE_USA (
        id_ataque INTEGER, id_arma INTEGER, PRIMARY KEY (id_ataque, id_arma),
        FOREIGN KEY (id_ataque) REFERENCES FACT_ATAQUES (id_ataque),
        FOREIGN KEY (id_arma) REFERENCES ARMA (id_arma));"""

    for sql in [sql_tiempo, sql_ubicacion, sql_grupo, sql_metodo, sql_objetivo, sql_arma, sql_fact, sql_puente]:
        execute_sql(conn, sql)

def procesar_dimension(df_principal, columnas_clave, nombre_id):
    df_dim = df_principal.select(columnas_clave).unique().drop_nulls()
    df_dim = df_dim.with_row_index(name=nombre_id, offset=1)
    df_dim = df_dim.select([nombre_id] + columnas_clave)
    df_con_fk = df_principal.join(df_dim, on=columnas_clave, how="left")

    return df_dim.to_numpy().tolist(), df_con_fk

def procesar_e_insertar(conn, df_raw, columnas_clave, nombre_id, funcion_insertar):
    print(f"Procesando dimensión: {nombre_id}...")
    datos, df_con_fk = procesar_dimension(df_raw, columnas_clave, nombre_id)
    funcion_insertar(conn, datos)
    return df_con_fk

def insert_generic(conn, sql, data):
    try:
        c = conn.cursor()
        c.executemany(sql, data)
        conn.commit()
    except Error as e:
        print(f"Error insert: {e}")

def insertar_tiempo(conn, d): insert_generic(conn,'INSERT INTO TIEMPO(id_tiempo, iyear, imonth, iday) VALUES(?,?,?,?)', d)
def insertar_ubicacion(conn, d): insert_generic(conn,'INSERT INTO UBICACION(id_ubicacion, country_txt, region_txt, provstate, city, latitude, longitude) VALUES(?,?,?,?,?,?,?)', d)
def insertar_grupo(conn, d): insert_generic(conn, 'INSERT INTO GRUPO(id_grupo, gname, subgname) VALUES(?,?,?)', d)
def insertar_metodo(conn, d): insert_generic(conn,'INSERT INTO METODO(id_metodo, attacktype1_txt, suicide) VALUES(?,?,?)', d)
def insertar_objetivo(conn, d): insert_generic(conn,'INSERT INTO OBJETIVO(id_objetivo, targtype1_txt, corp1, target1) VALUES(?,?,?,?)', d)
def insertar_arma(conn, d): insert_generic(conn,'INSERT INTO ARMA(id_arma, weaptype1_txt, weapsubtype1_txt) VALUES(?,?,?)', d)
def insertar_fact(conn, d): insert_generic(conn,'INSERT INTO FACT_ATAQUES(id_ataque, nkill, nwound, success, propvalue, id_tiempo, id_ubicacion, id_grupo, id_metodo, id_objetivo) VALUES(?,?,?,?,?,?,?,?,?,?)', d)
def insertar_puente(conn, d): insert_generic(conn,'INSERT INTO PUENTE_USA(id_ataque, id_arma) VALUES(?,?)', d)

def ejecutar_pipeline_sql(df):
    db_file = "data/terrorismo_gtd.db"
    print(f"\nIniciando SQL Pipeline en: {db_file}")

    conn = create_connection(db_file)
    if not conn: return

    crear_esquema(conn)
    limpiar_tablas(conn)

    df = procesar_e_insertar(conn, df, ["iyear", "imonth", "iday"], "id_tiempo", insertar_tiempo)
    df = procesar_e_insertar(conn, df, ["country_txt", "region_txt", "provstate", "city", "latitude", "longitude"],
                             "id_ubicacion", insertar_ubicacion)
    df = procesar_e_insertar(conn, df, ["gname", "gsubname"], "id_grupo", insertar_grupo)
    df = procesar_e_insertar(conn, df, ["attacktype1_txt", "suicide"], "id_metodo", insertar_metodo)
    df = procesar_e_insertar(conn, df, ["targtype1_txt", "corp1", "target1"], "id_objetivo", insertar_objetivo)

    print("Procesando Armas y Puente...")
    datos_arma, df_con_arma = procesar_dimension(df, ["weaptype1_txt", "weapsubtype1_txt"], "id_arma")
    insertar_arma(conn, datos_arma)

    cols_fact = ["eventid", "nkill", "nwound", "success", "propvalue",
                 "id_tiempo", "id_ubicacion", "id_grupo", "id_metodo", "id_objetivo"]
    
    if "eventid" not in df.columns:
        print("ERROR: Falta la columna 'eventid' en el DataFrame. No se pueden insertar hechos.")
        conn.close()
        return

    df_fact = df.select(cols_fact).drop_nulls(subset=["eventid"])
    insertar_fact(conn, df_fact.to_numpy().tolist())
    print(f"Hechos insertados: {df_fact.height}")

    df_puente = df_con_arma.select(["eventid", "id_arma"]).drop_nulls()
    insertar_puente(conn, df_puente.to_numpy().tolist())
    conn.close()
   

def extraer_dataframe_analitico(db_file="data/terrorismo_gtd.db"):

    print(f"Extrayendo datos de: {db_file}...")
    
    conn = create_connection(db_file)
    if not conn: return None

    query = """
    SELECT 
        f.id_ataque as eventid,
        f.nkill, 
        f.nwound, 
        f.success, 
        f.propvalue,
        
        -- Dimension Tiempo
        t.iyear, t.imonth, t.iday,
        
        -- Dimension Ubicacion
        u.country_txt, u.region_txt, u.provstate, u.city, u.latitude, u.longitude,
        
        -- Dimension Grupo
        g.gname, g.subgname as gsubname,
        
        -- Dimension Metodo
        m.attacktype1_txt, m.suicide,
        
        -- Dimension Objetivo
        o.targtype1_txt, o.corp1, o.target1,
        
        -- Dimension Arma (Unida via Puente)
        a.weaptype1_txt, a.weapsubtype1_txt

    FROM FACT_ATAQUES f
    LEFT JOIN TIEMPO t ON f.id_tiempo = t.id_tiempo
    LEFT JOIN UBICACION u ON f.id_ubicacion = u.id_ubicacion
    LEFT JOIN GRUPO g ON f.id_grupo = g.id_grupo
    LEFT JOIN METODO m ON f.id_metodo = m.id_metodo
    LEFT JOIN OBJETIVO o ON f.id_objetivo = o.id_objetivo
    -- Join especial para la relación N:M de armas
    LEFT JOIN PUENTE_USA pu ON f.id_ataque = pu.id_ataque
    LEFT JOIN ARMA a ON pu.id_arma = a.id_arma
    """

    try:
        df_sql = pl.read_database(query, conn)
        print(f"Extracción completada: {df_sql.height} filas recuperadas.")
        conn.close()
        return df_sql
        
    except Exception as e:
        print(f"Error al extraer datos: {e}")
        conn.close()
        return None
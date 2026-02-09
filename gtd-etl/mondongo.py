import os
import csv
import json
import requests
from pymongo import MongoClient

def upload_data():
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "gtd_database")
    COLLECTION_NAME = "incidents"
    CSV_URL = "https://media.githubusercontent.com/media/moonlightKiR/GTD/refs/heads/main/global_terrorism_data.csv"
    
    base_path = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(base_path, "global_terrorism_data.csv")
    json_file = os.path.join(base_path, "global_terrorism_data.json")

    print(f"Descargando CSV desde: {CSV_URL}...")
    try:
        response = requests.get(CSV_URL, stream=True)
        response.raise_for_status()
        with open(csv_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"CSV descargado correctamente.")
    except Exception as e:
        print(f"Error descargando el CSV: {e}")
        return

    print(f"Transformando CSV a JSON (Streaming)...")
    try:
        with open(csv_file, 'r', encoding='latin-1') as f_in, open(json_file, 'w', encoding='utf-8') as f_out:
            reader = csv.DictReader(f_in)
            f_out.write("[\n")
            first = True
            for row in reader:
                if not first:
                    f_out.write(",\n")
                json.dump(row, f_out, indent=4)
                first = False
            f_out.write("\n]")
        print(f"Archivo JSON creado correctamente.")
    except Exception as e:
        print(f"Error en la transformacion: {e}")
        return

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Conectado a MongoDB.")

        print(f"Subiendo datos por bloques...")
        with open(json_file, 'r', encoding='utf-8') as f:
            # En lugar de cargar todo el JSON, volvemos a leer el CSV para subirlo
            # porque leer un JSON gigante por bloques es muy costoso.
            # Volver a leer el CSV es instantáneo y ahorra RAM.
            with open(csv_file, 'r', encoding='latin-1') as f_csv:
                reader = csv.DictReader(f_csv)
                batch = []
                count = 0
                for row in reader:
                    batch.append(row)
                    count += 1
                    if len(batch) >= 5000:
                        collection.insert_many(batch)
                        print(f"   - {count} registros subidos...")
                        batch = []
                if batch:
                    collection.insert_many(batch)
            
        print(f"Datos subidos correctamente.")

    except Exception as e:
        print(f"Error al subir a MongoDB: {e}")
    finally:
        if client:
            client.close()
            print("Conexion con MongoDB cerrada.")
        
        if os.path.exists(csv_file):
             os.remove(csv_file)
        if os.path.exists(json_file):
             os.remove(json_file)
        print(f"Archivos temporales eliminados.")

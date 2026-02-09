import os
import csv
import json
import requests
from pymongo import MongoClient, UpdateOne

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

        print(f"Sincronizando registros (Insertar solo si no existe)...")
        with open(csv_file, 'r', encoding='latin-1') as f_csv:
            reader = csv.DictReader(f_csv)
            operations = []
            count = 0
            for row in reader:
                event_id = row.get("eventid")
                if event_id:
                    operations.append(
                        UpdateOne(
                            {"eventid": event_id},
                            {"$setOnInsert": row},
                            upsert=True
                        )
                    )
                count += 1
                if len(operations) >= 2000:
                    collection.bulk_write(operations)
                    print(f"   - {count} registros procesados...")
                    operations = []
            if operations:
                collection.bulk_write(operations)
            
        print(f"Sincronizacion completada.")

    except Exception as e:
        print(f"Error al sincronizar con MongoDB: {e}")
    finally:
        if client:
            client.close()
            print("Conexion con MongoDB cerrada.")
        
        if os.path.exists(csv_file):
             os.remove(csv_file)
        if os.path.exists(json_file):
             os.remove(json_file)
        print(f"Archivos temporales eliminados.")

import json
import os
import csv
import requests
from pymongo import MongoClient, UpdateOne

def upload_data():
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    DATABASE_NAME = "gtd_database"
    COLLECTION_NAME = "incidents"
    CSV_URL = "https://media.githubusercontent.com/media/moonlightKiR/GTD/refs/heads/main/global_terrorism_data.csv"
    
    base_path = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(base_path, "global_terrorism_data.csv")

    print(f"Descargando CSV desde: {CSV_URL}...")
    try:
        response = requests.get(CSV_URL, stream=True)
        response.raise_for_status()
        with open(csv_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"CSV descargado.")
    except Exception as e:
        print(f"Error descargando: {e}")
        return

    data = []
    print(f"Transformando CSV...")
    try:
        with open(csv_file, encoding='latin-1') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        print(f"Transformación completada. Registros: {len(data)}")
    except Exception as e:
        print(f"Error procesando CSV: {e}")
        return

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Conectado a MongoDB.")

        print(f"Iniciando subida inteligente de {len(data)} registros...")
        
        batch_size = 2000
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            operations = []
            
            for record in batch:
                event_id = record.get("eventid")
                if event_id:
                    operations.append(
                        UpdateOne(
                            {"eventid": event_id}, 
                            {"$set": record}, 
                            upsert=True
                        )
                    )
            
            if operations:
                collection.bulk_write(operations)

        print(f"Datos sincronizados correctamente.")

    except Exception as e:
        print(f"Error al sincronizar con MongoDB: {e}")
    finally:
        if client:
            client.close()
            print("Conexion cerrada.")
        
        if os.path.exists(csv_file):
             os.remove(csv_file)
             print(f"Archivo temporal eliminado.")
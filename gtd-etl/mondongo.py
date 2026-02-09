import os
import csv
import requests
from pymongo import MongoClient

def upload_data():
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "gtd_database")
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
        print(f"CSV descargado correctamente.")
    except Exception as e:
        print(f"Error descargando el CSV: {e}")
        return

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Conectado a MongoDB.")

        print(f"Procesando y subiendo registros...")
        
        with open(csv_file, encoding='latin-1') as f:
            reader = csv.DictReader(f)
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
                print(f"   - {count} registros totales subidos.")
            
            print(f"Datos subidos correctamente.")

    except Exception as e:
        print(f"Error al subir a MongoDB: {e}")
    finally:
        if client:
            client.close()
            print("Conexion con MongoDB cerrada.")
        
        if os.path.exists(csv_file):
             os.remove(csv_file)
             print(f"Archivo temporal eliminado.")

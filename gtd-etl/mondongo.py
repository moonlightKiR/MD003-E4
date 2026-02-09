import json
import os
import csv
import requests
from pymongo import MongoClient

def upload_data():
    # Configuración
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
        print(f"✅ CSV descargado correctamente en {csv_file}")
    except Exception as e:
        print(f"❌ Error descargando el CSV: {e}")
        return

    data = []
    print(f"Transformando CSV a JSON (diccionarios)...")
    try:

        with open(csv_file, encoding='latin-1') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        print(f"Transformación completada. Registros procesados: {len(data)}")
    except Exception as e:
        print(f"Error procesando el CSV: {e}")
        return


    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Conectado a MongoDB en {MONGO_URI}")

        print(f"Subiendo {len(data)} registros a la colección '{COLLECTION_NAME}'...")
        if data:
            batch_size = 5000
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                collection.insert_many(batch)
                print(f"   - {min(i + batch_size, len(data))}/{len(data)} insertados...")
            
            print(f"🚀 ¡Todo listo! Datos subidos correctamente.")
        else:
            print("No hay datos para insertar.")

    except Exception as e:
        print(f"Error al subir a MongoDB: {e}")
    finally:
        if client:
            client.close()
            print("Conexión con MongoDB cerrada.")
        
        if os.path.exists(csv_file):
             os.remove(csv_file)
             print(f"Archivo temporal {csv_file} eliminado.")

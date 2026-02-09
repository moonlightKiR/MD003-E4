import json
import os
from pymongo import MongoClient

def upload_data():
    MONGO_URI = "mongodb://localhost:27017/"
    DATABASE_NAME = "gtd_database"
    COLLECTION_NAME = "incidents"
    
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(base_path, "..", "datos.json")

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Conectado a MongoDB en {MONGO_URI}")

        if not os.path.exists(data_file):
            print(f"Error: No se encuentra el fichero en {data_file}")
            return

        print(f"Leyendo {data_file}... Espere un momento.")
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"Fichero cargado correctamente. Registros encontrados: {len(data)}")

        print(f"Insertando datos en la colección '{COLLECTION_NAME}'...")
        if isinstance(data, list):
            collection.insert_many(data)
            print(f"Inserción masiva completada.")
        else:
            collection.insert_one(data)
            print(f"Inserción individual completada.")

    except Exception as e:
        print(f"Ocurrió un error en mondongo.upload_data: {e}")
    finally:
        if client:
            client.close()
            print("Conexión con MongoDB cerrada.")

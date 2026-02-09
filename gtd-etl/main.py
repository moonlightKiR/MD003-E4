import mondongo
import cleanfiles
import eda

if __name__ == "__main__":
    # 1. Subida a MongoDB
    mondongo.upload_data()
    
    # 2. Analisis Exploratorio con PySpark
    eda.perform_eda()
    
    # 3. Limpieza de temporales
    cleanfiles.clean_files()

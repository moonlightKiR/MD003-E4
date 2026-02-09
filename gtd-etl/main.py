import mondongo
import cleanfiles
import eda

if __name__ == "__main__":
    mondongo.upload_data()
    cleanfiles.clean_files()
    eda.perform_eda()

import mondongo
import cleanfiles

if __name__ == "__main__":
    mondongo.upload_data()
    cleanfiles.clean_files()

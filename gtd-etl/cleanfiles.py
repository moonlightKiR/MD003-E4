import os
import glob

def clean_files():
    base_path = os.path.dirname(os.path.abspath(__file__))
    pattern = os.path.join(base_path, "*.csv")
    files = glob.glob(pattern)
    
    for f in files:
        if os.path.exists(f):
            os.remove(f)
            print(f"Archivo eliminado: {f}")

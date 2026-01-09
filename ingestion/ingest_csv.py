import shutil
import os

os.makedirs("/data/bronze", exist_ok=True)

shutil.copy(
    "/data/source/ventes.csv",
    "/data/bronze/ventes.csv"
)

print("CSV ingéré en Bronze")

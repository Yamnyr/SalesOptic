import shutil
import os

os.makedirs("/data/bronze", exist_ok=True)

shutil.copy(
    "/data/source/logs.txt",
    "/data/bronze/logs.txt"
)

print("Logs ingérés en Bronze")

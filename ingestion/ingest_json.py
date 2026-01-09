import shutil
import os

os.makedirs("/data/bronze", exist_ok=True)

shutil.copy(
    "/data/source/events.json",
    "/data/bronze/events.json"
)

print("JSON ingéré en Bronze")

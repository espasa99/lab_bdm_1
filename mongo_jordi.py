import pandas as pd
from pymongo import MongoClient
import json

# Reemplaza estos valores con la información de tu conexión a MongoDB
mongo_connection_string = "mongodb://localhost:27017/"
database_name = "persisten_landind"
collection_name = "compraventa_api"

# Archivos CSV y JSON
csv_file = "./temporal_landing/compraventa_api/records_compraventa.csv"
# json_file = "example.json"

# Conectarse a MongoDB
client = MongoClient(mongo_connection_string)
db = client[database_name]
collection = db[collection_name]

# Leer y cargar datos CSV
csv_data = pd.read_csv(csv_file)
csv_records = csv_data.to_dict("records")
collection.insert_many(csv_records)

# Leer y cargar datos JSON
# with open(json_file, "r") as f:
#     json_data = json.load(f)
#     collection.insert_many(json_data)

# print(f"Los datos de {csv_file} y {json_file} se han cargado correctamente en MongoDB Compass.")

import os
import pandas as pd
from pymongo import MongoClient

def connect_mongo(mongo_connection_string, database_name):
    '''
    Función para conectarse a MongoDB
    '''

    client = MongoClient(mongo_connection_string)
    db = client[database_name]

    return db

def insert_data_mongo(csv_folder, collection_name, data_base_mongo, insert_type=['all']):
    '''
    Función para leer y cargar los archivos CSV en MongoDB
    '''

    collection = data_base_mongo[collection_name]

    if insert_type[0] == 'all':
        csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]

    else:
        csv_files = insert_type

    for csv_file in csv_files:
        file_path = os.path.join(csv_folder, csv_file)
        csv_data = pd.read_csv(file_path)
        csv_records = csv_data.to_dict("records")
        collection.insert_many(csv_records)
        print(f"Los datos de {csv_file} se han cargado correctamente en MongoDB.")

if __name__ == "__main__":

    # Conectarse a MongoDB
    mongo_connection_string = "mongodb://localhost:27017/"
    database_name = "persistent_landing"

    # Conectarse a MongoDB
    data_base_mongo = connect_mongo(mongo_connection_string, database_name)

    # Income data
    csv_folder = "./temporal_landing/opendatabcn-income/"
    collection_name = "income"
    insert_type = ['all']
    insert_data_mongo(csv_folder, collection_name, data_base_mongo, insert_type)

    # lookup_tables data
    csv_folder = "./temporal_landing/lookup_tables/"
    collection_name = "lookup_tables"
    insert_type = ['all']
    insert_data_mongo(csv_folder, collection_name, data_base_mongo, insert_type)

    # compraventa_api data
    csv_folder = "./temporal_landing/compraventa_api/"
    collection_name = "compraventa_api"
    insert_type = ['all']
    insert_data_mongo(csv_folder, collection_name, data_base_mongo, insert_type)
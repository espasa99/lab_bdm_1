import os
import json
import pandas as pd
from pymongo import MongoClient

def connect_mongo(mongo_connection_string: str, database_name: str) -> object:
    '''
    Función para conectarse a MongoDB
    '''

    client = MongoClient(mongo_connection_string)
    db = client[database_name]

    return db

def insert_data_mongo(temporal_landing_path: str, collection_name: str, data_base_mongo: object, insert_type=['all']) -> None:
    '''
    Función para leer y cargar los archivos CSV en MongoDB
    '''
    
    folder_path = f'{temporal_landing_path}{collection_name}/'

    collection = data_base_mongo[collection_name]
    collection.delete_many({}) # (?) hace falta borrar la colección antes de insertar los datos?

    if insert_type[0] == 'all':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = insert_type

    for file_name in files_list:

        file_path = os.path.join(folder_path, file_name)

        df_file = pd.read_csv(file_path)
        data = df_file.to_dict("records")

        if len(data) > 0:    # (?) hace falta comprobar que la lista no esté vacía
            collection.insert_many(data)
            print(f"Los datos de {file_name} se han cargado correctamente en MongoDB.")

def insert_idealista_data_mongo(temporal_landing_path: str, collection_name: str, data_base_mongo: object, insert_type=['all']) -> None:
    '''
    Función para leer y cargar los archivos CSV en MongoDB
    '''
    
    folder_path = f'{temporal_landing_path}{collection_name}/'

    collection = data_base_mongo[collection_name]
    collection.delete_many({}) # (?) hace falta borrar la colección antes de insertar los datos?

    if insert_type[0] == 'all':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = insert_type

    for file_name in files_list:
        
        file_path = os.path.join(folder_path, file_name)
        year = file_name.split('_')[0]

        with open(file_path, 'r') as file:

            data = json.load(file)

            for doc in data:       # (?) fa falta afegir la data a cada document?
                doc['date'] = year

        if len(data) > 0:    # (?) hace falta comprobar que la lista no esté vacía
            collection.insert_many(data)
            print(f"Los datos de {file_name} se han cargado correctamente en MongoDB.")

import os
import json
import pandas as pd
from pymongo import MongoClient
import sqlite3
from datetime import datetime

def connect_mongo(mongo_connection_string: str, database_name: str) -> object:
    '''
    Function to connect to MongoDB

    Parameters
    ----------
    mongo_connection_string : str
        Connection string to MongoDB
    database_name : str
        Name of the database to connect to

    Returns
    -------
    object
        MongoDB database object
        
    '''

    client = MongoClient(mongo_connection_string)
    db = client[database_name]

    return db

def insert_csv_data_to_mongo(temporal_landing_path: str, collection_name: str, data_base_mongo: object, insert_type=['all']) -> None:
    '''
    Function to read and load the CSV files into MongoDB

    Parameters
    ----------
    temporal_landing_path : str
        Path to the folder where the CSV files are stored
    collection_name : str
        Name of the collection where the data will be stored
    data_base_mongo : object
        MongoDB database object
    insert_type : list, optional
        List of files to insert. If 'all' is passed, all the files in the folder will be inserted, by default ['all']
        
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

        if len(data) > 0:    
            collection.insert_many(data)

            date = file_name.split('_')[0]
            
            register_upload(date, file_name, 'json', collection_name)
            print(f"Los datos de {file_name} se han cargado correctamente en MongoDB.")

def insert_json_data_mongo(temporal_landing_path: str, collection_name: str, data_base_mongo: object, insert_type=['all']) -> None:
    '''
    Function to read and load the idealista files into MongoDB

    Parameters
    ----------
    temporal_landing_path : str
        Path to the folder where the JSON files are stored
    collection_name : str
        Name of the collection where the data will be stored
    data_base_mongo : object
        MongoDB database object
    insert_type : list, optional
        List of files to insert. If 'all' is passed, all the files in the folder will be inserted, by default ['all']

    '''
    
    folder_path = f'{temporal_landing_path}{collection_name}/'

    collection = data_base_mongo[collection_name]

    if insert_type[0] == 'all':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = insert_type

    for file_name in files_list:
        
        file_path = os.path.join(folder_path, file_name)
        date = '_'.join(file_name.split('_')[0:3])

        with open(file_path, 'r') as file:

            data = json.load(file)      
            if len(data) > 0:
                collection.insert_many(data)

            register_upload(date, file_name, 'json', collection_name)
            print(f"Los datos de {file_name} se han cargado correctamente en MongoDB.")

def register_upload(valid_date: str, file_name: str, file_format: str, collection_name: str) -> None:
    '''
    Function to register the upload in the database (SQLite)

    Parameters
    ----------
    valid_date : str
        Date of the data
    file_name : str
        Name of the file
    file_format : str
        Format of the file
    collection_name : str
    '''

    conn = sqlite3.connect('./register_uploads/register_uploads.db')
    c = conn.cursor()

    now = datetime.now()
    upload_date = now.strftime("%Y/%m/%d %H:%M:%S")
    query = f"INSERT INTO log VALUES ('{upload_date}', '{valid_date}', '{file_name}', '{file_format}', '{collection_name}')"
    c.execute(query)

    conn.commit()
    conn.close()
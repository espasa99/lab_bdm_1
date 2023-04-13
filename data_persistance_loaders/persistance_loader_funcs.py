import os
import json
import sqlite3
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

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

def insert_csv_data_to_mongo(temporal_landing_path: str, collection_name: str, data_base_mongo: object, update_type: str, update_frequency: int) -> None:
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
    update_type : string
        Complete or incremental update. 
        If the update its complete we will insert all the files in the folder, 
        if not we will insert only the files modified since the last week in the 
        temporal_landing by querying the register_upload database.
        
    '''
    
    folder_path = f'{temporal_landing_path}{collection_name}/'

    collection = data_base_mongo[collection_name]
    collection.delete_many({}) # (?) hace falta borrar la colección antes de insertar los datos?

    if update_type == 'complete':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = temporal_files_modified_since_last_week(collection_name, update_frequency)

    for file_name in files_list:

        file_path = os.path.join(folder_path, file_name)

        df_file = pd.read_csv(file_path)
        data = df_file.to_dict("records")

        if len(data) > 0:    
            collection.insert_many(data)

            date = file_name.split('_')[0]
            
            register_upload(date, file_name, 'json', collection_name)
            print(f"Los datos de {file_name} se han cargado correctamente en MongoDB.")

def insert_json_data_mongo(temporal_landing_path: str, collection_name: str, data_base_mongo: object, update_type: str, update_frequency: int) -> None:
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
    update_type : string
        Complete or incremental update. 
        If the update its complete we will insert all the files in the folder, 
        if not we will insert only the files modified since the last week in the 
        temporal_landing by querying the register_upload database.
    '''
    
    folder_path = f'{temporal_landing_path}{collection_name}/'

    collection = data_base_mongo[collection_name]

    if update_type == 'complete':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = temporal_files_modified_since_last_week(collection_name, update_frequency)

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
    query = f"INSERT INTO uploads_persitent_landing VALUES ('{upload_date}', '{valid_date}', '{file_name}', '{file_format}', '{collection_name}')"
    c.execute(query)

    conn.commit()
    conn.close()

def temporal_files_modified_since_last_week(collection_name: str, update_frequency: int) -> list:
    '''
    Function to get the files modified since the last week in the temporal_landing

    Returns
    -------
    list
        List of files modified since the last week in the temporal_landing
    '''

    conn = sqlite3.connect('./register_uploads/register_uploads.db')
    c = conn.cursor()

    today = datetime.now()
    last_week = today - timedelta(weeks=update_frequency)
    last_week = last_week.strftime("%Y/%m/%d %H:%M:%S")
    
    query = f"SELECT file_name FROM uploads_temporal_landing WHERE upload_date >= '{last_week}' AND collection_name = '{collection_name}'"
    c.execute(query)

    files_list = [file[0] for file in c.fetchall()]

    conn.close()

    return files_list

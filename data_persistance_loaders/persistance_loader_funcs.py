import os
import json
import pandas as pd
# from pymongo import MongoClient
import happybase
import sqlite3
from datetime import datetime, timedelta


def connect_hbase(connection_string: str, table_name: str) -> happybase.Table:
    '''
    Function to connect to HBase

    Parameters
    ----------
    connection_string : str
        Connection string to HBase, 'host:port'. If no port is specified default port is used.
    table_name : str
        Name of the table that will be returned.

    Returns
    -------
    happybase.Table
        HBase table object

    '''

    # connection_string = "192.168.1.62"

    connection = happybase.Connection(connection_string, autoconnect=True)
    if table_name.encode() in connection.tables():
        return connection.table(table_name)
    else:
        connection.create_table(table_name, {'file': dict(), 'metadata': dict()})
        return connection.table(table_name)

def insert_csv_data_to_hbase(temporal_landing_path: str, source_name: str, hbase_table: happybase.Table, update_type: str, update_frequency: int) -> None:
    '''
    Function to read and load the CSV files into MongoDB

    Parameters
    ----------
    temporal_landing_path : str
        Path to the folder where the CSV files are stored
    source_name : str
        Name of the source where data comes from
    hbase_table : happybase.Table
        HBase table object
    update_type : str
        Type of update to be performed. 'complete' or 'incremental'
    update_frequency : int
        Frequency of the update in days
    '''

    folder_path = f'{temporal_landing_path}{source_name}/'

    if update_type == 'complete':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = temporal_files_modified(source_name, update_frequency)

    for file_name in files_list:

        file_path = os.path.join(folder_path, file_name)
        print(file_path)

        file = open(file_path, 'rb')
        schema = file.readline()
        file_content = file.read()
        file.close()
        date = file_name.split('_')[0]
        key = '$'.join([source_name, date]).encode()

        hbase_table.put(key, {b'file:content': file_content})

        # schema =
        hbase_table.put(key, {b'metadata:schema': schema})

        register_upload(date, file_name, 'json', source_name)
        print(f"Los datos de {file_name} se han cargado correctamente en HBase.")

def get_schema(document):
    '''
        Function to get the schema of a document

        Parameters
        ----------
        document : dict
            dict, whose schema we want to obtain

        '''
    schema = {}
    for key in document.keys():
        value = document[key]
        schema[key] = get_schema(value) if type(value).__name__ == 'dict' else type(value).__name__
    return schema

def insert_json_data_to_hbase(temporal_landing_path: str, source_name: str, hbase_table: happybase.Table, update_type: str, update_frequency: int) -> None:
    '''
    Function to read and load the idealista files into HBase

    Parameters
    ----------
    temporal_landing_path : str
        Path to the folder where the JSON files are stored
    source_name : str
        Name of the source where data comes from
    hbase_table : happybase.Table
        hbase table
    update_type : str
        Type of update to be performed. 'complete' or 'incremental'
    update_frequency : int
        Frequency of the update in days
    '''

    folder_path = f'{temporal_landing_path}{source_name}/'

    if update_type == 'complete':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = temporal_files_modified(source_name, update_frequency)

    for file_name in files_list:

        file_path = os.path.join(folder_path, file_name)
        date = '_'.join(file_name.split('_')[0:3])

        file = open(file_path, 'rb')
        file_content = file.read()
        file.close()
        key = '$'.join([source_name, date]).encode()

        hbase_table.put(key, {b'file:content': file_content})

        file = open(file_path, 'r')
        doc0 = json.load(file)[0]
        file.close()

        schema = str(get_schema(doc0))
        hbase_table.put(key, {b'metadata:schema': schema})

        register_upload(date, file_name, 'json', source_name)
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
    query = f"INSERT INTO uploads_persistent_landing VALUES ('{upload_date}', '{valid_date}', '{file_name}', '{file_format}', '{collection_name}')"
    c.execute(query)

    conn.commit()
    conn.close()

def temporal_files_modified(collection_name: str, update_frequency: int) -> list:
    '''
    Function to get the files modified since last week (or another frequency depending on the config.) in the temporal_landing

    Returns
    -------
    list
        List of files modified since the last week in the temporal_landing
    '''

    conn = sqlite3.connect('./register_uploads/register_uploads.db')
    c = conn.cursor()

    today = datetime.now()
    last_week = today - timedelta(days=update_frequency)
    last_week = last_week.strftime("%Y/%m/%d %H:%M:%S")
    
    query = f"SELECT file_name FROM uploads_temporal_landing WHERE upload_date >= '{last_week}' AND collection_name = '{collection_name}'"
    c.execute(query)

    files_list = [file[0] for file in c.fetchall()]

    conn.close()

    return files_list

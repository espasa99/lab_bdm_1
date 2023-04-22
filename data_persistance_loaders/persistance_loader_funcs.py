import os
import json
import pandas as pd
# from pymongo import MongoClient
import happybase
import sqlite3
from datetime import datetime


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


def insert_csv_data_to_hbase(temporal_landing_path: str, source_name: str, hbase_table: happybase.Table, insert_type=['all']) -> None:
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
    insert_type : list, optional
        List of files to insert. If 'all' is passed, all the files in the folder will be inserted, by default ['all']

    '''

    folder_path = f'{temporal_landing_path}{source_name}/'

    if insert_type[0] == 'all':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = insert_type

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

def insert_json_data_to_hbase(temporal_landing_path: str, source_name: str, hbase_table: happybase.Table, insert_type=['all']) -> None:
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
    insert_type : list, optional
        List of files to insert. If 'all' is passed, all the files in the folder will be inserted, by default ['all']

    '''

    folder_path = f'{temporal_landing_path}{source_name}/'

    if insert_type[0] == 'all':
        files_list = [f for f in os.listdir(folder_path)]

    else:
        files_list = insert_type

    for file_name in files_list:

        file_path = os.path.join(folder_path, file_name)
        date = '_'.join(file_name.split('_')[0:3])

        file = open(file_path, 'rb')
        file_content = file.read()
        file.close()
        key = '$'.join([source_name, date]).encode()

        hbase_table.put(key, {b'file:content': file_content})

        # schema =
        # table.put(key, {b'metadata:schema': schema})

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
    query = f"INSERT INTO log VALUES ('{upload_date}', '{valid_date}', '{file_name}', '{file_format}', '{collection_name}')"
    c.execute(query)

    conn.commit()
    conn.close()
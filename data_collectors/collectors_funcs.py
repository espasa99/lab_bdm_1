import sqlite3
from datetime import datetime
import os
import shutil
import urllib.request
import json
import pandas as pd

def collect_data_from_data_source(src_folder, dest_folder, file_format, collection_name, update_type="complete", files_to_move=None):

    for file_name in os.listdir(src_folder):

        if update_type == "incremental" and file_name not in files_to_move:
            continue

        src_file_path = os.path.join(src_folder, file_name)
        dest_file_path = os.path.join(dest_folder, file_name)

        if os.path.isfile(src_file_path):

            shutil.move(src_file_path, dest_file_path)
            print(f"Moved '{src_file_path}' to '{dest_file_path}'")

            date = file_name.split('_')[0]
            register_upload(date, file_name, file_format, collection_name)

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
    query = f"INSERT INTO uploads_temporal_landing VALUES ('{upload_date}', '{valid_date}', '{file_name}', '{file_format}', '{collection_name}')"
    c.execute(query)

    conn.commit()
    conn.close()

def collect_data_from_api(api_urls_path, file_format, collection_name, dest_path, file_termination_name, limit_rows = 10000):

    with open(api_urls_path, 'r') as archivo_json:
        url_for_year = json.load(archivo_json)

    for year in url_for_year:

        url = url_for_year[year]
        with urllib.request.urlopen(url + str(limit_rows)) as url:
            s = url.read()

        data_str = s.decode('utf-8')

        data_json = json.loads(data_str)

        records_data_frame = pd.DataFrame(data_json['result']['records'])

        records_data_frame.to_csv(dest_path + year + file_termination_name, index=False)
    
        file_name = year + file_termination_name
        date = file_name.split('_')[0]

        register_upload(date, file_name, file_format, collection_name)

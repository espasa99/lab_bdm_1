import sqlite3
from datetime import datetime

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
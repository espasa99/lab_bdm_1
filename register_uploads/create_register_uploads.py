import sqlite3

# This script creates the database and the tables. It just has to be run once at the beginning.
conn = sqlite3.connect('./register_uploads/register_uploads.db')

c = conn.cursor()

c.execute('''CREATE TABLE uploads_temporal_landing
             (upload_date text, valid_date text, file_name text, file_format text, collection_name text)''')

c.execute('''CREATE TABLE uploads_persistent_landing
             (upload_date text, valid_date text, file_name text, file_format text, collection_name text)''')

conn.commit()

conn.close()
import sqlite3

conn = sqlite3.connect('./register_uploads/register_uploads.db')

c = conn.cursor()

c.execute('''CREATE TABLE log
             (upload_date text, valid_date text, file_name text, file_format text, collection_name text)''')

conn.commit()

conn.close()
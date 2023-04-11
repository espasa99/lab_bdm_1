import json
import os
from pymongo import MongoClient
# from os import listdir

directory = "idealista"


client = MongoClient()
db = client['PLZ']
collection = db[directory]
collection.remove({})
# print("ara hi ha ", collection.count_documents({}), "documents")

for f in os.listdir(directory):

    with open(os.path.join(directory, f), 'r') as file:
        data = json.load(file)
        for doc in data: # canviar el district i el neighbourhood pels reconciled i afegir els IDs correspoents


        # print(json.dumps(data[0], indent=2))
        2/0
        collection.insert_many(data)
    print("ara hi ha ", collection.count_documents({}), "documents")
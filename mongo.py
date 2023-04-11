import json
import os
from pymongo import MongoClient
# from os import listdir
# import pandas

directory = "idealista"
# directory = "opendatabcn-income"
#idealista: mirar duplicats, afegir la data, fer que propertycode sigui el id.

client = MongoClient()
db = client['PLZ']
collection = db[directory]
collection.delete_many({}) # per deletejar tots els docs de la collection
# collection.drop() # per dropejar la collection
print("ara hi ha ", collection.count_documents({}), "documents")

for f in os.listdir(directory):

    with open(os.path.join(directory, f), 'r') as file:
        # print(file)
        # data = pandas.read_csv(file, encoding='utf-8').to_dict("records")
        data = json.load(file)
        # for doc in data: # canviar el district i el neighbourhood pels reconciled i afegir els IDs correspoents
        if len(data) > 0:
            collection.insert_many(data)
        # print(json.dumps(data[2], indent=2))
        # 2/0

    print("ara hi ha ", collection.count_documents({}), "documents")
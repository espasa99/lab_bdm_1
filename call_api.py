import urllib.request
import json

url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=416c6da9-f546-48f4-858d-836c25fa2ed3&limit='
limit_rows = 5

with urllib.request.urlopen(url + str(limit_rows)) as url:
    s = url.read()

# Decode the bytes object into a string
data_str = s.decode('utf-8')

# Parse the string into a JSON object (dictionary)
data_json = json.loads(data_str)

print(data_json['result'])  

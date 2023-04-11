import urllib.request
import json
import pandas as pd

# Download the data from the API
url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=416c6da9-f546-48f4-858d-836c25fa2ed3&limit='
limit_rows = 2000

with urllib.request.urlopen(url + str(limit_rows)) as url:
    s = url.read()

# Decode the bytes object into a string
data_str = s.decode('utf-8')

# Parse the string into a JSON object (dictionary)
data_json = json.loads(data_str)

# Transform the JSON records into a Pandas DataFrame
records_data_frame = pd.DataFrame(data_json['result']['records'])
print(records_data_frame)

# Transform the JSON metadata into a Pandas DataFrame
metadata_data_frame = pd.DataFrame(data_json['result']['fields'])
print(metadata_data_frame)

# save csv files
records_data_frame.to_csv('./temporal_landing/compraventa_api/records_compraventa.csv', index=False)
metadata_data_frame.to_csv('./temporal_landing/compraventa_api/metadata_compraventa.csv', index=False)
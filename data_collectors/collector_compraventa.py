import urllib.request
import json
import pandas as pd
from collectors_funcs import register_upload

# Download the data from the API
limit_rows = 10000

with open('./data_collectors/url_api_compraventa.json', 'r') as archivo_json:
    url_for_year = json.load(archivo_json)

for year in url_for_year:

    url = url_for_year[year]
    with urllib.request.urlopen(url + str(limit_rows)) as url:
        s = url.read()

    data_str = s.decode('utf-8')

    data_json = json.loads(data_str)

    records_data_frame = pd.DataFrame(data_json['result']['records'])

    records_data_frame.to_csv('./temporal_landing/compraventa_api/' + year + '_records_compraventa.csv', index=False)

    file_name = year + '_records_compraventa.csv'
    date = file_name.split('_')[0]

    register_upload(date, file_name, 'csv', 'collector_compraventa')
    print(f"Los datos de {file_name} se han guardado correctamente.")

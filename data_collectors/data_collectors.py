from collectors_funcs import collect_data_from_data_source, collect_data_from_api

TEMPORAL_LANDING_PATH = "./temporal_landing/"
DATA_SOURCE_PATH = "./data_source/"

# Income data
update_type = 'complete'    # 'incremental' or 'complete'
files_to_upload = ['2017_Distribució_territorial_renda_familiar.json', '2016_Distribucio_territorial_renda_familiar.json']
collect_data_from_data_source(DATA_SOURCE_PATH + 'opendatabcn-income/', TEMPORAL_LANDING_PATH + 'opendatabcn-income/', 'csv', 'income', update_type, files_to_upload)

# lookup_tables data
update_type = 'complete'    # 'incremental' or 'complete'
files_to_upload = ['2017_Distribució_territorial_renda_familiar.json', '2016_Distribucio_territorial_renda_familiar.json']
collect_data_from_data_source(DATA_SOURCE_PATH + 'lookup_tables/', TEMPORAL_LANDING_PATH + 'lookup_tables/', 'csv', 'lookup_tables', update_type, files_to_upload)

# idealista data
update_type = 'complete'    # 'incremental' or 'complete'
files_to_upload = ['2017_Distribució_territorial_renda_familiar.json', '2016_Distribucio_territorial_renda_familiar.json']
collect_data_from_data_source(DATA_SOURCE_PATH + 'idealista/', TEMPORAL_LANDING_PATH + 'idealista/', 'json', 'idealista', update_type, files_to_upload)

# compraventa data
update_type = 'complete'    # 'incremental' or 'complete'
files_to_upload = ['2017_Distribució_territorial_renda_familiar.json', '2016_Distribucio_territorial_renda_familiar.json']
collect_data_from_api('./data_collectors/url_api_compraventa.json', 'csv', 'compraventa_api', './temporal_landing/compraventa_api/', '_records_compraventa.csv')
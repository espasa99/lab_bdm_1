from persistance_loader_funcs import connect_mongo, insert_data_to_mongo, insert_idealista_data_mongo

temporal_landing_path = "./temporal_landing/"

# Conectarse a MongoDB
database_name = "persistent_landing"
mongo_connection_string = "mongodb://localhost:27017/"
data_base_mongo = connect_mongo(mongo_connection_string, database_name)

# Income data
collection_name = "opendatabcn-income"
insert_type = ['all'] 
insert_data_to_mongo(temporal_landing_path, collection_name, data_base_mongo, insert_type)

# lookup_tables data
collection_name = "lookup_tables"
insert_type = ['all']
insert_data_to_mongo(temporal_landing_path, collection_name, data_base_mongo, insert_type)

# compraventa_api data
collection_name = "compraventa_api"
insert_type = ['all']
insert_data_to_mongo(temporal_landing_path, collection_name, data_base_mongo, insert_type)

# idealista data
collection_name = "idealista"
insert_type = ['all']
insert_idealista_data_mongo(temporal_landing_path, collection_name, data_base_mongo, insert_type)
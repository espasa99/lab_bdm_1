from persistance_loader_funcs import connect_mongo, insert_csv_data_to_mongo, insert_json_data_mongo

UPDATE_FREQUENCY = 7 # days
DATABASE_NAME = "persistent_landing"
temporal_landing_path = "./temporal_landing/"
mongo_connection_string = "mongodb://localhost:27017/"

data_base_mongo = connect_mongo(mongo_connection_string, DATABASE_NAME)

# Income data
collection_name = "opendatabcn-income"
update_type = 'complete'    # 'incremental' or 'complete' 
insert_csv_data_to_mongo(temporal_landing_path, collection_name, data_base_mongo, update_type, UPDATE_FREQUENCY)

# lookup_tables data
collection_name = "lookup_tables"
update_type = 'complete'
insert_csv_data_to_mongo(temporal_landing_path, collection_name, data_base_mongo, update_type, UPDATE_FREQUENCY)

# compraventa_api data
collection_name = "compraventa_api"
update_type = 'complete'
insert_csv_data_to_mongo(temporal_landing_path, collection_name, data_base_mongo, update_type, UPDATE_FREQUENCY)

# idealista data
collection_name = "idealista"
update_type = 'complete'
insert_json_data_mongo(temporal_landing_path, collection_name, data_base_mongo, update_type, UPDATE_FREQUENCY)
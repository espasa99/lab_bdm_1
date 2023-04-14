from persistance_loader_funcs import connect_hbase, insert_csv_data_to_hbase, insert_json_data_to_hbase

UPDATE_FREQUENCY = 7 # days
TABLE_NAME = "persistent_landing"
VM_IP_ADDRESS = "192.168.1.62"
temporal_landing_path = "./temporal_landing/"
mongo_connection_string = "mongodb://localhost:27017/"

hbase_table = connect_hbase(VM_IP_ADDRESS, TABLE_NAME)

# Income data
collection_name = "opendatabcn-income"
update_type = 'complete'    # 'incremental' or 'complete' 
insert_csv_data_to_hbase(temporal_landing_path, collection_name, hbase_table, update_type, UPDATE_FREQUENCY)

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
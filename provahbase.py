import happybase

# connection = happybase.Connection('hbase-docker', port=9090, autoconnect=True)
connection = happybase.Connection(host="192.168.1.63", autoconnect=True)


# FETCHING LIST OF ALL TABLES IN DB
def fetch_table():
    return connection.tables()


TABLE_NAME = "persistent_landing"
table = connection.table(TABLE_NAME)
i = 0
for k,v in table.scan():
    i = i+1
    print(k,v)
    if i == 10:
        break

a = fetch_table()
table = connection.table('idealista')
table.put(b'2020', {b'file:content': b'record1; record2; record3'})
for k, v in table.scan():
    print(k, v)

'''inicialitzar Hbase:
BDM_Software/hadoop/sbin/start-dfs.sh
BDM_Software/hbase/bin/start-hbase.sh

#per poder connectar amb el happybase hem de fer també:
BDM_Software/hbase/bin/hbase thrift start


per interactuar amb hbase
BDM_Software/hbase/bin/hbase shell
i aquí es pot fer create table o el que sigui:

create 'idealista', 'file', 'schema'

'''
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()

# CREATE TABLE station.stations (
#   city text,
#   station_id text,
#   bikes smallint,
#   capacity smallint,
#   updated_at timestamp,
#     PRIMARY KEY ((city), station_id, updated_at) );
# INSERT
# row = session.execute("INSERT INTO station.stations (city, station_id, bikes, capacity, updated_at) VALUES ('Paris', '123', 1, 2, toTimeStamp(now()));")
# get every rows 

# remove every rows
# session.execute("TRUNCATE station.stations;")
rows = session.execute("SELECT count(*) FROM station.stations;")
# # print every rows
for row in rows:
    print(row)

import json
import os
import statistics
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession

load_dotenv()

SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")
CASSANDRA_ADDRESS = os.getenv("CASSANDRA_ADDRESS")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT")

cluster = Cluster([CASSANDRA_ADDRESS], port=CASSANDRA_PORT)
session = cluster.connect()


if __name__ == "__main__":
    try:
        today = date.today()

        date_end = datetime(today.year, today.month, today.day, 0, 0, 0)
        date_start = date_end - timedelta(days=15)

        query = f'''SELECT bikes FROM station.stations 
            WHERE city = 'paris' 
            AND updated_at >= '{date_start.strftime("%Y-%m-%d %H:%M:%S")}' 
            AND updated_at < '{date_end.strftime("%Y-%m-%d %H:%M:%S")}'
            ALLOW FILTERING;'''

        result = session.execute(query).all()
        bikes_data = [row.bikes for row in result]
        average_bikes = statistics.mean(bikes_data)
        print(f"M1 : val avg: {average_bikes}")


        # requete distribuee spark cassandra
        spark = SparkSession.builder\
                .appName("CassandraQuery") \
                .config("spark.cassandra.connection.host", CASSANDRA_ADDRESS)\
                .config("spark.cassandra.connection.port", CASSANDRA_PORT)\
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        cassandra_df = spark.read.format("org.apache.spark.sql.cassandra")\
                        .options(table="stations", keyspace="station")\
                        .load()

        cassandra_df_filtered = cassandra_df.filter((cassandra_df["city"] == "paris") & (cassandra_df["updated_at"] >= date_start) & (cassandra_df["updated_at"] < date_end))

        average_bikes = cassandra_df_filtered.agg({"bikes": "avg"}).collect()[0][0]
        print(f"M2 : val avg: {average_bikes}")

        spark.stop()


    except KeyboardInterrupt:
        print("-quit")


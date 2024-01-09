from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit



CASSANDRA_ADDRESS = "cassandra-service.database.svc.cluster.local"
CASSANDRA_PORT = 9042



if __name__ == "__main__":
    today = date.today()
    date_start = today - timedelta(days=1)
    date_start = datetime(date_start.year, date_start.month, date_start.day, 0, 0, 0)
    date_end =  datetime(date_start.year, date_start.month, date_start.day, 23, 0, 0)

    # requete distribuee spark cassandra
    spark = SparkSession.builder\
            .appName("DailyBatch") \
            .config("spark.cassandra.connection.host", CASSANDRA_ADDRESS)\
            .config("spark.cassandra.connection.port", CASSANDRA_PORT)\
            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    cassandra_df = spark.read.format("org.apache.spark.sql.cassandra")\
                    .options(table="stations", keyspace="station")\
                    .load()

    cassandra_df_filtered = cassandra_df.filter( (cassandra_df["updated_at"] >= date_start) & (cassandra_df["updated_at"] < date_end))

    # add occupancy rate col
    cassandra_df_filtered = cassandra_df_filtered.withColumn("occupancy_rate", cassandra_df_filtered["bikes"] / cassandra_df_filtered["capacity"])

    # group by station_id and city, compute average of bikes and occupancy_rate
    cassandra_df_filtered = cassandra_df_filtered.groupBy("station_id", "city").avg("bikes", "occupancy_rate")

    # select station_id, avg(bikes)
    result_df = cassandra_df_filtered.select("station_id", "city","avg(bikes)", "avg(occupancy_rate)")

    result_df = result_df.withColumnRenamed("avg(bikes)", "avg_bikes")
    result_df = result_df.withColumnRenamed("avg(occupancy_rate)", "avg_occupancy_rate")

    # add third column of today timestamp for each row
    result_df = result_df.withColumn("updated_at", lit(date_end))

    # add result_df into cassandra
    result_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .options(table="daily", keyspace="station")\
            .mode("append")\
            .save()

    # display without logs
    result_df.show()
    spark.stop()
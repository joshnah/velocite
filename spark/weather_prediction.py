from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, explode, from_unixtime,window,avg,date_format, expr, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType
from datetime import datetime, date, timedelta

import sys
# Define the schema for the Kafka message
schema = StructType([
    StructField("stations", ArrayType(StructType([
        StructField("is_installed", IntegerType(), True),
        StructField("is_renting", IntegerType(), True),
        StructField("is_returning", IntegerType(), True),
        StructField("last_reported", StringType(), True),
        StructField("num_bikes_available", IntegerType(), True),
        StructField("num_docks_available", IntegerType(), True),
        StructField("station_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("capacity", IntegerType(), True)
    ])), True),
    StructField("city", StringType(), True)
])

def write_to_cassandra(df, epoch_id):
    print("\nWriting to Cassandra...\n")
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "station") \
        .option("table", "stations") \
        .mode("append") \
        .save()
def main():
    CASSANDRA_HOST = "cassandra-service.database.svc.cluster.local"
    CASSANDRA_PORT = "9042"

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Weather-Prediction-App") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    today = date.today()
    date_start = today - timedelta(days=7)
    date_start = datetime(date_start.year, date_start.month, date_start.day, 0, 0, 0)
    date_end =  datetime(date_start.year, date_start.month, date_start.day, 23, 0, 0)


    df_weather = spark.read.format("org.apache.spark.sql.cassandra").options(table="weather", keyspace="station").load()
    df_stations = spark.read.format("org.apache.spark.sql.cassandra").options(table="stations", keyspace="station").load()

    # depuis 7 jours
    df_stations = df_stations.filter( (df_stations["updated_at"] >= date_start) & (df_stations["updated_at"] < date_end))


    df_stations = df_stations.groupBy("city","station_id").agg(F.avg("bikes"))

    result_df = df_weather.join(df_stations, on="city", how="inner")


    # Simple prediction: if proba_rain > 50, then we predict half the number of bikes
    result_df = result_df.withColumn(
        "prediction",
        F.when(F.col("proba_rain") > 50, F.col("avg(bikes)") * 0.5).otherwise(F.col("avg(bikes)"))
    )

    result_df = result_df.drop("avg(bikes)")

    result_df.show()

    # update rows where station_id is the same, city is the same hour (int) is the same hour of updated_at
    result_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="prediction", keyspace="station") \
        .mode("overwrite").option("confirm.truncate","true") \
        .save()
    # Apply updates from the temporary table to the main table

    spark.stop()


if __name__ == "__main__":
    main()

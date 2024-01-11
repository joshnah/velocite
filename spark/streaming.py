from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, from_unixtime, window, avg, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType
import freeza

import sys
from pyspark.sql.functions import to_json
from pyspark.sql.functions import struct
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
    KAFKA_ADDRESS = sys.argv[1]
    RESULT_TOPIC = sys.argv[2]
    CASSANDRA_HOST = sys.argv[3]
    CASSANDRA_PORT = sys.argv[4]

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Streaming-App") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_ADDRESS)
        .option("subscribe", RESULT_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-streaming")
        .load()
    )
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(
        from_json("value", schema).alias("data")).select("data.*")

    exploded_df = parsed_df.select(
        "city", explode("stations").alias("station"))

    # Select relevant columns from the exploded DataFrame
    final_df = exploded_df.select(
        col("city"),
        col("station.station_id").alias("station_id"),
        col("station.num_bikes_available").alias("bikes"),
        col("station.capacity").alias("capacity"),
        from_unixtime("station.last_reported").alias("last_reported").cast(
            TimestampType()),  # Convert epoch timestamp to timestamp
        # current_timestamp().alias("current_timestamp")  # Add a timestamp column
    )

    window_spec = (
        final_df
        .groupBy("station_id", "city", window("current_timestamp", "1 hour").alias("updated_at"))
        .agg(avg("bikes").alias("bikes"), avg("capacity").alias("capacity"))
    ).withColumn(
        "updated_at",
        date_format("updated_at.start", "yyyy-MM-dd HH:mm:ss")
    )

    final_df.printSchema()

    # display real time data
    query = final_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Start the commiter thread
    tr = freeza.start_commiter_thread(
        query=query,
        bootstrap_servers=KAFKA_ADDRESS,
        group_id="spark-streaming"
    )
    tr.is_alive()

    # Write to cassandra each minute the average of bikes available
    cassandra_query = window_spec.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_cassandra) \
        .trigger(processingTime="1 minutes") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()

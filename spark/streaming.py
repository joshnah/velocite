from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# Define the schema for the Kafka message
schema = StructType([
    StructField("stations", ArrayType(StructType([
        StructField("is_installed", IntegerType(), True),
        StructField("is_renting", IntegerType(), True),
        StructField("is_returning", IntegerType(), True),
        StructField("last_reported", IntegerType(), True),
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
# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark-Cassandra-App") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Read data from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "api_result")
    .load()
)

# Parse the JSON data from the Kafka message
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# Expand the 'stations' JSON array into separate rows
stations_df = parsed_df.selectExpr("explode(stations) as station")

# Select relevant columns from the 'station' struct
final_df = stations_df.select(
    col("city"),
    col("station.station_id"),
    col("station.name"),
    col("station.lat"),
    col("station.lon"),
    col("station.num_bikes_available"),
    col("station.num_docks_available"),
    col("station.capacity")
)

# Print the DataFrame to the console
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start().awaitTermination()
# Write the data to Cassandra
# final_df.writeStream \
#     .outputMode("append") \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("keyspace", "station") \
#     .option("table", "stations") \
#     .option("checkpointLocation", "/tmp/checkpoint") \
#     .start() \
#     .awaitTermination()

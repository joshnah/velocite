from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp,explode
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
    .option("failOnDataLoss", "false") 
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")


exploded_df = parsed_df.select("city", explode("stations").alias("station"))

# Select relevant columns from the exploded DataFrame
final_df = exploded_df.select(
    col("city"),
    col("station.station_id").alias("station_id"),
    col("station.num_bikes_available").alias("bikes"),
    col("station.capacity").alias("capacity"),
    current_timestamp().alias("updated_at")  # Create 'updated_at' column with current timestamp
)
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

# final_df.writeStream \
#     .outputMode("append") \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("keyspace", "station") \
#     .option("table", "stations") \
#     .option("checkpointLocation", "/tmp/checkpoint") \
#     .start() \
#     .awaitTermination()

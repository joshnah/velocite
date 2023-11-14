"""
Demo Spark Structured Streaming + Apache Kafka + Cassandra
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from dotenv import load_dotenv
load_dotenv()
import os

SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")
RESULT_TOPIC = os.getenv("RESULT_TOPIC")

# def writeToCassandra(df, epochId):
#     df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .options(table="transactions", keyspace="demo") \
#         .mode("append") \
#         .save()

def main():
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", SERVER_ADDRESS) \
        .option("subscribe", RESULT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .start() \
        .awaitTermination()
    

    
    # print to console
    # .writeStream \



if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Spark-Cassandra-App") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    # Read data from Cassandra
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="stations", keyspace="station") \
        .load()

    # Show the data
    df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()

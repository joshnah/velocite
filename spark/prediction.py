from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, explode, from_unixtime,window,avg,date_format, expr, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType

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
    CASSANDRA_HOST = sys.argv[1]
    CASSANDRA_PORT = sys.argv[2]

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Spark-Cassandra-App") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df_stations = spark.read.format("org.apache.spark.sql.cassandra").options(table="stations", keyspace="station").load()
    # Trier le DataFrame par city, station_id et updated_at
    sorted_stations = df_stations.orderBy("city", "station_id", "updated_at")

    # Définir la fenêtre de partition
    window_spec = Window.partitionBy("city", "station_id").orderBy("updated_at")

    # Calculer la différence de vélos entre chaque heure
    diff_bikes = sorted_stations.withColumn("diff_bikes", F.col("bikes") - F.lag("bikes").over(window_spec))

    # Calculer la moyenne
    avg_diff_bikes = diff_bikes.groupBy("city", "station_id", F.hour("updated_at").alias("hour")) \
        .agg(F.avg(F.when(F.col("proba_rain").isNotNull(), F.col("diff_bikes")).otherwise(F.col("diff_bikes"))).alias("avg_diff_bikes"))
    avg_diff_bikes.show()
    # Joindre les données de probabilité future de pluie
    final_data = avg_diff_bikes.join(
    df_stations.select("city", "station_id", F.hour("updated_at").alias("hour"), "proba_rain", "bikes", "capacity"),
    on=["city", "station_id", "hour"],
    how="left_outer"
    )

    # Utiliser la colonne avg_diff_bikes pour le calcul de la prédiction
    final_data = final_data.withColumn("prediction",
        F.when(F.col("proba_rain").isNotNull(), F.col("avg_diff_bikes") * (100-F.col("proba_rain"))/100).otherwise(F.col("bikes"))
    )
    final_data = final_data.join(df_stations.select("city", "station_id", "updated_at",F.hour("updated_at").alias("hour"), "bikes", "capacity").filter(F.col("bikes").isNull()), on=["city", "station_id", "hour"], how="left_outer").filter(F.col("updated_at").isNotNull())
    final_data.show()
    # update rows where station_id is the same, city is the same hour (int) is the same hour of updated_at
    spark.cql("TRUNCATE station.predictions")
    final_data.select("city","station_id","updated_at","prediction").write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="predictions", keyspace="station") \
        .mode("append") \
        .save()
    # Apply updates from the temporary table to the main table


#     df_stations_null = df_stations.filter(df_stations.bikes.isNull())
#     # filter rows where bikes is null
#     df_stations = df_stations.filter(df_stations.bikes.isNotNull())

#     # Define a window specification based on the "updated_at" column
#     window_spec = Window.partitionBy("city", "station_id").orderBy("updated_at")

#     # On crée une colonne dans la table stations pour avoir la différence entre chaque heure
#     df_stations_diff = df_stations.withColumn("bikes_diff", F.col("bikes") - F.lag("bikes").over(window_spec))
#     # replace null value by 0
#     df_stations_diff = df_stations_diff.na.fill(0)
#     df_hour = df_stations_diff.withColumn("hour_of_forecast", F.hour("updated_at"))
#     # order by updated_at desc
#     df_hour = df_hour.orderBy("updated_at", ascending=False)
#     result_grouped = df_hour.groupBy("station_id", "city", "hour_of_forecast").agg(
#     F.first("bikes").alias("initial_bikes"),
#     (F.avg(F.expr("bikes_diff * 100 / (100 - proba_rain)"))).alias("weighted_avg_bikes_diff")
# )
#     # join result_grouped with df_stations to get the capacity
#     df_final = result_grouped.join(df_stations, ["station_id", "city"])
#     # show the result
#     df_final = df_final.withColumn("prediction", F.col("initial_bikes") + F.coalesce(F.col("weighted_avg_bikes_diff"), F.lit(0)))

#     window_forecast = Window.partitionBy("city", "station_id").orderBy("hour_of_forecast")

#     df_final_diff = df_final.withColumn(
#     "prediction",
#     F.when(F.col("weighted_avg_bikes_diff") + F.lag("prediction").over(window_forecast) < 0, 0)
#     .otherwise(F.col("weighted_avg_bikes_diff") + F.lag("prediction").over(window_forecast))
# ).withColumnRenamed("updated_at", "up_at").withColumnRenamed("forecast_date", "updated_at")
#     df_final_diff.select("station_id", "city", "updated_at", "prediction").write \
#     .format("org.apache.spark.sql.cassandra") \
#     .options(table="stations", keyspace="station") \
#     .mode("append") \
#     .save()


if __name__ == "__main__":
    main()
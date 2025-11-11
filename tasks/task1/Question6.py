import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, col, to_date,concat_ws, sum as _sum, concat_ws, sum, round, month, to_timestamp, count
from pyspark.sql.types import FloatType, IntegerType


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("task1")\
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Load the dataset
    taxi_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/yellow_tripdata/2023/"
    taxi_df = spark.read.csv(taxi_data_path, header=True, inferSchema=True)
    #'header=True' ensures the first row is used as column names 
    #'inferSchema=True' automatically detects column data types

    # Load the taxi_zone_lookup.csv dataset into a DataFrame
    taxi_zone_lookup_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    taxi_zone_df = spark.read.csv(taxi_zone_lookup_path, header=True, inferSchema=True)

    # Alias columns to avoid ambiguity
    taxi_zone_df_pu = taxi_zone_df.withColumnRenamed("LocationID", "PULocationID") \
                                  .withColumnRenamed("Borough", "Pickup_Borough") \
                                  .withColumnRenamed("Zone", "Pickup_Zone") \
                                  .withColumnRenamed("service_zone", "Pickup_service_zone")

    taxi_zone_df_do = taxi_zone_df.withColumnRenamed("LocationID", "DOLocationID") \
                                  .withColumnRenamed("Borough", "Dropoff_Borough") \
                                  .withColumnRenamed("Zone", "Dropoff_Zone") \
                                  .withColumnRenamed("service_zone", "Dropoff_service_zone")

    # Join with PULocationID and rename columns
    taxi_df = taxi_df.join(taxi_zone_df_pu, taxi_df.PULocationID == taxi_zone_df_pu.PULocationID, "left").drop("PULocationID")

    # Join with DOLocationID and rename columns
    taxi_df = taxi_df.join(taxi_zone_df_do, taxi_df.DOLocationID == taxi_zone_df_do.DOLocationID, "left").drop("DOLocationID")

    # Add 'route' and 'Month' columns
    taxi_df = taxi_df.withColumn("route", concat_ws(" to ", col("Pickup_Borough"), col("Dropoff_Borough"))) \
           .withColumn("Month", month(col("tpep_pickup_datetime")))

    # Filter valid rows
    taxi_df = taxi_df.filter((col("route").isNotNull()) & (col("Month").isNotNull()))

    # Group by 'Month' and 'route' to aggregate data and calculate aggregations
    taxi_grouped_df = taxi_df.groupBy("Month", "route") \
                   .agg(
                       _sum("tip_amount").alias("total_tip_amount"),
                       _sum("passenger_count").alias("total_passenger_count")
                   ) \
                   .withColumn(
                       "average_tip_per_passenger",
                       round(col("total_tip_amount") / col("total_passenger_count"), 2)
                   )

# Filter entries where average_tip_per_passenger is 0
taxi_zero_tip_df = taxi_grouped_df.filter(col("average_tip_per_passenger") == 0)

# Show the results without truncating fields (taking the first 10 rows)
taxi_zero_tip_df.select(
    "Month", "route", "total_tip_amount", "total_passenger_count", "average_tip_per_passenger"
).show(20, truncate=False)

# Stop Spark session
spark.stop()
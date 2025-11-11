import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count
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

    # Rename columns in the lookup DataFrame for joining with the taxi data on Pickup Location and Dropoff  Location
    zone_lookup_pickup = taxi_zone_df.withColumnRenamed("LocationID", "PULocationID").withColumnRenamed("Borough", "Pickup_Borough")
    zone_lookup_dropoff = taxi_zone_df.withColumnRenamed("LocationID", "DOLocationID").withColumnRenamed("Borough", "Dropoff_Borough")

    # Perform a left join with the zone lookup DataFrame to add Pickup_Borough information
    taxi_df = taxi_df.join(zone_lookup_pickup, on="PULocationID", how="left")

    # Perform another left join to add Dropoff_Borough information
    taxi_df = taxi_df.join(zone_lookup_dropoff, on="DOLocationID", how="left")

   # Add a new column 'route' by concatenating 'Pickup_Borough' and 'Dropoff_Borough' with " to "
    df_with_route = taxi_df.withColumn("route", concat_ws(" to ", col("Pickup_Borough"), col("Dropoff_Borough")))

    # Add a new column 'Month' by extracting the numeric month value from 'tpep_pickup_datetime'
    df_with_route_and_month = df_with_route.withColumn("Month", month(to_date(col("tpep_pickup_datetime"))))

    # Select and reorder the columns for the final DataFrame
    final_df = df_with_route_and_month.select(
        "tpep_pickup_datetime", "route", "Month", "Pickup_Borough", "Dropoff_Borough"
    )

   # Display the first 10 rows of the resulting DataFrame, ensuring no truncation of field values
    final_df.show(10, truncate=False)

    # Stop Spark session
    spark.stop()
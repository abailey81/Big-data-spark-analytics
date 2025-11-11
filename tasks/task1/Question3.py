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
    #This dataset maps LocationID to Borough, Zone, and service_zone

    # Join the taxi data with the lookup table using PULocationID and rename columns
    joined_df = taxi_df.join(
        taxi_zone_df,
        taxi_df.PULocationID == taxi_zone_df.LocationID,
        "left"# Use a left join to retain all records from the taxi data
    ).withColumnRenamed("Borough", "Pickup_Borough") \
     .withColumnRenamed("Zone", "Pickup_Zone") \
     .withColumnRenamed("service_zone", "Pickup_service_zone") \
     .drop("LocationID", "PULocationID")



    # Perform the second join  using DOLocationID and rename columns
    final_df = joined_df.join(
        taxi_zone_df,
        joined_df.DOLocationID == taxi_zone_df.LocationID,
        "left"
    ).withColumnRenamed("Borough", "Dropoff_Borough") \
     .withColumnRenamed("Zone", "Dropoff_Zone") \
     .withColumnRenamed("service_zone", "Dropoff_service_zone") \
     .drop("LocationID", "DOLocationID")#Drop the columns no longer needed after join 

    # Drop DOLocationID and LocationID (already done in the previous step)

    # Print schema of the resulting Dataframe
    final_df.printSchema()

    

    # Stop the Spark session
    spark.stop()
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

    # Question 2: Apply filters to extract relevant trips from the dataset
    #The conditions are:
    # - fare_amount > 50: Trips with fare greater than 50 
    # - trip_distance < 1:Cover a distance less than 1 mile
    # - date within 2023-02-01 and 2023-02-07
    filtered_trips_df = taxi_df.filter(
        (col("fare_amount") > 50) & 
        (col("trip_distance") < 1) & 
        (col("tpep_pickup_datetime").between("2023-02-01", "2023-02-07"))
    )

    # Transform 'tpep_pickup_datetime' to 'YYYY-MM-DD' format and count trips by date
    #Group the filtered trips by date and count the number of trips for each day
    date_results_df = filtered_trips_df \
        .withColumn("trip_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd")) \
        .groupBy("trip_date") \
        .agg(count("*").alias("trip_count")) \
        .orderBy("trip_date")

# Show the results in the console in the required format
# Apply truncate = False to ensure that the output is fully visible without truncating column values
    date_results_df.show(truncate=False)

    

    # Stop the Spark session
    spark.stop()

import sys
import string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count
from pyspark.sql.types import FloatType, IntegerType

if __name__ == "__main__":
# Starter Kit (Provided withing the instruction) Initialising a Spark session
    spark = SparkSession\
        .builder\
        .appName("task1")\
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
# From  Starter Kit,  setting up the Hadoop configurations
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")



#Task 1.1 Start 
#We firstly load the NYC Yellow Taxi  dataset for this task from the repository(S3 bucket).
# `spark.read.csv` is used to read the data from multiple CSV files located under the specified path mentioned in the code.
# `header=True` specifies that the first row of the CSV file contains column names.
# `inferSchema=True` Enables Spark to infer the data types for each column automatically .
    taxi_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/yellow_tripdata/2023/"
    taxi_df = spark.read.csv(taxi_data_path, header=True, inferSchema=True) #Fit into the dataframe

# Count the total number of entries(rows) in the NYC Yellow Taxi dataset 
#.count() is an action in Spark that triggers computation to return the total number of rows 
    entry_count = taxi_df.count()  # Calculate the total number of rows in the DataFrame 
    
#Print the total numbers of entries to the console
# We use here f-strings from Python to include the count in a specified message
    print(f"The total number of entries in the NYC Yellow Taxi dataset is: {entry_count}")

# This stops the Spark Session
#This also ensurres that all resources are released properly after the computation is complete
    spark.stop()

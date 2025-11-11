import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

from functools import reduce
from pyspark.sql.functions import col, lit, when, concat_ws
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import graphframes
from graphframes import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12")\
        .appName("graphframes")\
        .getOrCreate()

    sqlContext = SQLContext(spark)
    # shared read-only object bucket containing datasets
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

# Define the schema for the vertex DataFrame (vertices of the graph)
    vertexSchema = StructType([
        StructField("id", IntegerType(), False),  # "id" is the unique identifier for each vertex
        StructField("Borough", StringType(), True),  # Borough field, can be nullable
        StructField("Zone", StringType(), True),  # Zone field, can be nullable
        StructField("service_zone", StringType(), True)  # Service zone field, can be nullable
    ])

    # Define the schema for the edge DataFrame (edges of the graph)
    edgeSchema = StructType([
        StructField("src", IntegerType(), False),  # "src" represents the source vertex ID
        StructField("dst", IntegerType(), False)  # "dst" represents the destination vertex ID
    ])

    # Load the taxi_zone_lookup.csv dataset into a DataFrame using the vertex schema
    taxi_zone_lookup_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    vertices_df = spark.read.format("csv")\
        .options(header=True, inferSchema=True)\
        .schema(vertexSchema)\
        .load(taxi_zone_lookup_path)
    # `header=True` ensures the first row of the CSV is treated as column headers
    # `schema=vertexSchema` enforces the predefined structure of the DataFrame

    # Load the NYC Green Taxi data as the edges DataFrame using the edge schema
    green_taxi_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/green_tripdata/2023/*.csv"
    edges_df = spark.read.format("csv")\
        .options(header=True, inferSchema=True)\
        .load(green_taxi_data_path)\
        .select(col("PULocationID").alias("src"), col("DOLocationID").alias("dst"))
    # `header=True` here ensures the first row of the CSV is treated as column headers
    # `select` here extracts the columns "PULocationID" and "DOLocationID" and renames them as "src" and "dst"

    # Display the first 5 rows of the vertices DataFrame
    print("Sample Vertices DataFrame:")
    vertices_df.show(5, truncate=False)
    # `show(5, truncate=False)` displays the first 5 rows without truncating field names or contents

    # Display the first 5 rows of the edges DataFrame
    print("Sample Edges DataFrame:")
    edges_df.show(5, truncate=False)
    # `show(5, truncate=False)` displays the first 5 rows without truncating field names or contents

    # Stop the Spark session
    spark.stop()
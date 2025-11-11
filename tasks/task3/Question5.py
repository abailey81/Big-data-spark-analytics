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

 # Define the schema for the vertices dataframe
    vertexSchema = StructType([
        StructField("id", IntegerType(), False),  # Unique identifier for each vertex
        StructField("Borough", StringType(), True),  # Borough of the location
        StructField("Zone", StringType(), True),  # Zone of the location
        StructField("service_zone", StringType(), True)  # Service zone of the location
    ])

    # Define the schema for the edges dataframe
    edgeSchema = StructType([
        StructField("src", IntegerType(), False),  # Source location ID
        StructField("dst", IntegerType(), False)  # Destination location ID
    ])

    # Load the vertices dataset from the taxi zone lookup CSV file
    taxi_zone_lookup_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    vertices_df = spark.read.format("csv") \
        .options(header=True, inferSchema=True) \
        .schema(vertexSchema) \
        .load(taxi_zone_lookup_path)

    # Load the edges dataset from the NYC Green Taxi trip data
    green_taxi_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/green_tripdata/2023/*.csv"
    edges_df = spark.read.format("csv") \
        .options(header=True, inferSchema=True) \
        .load(green_taxi_data_path) \
        .select(col("PULocationID").alias("src"), col("DOLocationID").alias("dst"))

    # Create a GraphFrame using the vertices and edges DataFrames
    graph = GraphFrame(vertices_df, edges_df)

    # Use the shortestPaths function to calculate the shortest path from all vertices to the target vertex with LocationID=1
    # "1" represents the target location for which shortest paths are calculated
    shortest_paths = graph.shortestPaths(landmarks=["1"])

    # Extract relevant columns: the vertex ID and the shortest distance to LocationID=1
    paths_df = shortest_paths.select(
        col("id").alias("id_to_1"),  # ID of the source vertex
        col("distances.1").alias("shortest_distance")  # Distance to the target vertex "1"
    )

    # Format the "id_to_1" column to follow the "ID->1" format
    formatted_paths_df = paths_df.withColumn(
        "id_to_1", concat_ws("->", col("id_to_1"), lit("1"))
    )

    # Display the first 10 rows of the formatted DataFrame
    print("Shortest paths to LocationID=1:")
    formatted_paths_df.show(10, truncate=False)

    # Stop the Spark session 
    spark.stop()
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
  # Define schema for the vertices DataFrame
    # The vertices represent locations with associated metadata
    vertexSchema = StructType([
        StructField("id", IntegerType(), False),  # Unique ID for each location
        StructField("Borough", StringType(), True),  # Borough where the location is situated
        StructField("Zone", StringType(), True),  # Zone name
        StructField("service_zone", StringType(), True)  # Service zone
    ])

    # Define schema for the edges DataFrame
    # The edges represent trips between locations
    edgeSchema = StructType([
        StructField("src", IntegerType(), False),  # Source location ID
        StructField("dst", IntegerType(), False)  # Destination location ID
    ])

    # Load vertices data from the taxi zone lookup file
    # This file contains metadata for each location
    taxi_zone_lookup_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    vertices_df = spark.read.format("csv") \
        .options(header=True, inferSchema=True) \
        .schema(vertexSchema) \
        .load(taxi_zone_lookup_path)

    # Load edges data from the NYC Green Taxi trip data
    green_taxi_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/green_tripdata/2023/*.csv"
    edges_df = spark.read.format("csv") \
        .options(header=True, inferSchema=True) \
        .load(green_taxi_data_path) \
        .select(col("PULocationID").alias("src"), col("DOLocationID").alias("dst"))

    #Create a GraphFrame using the vertices and edges DataFrames
    # The GraphFrame combines vertices and edges into a graph structure
    graph = GraphFrame(vertices_df, edges_df)

    # Perform PageRank algorithm on the graph
    # Set resetProbability to 0.17 and tolerance (tol) to 0.01 as asked in the question
    pagerank_results = graph.pageRank(resetProbability=0.17, tol=0.01)

    # Extract and sort the PageRank results
    # Select the "id" and "pagerank" columns from the vertices DataFrame
    # Sort the results in descending order of PageRank values
    sorted_pagerank_df = pagerank_results.vertices \
        .select(col("id"), col("pagerank")) \
        .orderBy(col("pagerank").desc())

    # Display the top 5 rows of the sorted PageRank results
    # This shows the vertices with the highest PageRank scores
    print("Top 5 vertices by PageRank:")
    sorted_pagerank_df.show(5, truncate=False)

    # Stop the Spark 
    spark.stop()
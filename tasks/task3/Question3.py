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


 # Define the schema for the vertex DataFrame, which will represent the nodes of the graph.
    # The schema includes:
    # `id` is the unique identifier for each vertex.
    # `Borough`, `Zone`, `service_zone`these are the additional attributes describing the location.
    vertexSchema = StructType([
        StructField("id", IntegerType(), False),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])

    # Define the schema for the edge DataFrame, which will represent the relationships between nodes.
    # The schema includes:
    # `src` which is the source vertex ID.
    # `dst` which is the destination vertex ID.
    edgeSchema = StructType([
        StructField("src", IntegerType(), False),
        StructField("dst", IntegerType(), False)
    ])

    # Now we load the taxi_zone_lookup.csv dataset into the vertices DataFrame.
    # This dataset contains location information, which will be used as the nodes of the graph.
    # The `inferSchema=True` ensures data types are inferred.l
    taxi_zone_lookup_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    vertices_df = spark.read.format("csv") \
        .options(header=True, inferSchema=True) \
        .schema(vertexSchema) \
        .load(taxi_zone_lookup_path)

    # Load the NYC Green Taxi trip data into the edges DataFrame.
    # The trip data will be used to create edges in the graph by mapping pickup and dropoff locations.
    # The `select` function renames the `PULocationID` and `DOLocationID` columns to `src` and `dst`.
    green_taxi_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/green_tripdata/2023/*.csv"
    edges_df = spark.read.format("csv") \
        .options(header=True, inferSchema=True) \
        .load(green_taxi_data_path) \
        .select(col("PULocationID").alias("src"), col("DOLocationID").alias("dst"))

    # Create a graph using the vertices and edges DataFrames.
    # The GraphFrame combines the vertices locations and edges trip data to form a graph structure.
    graph = GraphFrame(vertices_df, edges_df)

    # Create a DataFrame of triplets to display the graph relationships.
    # The `triplets` method generates a DataFrame with `src`, `edge`, and `dst` columns.
    # Each triplet describes a relationship from a source node to a destination node via an edge.
    triplets_df = graph.triplets \
        .select(
            concat_ws(", ", col("src.id"), col("src.Borough"), col("src.Zone"), col("src.service_zone")).alias("src"),
            concat_ws(", ", col("edge.src"), col("edge.dst")).alias("edge"),
            concat_ws(", ", col("dst.id"), col("dst.Borough"), col("dst.Zone"), col("dst.service_zone")).alias("dst")
        )

    # Display the first 10 rows of the triplets DataFrame.
    # The `truncate=False` option ensures that all field values are not truncated.
    print("Graph triplets with src, edge, and dst:")
    triplets_df.show(10, truncate=False)

    # Stop the Spark session to release resources.
    spark.stop()
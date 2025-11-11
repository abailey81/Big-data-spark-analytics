import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode, split, window, col, count
from pyspark.sql.types import IntegerType, DateType, StringType, StructType
from pyspark.sql.functions import sum, avg, max, when
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("NasaLogSparkStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # We firstly need to  define the source of the stream
    # Read data from the socket stream specified by the host and port from question
    # The `includeTimestamp` option adds a timestamp to each record
    nasa_logs_stream = spark.readStream.format("socket") \
        .option("host", "stream-emulator.data-science-tools.svc.cluster.local") \
        .option("port", 5551) \
        .option("includeTimestamp", "true") \
        .load()

    # Process the incoming data
    # Split the streamed data by spaces and explode the resulting array to create individual log entries
    # Add the original timestamp column for each exploded log entry
    nasa_logs_df = nasa_logs_stream.select(
        explode(split(nasa_logs_stream.value, " ")).alias("log_entry"),  # Extract individual log entries
        nasa_logs_stream.timestamp  # Retain the timestamp for each log entry
    )
    # We need to filter for gif entries
    # Use the `filter` function to retain rows where the `log_entry` column contains the keyword "gif"
    gif_logs_df = nasa_logs_df.filter(col("log_entry").contains("gif"))

    # Define the windowing operation
    # Group the data by a time window (60 seconds) with a sliding duration of 30 seconds
    # Use the `count` function to count the number of gif-related logs in each window
    gif_windowed_count_df = gif_logs_df \
        .groupBy(window(gif_logs_df.timestamp, "60 seconds", "30 seconds")) \
        .agg(count("*").alias("gif_count"))  # Name the result column as "gif_count"

    # Here we write the results to the terminal
    # Use `outputMode("update")` to only display updated rows in each batch
    # Set `truncate` to false to ensure that the full data is displayed 
    streaming_query = gif_windowed_count_df.writeStream.outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    # Allow the query to run for 60 seconds
    # Use `time.sleep(60)` to let the streaming query process data for 60 seconds
    time.sleep(60)

    # Stop the query and Spark session
    streaming_query.stop()
    spark.stop()
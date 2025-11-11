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

    # Define a watermark on the timestamp column
    # This watermark ensures that any data arriving later than the threshold (3 seconds) will be ignored
    nasa_logs_df = nasa_logs_df.withWatermark("timestamp", "3 seconds")


    # Here we W\write the streaming DataFrame to the terminal
    # Use `append` mode to display only new data added to the stream
    # `numRows` is set to display up to 100,000 rows, and truncation is false to show full data
    streaming_query = nasa_logs_df.writeStream.outputMode("append") \
        .option("numRows", "100000") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    # Here we allow the query to run for 60 seconds
    # Use `time.sleep` to keep the streaming query running and processing data for a specified in brackets duration
    time.sleep(60)  # Run for 60 seconds

    # Stop the streaming query
    streaming_query.stop()

    # Stop the Spark
    spark.stop()
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

 # Extract columns from the log entries to make the data more structured and meaningful
    processed_logs_df = nasa_logs_df.withColumn('idx', split(nasa_logs_df['log_entry'], ',').getItem(0)) \
        .withColumn('hostname', split(nasa_logs_df['log_entry'], ',').getItem(1)) \
        .withColumn('time', split(nasa_logs_df['log_entry'], ',').getItem(2)) \
        .withColumn('method', split(nasa_logs_df['log_entry'], ',').getItem(3)) \
        .withColumn('url', split(nasa_logs_df['log_entry'], ',').getItem(4)) \
        .withColumn('responsecode', split(nasa_logs_df['log_entry'], ',').getItem(5)) \
        .withColumn('bytes', split(nasa_logs_df['log_entry'], ',').getItem(6))

    # Define the streaming query
    # Write the processed DataFrame to the console
    # Use append mode to display only new data added to the stream
    # Set `numRows` to display up to 100,000 rows and disable truncation to show full data
    streaming_query = processed_logs_df.writeStream.outputMode("append") \
        .option("numRows", "100000") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    # Allow the query to run for 60 seconds
    # Use `time.sleep` to let the streaming query process data for 60 seconds
    time.sleep(60)

    # Stop the streaming query and Spark 
    streaming_query.stop()
    spark.stop()
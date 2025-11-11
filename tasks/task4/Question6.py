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
     # Extract columns from the log entries to make the data more structured
    processed_logs_df = nasa_logs_df.withColumn('idx', split(nasa_logs_df['log_entry'], ',').getItem(0)) \
        .withColumn('hostname', split(nasa_logs_df['log_entry'], ',').getItem(1)) \
        .withColumn('time', split(nasa_logs_df['log_entry'], ',').getItem(2)) \
        .withColumn('method', split(nasa_logs_df['log_entry'], ',').getItem(3)) \
        .withColumn('url', split(nasa_logs_df['log_entry'], ',').getItem(4)) \
        .withColumn('responsecode', split(nasa_logs_df['log_entry'], ',').getItem(5)) \
        .withColumn('bytes', split(nasa_logs_df['log_entry'], ',').getItem(6).cast(IntegerType()))
    # We now filter GET requests with a response code of 200 as asked in question
    # Applying filters to keep only rows where the method is "GET" and the response code is "200"
    successful_get_requests_df = processed_logs_df.filter(
        (col("method") == "GET") & (col("responsecode") == "200")
    )

    # Here we are grouping by hostname and counting successful GET requests
    # Grouping the filtered rows by the `hostname` column and counting the number of rows for each hostname
    get_request_count_df = successful_get_requests_df.groupBy(
        "hostname"  # Group by the hostname column
    ).agg(count("*").alias("correct_count"))  # Count all successful GET requests for each host
    
    # Here we are writing the aggregated results to the console
    # Outputting the results in complete mode, ensuring that all rows are displayed in each batch
    # Using `trigger(processingTime="10 seconds")` to execute the query every 10 seconds
    # Setting `truncate=false` to display the full contents of the DataFrame in the console
    streaming_query = get_request_count_df.writeStream.outputMode("complete") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .format("console") \
        .start()
        
    # Allow the query to run for 120 seconds
    # Use `time.sleep(120)` to let the streaming query process data for 120 seconds
    time.sleep(120)

    # Stop the query and Spark session
    streaming_query.stop()
    spark.stop()

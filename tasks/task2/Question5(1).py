import sys, string
import os
import math
import socket
from pyspark.sql import SparkSession
from datetime import datetime

from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, sum as _sum, \
    year, countDistinct, expr, round, unix_timestamp, udf
from pyspark.sql.types import FloatType, IntegerType, DoubleType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("task2") \
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")



 # Load the blocks.csv dataset into a DataFrame
    blocks_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum/blocks.csv"
    blocks_df = spark.read.csv(blocks_data_path, header=True, inferSchema=True)
    # 'header=True' ensures the first row of the CSV is used as column headers.
    # 'inferSchema=True' automatically infers the data types for each column in the dataset.
# Load the transactions.csv dataset into a DataFrame
    transactions_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum/transactions.csv"
    transactions_df = spark.read.csv(transactions_data_path, header=True, inferSchema=True)
    # Similarly, header=True and inferSchema=True are used to structure the data.

    # This performs an inner join to merge the two datasets on the specified fields
    merged_df = transactions_df.join(blocks_df, transactions_df.block_hash == blocks_df.hash, "inner")
    #  Here the transactions_df.block_hash is the field from the transactions.csv dataset.
    # Also here blocks_df.hash is the field from the blocks.csv dataset.
    # "inner" specifies the type of join. Only matching rows from both DataFrames are included.

    #Count the number of rows in the merged DataFrame
    merged_row_count = merged_df.count()
    # The count() function triggers a computation to count the total number of rows in the merged DataFrame.

    # Print the number of rows in the merged DataFrame
    print(f"The number of lines in the merged dataset is: {merged_row_count}")
    # I have used f-string to include the result in a readable format so it would be easier to find it.
   #For the merged file include the timestamp dates 
    merged_date = merged_df.withColumn("formatted_date", date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd"))

    # Filter the DataFrame to include only rows from September 2015
    september_2015_df = merged_date.filter(
        (col("formatted_date") >= "2015-09-01") & (col("formatted_date") <= "2015-09-30")
    )
    # The filter condition ensures only rows where 'formatted_date' is within the specified range are included

    # Here we group the data by formatted_date and calculate required metrics
    results_2015_df = september_2015_df.groupBy("formatted_date").agg(
        count("block_hash").alias("block_count"),  # Count the number of blocks produced on each date
        countDistinct("from_address").alias("unique_senders_count_number")  # Count the unique senders (from_address)
    ).orderBy("formatted_date")  # Sort the results by date

    # Now we display the results in the console without truncating fields
    results_2015_df.show(20, truncate=False)  # Show the top 20 rows; truncate=False ensures all columns are fully displayed

    # Generate a timestamp for the output file (from starter kit provided)
    now = datetime.now()  # Get the current timestamp(from starter kit)
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")  # Format the timestamp as 'dd-MM-yyyy_HH:mm:ss'(from starter kit)

    # Save the results to a CSV file in the  bucket
    results_2015_df.repartition(1).write.csv(
        "s3a://" + s3_bucket + "/september_2015_document.csv", header=True
    )
    # 'repartition(1)' ensures that the output is written to a single CSV file
    # 'header=True' includes the column names in the output file

    # Stop the Spark session
    spark.stop()  
   
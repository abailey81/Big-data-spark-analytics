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

    # Filter the data for October 2015 only
    october_2015_df = merged_date.filter(
        (col("formatted_date") >= "2015-10-01") &
        (col("formatted_date") <= "2015-10-31")
    )
    # This keeps rows where 'formatted_date' which falls between October 1, 2015, and October 31, 2015

    # Implement 'gas' and 'gas_price' columns to DoubleType for precise mathematical operations
    results_gas = october_2015_df.withColumn("gas", col("gas").cast(DoubleType())) \
                                 .withColumn("gas_price", col("gas_price").cast(DoubleType()))
    # Ensures the 'gas' and 'gas_price' fields are stored as DoubleType for accurate calculations

    # Filter for rows where transaction_index = 0, and ensure 'gas' and 'gas_price' are non-null and non-negative
    transfilter_df = results_gas.filter(
        (col("transaction_index") == 0) &
        (col("gas").isNotNull()) & (col("gas") >= 0) &
        (col("gas_price").isNotNull()) & (col("gas_price") >= 0)
    )
    # By doing this we remove invalid rows by applying conditions: valid 'transaction_index', non-null and positive 'gas' and 'gas_price'

    # Here we group by formatted_date and calculate total transaction fee and block count
    tottransfee_df = transfilter_df.groupBy("formatted_date").agg(
        _sum(col("gas") * col("gas_price")).alias("total_transaction_fee"),  # Calculate total transaction fee
        count("block_hash").alias("block_count")  # Count the number of blocks
    ).orderBy("formatted_date")
    # Aggregates data by date, calculates total_transaction_fee, and counts blocks per day, then sorts the results

    # Round the total_transaction_fee to 10 decimal places 
    tottransfee_df = tottransfee_df.withColumn(
        "total_transaction_fee", round(col("total_transaction_fee"), 10)
    )
    # Rounds off the calculated fee 

    # Display the results in the console 
    tottransfee_df.show(20, truncate=False)
    # truncate set to false ensures that all columns are fully displayed and not truncated

    # Generate a timestamp for the output file
    now = datetime.now()  # Get the current timestamp
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")  # Format the timestamp as 'dd-MM-yyyy_HH:mm:ss'

    # Save the results to a CSV file in the bucket
    tottransfee_df.repartition(1).write.csv(
        "s3a://" + s3_bucket + "/october_2015_document.csv", header=True
    )
    # Repartition to ensure the output is a single CSV file, and include column headers

    # Stop the Spark
    spark.stop()
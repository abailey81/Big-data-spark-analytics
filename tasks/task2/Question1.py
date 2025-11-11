import sys, string
import os
import math
import socket
from pyspark.sql import SparkSession
from datetime import datetime

from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, \
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

    # Display the schema of the blocks dataset to verify the structure and column data types
    print("Blocks CSV Schema:")
    blocks_df.printSchema()
    # printSchema() outputs the schema of the blocks DataFrame, including column names, data types, and nullability.

    # Load the transactions.csv dataset into a DataFrame
    transactions_data_path = "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum/transactions.csv"
    transactions_df = spark.read.csv(transactions_data_path, header=True, inferSchema=True)
    # Similar to blocks_df, the transactions.csv dataset is loaded with headers and inferred column data types.

    # Now we display the schema of the transactions dataset to verify the structure and column data types
    print("\nTransactions CSV Schema:")    
    transactions_df.printSchema()
    # Here, printSchema() outputs the schema of the transactions DataFrame, including column names, data types, and nullability.



    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
   # rideshare_tripZone_df.coalesce(1).write.csv("s3a://" + s3_bucket + "/merged_data_" + date_time + ".csv", header=True)
   # rideshare_tripZone_df.write.csv("s3a://" + s3_bucket + "/processed_data_" + date_time, header=True)

    spark.stop()
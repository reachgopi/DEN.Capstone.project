import configparser
from datetime import datetime
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime as dt

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def process_airport_data(spark, input_data, output_data):
    print("processing airport data -->>>>")
    # get filepath for the airport data file
    input_file_path = input_data + "airport-codes_csv_less.csv"

    # read airport data file
    df_airport_data = spark.read.csv(input_file_path, inferSchema=True, header=True)
    
    # create a airport_data view from the spark dataframe
    df_airport_data.createOrReplaceTempView("airport_data")

    # extract columns for country dimensions table    
    airport_dim = spark.sql ("""select distinct trim(local_code) as airport_code,
                        trim(local_code) as partition_airport_code,
                        trim(iata_code) as iata_code,
                        trim(name) as airport_name,
                        trim(type) as airport_type,
                        trim(iso_region) as region,
                        trim(iso_country) as country_code,
                        trim(municipality) as municipality,
                        trim(gps_code) as gps_code,
                        CAST(coordinates as String) as coordinates
                        from 
                        airport_data 
                        where
                        local_code is not null
                        """)
    
    # write users table to parquet files
    output_file_path = output_data + "airport_dim.parquet"
    
    airport_dim.write \
        .mode("append") \
        .partitionBy("partition_airport_code") \
        .parquet(output_file_path)

def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    print("printing keys --->>>>")
    print(config['AWS']['AWS_ACCESS_KEY_ID'])
    print(config['AWS']['AWS_SECRET_ACCESS_KEY'])
    os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    spark = create_spark_session()
    input_data = "s3a://data-engineering-nd/capstone-project/"
    output_data = "s3a://data-engineering-nd/capstone-project/output/"

    #local file path
    #input_data = ""
    #output_data = ""
    
    process_airport_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()

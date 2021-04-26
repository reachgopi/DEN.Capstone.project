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

def process_temperature_data(spark, input_data, output_data):
    print("processing temperature data -->>>>")
    # get filepath for the temperature data file
    temp_data = input_data + "GlobalLandTemperaturesByCity_Kaggle.csv"

    # read temperature data file
    df_temp_data = spark.read.csv(temp_data, inferSchema=True, header=True)
        
    # create a temperature_data view from the spark dataframe
    df_temp_data.createOrReplaceTempView("temperature_data")

    #Retrieving country data to join with immigration data
    country_data = input_data + "country_code.csv"

    df_country = spark.read.csv(country_data, inferSchema=True, header=True)

    df_country.createOrReplaceTempView("country_table")

    # extract columns for country dimensions table    
    country_dim = spark.sql ("""select c.country_code as country_code,
                    t.Country as country,
                    t.avg_country_temperature as avg_country_temperature,
                    t.avg_country_temperature_uncertainty as avg_country_temperature_uncertainty
                    from 
                    country_table c
                    left join
                    (select 
                        avg(AverageTemperature) as avg_country_temperature,
                        avg(AverageTemperatureUncertainty) as avg_country_temperature_uncertainty,
                        Country 
                        from 
                        temperature_data 
                        group by Country ) t  
                        on t.Country = c.country
                    where t.country is not null
                    """)
        
    # write users table to parquet files
    country_dim_out = output_data + "country_dim.parquet"
        
    country_dim.write \
        .mode("append") \
        .parquet(country_dim_out)

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

    process_temperature_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()

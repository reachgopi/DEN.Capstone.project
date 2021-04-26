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

def process_demographics_data(spark, input_data, output_data):
    print("processing demographics data -->>>>")
    # get filepath for the demographics data file
    input_file_path = input_data + "us-cities-demographics.csv"

    demographics_schema = StructType([
        StructField("city",StringType()),
        StructField("state",StringType()),
        StructField("medianAge",DoubleType()),
        StructField("malePopulation",IntegerType()),
        StructField("femalePopulation",IntegerType()),
        StructField("totalPopulation",IntegerType()),
        StructField("noOfVeterans",IntegerType()),
        StructField("foreignBorn",IntegerType()),
        StructField("averageHouseHoldSize",DoubleType()),
        StructField("stateCode",StringType()),
        StructField("race",StringType()),
        StructField("count",IntegerType())
    ])

    # read demographics data file
    df_demographics_data = spark.read.csv(input_file_path, sep=';', schema = demographics_schema, header=True)
        
    # Removing race and count column from data frame and then distinct is called to remove duplicates
    df_demographics_drop = df_demographics_data.drop("race","count")
    df_demographics_distinct = df_demographics_drop.distinct()
    
    # create a demography_data view from the spark dataframe
    df_demographics_distinct.createOrReplaceTempView("demography_data")

    # extract columns for demographics dimensions table    
    demographics_dim = spark.sql("""select 
                            distinct stateCode,
                            avg(medianAge) as average_median_age,
                            avg(malePopulation) as average_male_population,
                            avg(femalePopulation) as average_female_population,
                            avg(totalPopulation) as average_total_population,
                            avg(noOfVeterans) as average_no_of_veterans,
                            avg(foreignBorn) as average_foreign_born,
                            avg(averageHouseHoldSize) as average_house_hold_size
                            from demography_data
                            group by stateCode""")
        
    # write users table to parquet files
    output_file_path = output_data + "demographics_dim.parquet"
        
    demographics_dim.write \
        .mode("append") \
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
 
    process_demographics_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()

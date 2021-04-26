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


def process_immigration_data(spark, input_data, output_data):
    print("processing immigration data -->>>>")
    # get filepath to immigration data file
    immig_data = input_data + "sas_data/*.parquet"

    immig_fact_schema = StructType([
        StructField("cicid",IntegerType()),
        StructField("i94yr",ShortType()),
        StructField("i94mon",ShortType()),
        StructField("i94cit",ShortType()),
        StructField("i94res",ShortType()),
        StructField("i94port",StringType()),
        StructField("arrdate",FloatType()),
        StructField("i94mode",ShortType()),
        StructField("i94addr",StringType()),
        StructField("depdate",FloatType()),
        StructField("i94bir",ShortType()),
        StructField("i94visa",ShortType()),
        StructField("count",ShortType()),
        StructField("dtadfile",LongType()),
        StructField("visapost",StringType()),
        StructField("occup",StringType()),
        StructField("entdepa",StringType()),
        StructField("entdepd",StringType()),
        StructField("entdepu",StringType()),
        StructField("matflag",StringType()),
        StructField("biryear",ShortType()),
        StructField("dtaddto",LongType()),
        StructField("gender",StringType()),
        StructField("insnum",IntegerType()),
        StructField("airline",StringType()),
        StructField("admnum",FloatType()),
        StructField("fltno",IntegerType()),
        StructField("visatype",StringType()),
    ])
    
    # read immigration data in parquet format
    print(immig_data)
    #df_immig =spark.read.parquet(immig_data, schema = immig_fact_schema)

    #Schema not working in AWS EMR environment
    df_immig =spark.read.parquet(immig_data)

    df_immig.createOrReplaceTempView("immigration_data")

    # extract columns to create immigration fact table
    immig_fact = spark.sql (""" select 
                    CAST(cicid as INT) as cicid,
                    CAST(i94mon as INT) as i94_month,
                    trim(CAST(i94port as String)) as partition_port,
                    trim(CAST(i94port as String)) as i94_port,
                    CAST(i94cit as INT) as i94_citizen,
                    CAST(i94res as INT) as i94_resident, 
                    CAST(arrdate as INT) as i94_arrival_date,
                    CAST(i94mode as INT) as travel_mode_id,
                    CAST(i94addr as String) as state_code,
                    CAST(depdate as INT) as i94_departure_date,
                    CAST(i94bir as INT) as i94_age,
                    CAST(i94visa as INT) as visa_category_id,
                    to_date(dtadfile,'yyyyMMdd') as i94_date_added,
                    to_date(dtaddto, 'yyyyMMdd') as i94_date_admitted,
                    occup as i94_occupation,
                    entdepa as i94_arrival_flag,
                    entdepd as i94_departure_flag,
                    entdepu as i94_update_flag,
                    matflag as i94_match_flag,
                    CAST(biryear as INT) as i94_birth_year,
                    gender as i94_gender,
                    insnum as i94_insnum,
                    airline as i94_airline,
                    CAST(admnum as Double) as i94_admission_number,
                    CAST(fltno as String) as i94_flight_number,
                    CAST(visatype as String) as i94_visa_type
                    from 
                    immigration_data""")

    immig_fact_updated = immig_fact.na.fill(0, subset=["travel_mode_id"])
    
    # write songs table to parquet files partitioned by year and artist
    immig_fact_out = output_data + "immig_fact.parquet"
    immig_fact_updated.write \
        .mode("append") \
        .partitionBy("partition_port", "i94_month") \
        .parquet(immig_fact_out)

    #Visa Type dimension table
    print("writing visa_type_dim parquet files -->>>>")
    visa_type_dim = spark.sql (""" select 
                        distinct CAST(i94visa as INT) as visa_category_id,
                        CASE(CAST(i94visa as INT)) WHEN 1 then 'Business' WHEN 2 THEN 'Pleasure' ELSE 'Student' END as visa_type 
                        from 
                        immigration_data""")
    
    visa_type_dim_out = output_data + "visa_type_dim.parquet"

    visa_type_dim.write \
        .mode("append") \
        .parquet(visa_type_dim_out)

    #Travel mode dimension table
    print("writing travel_mode_dim parquet files -->>>>")
    travel_mode_dim = spark.sql (""" select 
                            distinct CAST(i94mode as INT) as travel_mode_id,
                            CASE(CAST(i94mode as INT)) WHEN 1 then 'Air' WHEN 3 THEN 'Land' WHEN 2 THEN 'Sea' WHEN 9 THEN 'Not reported' ELSE 'Undefined' END as travel_mode 
                            from 
                            immigration_data""")
    
    travel_mode_dim_updated = travel_mode_dim.na.fill(0, subset=["travel_mode_id"])

    travel_mode_dim_out = output_data + "travel_mode_dim.parquet"

    travel_mode_dim_updated.write \
        .mode("append") \
        .parquet(travel_mode_dim_out)

    
    #Time dimension table
    time_dim = spark.sql (""" select 
                            distinct CAST(arrdate as INT) as arrival_sas,
                            date_add(to_date('1960-01-01', "yyyy-MM-dd"), CAST(arrdate as INT)) as arrival_date,
                            dayofmonth(date_add(to_date('1960-01-01', "yyyy-MM-dd"), CAST(arrdate as INT))) as day,
                            month(date_add(to_date('1960-01-01', "yyyy-MM-dd"), CAST(arrdate as INT))) as month,
                            month(date_add(to_date('1960-01-01', "yyyy-MM-dd"), CAST(arrdate as INT))) as parition_month,
                            year(date_add(to_date('1960-01-01', "yyyy-MM-dd"), CAST(arrdate as INT))) as year,
                            date_format(date_add(to_date('1960-01-01', "yyyy-MM-dd"), CAST(arrdate as INT)),"E") as weekday
                            from 
                            immigration_data""")
    
    time_dim_out = output_data + "time_dim.parquet"

    time_dim.write \
        .mode("append") \
        .partitionBy("parition_month") \
        .parquet(time_dim_out)

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
    
    process_immigration_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()

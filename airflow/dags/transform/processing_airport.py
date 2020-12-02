import numpy as np
import pandas as pd
import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from us_states_abbrev import us_states_abbrev

S3_HEADER = 's3a://'
S3_BUCKET = 'data-eng-jun'
AIRPORT_FILE_PATH = '/raw_data/airport-codes_csv.csv'


config = configparser.ConfigParser()
config.read('/home/workspace/airflow/dags/dl.cfg')

AWS_ACCESS_KEY_ID=config['aws']['AWS_ACCESS_KEY_ID']
print("The ACCESS KEY is ", AWS_ACCESS_KEY_ID)
AWS_SECRET_ACCESS_KEY=config['aws']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
  
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    return spark

spark = create_spark_session()
df_air = spark.read.format('csv').load(S3_HEADER+S3_BUCKET+AIRPORT_FILE_PATH, header=True, inferSchema=True)

df_air.createOrReplaceTempView("airport_table")


# aggregate airport data by counting the number of differnt types of airports in each city
df_air_transformed = spark.sql("""
SELECT iso_country, RIGHT(iso_region, 2) as state, municipality as city, 
        SUM(CASE WHEN type='small_airport' THEN 1 ELSE 0 END) AS small_airport,
        SUM(CASE WHEN type='medium_airport' THEN 1 ELSE 0 END) AS medium_airport,
        SUM(CASE WHEN type='large_airport' THEN 1 ELSE 0 END) AS large_airport
FROM airport_table
WHERE iso_country = 'US' AND iata_code IS NOT NULL
GROUP BY iso_country, iso_region, municipality
""")



df_air_transformed.write.parquet(S3_HEADER+S3_BUCKET+'/transformed_data/us_city_airport_summary')
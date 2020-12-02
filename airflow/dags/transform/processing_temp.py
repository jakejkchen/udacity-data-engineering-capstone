import numpy as np
import pandas as pd
import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

S3_HEADER = 's3a://'
S3_BUCKET = 'data-eng-jun'
TEMP_FILE_PATH = '/raw_data/GlobalLandTemperaturesByCity.csv'


config = configparser.ConfigParser()
config.read('/home/workspace/airflow/dags/dl.cfg')

AWS_ACCESS_KEY_ID=config['aws']['AWS_ACCESS_KEY_ID']
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

print('Reading in file from path', S3_HEADER+S3_BUCKET+TEMP_FILE_PATH)

df_temp = spark.read.format('csv').load(S3_HEADER+S3_BUCKET+TEMP_FILE_PATH, header=True, inferSchema=True)

df_temp_transformed = df_temp.filter("Country = 'United States'").\
    selectExpr('dt as date', 'AverageTemperature as avg_temp', 
                'AverageTemperatureUncertainty as temp_std', 'city as city',
                'Country as country')


df_temp_transformed.write.parquet(S3_HEADER+S3_BUCKET+'/transformed_data/us_city_temp')
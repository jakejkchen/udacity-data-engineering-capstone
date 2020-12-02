import numpy as np
import pandas as pd
import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

S3_HEADER = 's3a://'
S3_BUCKET = 'data-eng-jun'
IMMIGRATION_FILE_PATH = '/raw_data/I94_DATA/i94_apr16_sub.sas7bdat'


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

print('Reading in file from path', S3_HEADER+S3_BUCKET+IMMIGRATION_FILE_PATH)

df_immigration = spark.read.format("com.github.saurfang.sas.spark")\
    .load(S3_HEADER+S3_BUCKET+IMMIGRATION_FILE_PATH)

df_immigration_transformed = df_immigration.selectExpr('cast(cicid as int)', 'cast(i94yr as int) as entry_year',
             'cast(i94mon as int) as entry_month', 'cast(i94res as int) as origin_country_code', 
             'i94port as destination_port_code', 'cast(i94mode as int)', 'i94bir as age', 'gender', 'visatype')


df_immigration_transformed.write.partitionBy('entry_year', 'entry_month').parquet(S3_HEADER+S3_BUCKET+'/transformed_data/immigration')
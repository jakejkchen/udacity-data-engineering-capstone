import numpy as np
import pandas as pd
import configparser
import os
from pyspark.sql import SparkSession

S3_HEADER = 's3a://'
S3_BUCKET = 'data-eng-jun'
S3_DATA_FOLDER = '/transformed_data'

table_ref = {'airport': '/us_city_airport_summary/',
            'demographics': '/us_city_demographics/',
            'immigration': '/immigration/',
            'temperature': '/us_city_temp/'}

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

def data_quality_check():
    for table in table_ref.keys():
        file_path = S3_HEADER + S3_BUCKET + S3_DATA_FOLDER + table_ref.get(table)
        df = spark.read.parquet(file_path)
        if len(df.columns) > 0 and df.count() > 0:
            print(f"{table} data quality check succeeded")
        else:
            raise ValueError(f"{table} data quality check failed")
            
data_quality_check()
        

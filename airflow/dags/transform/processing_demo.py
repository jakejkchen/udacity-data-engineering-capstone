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
DEMO_FILE_PATH = '/raw_data/us-cities-demographics.csv'

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

print('Reading in file from path', S3_HEADER+S3_BUCKET+DEMO_FILE_PATH)

df_demo = spark.read.format('csv').load(S3_HEADER+S3_BUCKET+DEMO_FILE_PATH, header=True, inferSchema=True, sep=';')

df_demo.createOrReplaceTempView("demo_table")

df_demo_transformed = spark.sql("""
    SELECT DISTINCT City AS city, State As state, `Median Age` AS median_age, `Male Population`/`Total Population` AS male_pct,
        `Female Population`/`Total Population` AS female_pct, `Number of Veterans`/`Total Population` AS veteran_pct,
        `Foreign-born`/`Total Population` AS foreigner_pct
    FROM demo_table
""")

abbreviate_state = udf(lambda x: us_states_abbrev[x], StringType())

# convert full state name into abbreviation
df_demo_transformed = df_demo_transformed.withColumn('state', abbreviate_state('state'))

df_demo_transformed.write.parquet(S3_HEADER+S3_BUCKET+'/transformed_data/us_city_demographics')
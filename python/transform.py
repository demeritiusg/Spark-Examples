from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


#to be ran in airflow. all params to be pass in via airflow

def spark_etl_function():
    pys = SparkSession.builder.appName('ETL') \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()

    df = pys.read.csv('/data/*.csv', sep=',', inferSchema=True, header=True)

    #pandas_df = df.toPandas()

    df = df.drop_duplicates()
    df = df.withColumnRenamed('COL1', 'col1')

    drop_cols = df.columns[4:5]
    df = df.drop(*drop_cols)

    df = df.withColumn('dateColumn', df['timestampColumn'].cast('date'))
    df.write.parquet()


if __name__ == "__main__":
    spark_etl_function()
    
import logging
import sys, os

import urllib.request as requests
from pyspark import SparkContext, SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

from pyspark.sql.types import IntegerType, StructType, DateType, DateConverter, StructField, DoubleType


def get_logger(message_prefix):
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)
    # add handler to the logger
    handler = logging.StreamHandler(sys.stdout)
    # add formatter to the handler
    formatter = logging.Formatter(f'%(asctime)s  {message_prefix} %(levelname)s - %(message)s')
    handler.formatter = formatter
    logger.addHandler(handler)
    return logger


def download_files():
    file_name = "/opt/spark/work-dir/walmart_stock.csv"
    if os.path.isfile(file_name):
        os.remove(file_name)

    url = 'https://raw.githubusercontent.com/sandipnahak/spark_learning/master/walmart_stock.csv'
    requests.urlretrieve(url, filename=file_name)


def run_spark():
    spark = SparkSession.builder.appName("WalmartStock").getOrCreate()
    conf = spark.sparkContext.getConf()
    app_id = conf.get('spark.app.id')
    app_name = conf.get('spark.app.name')
    message_prefix = '<' + app_name + ' ' + app_id + '>'
    logger = get_logger(message_prefix)
    logger.info("Spark app id %s" % app_id)
    logger.info("Spark app name %s" % app_name)

    logger.info("downloading walmart stock files to local..")
    download_files()

    # Convert to schema type from default schema type
    df_fields= [StructField('Date', DateType(), nullable=False),
                StructField('Open', DoubleType(), nullable=False),
                 StructField('High', DoubleType(),nullable=False),
                 StructField('Low', DoubleType(), nullable=False),
                 StructField('Close', DoubleType(), nullable=False),
                 StructField('Volume', IntegerType(),nullable=False),
                 StructField('Adj Close', DoubleType(), nullable=False)]

    df_schema = StructType(fields=df_fields)
    logger.info("Reading the stock csv file.")
    df = spark.read.csv('/opt/spark/work-dir/walmart_stock.csv', header=True, schema=df_schema)
    logger.info("Data frame schema")
    df.printSchema()
    logger.info("Dataframe colmons")
    logger.info(df.columns)
    hv_df = df.withColumn('HV Ratio', df['High']/df['Volume'])
    hv_df.select(['HV Ratio']).show()
    df.createOrReplaceTempView('walmart_stock')
    high_value = spark.sql("select * from walmart_stock order by High desc limit 1;")
    high_value.show()
    logger.info(df.sort("High", ascending=False).head(1))
    df.agg({'Close': "avg"}).show()
    df.agg({'Volume': "min"}).show()
    df.agg({'Volume': "max"}).show()
    logger.info(df.filter("Close < 60.0").count())
    logger.info(df.filter("High > 80.0").count()/df.count() * 100)

    logger.info("Max high by year.")
    high_per_year_df = df.withColumn('Year', year(df['Date']))
    max_df = high_per_year_df.groupBy('Year').max()
    max_df.show()
    max_df.sort(['Year']).select(['Year', 'max(High)']).show()

    logger.info("Avg close by month")
    high_per_mon_df = df.withColumn('Month', month(df['Date']))
    mon_df = high_per_mon_df.groupBy('Month').avg()
    mon_df.sort(['Month']).select(['Month', 'avg(Close)']).show()


if __name__ == '__main__':
    run_spark()

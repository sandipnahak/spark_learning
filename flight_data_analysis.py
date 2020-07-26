from pyspark.sql import SparkSession


def run_job():
    spark = SparkSession.builder.appName('flight_data').getOrCreate()
    df = spark.read.json("s3a://analytics-learning-bucket-us-west-2/data/data/flight-data/json/2015-summary.json")
    df.printSchema()


if __name__ == '__main__':
    run_job()
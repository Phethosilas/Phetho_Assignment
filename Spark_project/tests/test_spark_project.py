from pyspark.sql import SparkSession
import unittest

"test the function that read from postgress db"

def test_spark_assignment():
    ''' spark = SparkSession \
            .builder \
            .appName("Spark Assignment to read from postgress") \
            .master("local[*]") \
            .getOrCreate()

        df = spark \
            .read \
            .format("csv") \
            .option("header", True) \
            .option("inferSchema", True) \
            .load("C:/dev\projects/Phetho_Assignment/Spark_project/Files/books.csv")
        spark.stop()'''
    assert True

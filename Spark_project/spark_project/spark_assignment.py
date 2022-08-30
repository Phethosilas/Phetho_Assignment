from pyspark.sql import SparkSession
def read_from_db():
    spark=SparkSession\
        .builder\
        .appName("Spark Assignment to read from postgress")\
        .master("local[*]")\
        .getOrCreate()

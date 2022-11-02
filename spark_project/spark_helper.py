from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkHelper:

    spark =None

    @staticmethod
    def get_spark_session(sparkAppName: str = "PySpark_Postgres",  jars: str = "../files/postgresql-42.5.0.jar") ->SparkSession:
        if (SparkHelper.spark is not None):
            return SparkHelper.spark

        sparkConf = SparkConf()
        sparkConf.setAppName(sparkAppName)
        sparkConf.set(
            "spark.jars",
            jars
        )
        sparkConf.set("spark.sql.crossJoin.enabled", "true")
        sparkConf.set("spark.sql.catalogImplementation", "hive")
        sparkConf.set("spark.sql.shuffle.partitions", "1")
        sparkConf.set("spark.cleaner.referenceTracking.cleanCheckpoints", True)
        # Added because there is a an implicit offset conversion that happens if you dont use the dev container
        sparkConf.set("spark.sql.session.timeZone", "UTC")



        spark = (SparkSession.builder.config(conf=sparkConf).getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        SparkHelper.spark = spark

        return spark

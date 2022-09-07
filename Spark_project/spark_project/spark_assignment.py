from pyspark.sql.session import SparkSession
from Spark_project.utils.spark_helper import SparkHelper


spark: SparkSession = SparkHelper.get_spark_session()

#Connection details
PSQL_SERVERNAME = "localhost"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "postgres"
PSQL_USERNAME = "postgres"
PSQL_PASSWORD = "pass"
URL= f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"
TABLE= "book"


#reading from a postgress db
def read_from_db():


    df = spark\
        .read\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "book") \
        .option("user", "postgres")\
        .option("password", "pass")\
        .load()
    df.show(2)

if __name__ == "__main__":
    read_from_db()

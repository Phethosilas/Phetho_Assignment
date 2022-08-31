from pyspark.sql import SparkSession

#Connection details
PSQL_SERVERNAME = "localhost"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "postgres"
PSQL_USERNAME = "postgres"
PSQL_PASSWORD = "pass"
URL= f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"
TABLE= "books"


# reading from CSV to Postgress DB in docker

def write_to_db():
    spark = SparkSession \
        .builder \
        .appName("Spark Assignment to read from postgress") \
        .master("local[*]") \
        .getOrCreate()
    df = spark \
        .read \
        .format("csv") \
        .option("header", True)\
        .option("inferSchema" , True) \
        .load("C:/dev\projects/Phetho_Assignment/Spark_project/Files/books.csv")

    df.show(5)
    spark.stop()



#reading from a postgress db
def read_from_db():
    spark = SparkSession.builder\
        .config("spark.jars", "/dev/tools/postgresql-42.5.0.jar") \
        .master("local")\
        .appName("PySpark_Postgres_test")\
        .getOrCreate()

    df = spark\
        .read\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/postgress") \
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "book") \
        .option("user", "postgres")\
        .option("password", "pass")\
        .load()
    df.show(2)

    spark.stop()
if __name__ == "__main__":
    write_to_db()
    read_from_db()

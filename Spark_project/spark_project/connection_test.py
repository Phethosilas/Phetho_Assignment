from pyspark.sql import SparkSession

#Connection details
PSQL_SERVERNAME = "localhost"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "postgres"
PSQL_USERNAME = "postgres"
PSQL_PASSWORD = "pass"
URL= f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"
TABLE= "book"


spark = SparkSession\
    .builder\
    .config("spark.jars", "postgresql-42.5.0.jar") \
    .master("local")\
    .appName("PySpark_Postgres_test")\
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres")\
    .option("dbtable", "book") \
    .option("user", "postgres") \
    .option("password", "pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()
df.show(5)
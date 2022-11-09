from pyspark.sql.session import SparkSession
from utils.spark_helper import SparkHelper

spark: SparkSession = SparkHelper.get_spark_session()


class Runner:

    def __init__(self):
        self.run = self.DataReader("localhost",
                                   5432, "postgres",
                                   "postgres",
                                   "pass",
                                   "jdbc:postgresql://localhost:5432/postgres",
                                   "book")

    class DataReader():
        # init method with db connection parameters
        def __init__(self, PSQL_SERVERNAME, PSQL_PORTNUMBER, PSQL_DBNAME, PSQL_USERNAME, PSQL_PASSWORD, URL, TABLE):
            self.PSQL_SERVERNAME = PSQL_SERVERNAME
            self.PSQL_PORTNUMBER = PSQL_PORTNUMBER
            self.PSQL_DBNAME = PSQL_DBNAME
            self.PSQL_USERNAME = PSQL_USERNAME
            self.PSQL_PASSWORD = PSQL_PASSWORD
            self.URL = URL
            self.TABLE = TABLE
            if self.TABLE is None:
                raise Exception("table name required")

        def read(self):
            df = spark \
                .read \
                .format("jdbc") \
                .option("url", f'{self.URL}') \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", f'{self.TABLE}') \
                .option("user", f'{self.PSQL_USERNAME}') \
                .option("password", f'{self.PSQL_PASSWORD}') \
                .load()
            return df.show(100)
if __name__ == '__main__':
    Runner().DataReader("localhost",
                                   5432, "postgres",
                                   "postgres",
                                   "pass",
                                   "jdbc:postgresql://localhost:5432/postgres",
                                   "book").read()
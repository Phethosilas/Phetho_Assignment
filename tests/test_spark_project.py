from pyspark.sql.session import SparkSession

from spark_project.spark_assignment import Runner
from utils.spark_helper import SparkHelper

spark: SparkSession = SparkHelper.get_spark_session()

#omiting a the table name in the db connections,table name=book
def test_no_table():
    tb=Runner().DataReader( "localhost",
                                  5432,
                                  "postgres",
                                  "postgres",
                                  "pass",
                                  "jdbc:postgresql://localhost:5432/postgres").read()



    assert True

#correct table name should be book not books
def test_wrong_table():
    tb=Runner().DataReader( "localhost",
                                  5432,
                                  "postgres",
                                  "postgres",
                                  "pass",
                                  "jdbc:postgresql://localhost:5432/postgres","books").read()
    assert True

#correct is 'pass' not password
def test_wrong_passwrd():
    tb=Runner().DataReader( "localhost",
                                  5432,
                                  "postgres",
                                  "postgres",
                                  "password",
                                  "jdbc:postgresql://localhost:5432/postgres","book").read()
    assert True

def test_correct_connection():
    tb=Runner().DataReader( "localhost",
                                  5432,#should be 5432
                                  "postgres",
                                  "postgres",
                                  "pass",
                                  "jdbc:postgresql://localhost:5432/postgres","book").read()
    assert True

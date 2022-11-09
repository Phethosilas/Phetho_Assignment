from pyspark.sql.session import SparkSession
import pytest
from spark_project.spark_assignment import Runner
from utils.spark_helper import SparkHelper

spark: SparkSession = SparkHelper.get_spark_session()

#omiting a the table name in the db connections,table name=book



def test_wrong_table():
    # Given we use the jdbc connection string without the correct table name

    table_name = "books"

    # When we connecting to the database

    with pytest.raises(Exception) as e :
        tb = Runner().DataReader("localhost",
                                 5432,
                                 "postgres",
                                 "postgres",
                                 "pass",
                                 "jdbc:postgresql://localhost:5432/postgres",table_name).read()

    # Then We expect an error to be thrown
    assert e.match(r".*books.* does not exist")


#correct is 'pass' not password
def test_wrong_passwrd():
    tb=Runner().DataReader( "localhost",
                                  5432,
                                  "postgres",
                                  "postgres",
                                  "password",
                                  "jdbc:postgresql://localhost:5432/postgres","book").read()
    assert True

def test_should_read_from_postgres_table():
    # when we read from the db
    df=Runner().DataReader( "localhost",
                                  5432,#should be 5432
                                  "postgres",
                                  "postgres",
                                  "pass",
                                  "jdbc:postgresql://localhost:5432/postgres","book").read()
    # Then should have the dataframe of that table
    assert df.count() == 22

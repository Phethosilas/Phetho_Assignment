from .utils import SparkHelper
from spark_project.writter import write
from spark_project.models import WritterConfig
spark = SparkHelper.get_spark_session()

def test_write_to_csv():
    # GIVEN: I have a configuration for writing a CSV files
    # Given I have a dataframe with some data
    deptDF = spark.read.format('csv').option('header','True').load('./tests/files/data.csv')
    deptDF.show()

    config = WritterConfig(type='csv', target='./tests/files/output', options={'header': True}, mode = "overwrite")

    # WHEN: I use the writter
    write(deptDF, config)

    # THEN: The file should be created and readable by spark
    df = spark.read.option('header', "True").csv('./tests/files/output')
    assert df.count() == deptDF.count()

def test_write_mode():
    # Given I have an incorrect mode in the configurations for writing a csv file
    deptDF = spark.read.format('csv').option('header','True').load('./tests/files/data.csv')
    deptDF.show()

    config = WritterConfig(type='csv', target='./tests/files/output', options={'header': True}, mode = "Overwrite")

    # WHEN: I use the writter
    write(deptDF, config)

    # THEN: assertion should fail
    df = spark.read.option('header', "True").csv('./tests/files/output')
    assert df.count() == deptDF.count()




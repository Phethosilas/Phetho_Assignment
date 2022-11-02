from .utils import SparkHelper
from spark_project.reader import read
from spark_project.models import ReaderConfig
spark = SparkHelper.get_spark_session()

def test_can_read_csv():
    # GIVEN: I have a configuration for a CSV files
    config = ReaderConfig(type='csv', source='./tests/files/data.csv', options={'header': True})

    # WHEN: I use the reader
    df = read(spark, config)

    # THEN: I should have a dataframe for the CSV file
    assert df.count() == 2
    row1 = df.collect()[0].asDict()
    row2 = df.collect()[1].asDict()
    assert row1.get('name') == 'asdas'

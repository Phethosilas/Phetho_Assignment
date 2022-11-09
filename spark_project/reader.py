from .models import ReaderConfig
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame

def read(spark: SparkSession, config: ReaderConfig) -> DataFrame:

    config.options.get('header')
    reader = spark \
                .read \
                .format(config.type) \
                .options(**config.options) \
    
    if type == 'jdbc':
        return reader \
            .option("dbtable", config.source) \
            .load()
    else:
        return reader.load(config.source)

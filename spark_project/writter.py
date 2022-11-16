from .models import WritterConfig
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame


def write(df, config: WritterConfig) ->None:
    config.options.get('header')
    writer = df \
        .write \
        .format(config.type) \
        .options(**config.options)\
        .mode(config.mode)\
        .save(config.target)


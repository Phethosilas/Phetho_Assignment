import argparse

from pyspark.sql.session import SparkSession
from typing import List


class Context:
    def __init__(self,spark: SparkSession,args:List[str]):
        self.spark = spark
        self.args = self.parse_args(args)

    def parse_args(self,args:List[str]):
        parser = argparse.ArgumentParser(
            prog  = 'MysparkApp',
            description='Add file type'
        )

        parser.add_argument('-c',required = True, action='store',  help='path to the configuration file')

        return parser.parse_args(args)




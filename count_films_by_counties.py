import argparse

from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from operator import itemgetter
from pyspark.mllib.stat import Statistics


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the rank correlation calculation')
    parser.add_argument('-d', '--database', help='database to connect to', required=True)
    parser.add_argument('-host', '--host', help='server to connect to', default='localhost')
    parser.add_argument('-port', '--port', help='port to connect to', default=27017)

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()

    uri = f'mongodb://{args.host}:{args.port}/{args.database}'
    spark = SparkSession.builder.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.0').getOrCreate()
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').options(uri=uri, collection='films').load()

    df = df.select('data.countries')

    test = df.rdd.flatMap(lambda countries: countries[0]).map(lambda country: (country[0], 1)).reduceByKey(add).collect()
    test.sort(reverse=True, key=itemgetter(1))
    print(test)


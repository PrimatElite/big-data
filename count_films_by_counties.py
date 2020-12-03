import argparse

from operator import add
from pyspark.sql import SparkSession

from utils import get_uri_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the films by each country counting')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()

    uri = get_uri_mongodb(args.database, args.username, args.password, args.host, args.port,
                          args.authenticationDatabase)
    spark = SparkSession.builder.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.0').getOrCreate()
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').options(uri=uri, collection='films').load()

    df = df.select('data.countries')

    data_films = df.rdd.flatMap(lambda countries: countries[0]).map(lambda country: (country[0], 1)).reduceByKey(add) \
        .sortBy(lambda x: x[1], ascending=False).collect()
    print(data_films)


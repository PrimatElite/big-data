import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

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

    df = df.select('rating.ratingFilmCritics', 'review.reviewAllPositiveRatio')
    df = df.filter(df.ratingFilmCritics.isNotNull() & (df.ratingFilmCritics != str()) & (df.reviewAllPositiveRatio != str()))

    convert_percent_to_float = udf(lambda p: float(p[:-1]), FloatType())
    df = df.withColumn('ratingFilmCritics', convert_percent_to_float(df.ratingFilmCritics))
    df = df.withColumn('reviewAllPositiveRatio', convert_percent_to_float(df.reviewAllPositiveRatio))

    rating_film_critics = df.rdd.map(lambda r: r[0])
    review_all_positive_ratio = df.rdd.map(lambda r: r[1])
    spearman_corr = Statistics.corr(rating_film_critics, review_all_positive_ratio, method='spearman')

    print('Spearman\'s rank correlation coefficient between ratingFilmCritics and reviewAllPositiveRatio =', spearman_corr)

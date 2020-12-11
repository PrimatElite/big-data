import argparse

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from scipy.stats import spearmanr

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the rank correlation calculation')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()

    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)

    df = df.select('rating.ratingFilmCritics', 'review.reviewAllPositiveRatio')
    df = df.filter(df.ratingFilmCritics.isNotNull() & (df.ratingFilmCritics != '') & (df.reviewAllPositiveRatio != ''))

    convert_percent_to_float = udf(lambda p: float(p[:-1]), FloatType())
    df = df.withColumn('ratingFilmCritics', convert_percent_to_float(df.ratingFilmCritics))
    df = df.withColumn('reviewAllPositiveRatio', convert_percent_to_float(df.reviewAllPositiveRatio))

    records = df.collect()

    rating_film_critics = list(map(lambda r: r[0], records))
    review_all_positive_ratio = list(map(lambda r: r[1], records))

    correlation, pvalue = spearmanr(rating_film_critics, review_all_positive_ratio)

    print(f'Spearman correlation coefficient = {correlation} with associated p-value = {pvalue}')

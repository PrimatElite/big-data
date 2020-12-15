import argparse

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from pyspark.mllib.stat import Statistics

from scipy.stats import linregress, rankdata

import plotly.graph_objects as go

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb

from plotly_scatter_confidence_ellipse import confidence_ellipse


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

    rating_film_critics = df.rdd.map(lambda r: r[0])
    review_all_positive_ratio = df.rdd.map(lambda r: r[1])
    r = Statistics.corr(rating_film_critics, review_all_positive_ratio, method='spearman')

    records = df.collect()
    rating_film_critics = list(map(lambda r: r[0], records))
    review_all_positive_ratio = list(map(lambda r: r[1], records))

    rating_film_critics_ranks = rankdata(rating_film_critics, method='ordinal')
    review_all_positive_ratio_ranks = rankdata(review_all_positive_ratio, method='ordinal')

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=rating_film_critics_ranks,
                             y=review_all_positive_ratio_ranks,
                             mode='markers',
                             name='Фильмы'))

    slope, intercept, rvalue, pvalue, stderr = linregress(rating_film_critics_ranks, review_all_positive_ratio_ranks)

    fig.add_trace(go.Scatter(x=rating_film_critics_ranks,
                             y=[slope * x + intercept for x in rating_film_critics_ranks],
                             name='Линейная регрессия'))

    ellipse_coords_x, ellipse_coords_y = confidence_ellipse(rating_film_critics_ranks, review_all_positive_ratio_ranks)

    fig.add_trace(go.Scatter(x=ellipse_coords_x,
                             y=ellipse_coords_y,
                             line={'color': 'black', 'dash': 'dot'},
                             name='Эллипс 95%-ой доверительной области'))

    r = float(format(r, '.2f'))

    fig.update_layout(width=1000,
                      height=1000,
                      title=f'Коэффициент корреляции Спирмена = {r}',
                      xaxis_title='Ранг рейтинга кинокритиков в мире',
                      yaxis_title='Ранг процента рецензий')

    fig.update_yaxes(scaleanchor='x',
                     scaleratio=1)

    fig.write_html('visualizations/rank_correlation.html')

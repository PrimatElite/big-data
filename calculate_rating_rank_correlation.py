import argparse
import plotly.graph_objects as go

from pyspark.mllib.stat import Statistics
from scipy.stats import linregress, rankdata

from plotly_scatter_confidence_ellipse import confidence_ellipse
from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the rating rank correlation calculation')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


def calculate_rating_rank_correlation(args):
    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)
    df = df.select('rating.ratingImdb', 'rating.rating')
    df = df.filter((df.rating != 0) & (df.ratingImdb != 0))

    rating_imdb = df.rdd.map(lambda r: r[0])
    rating = df.rdd.map(lambda r: r[1])
    corr = Statistics.corr(rating_imdb, rating, method='spearman')
    records = df.collect()
    return corr, records


def visualize(corr, records):
    rating_imdb, rating = tuple(zip(*records))
    rating_imdb_ranks = rankdata(rating_imdb, method='ordinal')
    rating_ranks = rankdata(rating, method='ordinal')

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=rating_imdb_ranks, y=rating_ranks, mode='markers', name='Фильмы'))

    slope, intercept, rvalue, pvalue, stderr = linregress(rating_imdb_ranks, rating_ranks)
    fig.add_trace(go.Scatter(x=rating_imdb_ranks, y=[slope * x + intercept for x in rating_imdb_ranks],
                             name='Линейная регрессия'))

    ellipse_coords_x, ellipse_coords_y = confidence_ellipse(rating_imdb_ranks, rating_ranks)
    fig.add_trace(go.Scatter(x=ellipse_coords_x, y=ellipse_coords_y, line={'color': 'black', 'dash': 'dot'},
                             name='Эллипс 95%-ой доверительной области'))

    corr = float(format(corr, '.2f'))
    fig.update_layout(width=1000, height=1000, title=f'Коэффициент корреляции Спирмена = {corr}',
                      xaxis_title='Ранг рейтинга IMDb', yaxis_title='Ранг рейтинга КиноПоиска')
    fig.update_yaxes(scaleanchor='x', scaleratio=1)
    fig.write_html('visualizations/rating_rank_correlation.html')


if __name__ == '__main__':
    args = register_launch_arguments()
    corr, records = calculate_rating_rank_correlation(args)
    visualize(corr, records)

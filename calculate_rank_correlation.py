import argparse
import plotly.express as px
import plotly.graph_objects as go

from operator import itemgetter
from scipy.stats import rankdata
from plotly.subplots import make_subplots
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from pyspark.mllib.stat import Statistics

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the rank correlation calculation')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


def tmp_1(args):
    pass


if __name__ == '__main__':
    args = register_launch_arguments()

    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)

    df = df.select('rating.ratingFilmCritics', 'review.reviewAllPositiveRatio', 'data.nameRu')
    df = df.filter(df.ratingFilmCritics.isNotNull() & (df.ratingFilmCritics != '') & (df.reviewAllPositiveRatio != ''))

    convert_percent_to_float = udf(lambda p: float(p[:-1]), FloatType())
    df = df.withColumn('ratingFilmCritics', convert_percent_to_float(df.ratingFilmCritics))
    df = df.withColumn('reviewAllPositiveRatio', convert_percent_to_float(df.reviewAllPositiveRatio))

    rating_film_critics = df.rdd.map(lambda r: r[0])
    review_all_positive_ratio = df.rdd.map(lambda r: r[1])

    spearman_corr = Statistics.corr(rating_film_critics, review_all_positive_ratio, method='spearman')

    print(*rating_film_critics.collect())
    print(*review_all_positive_ratio.collect())
    print(spearman_corr)

    data_film_rating = rating_film_critics.collect()
    data_film_positive_ratio = review_all_positive_ratio.collect()
    data_film = df.rdd.map(lambda r: r[2]).collect()

    tmp = list(zip(data_film, data_film_rating, data_film_positive_ratio))

    tmp = sorted(tmp, reverse=True, key=itemgetter(2, 1))

    films, rating, ratio = list(zip(*tmp))

    # fig = make_subplots(
    #     rows=2,
    #     cols=1,
    #     subplot_titles=['график 1', 'график 2'])
    #
    # fig.add_trace(go.Scatter(x=data_film_rating, y=data_film_positive_ratio, mode="markers", name=0), row=1, col=1)
    # fig.add_trace(go.Scatter(x=sorted(range(len(data_film_rating)), key=lambda i: data_film_rating[i]),
    #                          y=sorted(range(len(data_film_positive_ratio)), key=lambda i: data_film_positive_ratio[i]),
    #                          mode="markers", name=1), row=2, col=1)

    # fig.update_layout(title=f'Коэффициент корреляции оценок критиков и зрителей = {spearman_corr}')
    # fig.update_layout(yaxis=dict(scaleanchor="x"))
    # fig.update_layout(yaxis2=dict(scaleanchor=" x2"))
    # fig.show()

    # fig = px.scatter(x=rankdata(data_film_rating),
    #                           y=rankdata(data_film_positive_ratio),
    #                  labels={'x': 'Ранг оценки критика', 'y': 'Ранг оценки зрителя'})
    # fig.update_layout(autosize=False, width=1000, height=1000)
    #
    # fig.show()

    # fig = go.Figure(data=[
    #     go.Bar(name='Оценка критиков', x=data_film, y=data_film_rating),
    #     go.Bar(name='Оценка зрителей', x=data_film, y=data_film_positive_ratio)
    # ])
    # # Change the bar mode
    # fig.update_layout(barmode='group')
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=list(range(len(films))), y=rating,
                             mode='lines+markers',
                             name='Оценка критиков'))
    fig.add_trace(go.Scatter(x=list(range(len(films))), y=ratio,
                             mode='lines+markers',
                             name='Оценка зрителей'))
    fig.show()
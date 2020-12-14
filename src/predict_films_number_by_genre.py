import argparse

from pyspark.sql.functions import explode
from pyspark.sql.types import IntegerType

import pmdarima as pm

import plotly.graph_objects as go

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the films number by genre prediction')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()

    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)

    df = df.select('data.year', 'data.genres.genre')
    df = df.filter(df.year != '')
    df = df.withColumn('year', df.year.cast(IntegerType()))
    df = df.filter(df.year < 2020)

    df = df.withColumn('genre', explode('genre'))

    genres = list(map(lambda g: g[0], df.select('genre').distinct().collect()))

    for genre in genres:
        genre_df = df.filter(df.genre == genre)

        genre_df = genre_df.groupBy(df.year).count().sort(df.year)

        records = genre_df.collect()
        years = list(map(lambda r: r[0], records))
        numbers = list(map(lambda r: r[1], records))

        model = pm.auto_arima(y=numbers, max_order=None, stepwise=False)

        n_periods = 10
        fcs, conf_ints = model.predict(n_periods=n_periods, return_conf_int=True)

        fcs = list(fcs)
        fcs = [numbers[-1]] + fcs

        conf_ints = list(map(lambda i: list(i), conf_ints))

        fcs_years = list(range(years[-1], years[-1] + n_periods + 1))

        lower_conf_ints = list(map(lambda i: i[0], conf_ints))
        lower_conf_ints = [numbers[-1]] + lower_conf_ints

        upper_conf_ints = list(map(lambda i: i[1], conf_ints))
        upper_conf_ints = [numbers[-1]] + upper_conf_ints

        fig = go.Figure()

        fig.add_trace(go.Scatter(x=years,
                                 y=numbers,
                                 mode='lines+markers',
                                 name='Количество фильмов'))

        fig.add_trace(go.Scatter(x=fcs_years,
                                 y=fcs,
                                 mode='lines+markers',
                                 name='Прогнозируемое количество фильмов'))

        fig.add_trace(go.Scatter(x=fcs_years,
                                 y=lower_conf_ints,
                                 fill=None,
                                 mode='lines',
                                 line_color='grey',
                                 showlegend=False))

        fig.add_trace(go.Scatter(x=fcs_years,
                                 y=upper_conf_ints,
                                 fill='tonexty',
                                 mode='lines',
                                 line_color='grey',
                                 name='95%-ая доверительная область'))

        fig.update_layout(title=f'Прогноз количества фильмов жанра {genre} на {n_periods} лет',
                          xaxis_title='Год',
                          yaxis_title='Количество фильмов')

        fig.write_html('../results/htmls/films_number_by_genre_prediction/' + genre + '.html')

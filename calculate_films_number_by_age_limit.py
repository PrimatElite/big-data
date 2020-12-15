import argparse

from pyspark.sql.types import IntegerType

import plotly.graph_objects as go

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the films number by age limit calculation')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()

    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)

    df = df.select('data.year', 'data.ratingAgeLimits')
    df = df.filter((df.year != '') & df.ratingAgeLimits.isNotNull())
    df = df.filter(df.year < 2020)
    df = df.withColumn('year', df.year.cast(IntegerType()))

    age_limits = sorted(list(map(lambda a_l: a_l[0], df.select('ratingAgeLimits').distinct().collect())))

    fig = go.Figure()

    for age_limit in age_limits:
        age_limit_df = df.filter(df.ratingAgeLimits == age_limit)

        if age_limit_df.count() == 0:
            continue

        age_limit_df = age_limit_df.groupBy(df.year).count().sort(df.year)

        records = age_limit_df.collect()
        years = list(map(lambda r: r[0], records))
        numbers = list(map(lambda r: r[1], records))

        fig.add_trace(go.Scatter(x=years,
                                 y=numbers,
                                 mode='lines+markers',
                                 name=f'Количество фильмов с возрастным ограничением в {age_limit} лет'))

    fig.update_layout(title='Количество фильмов с возрастными ограничениями',
                      xaxis_title='Год',
                      yaxis_title='Количество фильмов')

    fig.write_html('visualizations/films_number_by_age_limit_calculation.html')

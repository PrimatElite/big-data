import argparse

from pyspark.sql.functions import explode
from pyspark.sql.types import IntegerType

import matplotlib as mpl
import matplotlib.pyplot as plt

import pmdarima as pm

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

        mng = plt.get_current_fig_manager()
        size = mng.window.maxsize()

        dpi = mpl.rcParams['figure.dpi']

        model = pm.auto_arima(y=numbers, max_order=None, stepwise=False)
        n_periods = 10
        fcs, conf_ints = model.predict(n_periods=n_periods, return_conf_int=True)
        fcs = list(fcs)
        conf_ints = list(map(lambda i: list(i), conf_ints))
        fcs = [numbers[-1]] + fcs
        fcs_years = list(range(years[-1], years[-1] + n_periods + 1))
        lower_conf_ints = list(map(lambda i: i[0], conf_ints))
        upper_conf_ints = list(map(lambda i: i[1], conf_ints))
        lower_conf_ints = [numbers[-1]] + lower_conf_ints
        upper_conf_ints = [numbers[-1]] + upper_conf_ints

        plt.figure(figsize=(size[0] / dpi, size[1] / dpi))

        plt.plot(years, numbers)
        plt.plot(fcs_years, fcs)
        plt.fill_between(fcs_years, lower_conf_ints, upper_conf_ints, color='k', alpha=0.15)
        plt.grid(True)
        plt.title(genre)
        plt.xlabel('year')
        plt.ylabel('number')

        plt.savefig('pngs/' + genre + '.png')
        plt.close()

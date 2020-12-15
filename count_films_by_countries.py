import argparse
import plotly.express as px
import pandas as pd

from operator import add

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the films by each country counting')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


def receiving_and_processing_data():
    if __name__ == '__main__':
        args = register_launch_arguments()

    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)
    df = df.select('data.countries')

    data_films = df.rdd.flatMap(lambda countries: countries[0]).map(lambda country: (country[0], 1)).reduceByKey(add) \
        .sortBy(lambda x: x[1], ascending=False).collect()
    return data_films


def visualization_and_save_data():
    data_films = receiving_and_processing_data()
    countries = list(map(lambda data_film: data_film[0], data_films))
    films = list(map(lambda data_film: data_film[1], data_films))

    frame = pd.DataFrame([countries, films])
    frame.to_csv('my_csv_export.csv', index=False)

    fig = px.bar(x=countries[0:100], y=films[0:100], labels={'x': 'Страна', 'y': 'Количество фильмов'})
    fig.show()


visualization_and_save_data()

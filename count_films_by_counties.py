import argparse
import plotly.express as px

from operator import add

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the films by each country counting')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()

    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)
    df = df.select('data.countries')

    data_films = df.rdd.flatMap(lambda countries: countries[0]).map(lambda country: (country[0], 1)).reduceByKey(add) \
        .sortBy(lambda x: x[1], ascending=False).collect()
    countries = list(map(lambda data_film: data_film[0], data_films))[0:100]
    films = list(map(lambda data_film: data_film[1], data_films))[0:100]
    #print(*data_films[0:100], sep='\n')
    fig = px.bar(x=countries, y=films, labels={'x': 'Страна', 'y': 'Количество фильмов'})
    fig.show()


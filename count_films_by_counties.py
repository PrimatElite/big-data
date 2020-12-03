import argparse

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
    print(*data_films, sep="\n")



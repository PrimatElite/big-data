import argparse
import plotly.graph_objects as go

from math import ceil
from operator import add
from plotly.subplots import make_subplots

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the films by each country and year counting')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


def count_films_by_counties_year(args):
    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)
    df = df.select('data.countries', 'data.year')
    df = df.filter(df.year != '')

    films_count = df.rdd \
        .flatMap(lambda r: [(int(r[1]), country[0]) for country in r[0] if country[0] != '']) \
        .map(lambda r: (r, 1)) \
        .reduceByKey(add) \
        .map(lambda r: (r[0][0], (r[0][1], r[1]))) \
        .groupByKey() \
        .mapValues(lambda it: sorted(it, key=lambda r: r[1], reverse=True)) \
        .sortBy(lambda x: x[0]) \
        .collect()
    return dict(films_count)


def visualize(films_count):
    top = 10
    years = list(filter(lambda y: y <= 2020, films_count.keys()))
    rows_count = ceil(len(years) / 2)
    fig = make_subplots(rows_count, 2, specs=[[{'type': 'domain'}, {'type': 'domain'}] for _ in range(rows_count)],
                        subplot_titles=years)

    for i, year in enumerate(years):
        row = i // 2 + 1
        column = i % 2 + 1
        countries, count = tuple(zip(*(films_count[year][:top])))
        if len(films_count[year]) > top:
            countries += tuple(['Другие страны'])
            count += tuple([sum(tuple(zip(*(films_count[year][top:])))[1])])
        fig.add_trace(go.Pie(labels=countries, values=count, name=str(year), hole=0.3), row, column)

    fig.update_traces(textposition='outside', textinfo='percent+label')
    fig.update_layout(title_text='Распределение фильмов между странами по годам', height=rows_count * 500)
    fig.write_html('visualizations/films_by_countries_year.html')


if __name__ == '__main__':
    args = register_launch_arguments()
    films_count = count_films_by_counties_year(args)
    visualize(films_count)

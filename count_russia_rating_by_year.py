import argparse
import plotly.graph_objects as go

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


CIVIL_WAR_TIME = 'РСФСР / Российское государство'
COUNTRIES = ['Российская империя', CIVIL_WAR_TIME, 'СССР', 'Россия']


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the Russian films rating by each year counting')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


def filter_countries(countries):
    for country in countries:
        if country[0] in COUNTRIES:
            return True
    return False


def count_russia_rating_by_year(args):
    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)
    df = df.select('data.countries', 'data.year', 'rating.rating')
    filter_countries_udf = udf(filter_countries, BooleanType())
    df = df.filter((df.year != '') & (df.rating != 0) & filter_countries_udf(df.countries))

    films_rating = df.rdd \
        .flatMap(lambda r: [(country[0], int(r[1]), r[2]) for country in r[0] if country[0] in COUNTRIES]) \
        .map(lambda r: ((r[0], r[1]), (r[2], 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .map(lambda r: (r[0][0], (r[0][1], r[1][0] / r[1][1], r[1][1]))) \
        .groupByKey() \
        .mapValues(lambda it: sorted(it, key=lambda r: r[0])) \
        .sortBy(lambda x: COUNTRIES.index(x[0])) \
        .collect()

    films_rating = dict(films_rating)
    idx = 0
    for i, (year, _, _) in enumerate(films_rating['Россия']):
        if year >= 1991:
            idx = i
            break
    films_rating[CIVIL_WAR_TIME] = films_rating['Россия'][:idx]
    films_rating['Россия'] = films_rating['Россия'][idx:]
    return films_rating


def visualize(films_rating):
    fig = go.Figure()
    for country in COUNTRIES:
        year, rating, count = tuple(zip(*films_rating[country]))
        hover_template = 'Страна=%s<br>Год=%%{x}<br>Средний рейтинг=%%{y}<br>Количество=%%{marker.size}<extra></extra>'\
                         % country
        fig.add_trace(go.Scatter(x=year, y=rating, name=country, mode='lines+markers', hovertemplate=hover_template,
                                 marker_size=count, marker=dict(sizemode='area', line_width=2)))
    fig.update_layout(title_text='Средний рейтинг российских фильмов')
    fig.update_xaxes(title_text='Год')
    fig.update_yaxes(title_text='Средний рейтинг')
    fig.write_html('visualizations/count_russia_rating_by_year.html')


if __name__ == '__main__':
    args = register_launch_arguments()
    films_rating = count_russia_rating_by_year(args)
    visualize(films_rating)

import argparse
import re

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, BooleanType, IntegerType, LongType

from currency_converter import CurrencyConverter
from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


specific_id = {
        '': 'USD',
        '$': 'USD',
        '€': 'EUR',
        '¥': 'JPY',
        '£': 'GBP',
        'руб.': 'RUB'
    }
conv = CurrencyConverter()


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the films budgets calculation')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


def parse_budget_string(bs):
    budget_id = re.sub(r'[0-9 ]+', '', bs)
    budget_value = re.sub(r'[^0-9]+', '', bs)
    
    return conv.convert(float(budget_value), specific_id.get(budget_id, budget_id), 'USD')


def gross_to_int(g):
    if g is None:
        return 0
    return g


def filter_budget_currency(x):
    if re.sub(r'[0-9 ]+', '', x) in ['UAH', 'COP', 'RUR', 'IRR', 'TWD', 'VEB', 'FIM', 'FRF', 'DEM', 'ATS', 'ITL',
                                     'ARS', 'NGN', 'CLP', 'BGL', 'ESP', 'JMD', 'IEP', 'BEF']:
        return False
    return True


def filter_films(x):
    if 'Мстители' in x:
        return True
    return False


def lam(r):
    # if 'Мстители' in r[0]:
    #     print(*r)
    return r[0], (r[1] + r[2] + r[3]) / r[4]


if __name__ == '__main__':
    args = register_launch_arguments()

    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase, update_schema={'budget.grossWorld': LongType(),
                                                                                 'budget.grossRu': LongType(),
                                                                                 'budget.grossUsa': LongType()})

    df = df.select('data.nameRu', 'budget.grossWorld', 'budget.grossRu', 'budget.grossUsa', 'budget.budget')
    filter_budget_currency_udf = udf(filter_budget_currency, BooleanType())
    df = df.filter((df.grossWorld.isNotNull() | df.grossUsa.isNotNull() | df.grossRu.isNotNull()) &
                   df.budget.isNotNull() & filter_budget_currency_udf(df.budget))

    # filter_name_udf = udf(filter_films, BooleanType())
    # df.filter(filter_name_udf(df.nameRu)).show()

    convert_gross_to_int = udf(gross_to_int, LongType())
    df = df.withColumn('grossWorld', convert_gross_to_int(df.grossWorld))
    df = df.withColumn('grossRu', convert_gross_to_int(df.grossRu))
    df = df.withColumn('grossUsa', convert_gross_to_int(df.grossUsa))

    convert_budget_to_float = udf(parse_budget_string, FloatType())
    df = df.withColumn('budget', convert_budget_to_float(df.budget))

    films = df.rdd.map(lam).sortBy(lambda r: r[1], ascending=False).collect()
    print(*films, sep='\n')

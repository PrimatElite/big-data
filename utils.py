import argparse

from pyspark.sql import SparkSession
from typing import Union


def update_argument_parser_mongodb(parser: argparse.ArgumentParser):
    parser.add_argument('-u', '--username', help='username for authentication')
    parser.add_argument('-p', '--password', help='password for authentication')
    parser.add_argument('-d', '--database', help='database to connect to', required=True)
    parser.add_argument('-host', '--host', help='server to connect to', default='localhost')
    parser.add_argument('-port', '--port', help='port to connect to', default=27017)
    parser.add_argument('--authenticationDatabase', help='user source')


def get_uri_mongodb(database: str, username: Union[str, None] = None, password: Union[str, None] = None,
                    host: str = 'localhost', port: Union[int, str] = 27017,
                    authentication_database: Union[str, None] = None) -> str:
    uri = 'mongodb://'
    if username is not None and password is not None:
        uri += f'{username}:{password}@'
    uri += f'{host}:{port}/{database}'
    uri += '?socketTimeoutMS=600000&connectTimeoutMS=600000&maxIdleTimeMS=600000'
    if authentication_database is not None:
        uri += f'&authSource={authentication_database}'
    return uri


def get_data_frame_from_mongodb(database: str, username: Union[str, None] = None, password: Union[str, None] = None,
                                host: str = 'localhost', port: Union[int, str] = 27017,
                                authentication_database: Union[str, None] = None,
                                update_schema: Union[dict, None] = None):
    uri = get_uri_mongodb(database, username, password, host, port, authentication_database)
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.0') \
        .config('spark.executor.memory', '10g') \
        .config('spark.driver.memory', '10g') \
        .config('spark.memory.offHeap.enabled', True) \
        .config('spark.memory.offHeap.size', '10g') \
        .config('spark.executor.heartbeatInterval', '1m') \
        .config('spark.network.timeout', '10m') \
        .config('spark.rpc.lookupTimeout', '10m') \
        .getOrCreate()
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').options(uri=uri, collection='films').load()
    if isinstance(update_schema, dict):
        for field, new_type in update_schema.items():
            subcommand = ''.join(map(lambda subfield: f"['{subfield}'].dataType", field.split('.')))
            command = f'df.schema{subcommand} = new_type'
            exec(command)
        df = spark.read.format('com.mongodb.spark.sql.DefaultSource').options(uri=uri, collection='films') \
            .load(schema=df.schema)
    return df

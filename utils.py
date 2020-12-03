import argparse

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
    if authentication_database is not None:
        uri += f'?authSource={authentication_database}'
    return uri

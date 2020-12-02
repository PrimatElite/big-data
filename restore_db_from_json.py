import argparse

from connector import Connector
from utils import update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the connector application')
    update_argument_parser_mongodb(parser)
    parser.add_argument('--clearDatabase', help='clear database', action='store_true')
    parser.add_argument('-i', '--inputFile', help='path to input file', default='films.json')
    parser.add_argument('-l', '--logFile', help='path to log file', default='connect.log')

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()
    connector = Connector('', args.database, args.username, args.password, args.host, args.port,
                          args.authenticationDatabase)
    connector.connect_from_file(args.inputFile, args.clearDatabase, args.logFile)


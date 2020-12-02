import argparse

from connector import Connector, FILMS_PER_PAGE
from utils import update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the connector application')
    parser.add_argument('--apiKey', help='API key', required=True)
    update_argument_parser_mongodb(parser)
    parser.add_argument('-s', '--sort', help='movies sorting', choices=['', 'year', 'popularity', 'title'])
    parser.add_argument('--startPage', help='start film page (from 1)', type=int, default=1)
    parser.add_argument('--endPage', help='end film page', type=int)
    parser.add_argument('--startFilm', help='start film index', type=int, choices=list(range(1, FILMS_PER_PAGE + 1)),
                        default=1)
    parser.add_argument('--endFilm', help='end film index', type=int, choices=list(range(1, FILMS_PER_PAGE + 1)),
                        default=FILMS_PER_PAGE)
    parser.add_argument('--clearDatabase', help='clear database', action='store_true')
    parser.add_argument('-l', '--logFile', help='path to log file', default='connect.log')

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()
    connector = Connector(args.apiKey, args.database, args.username, args.password, args.host, args.port,
                          args.authenticationDatabase, args.sort)
    connector.connect(args.startPage, args.endPage, args.startFilm, args.endFilm, args.clearDatabase, args.logFile)

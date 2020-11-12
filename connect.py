import argparse

from connector import Connector


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the connector application')
    parser.add_argument('--apiKey', help='API key', required=True)
    parser.add_argument('-u', '--username', help='username for authentication')
    parser.add_argument('-p', '--password', help='password for authentication')
    parser.add_argument('-d', '--database', help='database to connect to', required=True)
    parser.add_argument('-host', '--host', help='server to connect to', default='localhost')
    parser.add_argument('-port', '--port', help='port to connect to', default=27017)
    parser.add_argument('--authenticationDatabase', help='user source')
    parser.add_argument('-s', '--sort', help='movies sorting', choices=['', 'year', 'popularity', 'title'])
    parser.add_argument('--startPage', help='start film page (from 1)', type=int, default=1)
    parser.add_argument('--endPage', help='end film page', type=int)
    parser.add_argument('--startFilm', help='start film index', type=int, choices=list(range(1, 31)), default=1)
    parser.add_argument('--endFilm', help='end film index', type=int, choices=list(range(1, 31)), default=30)
    parser.add_argument('--clearDatabase', help='clear database', action='store_true')

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()
    connector = Connector(args.apiKey, args.database, args.username, args.password, args.host, args.port,
                          args.authenticationDatabase, args.sort)
    connector.connect(args.startPage, args.endPage, args.startFilm, args.endFilm, args.clearDatabase, is_log=True)

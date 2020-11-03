import argparse

from connector import Connector


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the connector application')
    parser.add_argument('--apiKey', help='API key', required=True)
    parser.add_argument('-u', '--username', help='username for authentication', required=True)
    parser.add_argument('-p', '--password', help='password for authentication', required=True)
    parser.add_argument('-d', '--database', help='database to connect to', required=True)
    parser.add_argument('-host', '--host', help='server to connect to', default='localhost')
    parser.add_argument('-port', '--port', help='port to connect to', default=27017)
    parser.add_argument('--authenticationDatabase', help='user source', default='admin')

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()
    connector = Connector(args.apiKey, args.username, args.password, args.database, args.host, args.port,
                          args.authenticationDatabase)
    connector.connect(is_log=True)

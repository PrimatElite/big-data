import argparse
import json

from pymongo import MongoClient
from tqdm import tqdm

from utils import get_uri_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the connector application')
    update_argument_parser_mongodb(parser)
    parser.add_argument('-o', '--outputFile', help='path to output file', default='films.json')

    return parser.parse_args()


def delete_from_dict(dictionary: dict, key) -> dict:
    if key in dictionary:
        dictionary.pop(key)
    return dictionary


if __name__ == '__main__':
    args = register_launch_arguments()
    uri = get_uri_mongodb(args.database, args.username, args.password, args.host, args.port,
                          args.authenticationDatabase)
    client = MongoClient(uri)
    db = client.get_database()
    films = db.films.find()
    count = films.count()
    with open(args.outputFile, 'w', encoding='utf8') as file_object:
        file_object.write(f'[\n{json.dumps({"count": count})},\n')
        for i, film in enumerate(tqdm(films, ascii=True, total=count)):
            s = json.dumps(delete_from_dict(film, '_id'), ensure_ascii=False)
            if i < count - 1:
                s += ',\n'
            file_object.write(s)
        file_object.write('\n]')

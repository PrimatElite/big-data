# big-data
Big data project to analyse kinopoisk data

## Connector

Simple program-connector (not program, but useful class) for transfer data from kinopoisk site to your own MongoDB database.

### Using

* Install all packages with `pip` from `requirements.txt`
  ```bash
  pip install -r requirements.txt
  ```

* Look at the `connect.py` as an example of using the `Connector` class for transfer data from API and kinopoisk site

  You must pass some arguments to `connect.py` for the connector's work  
  Use the following command for details
  ```bash
  python connect.py --help
  ```

* Look at the `restore_db_from_json.py` as an example of using the `Connector` class for transfer data from .json file with specific format
  You can find specific format of file in the `dump_db_to_json.py`

  You must pass some arguments to `restore_db_from_json.py` for the connector's work  
  Use the following command for details
  ```bash
  python restore_db_from_json.py --help
  ```

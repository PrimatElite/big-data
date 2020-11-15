# big-data
Big data project to analyse kinopoisk data

## Connector

Simple program-connector (not program, but useful class) for transfer data from kinopoisk site to your own MongoDB database.

### Using

* Install all packages with `pip` from `requirements.txt`
  ```bash
  pip install -r requirements.txt
  ```

* Look at the `connect.py` as an example of using the `Connector` class

  You must pass some arguments to `connect.py` for the connector's work  
  Use the following command for details
  ```bash
  python connect.py --help
  ```

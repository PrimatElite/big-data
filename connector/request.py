import requests

from .errors import ConnectionError


class Request:
    def __init__(self, headers: dict = None):
        self._session = requests.Session()
        self._headers = headers

        self._session.trust_env = False

    def get(self, url):
        try:
            response = self._session.get(url, headers=self._headers)
        except (requests.exceptions.ConnectionError, Exception):
            raise ConnectionError(url) from None
        response.connection.close()
        return response

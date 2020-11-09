import requests

from .errors import ConnectionError


class Request:
    def __init__(self, headers: dict = None):
        self.session = requests.Session()
        self.headers = headers

        self.session.trust_env = False

    def get(self, url):
        try:
            response = self.session.get(url, headers=self.headers)
        except requests.exceptions.ConnectionError:
            raise ConnectionError(url) from None
        response.connection.close()
        return response

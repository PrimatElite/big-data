import requests


class Request:
    def __init__(self, headers: dict = None):
        self.session = requests.Session()
        self.headers = headers

    def get(self, url):
        response = self.session.get(url, headers=self.headers)
        response.connection.close()
        return response

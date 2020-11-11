class KinopoiskError(Exception):
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return f"КиноПоиск said '{self.error}'"


class CaptchaError(Exception):
    def __str__(self):
        return 'Captcha was caught. Please change cookies.'


class ConnectionError(Exception):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return f'Failed to connect to {self.url}'


class DBConnectionError(Exception):
    def __str__(self):
        return 'Failed to connect to MongoDB'

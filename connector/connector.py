import traceback

from http import HTTPStatus
from lxml.html import fromstring
from pymongo import MongoClient
from time import sleep
from typing import Union

from .errors import CaptchaError, ConnectionError, DBConnectionError, KinopoiskError
from .insert_buffer import InsertBuffer
from .request import Request


KINOPOISK_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/85.0.4183.121 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru,en-us;q=0.7,en;q=0.3',
        'Accept-Encoding': 'deflate',
        'Accept-Charset': 'windows-1251,utf-8;q=0.7,*;q=0.7',
        'Keep-Alive': '300',
        'Connection': 'keep-alive',
        'Referer': 'http://www.kinopoisk.ru/',
        'Cookie': 'user-geo-region-id=2; user-geo-country-id=2; desktop_session_key=a879d130e3adf0339260b581e66d773df11'
                  'd8e9d3c7ea1053a6a7b473c166afff28b4d6c3e80e91249baaa7f3c3e90ef898a714ba131694d595c6a4f7e8f6df19d46c31'
                  'ce10d2837ff5ad61d138aefd65c01aa7acc1327ce6d0918deae0a3c71; '
                  'desktop_session_key.sig=drC4D-uw685k9LLTsxPhIFVyLFY; '
                  'i=Hn0YWarMxO/96XpUg9b7btBjrSjo+ItWSfeOXC4oUOtwp6TEcbOkk/ajoJbz1xD/0dPkdWRcJTTk3x1/kZ09uNlji8g=; '
                  'mda_exp_enabled=1; sso_status=sso.passport.yandex.ru:blocked; yandex_plus_metrika_cookie=true; '
                  '_ym_wasSynced=%7B%22time%22%3A1604668139580%2C%22params%22%3A%7B%22eu%22%3A0%7D%2C%22bkParams%22%3A%'
                  '7B%7D%7D; gdpr=0; _ym_uid=1604668140171070080; _ym_d=1604668140; mda=0; _ym_isad=1; '
                  '_ym_visorc_56177992=b; _ym_visorc_52332406=b; _ym_visorc_22663942=b; location=1',
    }

BUFFER_SIZE = 10

REVIEW_COUNTS = {
    'all': ('reviewAllCount', int),
    'pos': ('reviewPositiveCount', int),
    'neg': ('reviewNegativeCount', int),
    'neut': ('reviewNeutralCount', int),
    'perc': ('reviewAllPositiveRatio', str)
}


def parse_reviews_page(reviews_page):
    if reviews_page is None:
        reviews_data = {value[0]: value[1]() for value in REVIEW_COUNTS.values()}
    else:
        counts = {elem.attrib['class']: elem.xpath(".//b/text()")[0]
                  for elem in reviews_page.xpath("//ul[@class='resp_type']/li")}
        reviews_data = {value[0]: value[1](counts.get(count, value[1]())) for count, value in REVIEW_COUNTS.items()}
    return reviews_data


class Connector:
    def __init__(self, api_key: str, database: str, username: Union[str, None] = None,
                 password: Union[str, None] = None, host: str = 'localhost', port: Union[int, str] = 27017,
                 authentication_database: Union[str, None] = None, sorting: Union[str, None] = None):
        self.api_key = api_key
        self.username = username
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.authentication_database = authentication_database
        self.sorting = sorting
        self.is_log = False
        self.kinopoisk_request = Request(KINOPOISK_HEADERS)
        self.api_request = Request({'X-API-KEY': self.api_key})
        self.db = None
        self.film_buffer = None
        self.start_film_page = self.end_film_page = self.start_film = self.end_film = None
        self.current_film_page = self.current_film = None
        self.pages_count = None
        self.film_ids = None

        self._check_fields()

    def _check_fields(self):
        field_types = {
            'api_key': [str],
            'username': [str, type(None)],
            'password': [str, type(None)],
            'database': [str],
            'host': [str],
            'port': [int, str],
            'authentication_database': [str, type(None)],
            'sorting': [str, type(None)]
        }
        for field, types in field_types.items():
            value = getattr(self, field)
            if not any(isinstance(value, type_) for type_ in types):
                raise TypeError(f"Field '{field}' must have type{'s' if len(types) > 1 else ''} "
                                f"{', '.join(map(lambda type_: type_.__name__, types[:-1]))}"
                                f"{' or ' if len(types) > 1 else ''}{types[-1].__name__}, not {type(value).__name__}")
        if self.username is not None and self.password is None:
            raise TypeError(f"Field 'password' must have type str, not NoneType")

    def _init_database(self, is_clear_database):
        uri = 'mongodb://'
        if self.username is not None:
            uri += f'{self.username}:{self.password}@'
        uri += f'{self.host}:{self.port}/{self.database}'
        if self.authentication_database is not None:
            uri += f'?authSource={self.authentication_database}'
        client = MongoClient(uri)
        self.db = client.get_database()

        try:
            collections = self.db.collection_names()
            if 'films' in collections and is_clear_database:
                self.db.films.delete_many({})
            elif 'films' not in collections:
                self.db.create_collection('films')
        except Exception:
            raise DBConnectionError('collection initialization failed') from None
        try:
            self.film_ids = set()
            if not is_clear_database and 'films' in collections:
                self.film_ids = set(film['data']['filmId'] for film in self.db.films.find())
        except Exception:
            raise DBConnectionError('data from database initialization failed') from None
        self.film_buffer = InsertBuffer(self.db.films, BUFFER_SIZE, self._update_log)

    def _make_api_request(self, url, _depth=0):
        response = self.api_request.get(url)
        if response.status_code == HTTPStatus.OK:
            return response.json()
        elif response.status_code == HTTPStatus.UNAUTHORIZED:
            raise ValueError('Wrong API key')
        elif response.status_code == HTTPStatus.NOT_FOUND:
            print(f"Can't find information by url {url}")
            return None
        elif response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            if _depth == 0:
                sleep(1)
                return self._make_api_request(url, 1)
            else:
                raise Exception('Unknown error with count of requests')
        else:
            raise Exception(f'Undocumented error: {response.status_code}; {response.text}')

    def _make_kinopoisk_request(self, url):
        response = self.kinopoisk_request.get(url)
        if response.status_code == HTTPStatus.OK:
            content = response.content.decode(response.encoding)
            if 'captcha' in content:
                raise CaptchaError
            page = fromstring(content)
            errors = page.xpath("//h1[@class='error-message__title']")
            if len(errors) > 0:
                raise KinopoiskError(errors[0].text)
            return page
        elif response.status_code == HTTPStatus.NOT_FOUND:
            print(f'Page {url} not found')
            return None
        else:
            raise Exception(f'Unknown error: {response.status_code}; {response.text}')

    def _get_film_id_from_kinopoisk(self):
        self.current_film_page = self.start_film_page
        request_url = 'https://www.kinopoisk.ru/lists/navigator/?page=%s&quick_filters=films&tab=all'
        if self.sorting is not None:
            request_url += f'&sort={self.sorting}'
        while True:
            self.current_film = self.start_film if self.current_film_page == self.start_film_page else 1
            films_page = self._make_kinopoisk_request(request_url % self.current_film_page)
            if films_page is None:
                break

            self.pages_count = int(films_page.xpath("//a[@class='paginator__page-number']/text()")[-1])
            films_links = films_page.xpath("//a[@class='selection-film-item-meta__link']/@href")
            films_count = len(films_links)

            if self.current_film_page == self.end_film_page or self.current_film_page == self.pages_count:
                for i, film_link in enumerate(films_links[:self.end_film + 1]):
                    self.current_film = i + 1
                    film_id = int(film_link.replace('/', ' ').strip().split()[-1])
                    self._update_log(f'page: {self.current_film_page}/{self.pages_count}; '
                                     f'film: {self.current_film}/{films_count}; filmId: {film_id}')
                    yield film_id
                break
            else:
                start_film = self.current_film
                for i, film_link in enumerate(films_links[start_film - 1:]):
                    self.current_film = i + start_film
                    film_id = int(film_link.replace('/', ' ').strip().split()[-1])
                    self._update_log(f'page: {self.current_film_page}/{self.pages_count}; '
                                     f'film: {self.current_film}/{films_count}; filmId: {film_id}')
                    yield film_id

            self.current_film_page += 1

    def _get_film_persons(self, film_id):
        film_persons = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={film_id}')
        if film_persons is None:
            return []
        return [{'filmId': film_id, 'personId': film_person['staffId'], 'nameRu': film_person['nameRu'],
                 'nameEn': film_person['nameEn'], 'description': film_person['description'],
                 'professionText': film_person['professionText'], 'professionKey': film_person['professionKey']}
                for film_person in film_persons]

    def _get_film(self, film_id):
        film_data = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v2.1/films/{film_id}'
                                           f'?append_to_response=BUDGET&append_to_response=RATING')
        if film_data is None:
            raise Exception(f"Can't find information about film {film_id}")
        film_data['data'].pop('facts')
        reviews_page = self._make_kinopoisk_request(f'https://www.kinopoisk.ru/film/{film_id}/reviews/')
        film_data['review'] = parse_reviews_page(reviews_page)
        return film_data

    def _update_log(self, log_message):
        if self.is_log:
            print(log_message)

    def _process_db_connection_error(self, buffer_size=BUFFER_SIZE):
        # does not take into account unexpected repetitions of films
        successful_films = 30 * (self.current_film_page - self.start_film_page) - self.start_film + 1 \
                           + self.current_film - buffer_size
        previous_films = 30 * (self.start_film_page - 1) + self.start_film - 1
        all_films = successful_films + previous_films
        last_successful_page = all_films // 30
        last_successful_film = all_films % 30
        if successful_films == 0:
            self._update_log('No successful inserts in database')
        elif last_successful_film > 0:
            self._update_log(f'Last successful insert in database: page {last_successful_page + 1}, '
                             f'film {last_successful_film} ({successful_films} films)')
        else:
            self._update_log(f'Last successful insert in database: page {last_successful_page}, film 30 '
                             f'({successful_films} films)')

    def _flush_buffer(self):
        try:
            self.film_buffer.flush()
        except DBConnectionError as exc:
            self._process_db_connection_error(len(self.film_buffer))
            raise exc from None

    def _connect(self, start_film_page, end_film_page, start_film, end_film):
        self.start_film_page = start_film_page
        self.end_film_page = end_film_page
        self.start_film = start_film
        self.end_film = end_film

        try:
            for film_id in self._get_film_id_from_kinopoisk():
                if film_id in self.film_ids:
                    self._update_log(f'film {film_id} has been already gotten')
                    continue

                film_data = self._get_film(film_id)
                film_persons_data = self._get_film_persons(film_id)
                film_data['persons'] = film_persons_data

                self.film_buffer.add(film_data)
                self.film_ids.add(film_id)
        except DBConnectionError as exc:
            self._process_db_connection_error()
            raise exc from None
        except (ConnectionError, ValueError, CaptchaError, KinopoiskError, Exception) as exc:
            self._flush_buffer()
            raise exc from None

        self._flush_buffer()

    def connect(self, start_film_page: int = 1, end_film_page: Union[int, None] = None, start_film: int = 1,
                end_film: int = 30, is_clear_database: bool = True, is_log: bool = False):
        if is_log is not None and isinstance(is_log, bool):
            self.is_log = is_log

        self._init_database(is_clear_database)

        start_film_page = max(start_film_page, 1)
        end_film_page = end_film_page
        start_film = max(start_film, 1)
        end_film = min(max(end_film, 1), 30)
        if end_film_page is not None and start_film_page > end_film_page:
            return

        while True:
            try:
                self._connect(start_film_page, end_film_page, start_film, end_film)
                break
            except DBConnectionError:
                raise
            except KinopoiskError:
                if self.is_log:
                    traceback.print_exc()
                start_film_page = self.current_film_page
                start_film = self.current_film
                sleep(1)
            except (ConnectionError, ValueError, CaptchaError, Exception):
                raise

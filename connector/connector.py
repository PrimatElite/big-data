import traceback

from http import HTTPStatus
from lxml.html import fromstring
from pymongo import MongoClient
from time import sleep
from tqdm import tqdm
from typing import Union

from .errors import CaptchaError, ConnectionError, DBConnectionError, KinopoiskError
from .insert_buffer import InsertBuffer
from .request import Request


_KINOPOISK_HEADERS = {
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

_BUFFER_SIZE = 30

_REVIEW_COUNTS = {
    'all': ('reviewAllCount', int),
    'pos': ('reviewPositiveCount', int),
    'neg': ('reviewNegativeCount', int),
    'neut': ('reviewNeutralCount', int),
    'perc': ('reviewAllPositiveRatio', str)
}

FILMS_PER_PAGE = 50


def _parse_reviews_page(reviews_page):
    if reviews_page is None:
        reviews_data = {value[0]: value[1]() for value in _REVIEW_COUNTS.values()}
    else:
        counts = {elem.attrib['class']: elem.xpath(".//b/text()")[0]
                  for elem in reviews_page.xpath("//ul[@class='resp_type']/li")}
        reviews_data = {value[0]: value[1](counts.get(count, value[1]())) for count, value in _REVIEW_COUNTS.items()}
    return reviews_data


class Connector:
    def __init__(self, api_key: str, database: str, username: Union[str, None] = None,
                 password: Union[str, None] = None, host: str = 'localhost', port: Union[int, str] = 27017,
                 authentication_database: Union[str, None] = None, sorting: Union[str, None] = None):
        self._api_key = api_key
        self._username = username
        self._password = password
        self._database = database
        self._host = host
        self._port = port
        self._authentication_database = authentication_database
        self._sorting = sorting
        self._log_file = None
        self._kinopoisk_request = Request(_KINOPOISK_HEADERS)
        self._api_request = Request({'X-API-KEY': self._api_key})
        self._db = None
        self._film_buffer = None
        self._start_film_page = self._end_film_page = self._start_film = self._end_film = None
        self._current_film_page = self._current_film = None
        self._pages_count = None
        self._films_ids = None

        self._check_fields()

    @property
    def current_film_page(self):
        return self._current_film_page

    @property
    def current_film(self):
        return self._current_film

    def _check_fields(self):
        field_types = {
            '_api_key': [str],
            '_username': [str, type(None)],
            '_password': [str, type(None)],
            '_database': [str],
            '_host': [str],
            '_port': [int, str],
            '_authentication_database': [str, type(None)],
            '_sorting': [str, type(None)]
        }
        for field, types in field_types.items():
            value = getattr(self, field)
            if not any(isinstance(value, type_) for type_ in types):
                raise TypeError(f"Field '{field[1:]}' must have type{'s' if len(types) > 1 else ''} "
                                f"{', '.join(map(lambda type_: type_.__name__, types[:-1]))}"
                                f"{' or ' if len(types) > 1 else ''}{types[-1].__name__}, not {type(value).__name__}")
        if self._username is not None and self._password is None:
            raise TypeError(f"Field 'password' must have type str, not NoneType")

    def _init_database(self, is_clear_database):
        uri = 'mongodb://'
        if self._username is not None:
            uri += f'{self._username}:{self._password}@'
        uri += f'{self._host}:{self._port}/{self._database}'
        if self._authentication_database is not None:
            uri += f'?authSource={self._authentication_database}'
        client = MongoClient(uri)
        self._db = client.get_database()

        try:
            collections = self._db.collection_names()
            if 'films' in collections and is_clear_database:
                self._db.films.delete_many({})
            elif 'films' not in collections:
                self._db.create_collection('films')
        except Exception:
            raise DBConnectionError('collection initialization failed') from None
        try:
            self._films_ids = set()
            if not is_clear_database and 'films' in collections:
                self._films_ids = set(film['data']['filmId'] for film in self._db.films.find())
        except Exception:
            raise DBConnectionError('data from database initialization failed') from None
        self._film_buffer = InsertBuffer(self._db.films, _BUFFER_SIZE, self._update_log)

    def _make_api_request(self, url, _depth=0):
        response = self._api_request.get(url)
        if response.status_code == HTTPStatus.OK:
            return response.json()
        elif response.status_code == HTTPStatus.UNAUTHORIZED:
            raise ValueError('Wrong API key')
        elif response.status_code == HTTPStatus.NOT_FOUND:
            self._update_log(f"Can't find information by url {url}")
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
        response = self._kinopoisk_request.get(url)
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
            self._update_log(f'Page {url} not found')
            return None
        else:
            raise Exception(f'Unknown error: {response.status_code}; {response.text}')

    def _get_film_id_from_kinopoisk(self):
        self._current_film_page = self._start_film_page
        request_url = 'https://www.kinopoisk.ru/lists/navigator/?page=%s&quick_filters=films&tab=all'
        if self._sorting is not None:
            request_url += f'&sort={self._sorting}'
        while True:
            self._current_film = self._start_film if self._current_film_page == self._start_film_page else 1
            films_page = self._make_kinopoisk_request(request_url % self._current_film_page)
            if films_page is None:
                break

            self._pages_count = int(films_page.xpath("//a[@class='paginator__page-number']/text()")[-1])
            films_links = films_page.xpath("//a[@class='selection-film-item-meta__link']/@href")
            films_count = len(films_links)

            is_end = self._current_film_page == self._end_film_page or self._current_film_page == self._pages_count
            start_film = self._current_film
            end_film = self._end_film if is_end else films_count
            bar_desc = f'page: {self._current_film_page}/{self._pages_count}'
            bar = tqdm(films_links[start_film - 1:end_film + 1], initial=start_film - 1, ascii=True,
                       total=end_film - start_film + 1, desc=bar_desc)
            for i, film_link in enumerate(bar):
                self._current_film = i + start_film
                film_id = int(film_link.replace('/', ' ').strip().split()[-1])
                bar.set_description(f'{bar_desc}; filmId: {film_id}')
                self._update_log(f'page: {self._current_film_page}/{self._pages_count}; '
                                 f'film: {self._current_film}/{films_count}; filmId: {film_id}')
                yield film_id

            if is_end:
                break
            self._current_film_page += 1

    def _get_film_persons(self, film_id):
        film_persons = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={film_id}')
        if film_persons is None:
            return []
        self._update_log('film persons were got')
        return [{'filmId': film_id, 'personId': film_person['staffId'], 'nameRu': film_person['nameRu'],
                 'nameEn': film_person['nameEn'], 'description': film_person['description'],
                 'professionText': film_person['professionText'], 'professionKey': film_person['professionKey'],
                 'number': i + 1}
                for i, film_person in enumerate(film_persons)]

    def _get_film(self, film_id):
        film_data = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v2.1/films/{film_id}'
                                           f'?append_to_response=BUDGET&append_to_response=RATING')
        if film_data is None:
            self._update_log(f"Can't find information about film {film_id}")
            return None
        film_data['data'].pop('facts')
        reviews_page = self._make_kinopoisk_request(f'https://www.kinopoisk.ru/film/{film_id}/reviews/')
        film_data['review'] = _parse_reviews_page(reviews_page)
        self._update_log('film was got')
        return film_data

    def _update_log(self, log_message):
        if self._log_file is not None:
            print(log_message, file=self._log_file, flush=True)

    def _process_db_connection_error(self, buffer_size=_BUFFER_SIZE):
        # does not take into account unexpected repetitions and skips of films
        successful_films = FILMS_PER_PAGE * (self._current_film_page - self._start_film_page) - self._start_film + 1 \
                           + self._current_film - buffer_size
        previous_films = FILMS_PER_PAGE * (self._start_film_page - 1) + self._start_film - 1
        all_films = successful_films + previous_films
        last_successful_page = all_films // FILMS_PER_PAGE
        last_successful_film = all_films % FILMS_PER_PAGE
        if successful_films == 0:
            self._update_log('No successful inserts in database')
        elif last_successful_film > 0:
            self._update_log(f'Last successful insert in database: page {last_successful_page + 1}, '
                             f'film {last_successful_film} ({successful_films} films)')
        else:
            self._update_log(f'Last successful insert in database: page {last_successful_page}, film {FILMS_PER_PAGE} '
                             f'({successful_films} films)')

    def _flush_buffer(self):
        try:
            self._film_buffer.flush()
        except DBConnectionError as exc:
            self._process_db_connection_error(len(self._film_buffer))
            raise exc from None

    def _close_log_file(self):
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None

    def _connect(self, start_film_page, end_film_page, start_film, end_film):
        self._start_film_page = start_film_page
        self._end_film_page = end_film_page
        self._start_film = start_film
        self._end_film = end_film

        try:
            for film_id in self._get_film_id_from_kinopoisk():
                if film_id in self._films_ids:
                    self._update_log(f'film {film_id} has been already gotten')
                    continue

                film_data = self._get_film(film_id)
                if film_data is None:
                    continue
                film_persons_data = self._get_film_persons(film_id)
                film_data['persons'] = film_persons_data

                self._film_buffer.add(film_data)
                self._films_ids.add(film_id)
        except DBConnectionError as exc:
            self._process_db_connection_error()
            raise exc from None
        except (ConnectionError, ValueError, CaptchaError, KinopoiskError, KeyboardInterrupt, Exception) as exc:
            self._flush_buffer()
            raise exc from None

        self._flush_buffer()

    def connect(self, start_film_page: int = 1, end_film_page: Union[int, None] = None, start_film: int = 1,
                end_film: int = FILMS_PER_PAGE, is_clear_database: bool = True, log_file_path: Union[str, None] = None):
        if log_file_path is not None:
            self._log_file = open(log_file_path, 'w')
        else:
            self._log_file = None

        self._init_database(is_clear_database)

        start_film_page = max(start_film_page, 1)
        end_film_page = end_film_page
        start_film = max(start_film, 1)
        end_film = min(max(end_film, 1), FILMS_PER_PAGE)
        if end_film_page is not None and start_film_page > end_film_page:
            return

        while True:
            try:
                self._connect(start_film_page, end_film_page, start_film, end_film)
                self._close_log_file()
                break
            except DBConnectionError:
                self._close_log_file()
                raise
            except KinopoiskError:
                traceback.print_exc()
                if self._log_file is not None:
                    traceback.print_exc(file=self._log_file)
                start_film_page = self._current_film_page
                start_film = self._current_film
                sleep(1)
            except (ConnectionError, ValueError, CaptchaError, KeyboardInterrupt, Exception):
                self._close_log_file()
                raise

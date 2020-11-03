import json

from http import HTTPStatus
from lxml.html import fromstring
from pymongo import MongoClient
from time import sleep
from typing import Union

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
        'Cookie': 'i=UJB/wXq9g5QUeC3V4ga6xImWZhQzn+sybEgbPBXXl5hqijoTvHzFiPltVGANzLFa8ra9xDdWJvcHv+YHbirtSSMOkv8=; '
                  'mda_exp_enabled=1; desktop_session_key=87241c4c1f0d3fd339e153a4cb126489a731659f67924414d3a7ad1b45b0d'
                  '6dbfb24f35b72876f2aa5b3f393a932a29a90701a00ae7d33b22b309f34ba0bb6fd6705e70df019e49785624bceef01de24b'
                  '9d04bda885ba3b072d73e22601941f4; desktop_session_key.sig=tfPfnoBSyqYBW_8L5P3aCq4lA8I; location=1; '
                  'user-geo-region-id=2; user-geo-country-id=2; yandex_plus_metrika_cookie=true; '
                  'sso_status=sso.passport.yandex.ru:synchronized_no_beacon; gdpr=0; _ym_uid=1604431828152939832; '
                  '_ym_d=1604431828; mda=0; _ym_visorc_52332406=w; _ym_visorc_237742=w; _ym_isad=1; '
                  '_ym_visorc_56177992=w; _ym_visorc_22663942=w; _ym_visorc_31713861=b; kpunk=1',
    }


class Connector:
    def __init__(self, api_key: str, username: str, password: str, database: str, host: str = 'localhost',
                 port: Union[int, str] = 27017, authentication_database: str = 'admin'):
        self.api_key = api_key
        self.username = username
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.authentication_database = authentication_database
        self.is_log = False
        self.kinopoisk_request = Request(KINOPOISK_HEADERS)
        self.api_request = Request({'X-API-KEY': self.api_key})
        self.db = None

        self._check_fields()

    def _check_fields(self):
        field_types = {
            'api_key': [str],
            'username': [str],
            'password': [str],
            'database': [str],
            'host': [str],
            'port': [int, str],
            'authentication_database': [str]
        }
        for field, types in field_types.items():
            value = getattr(self, field)
            if not any(isinstance(value, type_) for type_ in types):
                raise TypeError(f"Field '{field}' must have type{'s' if len(types) > 1 else ''} "
                                f"{', '.join(map(lambda type_: type_.__name__, types[:-1]))}"
                                f"{' or ' if len(types) > 1 else ''}{types[-1].__name__}, not {type(value).__name__}")

    def _init_database(self):
        uri = f'mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}' \
              f'?authSource={self.authentication_database}'
        client = MongoClient(uri)
        self.db = client.get_database()

        collections = self.db.collection_names()
        for collection in ['films', 'persons', 'film_person', 'reviews']:
            if collection in collections:
                self.db.drop_collection(collection)
            self.db.create_collection(collection)

    def _make_api_request(self, url, _depth=0):
        response = self.api_request.get(url)
        if response.status_code == HTTPStatus.OK:
            return response.json()
        elif response.status_code == HTTPStatus.UNAUTHORIZED:
            raise ValueError('Wrong API key')
        elif response.status_code == HTTPStatus.NOT_FOUND:
            raise ValueError(f"Can't find information by url '{url}'")
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
                raise Exception('Oh, captcha')
            return fromstring(content)
        else:
            raise Exception(f'Unknown error: {response.status_code}; {response.text}')

    def _get_film_id_from_kinopoisk(self):
        page = 1
        while True:
            films_page = self._make_kinopoisk_request(f'https://www.kinopoisk.ru/lists/navigator/?page={page}&tab=all')

            pages_count = int(films_page.xpath("//a[@class='paginator__page-number']/text()")[-1])
            films_count = len(films_page.xpath("//a[@class='selection-film-item-meta__link']/@href"))

            self._update_log(f'page: {page}/{pages_count}')
            for i, film_link in enumerate(films_page.xpath("//a[@class='selection-film-item-meta__link']/@href")):
                self._update_log(f'film: {i + 1}/{films_count}')
                yield int(film_link.replace('/', ' ').strip().split()[-1])

            if page == pages_count:
                break
            page += 1

    def _get_review_id_from_kinopoisk(self, film_id):
        page = 1
        while True:
            reviews_page = self._make_kinopoisk_request(f'https://www.kinopoisk.ru/film/{film_id}/reviews/ord/date'
                                                        f'/status/all/perpage/25/page/{page}/')

            reviews = reviews_page.xpath("//div[@class='reviewItem userReview']/@data-id")
            if len(reviews) == 0:
                self._update_log(f'review page: {page} - no reviews')
                break

            self._update_log(f'review page: {page}')
            for review_data_id in reviews:
                yield int(review_data_id)

            last_page = reviews_page.xpath("//div[@class='navigator']/ul/li")[-1]
            if 'class' not in last_page.attrib:
                break
            page += 1

    def _connect_film_reviews(self, film_id):
        reviews = []
        review_ids = set()
        for review_id in self._get_review_id_from_kinopoisk(film_id):
            if review_id in review_ids:
                continue
            review_data = self._make_api_request(f"https://kinopoiskapiunofficial.tech/api/v1/reviews/details"
                                                 f"?reviewId={review_id}")
            review_data['filmId'] = film_id
            reviews.append(review_data)
            if len(reviews) > 0 and len(reviews) % 100 == 0:
                self.db.reviews.insert_many(reviews)
                reviews = []
            review_ids.add(review_id)
        if len(reviews) > 0:
            self.db.reviews.insert_many(reviews)

    def _connect_person(self, person_id):
        person_data = self._make_api_request(f"https://kinopoiskapiunofficial.tech/api/v1/staff/{person_id}")
        person_data.pop('films')
        self.db.persons.insert_one(person_data)

    def _connect_film_persons(self, film_id, persons):
        film_persons = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={film_id}')
        film_persons_ = []
        film_person_ids = set()
        for film_person in film_persons:
            if film_person['staffId'] in film_person_ids:
                continue
            if film_person['staffId'] not in persons:
                self._connect_person(film_person['staffId'])
                persons.add(film_person['staffId'])

            film_person_data = {'filmId': film_id, 'personId': film_person['staffId']}
            film_person_data.update({field: film_person[field]
                                     for field in ['description', 'professionText', 'professionKey']})
            film_persons_.append(film_person_data)
            film_person_ids.add(film_person['staffId'])
        self.db.film_person.insert_many(film_persons_)

    def _connect_film(self, film_id):
        film_data = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v2.1/films/{film_id}'
                                           f'?append_to_response=BUDGET&append_to_response=RATING')
        self.db.films.insert_one(film_data)

    def _update_log(self, log_message):
        if self.is_log:
            print(log_message)

    def connect(self, is_log: bool = False):
        self._init_database()
        if is_log is not None and isinstance(is_log, bool):
            self.is_log = is_log

        persons = set()
        films = set()
        for film_id in self._get_film_id_from_kinopoisk():
            if film_id in films:
                continue
            self._update_log(f'filmId: {film_id}')
            self._connect_film(film_id)
            self._update_log('film was connected')

            self._connect_film_reviews(film_id)
            self._update_log('reviews were connected')

            self._connect_film_persons(film_id, persons)
            self._update_log('persons were connected')
            films.add(film_id)

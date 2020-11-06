from http import HTTPStatus
from lxml.html import fromstring
from pymongo import MongoClient
from time import sleep
from typing import Union

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

BUFFER_SIZES = {
    'films': 5,
    'persons': 75,
    'film_person': 100,
    'reviews': 100
}

REVIEW_TYPES = {
    'good': 'POSITIVE',
    'bad': 'NEGATIVE',
    'neutral': 'NEUTRAL'
}

MONTHS = {
    'января': '01',
    'февраля': '02',
    'марта': '03',
    'апреля': '04',
    'мая': '05',
    'июня': '06',
    'июля': '07',
    'августа': '08',
    'сентября': '09',
    'октября': '10',
    'ноября': '11',
    'декабря': '12'
}


def parse_date(date_time):
    date, time_ = date_time.split(' | ')
    day, month, year = date.split()
    return f'{year}-{MONTHS[month]}-{day}T{time_}:00'


def parse_review(review_obj):
    review_data = {'reviewId': int(review_obj.attrib['data-id'])}
    review_elem = review_obj.xpath(f".//div[@id='div_review_{review_data['reviewId']}']")[0]
    review_data['reviewType'] = REVIEW_TYPES[review_elem.attrib['class'].split()[1]]
    review_data['reviewDate'] = parse_date(str(review_elem.xpath(".//span[@class='date']/text()")[0]))
    review_votes = review_elem.xpath(f".//ul[@class='useful']/li[@id='comment_num_vote_{review_data['reviewId']}']"
                                     f"/text()")[0]
    review_data['userPositiveRating'], review_data['userNegativeRating'] = list(map(int, review_votes.split(' / ')))
    review_meta = review_elem.xpath(f".//div[@itemprop='author']/div")[0]
    review_data['reviewAuthor'] = review_meta.xpath(f".//p[@class='profile_name']/a")[0].text or ''
    review_data['reviewTitle'] = review_meta.xpath(f".//p[@class='sub_title']")[0].text or ''
    # review_data['reviewDescription'] = ''.join(review_elem.xpath(f".//span[@itemprop='reviewBody']/text()"))
    return review_data


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
        self.buffers = {}

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
            self.buffers[collection] = InsertBuffer(self.db.get_collection(collection), BUFFER_SIZES[collection],
                                                    self._update_log)

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

    def _get_reviews_page_from_kinopoisk(self, film_id):
        page = 1
        while True:
            reviews_page = self._make_kinopoisk_request(f'https://www.kinopoisk.ru/film/{film_id}/reviews/ord/date'
                                                        f'/status/all/perpage/200/page/{page}/')

            reviews = reviews_page.xpath("//div[@class='reviewItem userReview']/@data-id")
            if len(reviews) == 0:
                self._update_log(f'review page: {page} - no reviews')
                break

            self._update_log(f'review page: {page}')
            yield reviews_page

            navigator_list = reviews_page.xpath("//div[@class='navigator']/ul/li")
            if len(navigator_list) == 0 or 'class' not in navigator_list[-1].attrib:
                break
            page += 1

    def _connect_film_reviews(self, film_id):
        review_ids = set()
        for reviews_page in self._get_reviews_page_from_kinopoisk(film_id):
            for review_obj in reviews_page.xpath("//div[@class='reviewItem userReview']"):
                review_data = parse_review(review_obj)
                if review_data['reviewId'] in review_ids:
                    continue
                review_data['filmId'] = film_id
                self.buffers['reviews'].add(review_data)
                review_ids.add(review_data['reviewId'])

    def _connect_person(self, person_id):
        person_data = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v1/staff/{person_id}')
        person_data.pop('films')
        self.buffers['persons'].add(person_data)

    def _connect_film_persons(self, film_id, person_ids):
        film_persons = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={film_id}')
        for film_person in film_persons:
            if film_person['staffId'] not in person_ids:
                self._connect_person(film_person['staffId'])
                person_ids.add(film_person['staffId'])

            film_person_data = {'filmId': film_id, 'personId': film_person['staffId']}
            film_person_data.update({field: film_person[field]
                                     for field in ['description', 'professionText', 'professionKey']})
            self.buffers['film_person'].add(film_person_data)

    def _connect_film(self, film_id):
        film_data = self._make_api_request(f'https://kinopoiskapiunofficial.tech/api/v2.1/films/{film_id}'
                                           f'?append_to_response=BUDGET&append_to_response=RATING')
        self.buffers['films'].add(film_data)

    def _update_log(self, log_message):
        if self.is_log:
            print(log_message)

    def connect(self, is_log: bool = False):
        if is_log is not None and isinstance(is_log, bool):
            self.is_log = is_log

        self._init_database()

        person_ids = set()
        film_ids = set()
        for film_id in self._get_film_id_from_kinopoisk():
            if film_id in film_ids:
                continue
            self._update_log(f'filmId: {film_id}')
            self._connect_film(film_id)
            self._update_log('film was connected')

            self._connect_film_reviews(film_id)
            self._update_log('reviews were connected')

            self._connect_film_persons(film_id, person_ids)
            self._update_log('persons were connected')
            film_ids.add(film_id)

        for buffer in self.buffers.values():
            buffer.flush()

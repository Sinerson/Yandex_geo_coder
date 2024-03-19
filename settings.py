import logging
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DbSecrets:
    """ Класс содержит переменные для строки подключения к БД """
    driver: str = os.getenv("DRIVER")
    server: str = os.getenv("SERVER")
    port: str = os.getenv("PORT")
    db_name: str = os.getenv("DB_NAME")
    user: str = os.getenv("USER")
    password: str = os.getenv("PASSW")
    lang: str = os.getenv("LANGUAGE")
    cn_lifetime: str = os.getenv("CONN_LIFETIME")
    idle: str = os.getenv("IDLE")
    autocommit: str = os.getenv("AUTOCOMMIT")
    hostname: str = os.getenv("CLIENT_HOST_NAME_DEV")
    # hostname: str = os.getenv("CLIENT_HOST_NAME_PROD")
    proc_name: str = os.getenv("CLIENT_HOST_PROC")
    app_name: str = os.getenv("APPLICATION_NAME_DEV")
    # app_name: str = os.getenv("APPLICATION_NAME_PROD")


@dataclass
class GeoCoders:
    yandex_geocoder: str = os.getenv("YandexGeocoder_token")


@dataclass
class ExternalLinks:
    """ Класс содержит одну переменную, со ссылкой на документ Google Spreadsheet """
    marketing_doc_link: str = os.getenv("MARKETING_ACTION_LINK")


@dataclass
class FilesPath:
    full_address_file_name = 'inner_datasets\svyazist_full_addresses.geojson'


@dataclass
class JwtSecret:
    jwt_secret: str = os.getenv("JWT_KEY")

import sys, os, logging, subprocess
from db.sybase import DbConnection
from queries.all_addresses_code import all_addr
from queries.geocoded_addresses import existing_addresses
from queries.insert_new_record import addr_ins
from yandex_geocoder import Client, YandexGeocoderException, NothingFound, InvalidKey
from settings import GeoCoders
from decorators import log_decorator


logging.basicConfig(level=logging.DEBUG,
                    filename='log.txt',
                    filemode='a',
                    format="%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
                    datefmt='%H:%M:%S')

yandex_client = Client(GeoCoders.yandex_geocoder)

all_addresses_list: list[dict] = []
geocoded_addresses: list = []

exist_cnt: int = 0
added_cnt: int = 0
notfound_cnt: int = 0


def create_virtualenv():
    logging.info("Создаю виртуальную среду выполнения")
    subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)


def install_requiremets():
    logging.info("Устанавливаю зависимости")
    subprocess.run(["venv/Scripts/pip", "install", "-r", "requirements.txt"], check=True)


def get_all_addresses() -> None:
    """ Получение списка всех адресов по задданым в условии запроса критериям """
    result = DbConnection.execute_query(all_addr)
    all_addresses_list.extend(result)


def get_geocoded_addresses() -> None:
    """ Список уже геокодированных кодов адреса """
    result = DbConnection.execute_query(existing_addresses)
    geocoded_addresses.extend(result)


def insert_new_address(address_code: int, longitude: float, latitude: float) -> None:
    """ Добавление нового адреса после геокодирования """
    DbConnection.execute_query(addr_ins, address_code, longitude, latitude)


@log_decorator
def geocode() -> any:
    global exist_cnt, added_cnt
    somelist = []
    get_geocoded_addresses()
    get_all_addresses()
    for a in geocoded_addresses:
        somelist.append(a['ADDRESS_CODE'])
    for el in all_addresses_list:
        if el['ADDRESS_CODE'] in somelist:
            exist_cnt += 1
        else:
            addr = f"{el['COUNTRY_NAME']},{el['COUNTRY_REGION_NAME']},{el['AREA_REGION_NAME']},{el['TOWN_NAME']},{el['STREET_NAME']},{el['HOUSE']}"
            try:
                lon, lat = list(map(float, yandex_client.coordinates(addr)))
                insert_new_address(address_code=el['ADDRESS_CODE'],
                                   longitude=lon,
                                   latitude=lat
                                   )
                logging.info(f"Добавили код: {el['ADDRESS_CODE']}")
                added_cnt += 1
            except InvalidKey:

                logging.error(
                    f"API Ключ некорректен. Геокодирование адреса: {el['ADDRESS_CODE']},"
                    f" {el['COUNTRY_NAME']},{el['COUNTRY_REGION_NAME']},{el['AREA_REGION_NAME']},"
                    f"{el['TOWN_NAME']},{el['STREET_NAME']},{el['HOUSE']}")
            except NothingFound:
                global notfound_cnt
                notfound_cnt += 1
                logging.warning(
                    f"Ничего не нашли для: {el['ADDRESS_CODE']}({el['COUNTRY_NAME']},{el['COUNTRY_REGION_NAME']},"
                    f"{el['AREA_REGION_NAME']},{el['TOWN_NAME']},{el['STREET_NAME']},{el['HOUSE']})")
            except YandexGeocoderException as y:
                logging.error(y)
            # finally:
            #     logging.info(f"Геокодирование завершено")


    # return exist_cnt, added_cnt


async def main():
    pass


if __name__ == '__main__':
    if os.getenv('VIRTUAL_ENV'):
        geocode()
        logging.info(f"Геокодировано: {added_cnt} записей, пропущено: {exist_cnt - notfound_cnt} записей, ошибочных адресов: {notfound_cnt}")
    else:
        logging.info('Running outside venv!')
        create_virtualenv()
        install_requiremets()
        geocode()

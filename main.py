import sys, os, logging, subprocess
from db.sybase import DbConnection
from queries.all_addresses_code import all_addr
from queries.geocoded_addresses import existing_addresses, address_without_post_code, update_post_code
from queries.insert_new_record import addr_ins, update_address_to_unload
from yandex_geocoder import Client, YandexGeocoderException, NothingFound, InvalidKey
from settings import GeoCoders
from decorators import log_decorator
import pickle
from time import sleep


logging.basicConfig(level=logging.DEBUG,
                    filename='log.txt',
                    filemode='a',
                    format="%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
                    datefmt='%Y-%m-%d %H:%M:%S')

yandex_client = Client(GeoCoders.yandex_geocoder)

all_addresses_list, geocoded_addresses = [], []
exist_cnt, added_cnt, notfound_cnt = 0, 0, 0


def create_virtualenv():
    logging.info("Создаю виртуальную среду выполнения")
    subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)


def install_requiremets():
    logging.info("Устанавливаю зависимости")
    subprocess.run(["venv/Scripts/activate.bat"])
    subprocess.run(["venv/Scripts/pip", "install", "-r", "requirements.txt"], check=True)


def get_all_addresses() -> None:
    """ Получение списка всех адресов по заданным в условии запроса критериям """
    result = DbConnection.execute_query(all_addr)
    all_addresses_list.extend(result)


def get_geocoded_addresses() -> None:
    """ Список уже геокодированных кодов адреса """
    result = DbConnection.execute_query(existing_addresses)
    geocoded_addresses.extend(result)


def insert_new_address(address_code: int, longitude: float, latitude: float) -> None:
    """ Добавление нового адреса после геокодирования """
    DbConnection.execute_query(addr_ins, address_code, longitude, latitude)


def get_addresses_wo_index() -> dict:
    """ Получение списка адресов у которых отсутствует почтовый индекс """
    address_list = DbConnection.execute_query(address_without_post_code)
    address_dict = {}
    # Заполним словарь результатом выборки
    for el in address_list:
        address_dict[int(el['ADDRESS_CODE'])] = f"{el['COUNTRY']},{el['REGION_NAME']},{el['AREA_NAME']},{el['TOWN']},{el['STREET']},{el['HOUSE']}"
    # Удалим лишнее из строки адреса
    for k in address_dict:
        address_dict[k] = address_dict[k].replace('None', '').lstrip(',')
    # Вернем нормализованый словарь
    return address_dict


def insert_post_index(index: str, address_code: int) -> any:
    """ Добавление почтового индекса  по address_code в базе"""
    res = DbConnection.execute_query(update_post_code, index, address_code)
    return res


def update_sorm_unload() -> None:
    """ Вызов запроса добавления почтовых индексов из таблицы адресов в таблицу для СОРМ  """
    try:
        res = DbConnection.execute_query(update_address_to_unload)
        logging.info(f"Запуск обновления справочника индексов для СОРМ. Результат: {res}")
    except Exception as e:
        print(e)


@log_decorator
def geocode() -> any:
    global exist_cnt, added_cnt, notfound_cnt
    all_addresses_list.clear()
    geocoded_addresses.clear()
    exist_cnt, added_cnt, notfound_cnt = 0, 0, 0
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
                notfound_cnt += 1
                logging.warning(
                    f"Ничего не нашли для: {el['ADDRESS_CODE']}({el['COUNTRY_NAME']},{el['COUNTRY_REGION_NAME']},"
                    f"{el['AREA_REGION_NAME']},{el['TOWN_NAME']},{el['STREET_NAME']},{el['HOUSE']})")
            except YandexGeocoderException as y:
                logging.error(y)
            # finally:
            #     logging.info(f"Процесс геокодирования завершен")


def post_index_processing() -> tuple:
    found_index: int = 0
    not_found_addr: int = 0
    update_cnt: int = 0
    normalized_addresses = get_addresses_wo_index()
    index_dict = {}

    if os.path.exists('not_founded_addresses.pickle'):
        with open("not_founded_addresses.pickle", "rb") as f:
            old_normalized_addresses = pickle.load(f)
    else:
        old_normalized_addresses = {}

    normalized_addresses = {k: v for k, v in normalized_addresses.items() if old_normalized_addresses.get(k) != v}
    logging.info(f"Обнаружено {len(normalized_addresses)} новых адресов для поиска индекса. Приступаю...\n")

    for addr_code in normalized_addresses:
        try:
            index = yandex_client.index_by_address(normalized_addresses[addr_code])
            index_dict[addr_code] = index
            found_index += 1
            try:
                res = insert_post_index(index=index, address_code=addr_code)
                update_cnt += 1
                # logging.info(f"Добавлен индекс {index} для адреса: {normalized_addresses[addr_code]}")
            except Exception as e:
                logging.error(f"Возникло исключение в блоке получения индекса: {e}, работа программы прекращена")
                break
        except NothingFound:
            # Добавим в словарь не найденные адреса
            old_normalized_addresses[addr_code] = normalized_addresses[addr_code]
            logging.warning(
                f"Адрес не найден или отсутствует индекс в выдаче: ({addr_code}: {normalized_addresses[addr_code]})\n"
            )
            not_found_addr += 1
    # Законсервируем словарь ненайденных адресов
    with open("not_founded_addresses.pickle", "wb") as f:
        pickle.dump(old_normalized_addresses, file=f)
    return found_index, not_found_addr, update_cnt


def main():
    while True:
        geocode()
        logging.info(f"Геокодировано: {added_cnt} , пропущено: {exist_cnt - notfound_cnt}, ошибочных адресов: {notfound_cnt}\n")
        fa, ona, uc = post_index_processing()
        logging.info(f"При поиске индексов найдено: {fa}, не найдено: {ona}, успешно добавлено в БД: {uc}\n")
        update_sorm_unload()
        logging.info("Засыпаю на 1 час\n")
        sleep(3600)


if __name__ == '__main__':
    if os.getenv('VIRTUAL_ENV'):
        main()
    else:
        logging.info('Running outside venv!')
        create_virtualenv()
        install_requiremets()
        # geocode()
        main()

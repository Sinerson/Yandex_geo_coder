import datetime
import logging


def log_decorator(func):
    def wrapper(*args, **kwargs):
        start = datetime.datetime.now()
        result = func(*args, **kwargs)
        stop = datetime.datetime.now()
        delta = stop - start
        logging.info(f"Геокодирование заняло {delta.seconds} секунд")
        return result
    return wrapper

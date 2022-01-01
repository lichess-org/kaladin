import logging
import logging.handlers
import os
import pickle
import pymongo
import sys
import time

from dotenv import dotenv_values
from functools import partial, wraps
from typing import Callable, Dict, Optional

def initialize_logging_for_modules(log: logging.Logger) -> None:
    config = load_config()
    log.setLevel(config['LOGGING_LEVEL'].upper())
    configure_logging(log)

def load_config() -> Dict[str, str]:
    config = {
    **dotenv_values(".env.base"),  # load default development variables
    **dotenv_values(".env"), # load custom variables
    **os.environ,  # override loaded values with environment variables
    }
    return config

def configure_logging(log: logging.Logger) -> None:
    format_string = "%(asctime)s | %(name)s | %(levelname)-8s | %(message)s"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(format_string))
    log.addHandler(handler)

def pickle_me(data, filename):
    directory = filename.rsplit('/', 1)[0]
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(filename, 'wb') as file:
        pickle.dump(data, file, protocol=4)

def unpickle_me(filename):
    with open(filename, 'rb') as file:
        f = pickle.load(file)
        return f

def retry(
    func: Optional[Callable]=None,
    n_tries: Optional[int]=5,
    exception=Exception, 
    delay=30, 
    backoff=2, 
    log: Optional[logging.Logger]=None):
    """Retry decorator with exponential backoff.

    Parameters
    ----------
    func : typing.Callable, optional
        Callable on which the decorator is applied, by default None
    exception : Exception or tuple of Exceptions
        Exception(s) that invoke retry, by default Exception
    n_tries : int, optional
        Number of tries before giving up. If set to `None`, retry indefinitely
    delay : int
        Initial delay between retries in seconds, by default 30
    backoff : int
        Backoff multiplier e.g. value of 2 will double the delay
    log : logging.Logger, optional
        if set to `None`, print

    Returns
    -------
    typing.Callable
        Decorated callable that calls itself when exception(s) occur.

    Examples
    --------
    >>> import random
    >>> @retry(exception=Exception, n_tries=4)
    ... def test_random(text):
    ...    x = random.random()
    ...    if x < 0.5:
    ...        raise Exception("Fail")
    ...    else:
    ...        print("Success: ", text)
    >>> test_random("It works!")
    """

    if func is None:
        return partial(
            retry,
            exception=exception,
            n_tries=n_tries,
            delay=delay,
            backoff=backoff
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        ntries, ndelay = n_tries, delay

        while ntries is None or ntries > 1:
            try:
                return func(*args, **kwargs)
            except exception as e:
                msg = f"In {func.__name__}: {str(e)}, Retrying in {ndelay} seconds..."
                if log is not None:
                    log.warn(msg)
                else:
                    print(msg)
                time.sleep(ndelay)
                if ntries is not None:
                    ntries -= 1
                ndelay *= backoff

        return func(*args, **kwargs)
    return wrapper

# Retry indefinitely mongo operations that failed due to connexion issues
def mongo_retry(log: logging.Logger) -> Callable:
    return partial(retry,
            exception=pymongo.errors.ConnectionFailure,
            delay=30,
            backoff=1,
            n_tries=None,
            log=log
            )

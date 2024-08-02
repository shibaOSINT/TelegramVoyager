import os
import logging
import time
import hashlib

import requests

log_datefmt = os.getenv("DATE_FORMAT", default="%Y-%m-%d %H:%M:%S")
log_formatting = os.getenv("LOG_FORMATING", default="%(levelname)s-[%(asctime)s] [%(thread)d] %(message)s")
log_level = logging.getLevelName(os.getenv("LOG_LEVEL", default="INFO"))

formatter = logging.Formatter(fmt=log_formatting, datefmt=log_datefmt)

file_handler = logging.FileHandler(filename="dispatcher.logs")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(log_level)
stream_handler.setFormatter(formatter)

log = logging.getLogger()
log.setLevel(logging.DEBUG)
log.addHandler(stream_handler)
log.addHandler(file_handler)


def get_next_chan(host, port, wait_flag, relief_time) -> str:
    error_count = 0
    while True:
        try:
            resp = requests.get(f"http://{host}:{port}/next")
            resp.raise_for_status()
            result = resp.text
            if result != wait_flag:
                error_count = 0
                data = resp.json()
                username = str(data)
                log.info(f"Got channel ID = {username}")
                return username
        except requests.exceptions.HTTPError as e:
            log.error(f"Error getting next channel: {e}. Status: {resp.status_code}")
            result = wait_flag
        except requests.exceptions.ConnectionError:
            result = wait_flag
        if result == wait_flag:
            error_count += 1
            total_sleep = relief_time * 2 ** error_count
            log.info(f"Got wait signal - Sleeping for {total_sleep} seconds")
            time.sleep(total_sleep)


def store_chan_username(folder, username):
    filename = hashlib.sha256(username.encode("utf8")).hexdigest() + ".dat"
    with open(os.path.join(folder, filename), 'w') as f:
        f.write(username)


def check_dispatched_channels(folder):
    """Returns the number of channels the dispatcher added to be crawled."""
    total_files = 0
    for fname in os.listdir(folder):
        if fname.endswith(".dat"):
            total_files += 1
    return total_files


host = os.getenv("HOST_CHANNEL", default="localhost")
port = os.getenv("PORT_CHANNEL", default="33445")
wait_flag = os.getenv("WAIT_FLAG", default="wait_pls")
relief_time = int(os.getenv("RELIEF_TIME", default=30))

folder_save = os.getenv("USERNAME_STORAGE_FOLDER", os.path.dirname(os.path.realpath(__file__)))
MAX_CHANNEL_TO_CRAWL = os.getenv("MAX_CHANNEL_TO_CRAWL", 3)
WAIT_TIME = os.getenv("WAIT_TIME", 10)

if __name__ == '__main__':
    log.info("=================================== Dispatcher started ===================================")
    log.debug("Listing ENV var")
    for key, val in os.environ.items():
        log.debug(f"ENV: {key}:{val}")
    while True:
        if check_dispatched_channels(folder=folder_save) >= MAX_CHANNEL_TO_CRAWL:
            log.info("Too many channels to crawl already. Waiting before asking for more.")
            time.sleep(WAIT_TIME)
        else:
            usr = get_next_chan(host=host, port=port, wait_flag=wait_flag, relief_time=relief_time)
            store_chan_username(folder=folder_save, username=usr)

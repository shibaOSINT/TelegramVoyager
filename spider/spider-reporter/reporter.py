import os
import time
import json
import pickle
import logging

import requests

from datachecker import validate_posts, validate_channel_info

log_datefmt = os.getenv("DATE_FORMAT", default="%Y-%m-%d %H:%M:%S")
log_formatting = os.getenv("LOG_FORMATING", default="%(levelname)s-[%(asctime)s] [%(thread)d] %(message)s")
log_level = logging.getLevelName(os.getenv("LOG_LEVEL", default="INFO"))

formatter = logging.Formatter(fmt=log_formatting, datefmt=log_datefmt)

file_handler = logging.FileHandler(filename="reporter.logs")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(log_level)
stream_handler.setFormatter(formatter)

log = logging.getLogger()
log.setLevel(logging.DEBUG)
log.addHandler(stream_handler)
log.addHandler(file_handler)

HOST = os.getenv("HOST_CHANNEL", default="localhost")
PORT = os.getenv("PORT_CHANNEL", default="33445")
DATA_STORAGE_FOLDER = os.getenv("DATA_STORAGE_FOLDER", "../devland/")
DEBUG_MODE_ACTIVE = json.loads(os.getenv("DEBUG_MODE_ACTIVE"))


class Reporter:

    def __init__(self, host: str, port: str, debug_mode_active: bool):
        self.host = host
        self.port = port
        self.debug_mode_active = debug_mode_active

    def save_data(self, data: dict[int: dict]):
        resp = requests.post(url=f"http://{self.host}:{self.port}/save_data", json=data)
        resp.raise_for_status()
        log.debug(f"Sucessfully saved data!")

    def save_data_xposted(self, data: dict):
        resp = requests.post(url=f"http://{self.host}:{self.port}/save_data_xposted", json=data)
        resp.raise_for_status()
        log.debug(f"Sucessfully saved data!")

    def run(self):
        for fname in os.listdir(DATA_STORAGE_FOLDER):
            if not fname.endswith(".pickle"):
                continue
            log.info(f"Found file: {fname}. Saving it!")
            filepath = os.path.join(DATA_STORAGE_FOLDER, fname)
            with open(filepath, 'rb') as f:
                content = f.read()
            content = pickle.loads(content)
            try:
                if filepath.endswith("-channel_info.pickle"):
                    if self.debug_mode_active is True:
                        validate_channel_info(info=content)
                    self.save_data_xposted(data=content)
                else:
                    if self.debug_mode_active is True:
                        validate_posts(posts=content)
                    self.save_data(data=content)
                if self.debug_mode_active is False:
                    log.info(f"{fname} was successfully saved. Deleting it.")
                    os.remove(filepath)
                else:
                    log.info(f"{fname} was successfully saved. Renaming it.")
                    # rename so we don't circle back to them
                    os.rename(filepath, filepath+".processed")
            except requests.HTTPError as err:
                log.error(f"No HTTP200 when saving {fname}.\n"
                          f"Response code: {err.response.status_code}.\n"
                          f"Response text: {err.response.text}")
                time.sleep(3)
            except requests.RequestException as err:
                log.error(f"Error saving {fname}. Error: {err}.")
                log.exception("Traceback")
                time.sleep(3)


if __name__ == '__main__':
    rep = Reporter(host=HOST, port=PORT, debug_mode_active=DEBUG_MODE_ACTIVE)
    log.info("=================================== Reporter started! ===================================")
    while True:
        rep.run()

import os
import re
import json
import time
import pickle
import logging
from collections import defaultdict
from urllib.parse import urlparse

from telegram import Client

log_datefmt = os.getenv("DATE_FORMAT", default="%Y-%m-%d %H:%M:%S")
log_formatting = os.getenv("LOG_FORMATING", default="%(levelname)s-[%(asctime)s] [%(thread)d] %(message)s")
log_level = logging.getLevelName(os.getenv("LOG_LEVEL", default="INFO"))

formatter = logging.Formatter(fmt=log_formatting, datefmt=log_datefmt)

file_handler = logging.FileHandler(filename="crawler.logs")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(log_level)
stream_handler.setFormatter(formatter)

log = logging.getLogger()
log.setLevel(logging.DEBUG)
log.addHandler(stream_handler)
log.addHandler(file_handler)

# We can't allow default value for that, better to fail here
API_ID = os.environ["API_ID"]
API_HASH = os.environ["API_HASH"]
MAX_MSG_CRAWL = int(os.getenv("MAX_MSG_CRAWL"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE"))
DATA_STORAGE_FOLDER = os.getenv("DATA_STORAGE_FOLDER", "../devland/")
USERNAME_STORAGE_FOLDER = os.getenv("USERNAME_STORAGE_FOLDER", "../devland/test_usr_folder/")
ERROR_GETTING_NAME_FLAG = os.getenv("ERROR_GETTING_NAME_FLAG")


class Spider:
    http_url_reg = re.compile(
        r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)")

    @staticmethod
    def _fusion_forward_chan_dict(dict1: defaultdict, dict2: defaultdict):
        """Fuse 2 defaultdict(int) into one"""
        for k, v in dict2.items():
            dict1[k] += v
        return dict1

    def crawl_channel(self, chan_id):
        log.info(f"Getting info on channel: {chan_id}")
        fwd_chan_dict = defaultdict(int)
        client = Client(session_name="Voyager", api_id=API_ID, api_hash=API_HASH, max_msg_crawl=MAX_MSG_CRAWL,
                        chunk_size=CHUNK_SIZE)
        chan_id, title, username, verified, nb_participants = client.get_channel_info(chan_id)
        log.info(f"Crawling channel (chan_id, title, username, verified, nb_participants)"
                 f"{[chan_id, title, username, verified, nb_participants]}")
        for count, chunk in enumerate(client.crawl_channel(chan_id)):
            log.info(f"Processing chunk #{count}")
            processed_posts, forwarded_channels = self._process_posts(chunk, tl_client=client)
            fwd_chan_dict = self._fusion_forward_chan_dict(fwd_chan_dict, forwarded_channels)

            log.info(f"Saving chunk #{count}")
            filename = f"{username}-chunk_{count}.pickle"
            filepath = os.path.join(DATA_STORAGE_FOLDER, filename)
            self._save_processed_info({chan_id: processed_posts}, filepath)
        log.info(f"Finished crawling channel: {chan_id}. Saving channel info.")

        # changing the fwd_chan_dict to a nicer format
        fwd_chan_dict = self.nicify_fwd_chan_info(fwd_chan_dict=fwd_chan_dict)

        # save the channel info
        channel_info = {"channel_info": {"chan_id": chan_id,
                                         "title": title,
                                         "username": username,
                                         "verified": verified,
                                         "nb_participants": nb_participants},
                        "fwd_chan_dict": fwd_chan_dict}
        filename = f"{username}-channel_info.pickle"
        filepath = os.path.join(DATA_STORAGE_FOLDER, filename)
        self._save_processed_info(data=channel_info, path=filepath)

    @classmethod
    def _process_posts(cls, posts: list, tl_client) -> tuple[dict[int:dict], defaultdict[str, int]]:
        """
        Transforms the post object into a dictionary and a default dict with the channel which message are
        forwarded in these posts.
        Message API:
        https://docs.telethon.dev/en/stable/quick-references/objects-reference.html?highlight=message#message
        :param posts:
        :return:
        """
        processed_posts = {}
        forwarded_channels = defaultdict(int)
        for po in posts:
            try:
                urls = [m.group(0) for m in cls.http_url_reg.finditer(po.text)]
                domains = [cls.extract_domain_from_url(url) for url in urls]

                # Never use a key that begins with "_", it will mess up the bulk index process in esinter.py

                info = {"text": po.text,
                        # "forward": None,
                        "forwards": po.forwards,  # nb of time this post was forwarded
                        "reply": po.is_reply,
                        "id": po.id,
                        "forwarded_from": "",
                        "urls": urls,
                        "domains": domains,
                        "date": int(po.date.timestamp())}

                if po.forward is not None and po.forward.chat is not None:
                    # if this message is forwarded from a convo with a user, the info will be in forward.sender.first_name.
                    # Thus, this will fail.
                    try:
                        fwd_chan_username = po.forward.chat.username
                        fwd_chan_id = po.forward.chat_id

                        # sometimes username is none, we want to keep the same type of data in the dict though
                        fwd_chan_username = fwd_chan_username if fwd_chan_username is not None else ""

                    except AttributeError as err:
                        log.warning(f"Error getting fwd chan name: {err}")
                        log.exception(err)
                        log.warning("Post info below")
                        for key, val in vars(po).items():
                            log.warning(f"{key}: \t{val}")
                        fwd_chan_username = ERROR_GETTING_NAME_FLAG
                    info["forwarded_from"] = str(fwd_chan_id)
                    if fwd_chan_username != ERROR_GETTING_NAME_FLAG:
                        forwarded_channels[(fwd_chan_username, fwd_chan_id)] += 1

                processed_posts[po.id] = info
            except Exception as e:
                log.error(f"Exception raised when processing post: {e}. See raw posts underneath.")
                for key, val in vars(po).items():
                    log.error(f"{key}: \t{val}")
                raise e
        return processed_posts, forwarded_channels

    @staticmethod
    def _save_processed_info(data, path):
        log.info(f"Saving data at {path}")
        finished_path = path
        temp_path = path + ".TEMP"
        # we write the file with a temporary filename, then change it once file is completely written. Else the
        # dispatcher will attempt to read it before it's done.
        with open(temp_path, "wb") as f:
            pickle.dump(data, file=f)
        os.rename(temp_path, finished_path)

    @staticmethod
    def extract_domain_from_url(url):
        try:
            return urlparse(url=url).hostname
        except Exception as err:
            log.error(f"Coulnd't get domain from URL: {url}. Skipping.")
            log.exception(err)

    @staticmethod
    def nicify_fwd_chan_info(fwd_chan_dict: dict) -> list[dict]:
        """
        {(chan_username, chan_id)} = nb_of_forwards

        TO

        [{"chan_username": str,
          "chan_id": str,
          "nb_of_forwards": int}]

        :return: see above
        """
        ret = []
        for key, nb_fwd in fwd_chan_dict.items():
            chan_username, chan_id = key
            ret.append({"chan_username": chan_username,
                        "chan_id": chan_id,
                        "nb_of_forwards": nb_fwd})
        return ret


if __name__ == '__main__':
    log.info("=================================== Crawler started! ===================================")
    while True:
        spd = Spider()
        for fname in os.listdir(USERNAME_STORAGE_FOLDER):
            log.info(f"Found file {fname}")
            fpath = os.path.join(USERNAME_STORAGE_FOLDER, fname)
            # we don't take into account file marked as being used in a crawl
            if fpath.endswith(".crawling"):
                continue
            with open(fpath, 'r') as g:
                channel_id = int(g.read())
            log.info(f"Channel ID from {fname} => {channel_id}. Crawling it.")
            # we rename the file containing the username we are currently crawling
            new_fpath = fpath + ".crawling"
            os.rename(src=fpath, dst=new_fpath)
            try:
                spd.crawl_channel(chan_id=channel_id)
            except Exception as err:
                log.error(f"Error while crawling {channel_id}")
                raise err
            else:
                os.remove(new_fpath)

        time.sleep(2)

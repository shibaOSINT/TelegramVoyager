import json
import pickle
import logging

from telethon.utils import get_display_name
from telethon.tl.types import PeerChannel, PeerChat, Message
from telethon.sync import TelegramClient
from telethon.hints import Entity

log = logging.getLogger(__name__)

from collections import namedtuple

ChannelInfo = namedtuple('ChannelInfo', ["id", "title", "username", "verified", "nb_participants"])


# TODO would it be better to get a bot token and create a bot??
# https://www.google.com/search?q=get+bot+token+telegram&oq=get+bot+token+telegram&aqs=chrome..69i57.11334j0j1&sourceid=chrome&ie=UTF-8

class Client:
    def __init__(self, session_name, api_id, api_hash, max_msg_crawl, chunk_size):
        self.client = TelegramClient(session_name, api_id=api_id, api_hash=api_hash)
        self.MAX_MSG_CRAWL = max_msg_crawl
        self.CHUNK_SIZE = chunk_size

    def _get_channel_entity(self, name_or_id):
        try:
            with self.client:
                res = self.client.get_entity(name_or_id)
            if res is None:
                raise Exception(f"Error getting channel entity from {name_or_id} ({type(name_or_id)})")
            else:
                return res
        except ValueError as e:
            log.error(f"Could not get client info with name_or_id: {name_or_id}")
            raise e

    def get_channel_info(self, name_or_id):
        chan = self._get_channel_entity(name_or_id)
        nb_participants = chan.participants_count if chan.participants_count is not None else 0
        username = chan.username if chan.username is not None else ""

        return ChannelInfo(chan.id, chan.title, username, chan.verified, nb_participants)

    def is_channel_user(self, name_or_id):
        if type(self._get_channel_entity(name_or_id=name_or_id)) is Entity[0]:  # Entity[0] are Telethon user
            return True
        else:
            return False

    def get_users_from_channel(self, channel):
        with self.client:
            for user in self.client.iter_participants(entity=channel):
                log.debug(f"User: {user}")
                yield user

    def get_messages_from_channel(self, channel, skip_no_text_msg=True, reverse=False):
        """

        :param channel: A channel Entity (obtained with _get_channel_entity)
        :param skip_no_text_msg: Skipping messages with no text (I.E admin decision to change names, only images, etc.)
        :param reverse: To start from older messages, mostly here for testing.
        :return:
        """
        with self.client:
            for msg in self.client.iter_messages(entity=channel, limit=self.MAX_MSG_CRAWL, reverse=reverse):
                if skip_no_text_msg is True and msg.raw_text is None:
                    continue
                log.debug(f"MSG raw text: {msg.raw_text[:30]}".replace('\n', ""))

                yield msg

    def crawl_channel(self, channel):
        """
        get channel
        get messages
        get users (as much as we can)
        :param channel: a name or ID from a channel
        :return:
        """
        chan = self._get_channel_entity(name_or_id=channel)
        buffer = []
        for count, msg in enumerate(self.get_messages_from_channel(chan)):
            buffer.append(msg)
            if len(buffer) == self.CHUNK_SIZE:
                yield buffer
                buffer = []
        # the last chunk will probably not be the exact buffer size, we still need to yield it
        yield buffer

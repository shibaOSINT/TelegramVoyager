import logging
import os
import json
import datetime
from enum import Enum


from elasticsearch import Elasticsearch, NotFoundError, helpers

from logging import getLogger

log = getLogger("esinter")
log.setLevel(logging.DEBUG)

ELASTIC_USERNAME = "elastic"
ELASTIC_PASSWORD = os.environ['ELASTIC_PASSWORD']
ELASTIC_PORT = os.environ["ES_PORT"]
ELASTIC_HOST = "es01"
ELASTIC_HTTP_CERT_PATH = "/certs/ca/ca.crt"

SERVER_HOST = os.environ['HOST_CHANNEL']
SERVER_PORT = int(os.environ['PORT_CHANNEL'])
WAIT_FLAG = os.environ['WAIT_FLAG']

POST_INDEX = os.getenv("POST_INDEX")
CHANNEL_INDEX = os.getenv("CHANNEL_INDEX")
QUEUE_INDEX = os.getenv("QUEUE_INDEX")
MAX_CHANNEL_CRAWLED = int(os.getenv("MAX_CHANNEL_CRAWLED"))
MIN_CRAWL_INTERVAL = int(os.getenv("MIN_CRAWL_INTERVAL"))

# https://www.elastic.co/guide/en/elasticsearch/reference/current/explicit-mapping.html
# Used integer for channel ID, this caused problem as channel ID can exceed the max value of an integer.
MAPPING_QUEUE = {
    "properties": {
        "priority": {"type": "short"},
        "status": {"type": "keyword"},
        "time_added": {"type": "date"},
        "time_crawling_started": {"type": "date"},
        "username": {"type": "keyword"},
        "chan_id": {"type": "long"}
    }
}

MAPPING_CHANNELS = {
    "properties": {
        "chan_id": {"type": "long"},
        "nb_participants": {"type": "integer"},
        "title": {"type": "text"},
        "username": {"type": "keyword"},
        "verified": {"type": "keyword"},  # either true or false.
        # Using this documentation page, seems like we need "nested" even though it's expensive.
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html#nested-fields-array-objects
        "x_posted_channels": {"type": "nested"}  # inside: {"username": key,
        #                                                   "xposts": val}
    }
}

MAPPING_POSTS = {
    "properties": {
        "text": {"type": "text"},
        "forwards": {"type": "integer"},
        "reply": {"type": "boolean"},
        "id": {"type": "long"},
        "forwarded_from": {"type": "keyword"},
        "urls": {"type": "keyword"},
        "domains": {"type": "keyword"},
        "date": {"type": "date"},
        "channel": {"type": "long"}
    }
}


# Purposefully not using Enum as parent class, Elasticsearch (python client) throws error when serializing it to JSON.
class ChannelStatus:
    to_crawl = "to_crawl"
    being_crawled = "being_crawled"
    crawled = "crawled"


class EmptyQueueException(Exception):
    "Raised when the queue is empty and the next channel to be crawled is requested"
    pass


class BaseElasticInteractor:
    GET_NEXT_CHANNEL_QUERY = \
        {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "status": ChannelStatus.to_crawl
                        }
                    }
                ],
            }
        }

    GET_CHANNEL_BY_USERNAME_QUERY = \
        {
            "match_phrase": {
                "username": {
                    "query": None
                }
            }
        }

    GET_CHANNEL_BY_ID_QUERY = \
        {
            "match_phrase": {
                "chan_id": {
                    "query": None
                }
            }
        }


    MATCH_ALL_IN_INDEX = \
        {
            "match_all": {}
        }

    def __init__(self, elastic_host, elastic_port, elastic_username, elastic_password, http_cert_path, post_index=None,
                 channel_index=None, queue_index=None):
        self.client = Elasticsearch(
            f"https://{elastic_host}:{elastic_port}",
            ca_certs=http_cert_path,
            basic_auth=(elastic_username, elastic_password)
        )
        log.info("Elastic search client started!")

        if post_index is None:
            self.post_index = POST_INDEX
        else:
            self.post_index = post_index

        if channel_index is None:
            self.channel_index = CHANNEL_INDEX
        else:
            self.channel_index = channel_index

        if queue_index is None:
            self.queue_index = QUEUE_INDEX
        else:
            self.queue_index = queue_index

    def check_and_create_indices(self, indices: list[str]):
        log.info("Checking for indices!")
        for index in indices:
            if self.client.indices.exists(index=index).body is False:
                log.warning(f"{index} not present in Elasticsearch: creating it!")
                if index == self.queue_index:
                    self.client.indices.create(index=index, mappings=MAPPING_QUEUE)
                elif index == self.channel_index:
                    self.client.indices.create(index=index, mappings=MAPPING_CHANNELS)
                elif index == self.post_index:
                    self.client.indices.create(index=index, mappings=MAPPING_POSTS)
                else:
                    self.client.indices.create(index=index)

    def save_data_xposted(self, channel_info: dict, fwd_chan_list: list):
        """
        Adds channels info and crossposted channels to CHANNEL_INDEX
        Adds crossposted channels to the queue
        :param channel_info (dict): ex: {"chan_id": 1214265894,
                                       "title": "Channel Title Example",
                                       "username": "channel_username_example",
                                       "verified": True,
                                       "nb_participants": 2626}
        :param fwd_chan_list (dict): ex: {"(xposted_channel_username_1, xposted_channel_id_1)": 11,
                                          "(xposted_channel_username_2, xposted_channel_id_2)": 2}
        :return:
        """
        channel_info["chan_id"] = int(channel_info["chan_id"])
        channel_id = channel_info["chan_id"]
        channel_username = channel_info["username"]
        resp_channel_index = self._save_channel_info(channel_id, channel_info, fwd_chan_list)
        # ---------------------------------------------------------------------------------------------------
        log.debug(f"Adding crossposted channels of channel {channel_username}({channel_id}) to {self.queue_index}")
        responses_queue = self._add_channels_to_queue(fwd_chan_list)

        # This method is called when a channel is finished crawling. We can mark it as crawled
        log.debug(f"Marking {channel_username} as {ChannelStatus.crawled}.")
        # self._change_channel_crawling_status_to_crawled(channel_username)
        self._change_channel_crawling_status_to_crawled(channel_id)

        return resp_channel_index, responses_queue

    def _save_channel_info(self, channel_id, channel_info, fwd_chan_list):
        log.debug(f"Saving info and crossposted channels of channel {channel_id}")

        document = channel_info.copy()
        document['x_posted_channels'] = list()

        for fwd_chan in fwd_chan_list:
            document['x_posted_channels'].append({"username": fwd_chan["chan_username"],
                                                  "xposts": fwd_chan["nb_of_forwards"]})

        # TODO what if channel already exists?

        resp_post_channel = self.client.index(index=self.channel_index,
                                              id=channel_id,
                                              document=document)
        log.debug(f"Response for adding {channel_id} to {self.channel_index}: {resp_post_channel['result']}")
        return resp_post_channel

    def _add_channels_to_queue(self, fwd_chan_list: list, force=False):
        crawl_queue_reps = list()

        log.debug(f"fwd_chan_list: {fwd_chan_list}")

        tt_chan = self._get_total_amount_of_channel_in_queue()

        if tt_chan >= MAX_CHANNEL_CRAWLED and force is False:
            log.warning(f"Total amount of channel in queue ({tt_chan}) >= to MAX_CHANNEL_CRAWLED ({MAX_CHANNEL_CRAWLED}"
                        f"). Not adding forwarded chans in the queue.")
            return {}

        for fwd_chan_info in fwd_chan_list:
            # checking if channels hasn't already been crawled
            xpost_chan_username = fwd_chan_info["chan_username"]
            xpost_chan_id = fwd_chan_info["chan_id"]
            priority = fwd_chan_info["nb_of_forwards"]
            # TODO DO WE REALLY NEED THAT ?????????????????
            chan_info = self.get_channel_by_id(chan_id=xpost_chan_id)
            if chan_info:
                log.info(f"Not adding {xpost_chan_username} to the queue, already crawled.")
                log.debug(chan_info)
                continue
            resp_crawl = self.client.index(index=self.queue_index,
                                           id=xpost_chan_id,
                                           document={"priority": priority,
                                                     "status": ChannelStatus.to_crawl,
                                                     "time_added": int(datetime.datetime.now().timestamp()),
                                                     "time_crawling_started": 0,
                                                     "username":xpost_chan_username,
                                                     "chan_id":xpost_chan_id})
            log.debug(f"Adding TO QUEUE {xpost_chan_username} to {self.queue_index}: {resp_crawl['result']}")
            crawl_queue_reps.append(resp_crawl['result'])

        return crawl_queue_reps

    def save_data(self, channel_id, posts):
        """Adding posts to the POST_INDEX"""
        try:
            self._save_posts_bulk(channel_username=channel_id, posts=posts)
        except helpers.BulkIndexError as err:
            log.error("Couldn't save posts:")
            for i in err.errors:
                log.error(i)
            raise err

    def _save_posts_bulk(self, channel_username, posts: dict):
        """
        Save posts using the bulk API.

        Modeled after: https://github.com/elastic/elasticsearch-py/blob/main/examples/bulk-ingest/bulk-ingest.py

        :param channel_username:
        :param posts:
        :return:
        """

        successes = 0
        for ok, action in helpers.streaming_bulk(client=self.client,
                                                 index=self.post_index,
                                                 actions=self.__generate_action_bulk_index(channel_username=channel_username,
                                                                                           posts=posts)):
            # doesn't work, the streaming_bulk raises an exception thus we don't get to see this
            if not ok:
                log.error('Failed indexing posts: info below')
                log.error(action)

            successes += ok
        log.info("Indexed %d/%d posts" % (successes, len(posts)))

    @staticmethod
    def __generate_action_bulk_index(channel_username, posts: dict):
        action = {}
        for id_post, post_info in posts.items():
            action["_id"] = f"{channel_username}:{id_post}"
            action['channel'] = channel_username
            for k, v in post_info.items():
                action[k] = v
            yield action


    def get_next_channel_to_be_crawled(self):
        """
                Get all channels in queue:
                    1. Any channel with status `to_crawl`? If so => return one with highest prio (normal way of operating)
                    2. Any channel with status `crawled`? If so => return the one with the oldest `time_crawling_started`
                        property
                    3. Only channels with status `being_crawled`? If so => return wait flag.
                :return:
                """

        # we sort channels between the ones that have been crawled and the ones that are to be crawled
        channels_to_crawl = []
        channels_crawled = []

        resp = self.client.search(index=self.queue_index, size=10000)
        # print(resp)
        for doc in resp['hits']['hits']:
            document = doc['_source']
            chan_id = document['chan_id']
            if document['status'] == ChannelStatus.to_crawl:
                channels_to_crawl.append((chan_id, document))
            elif document['status'] == ChannelStatus.crawled:
                channels_crawled.append((chan_id, document))

        log.info(f"Total channel {ChannelStatus.to_crawl}: {len(channels_to_crawl)}")
        log.info(f"Total channel {ChannelStatus.crawled}: {len(channels_crawled)}")

        # If there are more channels to crawl, we return the highest prio (same method we had before)
        if len(channels_to_crawl) > 0:
            resp = max(channels_to_crawl, key=lambda y: y[1]["priority"])
        elif len(channels_crawled) > 0:
            resp = min(channels_crawled, key=lambda y: int(y[1]["time_crawling_started"]))
            if int(datetime.datetime.now().timestamp()) - resp[1]["time_crawling_started"] < MIN_CRAWL_INTERVAL:
                raise EmptyQueueException
        else:
            raise EmptyQueueException

        chan_id = resp[0]

        while True:
            resp = self._change_channel_crawling_status_to_being_crawled(chan_id=chan_id)
            if resp['result'] == "updated":
                log.debug(f"Successfully changed status of chan {chan_id}")
                return chan_id
            else:
                log.warning(f"Couldn't change the status of chan {chan_id}. Trying again.")

    def _change_channel_crawling_status_to_being_crawled(self, chan_id):
        """
        Not to be used outside of get_next_channel_to_be_crawled. This doesn't change the status of the channel
        retrieved!
        :param chan_id: ID of the channel that must be updated
        :return:
        """
        resp = self.client.update(index=self.queue_index,
                                  id=chan_id,
                                  doc={"status": ChannelStatus.being_crawled,
                                       "time_crawling_started": int(datetime.datetime.now().timestamp())})

        log.info(f"{chan_id} status changed to being crawled: {resp['result']}")
        return resp.raw

    def _change_channel_crawling_status_to_crawled(self, chan_id: int):
        """
                Not to be used outside of save_data_xposted.
                :param chan_id: Username of the channel that must be updated
                :return:
                """
        resp = self.client.update(index=self.queue_index,
                                  id=chan_id,
                                  doc={"status": ChannelStatus.crawled})

        log.info(f"{chan_id} status changed to {ChannelStatus.crawled}: {resp['result']}")
        return resp.raw

    def _get_channel_to_be_crawled_with_highest_prio(self):

        resp = self._get_n_channels_to_be_crawled_with_highest_prio(size=1)
        doc_source = resp.body['hits']['hits'][0]['_source']
        chan_username = resp.body['hits']['hits'][0]['_id']
        log.debug(f"Getting next channel to be crawled: {chan_username} with priority {doc_source['priority']}")
        return chan_username

    def _get_n_channels_to_be_crawled_with_highest_prio(self, size):
        """
        Get the channel in the queue with the highest priority while having a ChannelStatus.to_crawl statys.
        Changes the status to being crawled
        :return: channel ID
        """

        resp = self.client.search(query=self.GET_NEXT_CHANNEL_QUERY,
                                  index=self.queue_index,
                                  size=size,
                                  sort=["priority:desc"])
        return resp

    def _get_total_amount_of_channel_in_queue(self) -> int:
        """
        Returns the total amount of channels in the queue, regardless of their status.
        :return: int
        """
        # from docs: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
        # (select python implementation)
        resp = self.client.count(index=self.queue_index)
        return resp["count"]

    def get_channel_by_username(self, username):
        username_query = self.GET_CHANNEL_BY_USERNAME_QUERY.copy()
        username_query["match_phrase"]["username"]["query"] = username
        resp = self.client.search(query=self.GET_CHANNEL_BY_USERNAME_QUERY,
                                  index=self.channel_index)
        try:
            chan_info = resp.raw['hits']['hits'][0]['_source']
        except IndexError:
            chan_info = dict()
        return chan_info


    def get_channel_by_id(self, chan_id):
        chan_id_query = self.GET_CHANNEL_BY_ID_QUERY.copy()
        chan_id_query["match_phrase"]["chan_id"]["query"] = chan_id
        resp = self.client.search(query=chan_id_query,
                                  index=self.channel_index)
        try:
            chan_info = resp.raw['hits']['hits'][0]['_source']
        except IndexError:
            chan_info = dict()
        return chan_info


    def check_connection(self):
        return self.client.ping()


class ElasticInteractor(BaseElasticInteractor):

    def __init__(self, *args, **kwargs):
        # creating the indices that we need if they aren't there
        super().__init__(*args, **kwargs)
        self.check_and_create_indices(indices=[self.post_index, self.queue_index, self.channel_index])


if __name__ == '__main__':
    eee = BaseElasticInteractor(elastic_host=ELASTIC_HOST,
                                elastic_port=ELASTIC_PORT,
                                elastic_username=ELASTIC_USERNAME,
                                elastic_password=ELASTIC_PASSWORD,
                                http_cert_path=ELASTIC_HTTP_CERT_PATH)


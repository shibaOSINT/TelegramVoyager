import os
import sys
import time
import unittest
from unittest.mock import MagicMock
import datetime
import elasticsearch

from esinter import (ElasticInteractor, EmptyQueueException, ELASTIC_HOST, ELASTIC_PORT, ELASTIC_PASSWORD,
                     ELASTIC_USERNAME, ELASTIC_HTTP_CERT_PATH, SERVER_PORT, SERVER_HOST, WAIT_FLAG)

TEST_POST_INDEX = "test_post_index"
TEST_QUEUE_INDEX = "test_queue_index"
TEST_CHANNEL_INDEX = "test_channel_index"


class TestOrchestrator(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # TODO: add something to nuke test indices so they are recreated


        cls.es_client = ElasticInteractor(elastic_host=ELASTIC_HOST,
                                          elastic_port=ELASTIC_PORT,
                                          elastic_username=ELASTIC_USERNAME,
                                          elastic_password=ELASTIC_PASSWORD,
                                          http_cert_path=ELASTIC_HTTP_CERT_PATH,
                                          post_index=TEST_POST_INDEX,
                                          channel_index=TEST_CHANNEL_INDEX,
                                          queue_index=TEST_QUEUE_INDEX)

        cls.es_client.client.indices.delete(index=TEST_POST_INDEX)
        cls.es_client.client.indices.delete(index=TEST_QUEUE_INDEX)
        cls.es_client.client.indices.delete(index=TEST_CHANNEL_INDEX)

        cls.es_client.check_and_create_indices(indices=[TEST_CHANNEL_INDEX, TEST_POST_INDEX, TEST_QUEUE_INDEX])

    def setUp(self):
        """Just removing all content from test index before each test method."""
        print("Removing all content from test indices.")
        # removing content from both the queue and channel index
        resp = self.es_client.client.delete_by_query(index=TEST_QUEUE_INDEX, query={"match_all": {}})
        print(f"Removed all document in index {TEST_QUEUE_INDEX}. Deleted: {resp['deleted']}")
        resp = self.es_client.client.delete_by_query(index=TEST_CHANNEL_INDEX, query={"match_all": {}})
        print(f"Removed all document in index {TEST_CHANNEL_INDEX}. Deleted: {resp['deleted']}")
        resp = self.es_client.client.delete_by_query(index=TEST_POST_INDEX, query={"match_all": {}})
        print(f"Removed all document in index {TEST_POST_INDEX}. Deleted: {resp['deleted']}")

        time.sleep(3)

    def test_NotAddingChannelsAlreadyCrawledToQueue(self):
        """
        When adding a xposted channel to the queue, we wanna make sure that they were not crawled before, i.e not in the
         channel_index.
        :return:
        """
        test_channel_name = "AlreadyCrawledChannel"

        print(f"Adding the test channel: {test_channel_name}")
        self.es_client._save_channel_info(channel_id=1214265894,
                                          channel_info={"chan_id": 1214265894,
                                                        "title": "AlreadyCrawledChannel",
                                                        "username": "AlreadyCrawledChannel",
                                                        "verified": True,
                                                        "nb_participants": 2626},
                                          fwd_chan_list={})

        time.sleep(2)

        info = self.es_client.get_channel_by_username(username=test_channel_name)
        print(f"Info from {test_channel_name}: {info}")

        # now we're testing that this channel won't be added to the queue again when it's in the xposted list
        total_chan_in_queue = \
            self.es_client._get_n_channels_to_be_crawled_with_highest_prio(size=10).body['hits']['total']['value']
        self.assertEqual(total_chan_in_queue, 0)

        mock_change_status = MagicMock()
        self.es_client._change_channel_crawling_status_to_crawled = mock_change_status

        self.es_client.save_data_xposted(channel_info={"chan_id": 123456789,
                                                       "title": "AnotherChannel",
                                                       "username": "AnotherChannel",
                                                       "verified": True,
                                                       "nb_participants": 2626},
                                         fwd_chan_list={"AlreadyCrawledChannel": 11})

        mock_change_status.assert_called()

        time.sleep(2)

        total_chan_in_queue = \
            self.es_client._get_n_channels_to_be_crawled_with_highest_prio(size=10).body['hits']['total']['value']
        self.assertEqual(total_chan_in_queue, 0)


if __name__ == '__main__':
    unittest.main()

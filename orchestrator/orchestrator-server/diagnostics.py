#!/usr/local/bin/python


import os
import json

import argparse
import sys
import time

from esinter import BaseElasticInteractor


from esinter import ELASTIC_USERNAME, ELASTIC_PORT, ELASTIC_HOST, ELASTIC_PASSWORD, ELASTIC_HTTP_CERT_PATH


class Diagnostics(BaseElasticInteractor):
    def get_all_posts(self, size):
        resp = self.client.search(index=self.post_index, query={"match_all": {}}, size=size)
        print(f"Total posts in post index: {resp.body['hits']['total']['value']}")
        return resp.raw

    def get_all_channels_in_crosspost(self, size):
        resp = self.client.search(index=self.channel_index, query={"match_all": {}}, size=size)
        print(f"Total docs in xpost channel index: {resp.body['hits']['total']['value']}")
        return resp.raw

    def get_n_channel_in_queue(self, size):
        resp = self.client.search(index=self.queue_index, size=size)
        print(f"Total channel in queue index: {resp.body['hits']['total']['value']}")
        return resp.raw['hits']['hits']

    def remove_all_docs_from_index(self, index):
        resp = self.client.delete_by_query(index=index, query={"match_all": {}})
        print(f"Removed all document in index {index}. Deleted: {resp['deleted']}")
        return resp.raw

    def get_doc_by_id(self, index, id_doc):
        resp = self.client.get(index=index, id=id_doc)
        return resp

    def get_info_from_cluster(self):
        resp = self.client.info()
        return resp.raw

    def get_mapping(self, index_name):
        if self.client.indices.exists(index=index_name) is False:
            print(f"{index_name} doesn't exist!")
            return []
        # TODO will raise an error if the index exists but no mapping is available
        resp = self.client.indices.get_mapping(index=index_name)
        return resp.body

    def list_all_indices(self, show_all=False):
        # resp = self.client.indices.get_alias(index="*")
        resp = self.client.indices.get(index="*")
        indices_names = []
        for key in resp.body.keys():
            if not key.startswith('.'):
                indices_names.append(key)
        return indices_names

    def inject_channel_into_queue(self, cmd_args, priority: int = 100):
        """
        Please use the channel username: ex: in https://t.me/JsonDumpBot the username is JsonDumpBot

        Please get the username of the channel, it can be obtained by forwarding a message from that
                        channel to https://t.me/JsonDumpBot and copy the value in the forwarded from chat section.
                        Ex:
                        "forward_from_chat": {
                          "id": -1001481919204,
                          "title": "Silvano Trotta Officiel",
                          "username": "trottasilvano",
                          "type": "channel"
                        },

                        What you want here is trottasilvano

        """
        channel_username, chan_id = cmd_args
        chan_id = int(chan_id)

        fwd_chan_list = [{"chan_username": channel_username,
                          "chan_id": chan_id,
                          "nb_of_forwards": priority}]

        self._add_channels_to_queue(fwd_chan_list=fwd_chan_list, force=True)

    def nuke_all_indices(self):
        if self.client.indices.exists(index=self.post_index):
            print(f"Deleting {self.post_index}")
            self.client.indices.delete(index=self.post_index)

        if self.client.indices.exists(index=self.queue_index):
            print(f"Deleting {self.queue_index}")
            self.client.indices.delete(index=self.queue_index)

        if self.client.indices.exists(index=self.channel_index):
            print(f"Deleting {self.channel_index}")
            self.client.indices.delete(index=self.channel_index)


def pprint(data):
    print(json.dumps(data, indent=4))


if __name__ == '__main__':

    # Learn if we're running in Docker or not. Helps with testing from inside and outside Docker.
    if os.path.exists("/home/mat/Downloads/ca-test_install_elastic_docker_compose.crt"):
        ELASTIC_HTTP_CERT_PATH = "/home/mat/Downloads/ca-test_install_elastic_docker_compose.crt"
        ELASTIC_HOST = "127.0.0.1"

    # To adjust display
    try:
        term_width, term_height = os.get_terminal_size()
    except OSError as err:
        if err.errno == 25:
            print("Couldn't get terminal width. Assuming 50")
            term_width = 50
            term_height = 50
        else:
            raise err

    # origin: https://stackoverflow.com/questions/8924173/how-can-i-print-bold-text-in-python
    RED = '\033[91m'
    END = '\033[0m'

    parser = argparse.ArgumentParser(prog='Elastic Diagnostic',
                                     description='The CLI interface you need to command the orchestrator.',
                                     epilog='Good luck. Tip: you can use the "diag" command to access this script.')
    parser.add_argument('-tq', '--total-queue', action='store_true', help='Returns the total amount of '
                                                                          'channels in the queue, regardless of status.'
                                                                          '')
    parser.add_argument('-q', '--queue', type=int, help='Show N channels in the queue (that are to be '
                                                        'crawled). Ordered by priority.')
    parser.add_argument('-p', '--post', type=int, help='Show N posts.')
    parser.add_argument('-xp', '--cross-posted', type=int, help='Show N cross posted channels')
    parser.add_argument('-i', '--inject', nargs=2, type=str, help='Inject a channel in the queue.')
    parser.add_argument('--nuke', action='store_true', help='WARNING: Remove all indices for that specific'
                                                            ' config. They will be recreated by running the '
                                                            'orchestrator.')
    parser.add_argument('-n', '--new-indices', action='store_true', default=False, help='Creates indices'
                                                                                        'like the Orchestrator would.'
                                                                                        'Indices that are already '
                                                                                        'present will not be recreated')
    parser.add_argument('-m', '--mapping', type=str, choices=['queue', 'post', 'channel', 'all'],
                        help='Displays the mapping for a given or all index.')
    parser.add_argument('-l', '--list-indices', action='store_true', help="List non system indices.")
    parser.add_argument('-r', '--raw', action='store_true', help="Output data from Elasticsearch as JSON "
                                                                 "without attempting to summarize it. More info but way"
                                                                 "longer output", default=False)
    args = parser.parse_args()

    print(args)

    client = Diagnostics(elastic_host=ELASTIC_HOST,
                         elastic_port=ELASTIC_PORT,
                         elastic_username=ELASTIC_USERNAME,
                         elastic_password=ELASTIC_PASSWORD,
                         http_cert_path=ELASTIC_HTTP_CERT_PATH)

    if args.total_queue is True:
        total_amount_chan_queue = client._get_total_amount_of_channel_in_queue()
        print(f"Total amount of channel in queue: {total_amount_chan_queue}")

    if args.nuke is True:
        client.nuke_all_indices()
        time.sleep(3)
        print("Remaining indices:")
        for ind in client.list_all_indices():
            print(ind)

    if args.new_indices is True:
        client.check_and_create_indices(indices=[client.channel_index, client.post_index, client.queue_index])

    if args.queue is not None:
        res = client.get_n_channel_in_queue(args.queue)
        for row in res:
            if args.raw is True:
                pprint(row['_source'])
            else:
                print(f" {row['_source']['priority']}: {row['_id']} ({row['_source']['status']})")

    if args.post is not None:
        ee = client.get_all_posts(size=args.post)
        for row in ee['hits']['hits']:
            info = row['_source']
            if args.raw is True:
                pprint(info)
            else:
                info['text'] = info['text'][:term_width - 10].replace('\n', '')
                print(info['text'])
                print(f"{RED}forwards{END}: {info['forwards']}. {RED}id{END}: {info['id']}. {RED}forwarded_from{END}: \
                      {info['forwarded_from']}. {RED}date{END}: {info['date']}. {RED}channel{END}: {info['channel']}")
                print(f"{RED}urls{END}: {'|'.join(info['urls'])}")
                print(f"{'-' * term_width}")

    if args.cross_posted is not None:
        ee = client.get_all_channels_in_crosspost(size=args.cross_posted)
        for row in ee['hits']['hits']:
            info = row['_source']
            if args.raw is True:
                pprint(info)
            else:
                print(f"{info['title']} ({info['chan_id']})")
                sorted_fw = sorted(info["x_posted_channels"], key=lambda y: y["xposts"], reverse=True)
                sorted_fw = [f"{fwd_chan['username']}: {fwd_chan['xposts']}" for fwd_chan in sorted_fw]
                print(f"{RED}Forwards{END}: {' | '.join(sorted_fw)}")
                print(f"{'-' * term_width}")

    if args.inject is not None:
        client.inject_channel_into_queue(args.inject)

    if args.mapping == 'queue':
        pprint(client.get_mapping(client.queue_index))
    if args.mapping == 'post':
        pprint(client.get_mapping(client.post_index))
    if args.mapping == 'channel':
        pprint(client.get_mapping(client.channel_index))
    if args.mapping == 'all':
        pprint(client.get_mapping(client.queue_index))
        print(f"{'-' * term_width}")
        pprint(client.get_mapping(client.post_index))
        print(f"{'-' * term_width}")
        pprint(client.get_mapping(client.channel_index))

    if args.list_indices is True:
        for ind in client.list_all_indices():
            print(ind)

import os
from neo4j import GraphDatabase


PASSWORD = os.getenv("NEO4J_PASSWORD")
USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_HOSTNAME = os.getenv("GRAPHDB_HOSTNAME")

NEO4J_URI = f"neo4j://{NEO4J_HOSTNAME}"
NEO4J_AUTH = (USERNAME, PASSWORD)


class GraphDB:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri=uri, auth=auth)
        self.driver.verify_connectivity()

    def create_db(self, db_name):
        """Not possible with the community edition apparently. Let's limit ourselves with one DB.
        https://stackoverflow.com/questions/60429947/error-occurs-when-creating-a-new-database-under-neo4j-4-0"""
        # https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/create-databases/
        raise Exception("IMPOSSIBLE WITH COMMUNITY EDITION")
        resp = self.driver.execute_query("CREATE DATABASE $dbname", dbname=db_name)
        return resp

    def get_all_db_info(self):
        # https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/listing-databases/
        resp = self.driver.execute_query(query_="SHOW DATABASES")
        # creating dict from the returned list for easier access
        records = []
        for rec in resp.records:
            records.append({k: v for k, v in zip(resp.keys, rec)})
        return records

    def verify_db_exists(self, db_name):
        records = self.get_all_db_info()
        for rec in records:
            if rec['name'] == db_name:
                return True

        return False

    def _add_channel(self, chan_info):
        """



        Doc: https://neo4j.com/docs/python-manual/current/

        :param chan_info:
        :return:
        """

        resp = self.driver.execute_query(query_="""
        MERGE (n:Channel {username: $username})
        ON CREATE
          SET
            n.chan_id = $chan_id,
            n.nb_participants = $nb_participants,
            n.verified = $verified,
            n.title = $title
        ON MATCH
          SET
            n.chan_id = $chan_id,
            n.nb_participants = $nb_participants,
            n.verified = $verified,
            n.title = $title
        RETURN n.username""",
                                         title=chan_info['title'],
                                         username=chan_info['username'],
                                         verified=chan_info['verified'],
                                         chan_id=chan_info['chan_id'],
                                         nb_participants=chan_info['nb_participants'])

        return resp.summary.summary_notifications

    def _update_forward_info(self, chan_info: dict, fwd_chan_list: dict):
        """
        Warning: this may lead to false numbers, if the messages forwarded are crawled twice, they will be added twice
        in the relationship as well!
        :param parent_chan_info:
        :return:
        """
        usernames = [k["chan_username"] for k in fwd_chan_list]
        chan_id_par = chan_info['chan_id']
        for usern in usernames:
            # Need the "*" on the second WITH, else, the parent variable isn't carried over.
            qv3 = """
            MATCH (parent:Channel {chan_id: $chan_id_par})
            WITH parent 
            MERGE (fwdchan:Channel {username: $username_fwd})
            WITH *
            MERGE (parent)-[rel:FORWARDS]->(fwdchan)
            ON CREATE
              SET rel.value = $val_rel
            ON MATCH
              SET rel.value= rel.value + $val_rel
            """

            resp = self.driver.execute_query(query_=qv3,
                                             chan_id_par=chan_id_par,
                                             username_fwd=usern,
                                             val_rel=fwd_chan_list[usern])

        # return resp.summary.summary_notifications

    def add_channel_info_and_fwd_channels(self, channel_info: dict, fwd_chan_list: dict):
        """Adds a channel information to the graph DB and adds its fwd to the relationships with the forwarded channel.
        """

        # TODO do some errors reporting by reading the returns of the following methods, at least raise exception when
        # isn't added to DB correctly: https://neo4j.com/docs/python-manual/current/result-summary/
        self._add_channel(chan_info=channel_info)
        self._update_forward_info(chan_info=channel_info, fwd_chan_list=fwd_chan_list)

    def delete_all_channels(self):
        self.driver.execute_query("""MATCH (n:Channel)
                                     DELETE n""")

    def nuke_db(self):
        self.driver.execute_query("""MATCH (n)
                                     DETACH DELETE n""")

    def __del__(self):
        self.driver.close()


if __name__ == '__main__':
    db = GraphDB(uri=NEO4J_URI, auth=NEO4J_AUTH)
    db.nuke_db()

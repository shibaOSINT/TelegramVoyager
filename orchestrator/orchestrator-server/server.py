import os
import logging

from flask import Flask, request, jsonify, g

from esinter import (ElasticInteractor, EmptyQueueException, ELASTIC_HOST, ELASTIC_PORT, ELASTIC_PASSWORD,
                     ELASTIC_USERNAME, ELASTIC_HTTP_CERT_PATH, SERVER_PORT, SERVER_HOST, WAIT_FLAG)

from neoperations import (GraphDB, NEO4J_AUTH, NEO4J_URI)

# def config_logging(level, format_log, datefmt, filename):
#     logging.basicConfig(filename=filename, level=level, format=format_log, datefmt=datefmt)
#     return logging.getLogger("orchestrator")


log_datefmt = os.getenv("DATE_FORMAT", default="%Y-%m-%d %H:%M:%S")
log_formatting = os.getenv("LOG_FORMATING", default="%(levelname)s-[%(asctime)s] [%(thread)d] %(message)s")
log_level = logging.getLevelName(os.getenv("LOG_LEVEL", default="INFO"))

formatter = logging.Formatter(fmt=log_formatting, datefmt=log_datefmt)

file_handler = logging.FileHandler(filename="orchestrator.logs")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(log_level)
stream_handler.setFormatter(formatter)

log = logging.getLogger()
log.addHandler(stream_handler)
log.addHandler(file_handler)


# log = config_logging(level=log_level, format_log=log_formatting, datefmt=log_datefmt, filename="orchestrator.logs")


class Orchestrator(Flask):
    def __init__(self, import_name, check_db_connection=True):
        if check_db_connection is True:
            # test DB access
            try:
                assert ElasticInteractor(elastic_host=ELASTIC_HOST,
                                         elastic_port=ELASTIC_PORT,
                                         elastic_username=ELASTIC_USERNAME,
                                         elastic_password=ELASTIC_PASSWORD,
                                         http_cert_path=ELASTIC_HTTP_CERT_PATH).check_connection()
                log.info("Test connection to ElasticSearch successful!")
            except AssertionError:
                log.error("Cannot connect to Elastic database! Quitting!")
        super().__init__(import_name)

    @staticmethod
    def get_elastic_db():
        if "elastic_db" not in g:
            g.elastic_db = ElasticInteractor(elastic_host=ELASTIC_HOST,
                                             elastic_port=ELASTIC_PORT,
                                             elastic_username=ELASTIC_USERNAME,
                                             elastic_password=ELASTIC_PASSWORD,
                                             http_cert_path=ELASTIC_HTTP_CERT_PATH)
        return g.elastic_db

    @staticmethod
    def get_neo4j_db():
        if "neo4j_db" not in g:
            g.neo4j_db = GraphDB(uri=NEO4J_URI, auth=NEO4J_AUTH)
        return g.neo4j_db

    def teardown_appcontext(self, f):
        db = g.pop('elastic_db', None)
        if db is not None:
            db.close()


app = Orchestrator(import_name="orchestrator")


@app.route("/")
def hello_world():
    return "I'm orchestratin in here!!!"


@app.route("/next", methods=['GET'])
def get_next():
    log.info(f"{request.remote_addr} - Asked for next channel")
    try:
        db = app.get_elastic_db()
        chan_id = db.get_next_channel_to_be_crawled()
        log.info(f"{str(chan_id)} removed from queue sent to {request.remote_addr}")
        return jsonify(chan_id)
    except EmptyQueueException:
        return str(WAIT_FLAG)
    except Exception as e:
        log.error(f"Cannot supply next in queue because: {e}")
        return


@app.route("/save_data", methods=['POST'])
def save_data():
    log.info(f"Saving posts from {request.remote_addr}")
    data = request.json
    db = app.get_elastic_db()
    for channel_id, posts in data.items():
        db.save_data(channel_id=int(channel_id), posts=posts)
    return jsonify(success=True)


@app.route("/save_data_xposted", methods=['POST'])
def save_data_xposted():
    log.info(f"Saving xposted data from {request.remote_addr}")
    data = request.json
    channel_info = data["channel_info"]
    fwd_chan_list = data["fwd_chan_dict"]

    edb = app.get_elastic_db()
    edb.save_data_xposted(channel_info=channel_info, fwd_chan_list=fwd_chan_list)

    gdb = app.get_neo4j_db()
    gdb.add_channel_info_and_fwd_channels(channel_info=channel_info, fwd_chan_list=fwd_chan_list)

    return jsonify(success=True)


if __name__ == '__main__':
    log.info("==================== SERVER STARTED ====================")
    app.run(host=SERVER_HOST, port=SERVER_PORT, debug=True)

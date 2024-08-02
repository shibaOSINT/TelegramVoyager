TEMPLATE_POSTS = {"text": str,
                  "forwards": int,  # nb of time this post was forwarded
                  "reply": bool,
                  "id": int,
                  "forwarded_from": str,    # fwd_chan_id
                  "urls": [str],
                  "domains": [str],
                  "date": int}

TEMPLATE_CHANNEL_INFO = {"channel_info": {"chan_id": int,
                                          "title": str,
                                          "username": str,
                                          "verified": bool,
                                          "nb_participants": int},
                         "fwd_chan_dict": [{"chan_username": str,
                                            "chan_id": int,
                                            "nb_of_forwards": int}]}


def validate_list(list_to_validate, type_in_list):
    """Asserts that the type of all items in list_to_validate is matching type_in_list"""
    for i in list_to_validate:
        assert type(i) is type_in_list


def validate_posts(posts: dict):
    """Raise an exception if any of the posts do not follow the format of TEMPLATE_POSTS"""
    for chan_id, chan_posts in posts.items():
        for post_id, post_info in chan_posts.items():
            assert type(post_id) is int
            assert len(post_info) == len(TEMPLATE_POSTS)
            assert type(post_info) is type(TEMPLATE_POSTS)

            # checking that all the info contained in the dict are of the expected type
            for key, val in post_info.items():
                if type(val) is list:
                    validate_list(list_to_validate=val, type_in_list=TEMPLATE_POSTS[key][0])
                else:
                    assert type(val) is TEMPLATE_POSTS[key]


def validate_channel_info(info: dict):
    assert len(info) == len(TEMPLATE_CHANNEL_INFO)

    # verifying the channel_info
    for key, val in info["channel_info"].items():
        assert type(val) is TEMPLATE_CHANNEL_INFO["channel_info"][key]

    # verifying the fwd_chan_dict
    for i in info["fwd_chan_dict"]:
        assert len(i) == len(TEMPLATE_CHANNEL_INFO['fwd_chan_dict'][0])
        for key, val in i.items():
            assert type(val) is TEMPLATE_CHANNEL_INFO['fwd_chan_dict'][0][key]


if __name__ == '__main__':
    import pickle

    # filepath = "/home/mat/Desktop/TelegramVoyagerDocker/spider/devland/log analysis/2024-07-21/infrarotsichtinsdunkel-chunk_0.pickle"
    filepath = "/home/mat/Desktop/TelegramVoyagerDocker/spider/devland/log analysis/2024-07-21/infrarotsichtinsdunkel-channel_info.pickle"
    with open(filepath, 'rb') as f:
        content = f.read()
    content = pickle.loads(content)
    validate_channel_info(content)

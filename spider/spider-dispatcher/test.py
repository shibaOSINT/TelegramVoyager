import unittest
from unittest import mock

from dispatcher import get_next_chan


class TestSpiderDispatcher(unittest.TestCase):

    def test_get_next_chan(self):
        # with mock.patch('dispatcher.log') as mock_log:
        with mock.patch('requests.get') as mock_req:
            res = get_next_chan(host="", port="", wait_flag="", relief_time=30)
            mock_req.assert_called_once()

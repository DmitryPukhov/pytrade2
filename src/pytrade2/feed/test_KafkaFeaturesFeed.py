from multiprocessing import Event, RLock
from unittest import TestCase

from pytrade2.feed.KafkaFeaturesFeed import KafkaFeaturesFeed


class TestKafkaFeaturesFeed(TestCase):

    @staticmethod
    def _new_feed():
        return KafkaFeaturesFeed({'pytrade2.feed.features.kafka.topic': "test"}, RLock(), Event())

    def test_on_message_empty_message(self):
        feed = self._new_feed()
        feed._on_message({})
        self.assertEqual(feed._buf, [])
        self.assertFalse(feed._new_data_event.is_set())

    def test_on_message(self):
        feed = self._new_feed()

        # Message came to empty buffer
        msg1 = {"value": 1}
        feed._on_message(msg1)
        self.assertEqual(feed._buf, [msg1])
        self.assertTrue(feed._new_data_event.is_set())

        # Next message came to non-empty buffer
        feed._new_data_event.clear()
        msg2 = {"value": 2}
        feed._on_message(msg2)
        self.assertEqual(feed._buf, [msg1, msg2])
        self.assertTrue(feed._new_data_event.is_set())

    def test_apply_buf_empty_buf(self):
        feed = self._new_feed()
        feed.apply_buf()
        self.assertTrue(feed.data.empty)
        self.assertFalse(feed._new_data_event.is_set())

    def test_apply_buf(self):
        feed = self._new_feed()

        # First data pack came
        feed._buf = [{"value": 1}, {"value": 2}]
        feed.apply_buf()
        self.assertEqual(feed.data["value"].to_list(), [1, 2])
        self.assertFalse(feed._new_data_event.is_set()) # it's set in on_message, not in apply_buf


        # First data pack came
        feed._buf = [{"value": 3}, {"value": 4}]
        feed.apply_buf()
        self.assertEqual(feed.data["value"].to_list(), [1, 2, 3, 4])
        self.assertFalse(feed._new_data_event.is_set()) # it's set in on_message, not in apply_buf

    def test_on_message_then_apply_buf(self):
        feed = self._new_feed()

        feed._on_message({"value": 1})
        feed._on_message({"value": 2})
        self.assertTrue(feed._new_data_event.is_set())
        feed.apply_buf()

        # First data pack came
        self.assertEqual(feed._buf, [])
        self.assertEqual(feed.data["value"].to_list(), [1, 2])

        # Second data pack came
        feed._on_message({"value": 3})
        feed._on_message({"value": 4})
        self.assertTrue(feed._new_data_event.is_set())
        feed.apply_buf()
        self.assertEqual(feed._buf, [])
        self.assertEqual(feed.data["value"].to_list(), [1, 2, 3,4])
        self.assertTrue(feed._new_data_event.is_set()) # It's parent code who clears the event

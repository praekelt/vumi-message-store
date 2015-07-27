from datetime import timedelta

from twisted.internet.defer import inlineCallbacks

from vumi.message import parse_vumi_date
from vumi.tests.helpers import VumiTestCase, MessageHelper
from vumi_message_store.tests.helpers import MessageSequenceHelper


class FakeBackend(object):
    """
    Simple stub riak backend for testing the MessageSequenceHelper
    """
    def __init__(self):
        self.inbound = []
        self.outbound = []
        self.events = []

    def batch_start(self):
        return "batch_id"

    def add_inbound_message(self, msg, batch_ids=()):
        self.inbound.append(msg)

    def add_outbound_message(self, msg, batch_ids=()):
        self.outbound.append(msg)

    def add_event(self, event, batch_ids=()):
        self.events.append(event)


class TestMessageSequenceHelper(VumiTestCase):
    def setUp(self):
        self.backend = FakeBackend()
        self.msg_helper = self.add_helper(MessageHelper())
        self.msg_seq_helper = (
            MessageSequenceHelper(self.backend, self.msg_helper))

    @inlineCallbacks
    def test_inbound_msg_count(self):
        """
        When creating an inbound message sequence, the correct number of
        messages are created and stored
        """
        _, all_keys = (
            yield self.msg_seq_helper.create_inbound_message_sequence(
                msg_count=3))

        self.assertEqual(len(all_keys), 3)
        self.assertEqual(len(self.backend.inbound), 3)

    @inlineCallbacks
    def test_outbound_msg_count(self):
        """
        When creating an outbound message sequence, the correct number of
        messages are created and stored
        """
        _, all_keys = (
            yield self.msg_seq_helper.create_outbound_message_sequence(
                msg_count=3))

        self.assertEqual(len(all_keys), 3)
        self.assertEqual(len(self.backend.outbound), 3)

    @inlineCallbacks
    def test_ack_event_count(self):
        """
        When creating an ack event sequence, the correct number of
        messages are created and stored
        """
        _, _, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence(
                event_count=3))

        self.assertEqual(len(all_keys), 3)
        self.assertEqual(len(self.backend.events), 3)

    @inlineCallbacks
    def test_inbound_delay_seconds(self):
        """
        When creating an inbound message sequence, the messages are returned in
        descending timestamp order and the delay between each timestamp is
        correct
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_inbound_message_sequence(
                msg_count=3, delay_seconds=2))

        timestamps = [parse_vumi_date(ts) for _, ts, _ in all_keys]
        self.assertEqual(timestamps[0] - timestamps[1], timedelta(seconds=2))
        self.assertEqual(timestamps[1] - timestamps[2], timedelta(seconds=2))

    @inlineCallbacks
    def test_outbound_delay_seconds(self):
        """
        When creating an outbound message sequence, the messages are returned
        in descending timestamp order and the delay between each timestamp is
        correct
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_outbound_message_sequence(
                msg_count=3, delay_seconds=2))

        timestamps = [parse_vumi_date(ts) for _, ts, _ in all_keys]
        self.assertEqual(timestamps[0] - timestamps[1], timedelta(seconds=2))
        self.assertEqual(timestamps[1] - timestamps[2], timedelta(seconds=2))

    @inlineCallbacks
    def test_ack_delay_seconds(self):
        """
        When creating an ack event message sequence, the messages are returned
        in descending timestamp order and the delay between each timestamp is
        correct
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence(
                event_count=3, delay_seconds=2))

        timestamps = [parse_vumi_date(ts) for _, ts, _ in all_keys]
        self.assertEqual(timestamps[0] - timestamps[1], timedelta(seconds=2))
        self.assertEqual(timestamps[1] - timestamps[2], timedelta(seconds=2))

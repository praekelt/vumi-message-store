from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks, returnValue
from zope.interface import implementer

from vumi.tests.helpers import IHelper
from vumi.message import format_vumi_date


@implementer(IHelper)
class MessageSequenceHelper(object):

    def __init__(self, backend, msg_helper):
        self._backend = backend
        self._msg_helper = msg_helper

    def setup(self, *args, **kwargs):
        pass

    def cleanup(self):
        pass

    @inlineCallbacks
    def create_inbound_message_sequence(self, msg_count=5, delay_seconds=1):
        """
        Generate a sequence of inbound messages in a new batch and add them to
        the backend.

        :param msg_count:
            The number of messages to create
        :param delay_seconds:
            The delay in seconds between the timestamps of sequential messages

        :returns:
            The batch id of the new batch and a list of tuples describing each
            message in the form (key, timestamp, address)
        """
        batch_id = yield self._backend.batch_start()
        all_keys = []
        start = datetime.utcnow().replace(microsecond=0)
        for i in xrange(msg_count):
            timestamp = start - timedelta(seconds=i * delay_seconds)
            addr = "addr%s" % (i,)
            msg = self._msg_helper.make_inbound(
                "Message %s" % (i,), timestamp=timestamp, from_addr=addr)
            yield self._backend.add_inbound_message(msg, batch_ids=[batch_id])

            all_keys.append((msg["message_id"],            # Key
                             format_vumi_date(timestamp),  # Timestamp
                             addr))                        # Address

        returnValue((batch_id, all_keys))

    @inlineCallbacks
    def create_outbound_message_sequence(self, msg_count=5, delay_seconds=1):
        """
        Generate a sequence of outbound messages in a new batch and add them to
        the backend.

        :param msg_count:
            The number of messages to create
        :param delay_seconds:
            The delay in seconds between the timestamps of sequential messages

        :returns:
            The batch id of the new batch and a list of tuples describing each
            message in the form (key, timestamp, address)
        """
        batch_id = yield self._backend.batch_start()
        all_keys = []
        start = datetime.utcnow().replace(microsecond=0)
        for i in xrange(msg_count):
            timestamp = start - timedelta(seconds=i * delay_seconds)
            addr = "addr%s" % (i,)
            msg = self._msg_helper.make_inbound(
                "Message %s" % (i,), timestamp=timestamp, to_addr=addr)
            yield self._backend.add_outbound_message(msg, batch_ids=[batch_id])

            all_keys.append((msg["message_id"],            # Key
                             format_vumi_date(timestamp),  # Timestamp
                             addr))                        # Address

        returnValue((batch_id, all_keys))

    @inlineCallbacks
    def create_ack_event_sequence(self, event_count=5, delay_seconds=1):
        """
        Generate a sequence of events in a new batch with a test message and
        add them to the backend.

        :param event_count:
            The number of events to create
        :param delay_seconds:
            The delay in seconds between the timestamps of sequential events

        :returns:
            The batch id of the new batch, the message id of the test message,
            and a list of tuples describing each event in the form
            (key, timestamp, status).
        """
        batch_id = yield self._backend.batch_start()
        msg = self._msg_helper.make_outbound("pears")
        all_keys = []
        start = datetime.utcnow().replace(microsecond=0)
        for i in xrange(event_count):
            timestamp = start - timedelta(seconds=i * delay_seconds)
            ack = self._msg_helper.make_ack(msg, timestamp=timestamp)
            yield self._backend.add_event(ack, batch_ids=[batch_id])

            all_keys.append((ack["event_id"],              # Key
                             format_vumi_date(timestamp),  # Timestamp
                             ack["event_type"]))           # Status

        returnValue((batch_id, msg["message_id"], all_keys))

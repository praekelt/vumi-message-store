# -*- test-case-name: vumi_message_store.tests.test_riak_backend -*-

"""
Riak backend for message store.
"""

from twisted.internet.defer import returnValue
from vumi.persist.model import Manager

from vumi_message_store.models import InboundMessage, OutboundMessage, Event


class MessageStoreRiakBackend(object):
    """
    Riak backend for message store operations.

    This implements all message store operations that use Riak. Higher-level
    message store objects should route all Riak things through here.
    """

    def __init__(self, manager):
        self.manager = manager
        self.inbound_messages = manager.proxy(InboundMessage)
        self.outbound_messages = manager.proxy(OutboundMessage)
        self.events = manager.proxy(Event)

    @Manager.calls_manager
    def add_inbound_message(self, msg, batch_ids=()):
        """
        Store an inbound message in Riak.
        """
        msg_id = msg['message_id']
        msg_record = yield self.inbound_messages.load(msg_id)
        if msg_record is None:
            msg_record = self.inbound_messages(msg_id, msg=msg)
        else:
            msg_record.msg = msg

        for batch_id in batch_ids:
            msg_record.batches.add_key(batch_id)

        yield msg_record.save()

    def get_raw_inbound_message(self, msg_id):
        """
        Get an InboundMessage model object from Riak.
        """
        return self.inbound_messages.load(msg_id)

    @Manager.calls_manager
    def get_inbound_message(self, msg_id):
        """
        Get an inbound TransportUserMessage object from Riak.
        """
        msg = yield self.get_raw_inbound_message(msg_id)
        returnValue(msg.msg if msg is not None else None)

    @Manager.calls_manager
    def add_outbound_message(self, msg, batch_ids=()):
        """
        Store an outbound message in Riak.
        """
        msg_id = msg['message_id']
        msg_record = yield self.outbound_messages.load(msg_id)
        if msg_record is None:
            msg_record = self.outbound_messages(msg_id, msg=msg)
        else:
            msg_record.msg = msg

        for batch_id in batch_ids:
            msg_record.batches.add_key(batch_id)

        yield msg_record.save()

    def get_raw_outbound_message(self, msg_id):
        """
        Get an OutboundMessage model object from Riak.
        """
        return self.outbound_messages.load(msg_id)

    @Manager.calls_manager
    def get_outbound_message(self, msg_id):
        """
        Get an outbound TransportUserMessage object from Riak.
        """
        msg = yield self.get_raw_outbound_message(msg_id)
        returnValue(msg.msg if msg is not None else None)

    @Manager.calls_manager
    def add_event(self, event):
        """
        Store an event in Riak.
        """
        event_id = event['event_id']
        msg_id = event['user_message_id']
        event_record = yield self.events.load(event_id)
        if event_record is None:
            event_record = self.events(event_id, event=event, message=msg_id)
        else:
            event_record.event = event
        yield event_record.save()

    def get_raw_event(self, event_id):
        """
        Get an Event model object from Riak.
        """
        return self.events.load(event_id)

    @Manager.calls_manager
    def get_event(self, event_id):
        """
        Get a TransportEvent object from Riak.
        """
        event = yield self.get_raw_event(event_id)
        returnValue(event.event if event is not None else None)

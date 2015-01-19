# -*- test-case-name: vumi.components.tests.test_message_store -*-

"""Message store."""

from zope.interface import implementer

from vumi_message_store.interfaces import IOperationalMessageStore
from vumi_message_store.riak_backend import MessageStoreRiakBackend


@implementer(IOperationalMessageStore)
class RiakOnlyOperationalMessageStore(object):
    """
    Operational message store that only uses Riak.

    This basically just proxies a subset of MessageStoreRiakBackend.
    """

    def __init__(self, manager):
        self.manager = manager
        self.riak_backend = MessageStoreRiakBackend(self.manager)

    def add_inbound_message(self, msg, batch_ids=()):
        """
        Add an inbound message to the message store.
        """
        return self.riak_backend.add_inbound_message(msg, batch_ids=batch_ids)

    def get_inbound_message(self, msg_id):
        """
        Get an inbound message from the message store.
        """
        return self.riak_backend.get_inbound_message(msg_id)

    def add_outbound_message(self, msg, batch_ids=()):
        """
        Add an outbound message to the message store.
        """
        return self.riak_backend.add_outbound_message(msg, batch_ids=batch_ids)

    def get_outbound_message(self, msg_id):
        """
        Get an outbound message from the message store.
        """
        return self.riak_backend.get_outbound_message(msg_id)

    def add_event(self, event):
        """
        Add an event to the message store.
        """
        return self.riak_backend.add_event(event)

    def get_event(self, event_id):
        """
        Get an event from the message store.
        """
        return self.riak_backend.get_event(event_id)

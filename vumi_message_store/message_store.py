# -*- test-case-name: vumi.components.tests.test_message_store -*-

"""Message store."""

from zope.interface import implementer

from vumi_message_store.interfaces import (
    IMessageStoreBatchManager, IOperationalMessageStore, IQueryMessageStore)
from vumi_message_store.riak_backend import MessageStoreRiakBackend


@implementer(IMessageStoreBatchManager)
class RiakOnlyMessageStoreBatchManager(object):
    """
    Message store batch manager.

    This basically just proxies a subset of MessageStoreRiakBackend.
    """

    def __init__(self, manager):
        self.manager = manager
        self.riak_backend = MessageStoreRiakBackend(self.manager)

    def batch_start(self, tags=(), **metadata):
        """
        Create a new message batch.

        :param tags:
            Sequence of tags to add to the new batch.
        :param **metadata:
            Keyword parameters containing batch metadata.

        :returns:
            The batch identifier for the new batch.
        """
        return self.riak_backend.batch_start(tags=tags, **metadata)

    def batch_done(self, batch_id):
        """
        Clear all references to a batch from its tags.
        """
        return self.riak_backend.batch_done(batch_id)

    def get_batch(self, batch_id):
        """
        Get a batch from the message store.
        """
        return self.riak_backend.get_batch(batch_id)

    def get_tag_info(self, tag):
        """
        Get tag information from the message store.
        """
        # TODO: accept tuple instead of flat tag?
        return self.riak_backend.get_tag_info(tag)


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


@implementer(IQueryMessageStore)
class RiakOnlyQueryMessageStore(object):
    """
    Query message store that only uses Riak.

    This basically just proxies a subset of MessageStoreRiakBackend.
    """

    def __init__(self, manager):
        self.manager = manager
        self.riak_backend = MessageStoreRiakBackend(self.manager)

    def get_inbound_message(self, msg_id):
        """
        Get an inbound message from the message store.
        """
        return self.riak_backend.get_inbound_message(msg_id)

    def get_outbound_message(self, msg_id):
        """
        Get an outbound message from the message store.
        """
        return self.riak_backend.get_outbound_message(msg_id)

    def get_event(self, event_id):
        """
        Get an event from the message store.
        """
        return self.riak_backend.get_event(event_id)

    def list_batch_inbound_keys(self, batch_id, max_results=None,
                                continuation=None):
        """
        List inbound message keys for the given batch.
        """
        return self.riak_backend.list_batch_inbound_keys(
            batch_id, max_results=max_results, continuation=continuation)

    def list_batch_outbound_keys(self, batch_id, max_results=None,
                                 continuation=None):
        """
        List outbound message keys for the given batch.
        """
        return self.riak_backend.list_batch_outbound_keys(
            batch_id, max_results=max_results, continuation=continuation)

    def list_message_event_keys(self, message_id, max_results=None,
                                continuation=None):
        """
        List event keys for the given outbound message.
        """
        return self.riak_backend.list_message_event_keys(
            message_id, max_results=max_results, continuation=continuation)

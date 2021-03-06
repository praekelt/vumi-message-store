# -*- test-case-name: vumi_message_store.tests.test_message_store -*-

"""Message store."""

from vumi.persist.redis_base import Manager
from twisted.internet.defer import returnValue
from zope.interface import implementer

from vumi_message_store.interfaces import (
    IMessageStoreBatchManager, IOperationalMessageStore, IQueryMessageStore)
from vumi_message_store.batch_info_cache import BatchInfoCache
from vumi_message_store.riak_backend import MessageStoreRiakBackend


@implementer(IMessageStoreBatchManager)
class MessageStoreBatchManager(object):
    """
    Message store batch manager.

    This proxies a subset of MessageStoreRiakBackend and BatchInfoCache.
    """

    def __init__(self, riak_manager, redis_manager):
        self.manager = riak_manager
        self.redis = redis_manager
        self.riak_backend = MessageStoreRiakBackend(self.manager)
        self.batch_info_cache = BatchInfoCache(self.redis)

    @Manager.calls_manager
    def batch_start(self, tags=(), **metadata):
        """
        Create a new message batch and initialise its info cache.

        :param tags:
            Sequence of tags to add to the new batch.
        :param **metadata:
            Keyword parameters containing batch metadata.

        :returns:
            The batch identifier for the new batch.
        """
        batch_id = yield self.riak_backend.batch_start(tags=tags, **metadata)
        yield self.batch_info_cache.batch_start(batch_id)
        returnValue(batch_id)

    def batch_done(self, batch_id):
        """
        Clear all references to a batch from its tags.

        NOTE: This does not clear the batch info cache.
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
        return self.riak_backend.get_tag_info(tag)

    def rebuild_cache(self, batch_id, qms):
        """
        Rebuild the cache using the provided IQueryMessageStore implementation.
        """
        return self.batch_info_cache.rebuild_cache(batch_id, qms)


@implementer(IOperationalMessageStore)
class OperationalMessageStore(object):
    """
    Operational message store that uses Riak directly.

    This proxies a subset of MessageStoreRiakBackend and BatchInfoCache.
    """

    def __init__(self, riak_manager, redis_manager):
        self.manager = riak_manager
        self.redis = redis_manager
        self.riak_backend = MessageStoreRiakBackend(self.manager)
        self.batch_info_cache = BatchInfoCache(self.redis)

    @Manager.calls_manager
    def add_inbound_message(self, msg, batch_ids=()):
        """
        Add an inbound message to the message store.
        """
        yield self.riak_backend.add_inbound_message(msg, batch_ids=batch_ids)
        for batch_id in batch_ids:
            yield self.batch_info_cache.add_inbound_message(batch_id, msg)

    def get_inbound_message(self, msg_id):
        """
        Get an inbound message from the message store.
        """
        return self.riak_backend.get_inbound_message(msg_id)

    @Manager.calls_manager
    def add_outbound_message(self, msg, batch_ids=()):
        """
        Add an outbound message to the message store.
        """
        yield self.riak_backend.add_outbound_message(msg, batch_ids=batch_ids)
        for batch_id in batch_ids:
            yield self.batch_info_cache.add_outbound_message(batch_id, msg)

    def get_outbound_message(self, msg_id):
        """
        Get an outbound message from the message store.
        """
        return self.riak_backend.get_outbound_message(msg_id)

    @Manager.calls_manager
    def add_event(self, event, batch_ids=()):
        """
        Add an event to the message store.
        """
        yield self.riak_backend.add_event(event, batch_ids=batch_ids)
        for batch_id in batch_ids:
            yield self.batch_info_cache.add_event(batch_id, event)

    def get_event(self, event_id):
        """
        Get an event from the message store.
        """
        return self.riak_backend.get_event(event_id)

    def get_tag_info(self, tag):
        """
        Get tag information from the message store.
        """
        return self.riak_backend.get_tag_info(tag)


@implementer(IQueryMessageStore)
class QueryMessageStore(object):
    """
    Query-only message store.

    This proxies a subset of MessageStoreRiakBackend and BatchInfoCache.
    """

    def __init__(self, riak_manager, redis_manager):
        self.manager = riak_manager
        self.redis = redis_manager
        self.riak_backend = MessageStoreRiakBackend(self.manager)
        self.batch_info_cache = BatchInfoCache(self.redis)

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

    def list_batch_inbound_messages(self, batch_id, start=None, end=None,
                                    page_size=None):
        """
        List inbound message keys with timestamps and addresses in descending
        timestamp order for the given batch.
        """
        return self.riak_backend.list_batch_inbound_messages(
            batch_id, start=start, end=end, page_size=page_size)

    def list_batch_outbound_messages(self, batch_id, start=None, end=None,
                                     page_size=None):
        """
        List outbound message keys with timestamps and addresses in descending
        timestamp order for the given batch.
        """
        return self.riak_backend.list_batch_outbound_messages(
            batch_id, start=start, end=end, page_size=page_size)

    def list_message_events(self, message_id, start=None, end=None,
                            page_size=None):
        """
        List event keys with timestamps and statuses in ascending timestamp
        order for the given outbound message.
        """
        return self.riak_backend.list_message_events(
            message_id, start=start, end=end, page_size=page_size)

    def list_batch_events(self, batch_id, start=None, end=None,
                          page_size=None):
        """
        List event keys with timestamps and statuses in descending timestamp
        order for the given batch.
        """
        return self.riak_backend.list_batch_events(
            batch_id, start=start, end=end, page_size=page_size)

    def get_batch_info_status(self, batch_id):
        """
        Return a dictionary containing the latest event stats for the given
        batch_id.
        """
        return self.batch_info_cache.get_batch_status(batch_id)

    def get_batch_inbound_count(self, batch_id):
        return self.batch_info_cache.get_inbound_message_count(batch_id)

    def get_batch_outbound_count(self, batch_id):
        return self.batch_info_cache.get_outbound_message_count(batch_id)

    def get_batch_event_count(self, batch_id):
        return self.batch_info_cache.get_event_count(batch_id)

    def get_batch_from_addr_count(self, batch_id):
        return self.batch_info_cache.get_from_addr_count(batch_id)

    def get_batch_to_addr_count(self, batch_id):
        return self.batch_info_cache.get_to_addr_count(batch_id)

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

    def list_batch_inbound_keys_with_timestamps(self, batch_id, start=None,
                                                end=None, max_results=None):
        """
        List inbound message keys with timestamps for the given batch.
        """
        return self.riak_backend.list_batch_inbound_keys_with_timestamps(
            batch_id, start=start, end=end, max_results=max_results)

    def list_batch_outbound_keys_with_timestamps(self, batch_id, start=None,
                                                 end=None, max_results=None):
        """
        List outbound message keys with timestamps for the given batch.
        """
        return self.riak_backend.list_batch_outbound_keys_with_timestamps(
            batch_id, start=start, end=end, max_results=max_results)

    def list_batch_inbound_keys_with_addresses(self, batch_id, start=None,
                                               end=None, max_results=None):
        """
        List inbound message keys with timestamps and addresses in descending
        timestamp order for the given batch.
        """
        return self.riak_backend.list_batch_inbound_keys_with_addresses(
            batch_id, start=start, end=end, max_results=max_results)

    def list_batch_outbound_keys_with_addresses(self, batch_id, start=None,
                                                end=None, max_results=None):
        """
        List outbound message keys with timestamps and addresses in descending
        timestamp order for the given batch.
        """
        return self.riak_backend.list_batch_outbound_keys_with_addresses(
            batch_id, start=start, end=end, max_results=max_results)

    def list_message_event_keys_with_statuses(self, message_id,
                                              max_results=None):
        """
        List event keys with timestamps and statuses for the given outbound
        message.
        """
        return self.riak_backend.list_message_event_keys_with_statuses(
            message_id, max_results=max_results)

    def list_batch_events(self, batch_id, start=None, end=None,
                          max_results=None):
        """
        List event keys with timestamps and statuses for the given outbound
        message.
        """
        return self.riak_backend.list_batch_events(batch_id,
                                                   start=start,
                                                   end=end,
                                                   max_results=max_results)

    def get_batch_info_status(self, batch_id):
        """
        Return a dictionary containing the latest event stats for the given
        batch_id.
        """
        return self.batch_info_cache.get_batch_status(batch_id)

    def list_batch_recent_inbound_keys(self, batch_id, with_timestamp=False):
        """
        Return the list of recent inbound message keys in descending order by
        timestamp.
        """
        return self.batch_info_cache.list_inbound_message_keys(
            batch_id, with_timestamp=with_timestamp)

    def list_batch_recent_outbound_keys(self, batch_id, with_timestamp=False):
        """
        Return the list of recent outbound message keys in descending order by
        timestamp.
        """
        return self.batch_info_cache.list_outbound_message_keys(
            batch_id, with_timestamp=with_timestamp)

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

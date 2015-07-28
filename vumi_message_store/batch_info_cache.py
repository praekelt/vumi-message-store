# -*- test-case-name: vumi_message_store.tests.test_batch_info_cache -*-
# -*- coding: utf-8 -*-

from calendar import timegm
from datetime import datetime

from twisted.internet.defer import returnValue

from vumi.persist.redis_base import Manager
from vumi.message import TransportEvent, VUMI_DATE_FORMAT
from vumi.errors import VumiError


def to_timestamp(timestamp):
    """
    Return a timestamp value for a datetime value.
    """
    if isinstance(timestamp, basestring):
        timestamp = datetime.strptime(timestamp, VUMI_DATE_FORMAT)
    # We can't use time.mktime(), because that takes local time and we have
    # UTC. The UTC equivalent, for some obscure reason, is in the calendar
    # module.
    return timegm(timestamp.timetuple())


class BatchInfoCacheException(VumiError):
    pass


class BatchInfoCache(object):
    """
    Redis-based cache for assorted batch-related information that is expensive
    to acquire from Riak but useful to have low-latency access to.
    """
    BATCH_KEY = 'batches'
    OUTBOUND_KEY = 'outbound'
    OUTBOUND_COUNT_KEY = 'outbound_count'
    INBOUND_KEY = 'inbound'
    INBOUND_COUNT_KEY = 'inbound_count'
    TO_ADDR_KEY = 'to_addr_hll'
    FROM_ADDR_KEY = 'from_addr_hll'
    EVENT_KEY = 'event'
    EVENT_COUNT_KEY = 'event_count'
    STATUS_KEY = 'status'
    TRUNCATE_MESSAGE_KEY_ZSET_AT = 2000

    def __init__(self, redis):
        # Store redis as `manager` as well since @Manager.calls_manager
        # requires it to be named as such.
        self.redis = self.manager = redis

    def key(self, *args):
        return ':'.join([unicode(a) for a in args])

    def batch_key(self, *args):
        return self.key(self.BATCH_KEY, *args)

    def outbound_key(self, batch_id):
        return self.batch_key(self.OUTBOUND_KEY, batch_id)

    def outbound_count_key(self, batch_id):
        return self.batch_key(self.OUTBOUND_COUNT_KEY, batch_id)

    def inbound_key(self, batch_id):
        return self.batch_key(self.INBOUND_KEY, batch_id)

    def inbound_count_key(self, batch_id):
        return self.batch_key(self.INBOUND_COUNT_KEY, batch_id)

    def to_addr_key(self, batch_id):
        return self.batch_key(self.TO_ADDR_KEY, batch_id)

    def from_addr_key(self, batch_id):
        return self.batch_key(self.FROM_ADDR_KEY, batch_id)

    def status_key(self, batch_id):
        return self.batch_key(self.STATUS_KEY, batch_id)

    def event_key(self, batch_id):
        return self.batch_key(self.EVENT_KEY, batch_id)

    def event_count_key(self, batch_id):
        return self.batch_key(self.EVENT_COUNT_KEY, batch_id)

    def obsolete_keys(self, batch_id):
        """
        Return a list of obsolete keys that should be cleared.
        """
        return [
            self.batch_key("to_addr", batch_id),
            self.batch_key("from_addr", batch_id),
        ]

    @Manager.calls_manager
    def _truncate_keys(self, redis_key, truncate_at):
        truncate_at = (truncate_at or self.TRUNCATE_MESSAGE_KEY_ZSET_AT)
        keys_removed = yield self.redis.zremrangebyrank(
            redis_key, 0, -truncate_at - 1)
        returnValue(keys_removed)

    def truncate_inbound_message_keys(self, batch_id, truncate_at=None):
        return self._truncate_keys(self.inbound_key(batch_id), truncate_at)

    def truncate_outbound_message_keys(self, batch_id, truncate_at=None):
        return self._truncate_keys(self.outbound_key(batch_id), truncate_at)

    def truncate_event_keys(self, batch_id, truncate_at=None):
        return self._truncate_keys(self.event_key(batch_id), truncate_at)

    @Manager.calls_manager
    def batch_start(self, batch_id):
        """
        Create the counter keys and status hash for a batch and add the batch
        identifier to the set of tracked batches.

        A call to this isn't strictly necessary, but is good for general
        housekeeping.

        This operation idempotent.
        """
        # TODO: Do we really want to keep a set full of batch identifiers?
        yield self.redis.sadd(self.batch_key(), batch_id)
        yield self.redis.set(self.inbound_count_key(batch_id), 0)
        yield self.redis.set(self.outbound_count_key(batch_id), 0)
        yield self.redis.set(self.event_count_key(batch_id), 0)
        # If the status hash already exists and has any keys in it, this will
        # not reset those keys to zero.
        events = (TransportEvent.EVENT_TYPES.keys() +
                  ['delivery_report.%s' % status
                   for status in TransportEvent.DELIVERY_STATUSES] +
                  ['sent'])
        for event in events:
            yield self.redis.hsetnx(self.status_key(batch_id), event, 0)

    def batch_exists(self, batch_id):
        return self.redis.sismember(self.batch_key(), batch_id)

    @Manager.calls_manager
    def clear_batch(self, batch_id):
        """
        Removes all cached values for the given batch_id, useful before a
        reconciliation happens to ensure that we start from scratch.

        NOTE:   This will reset all counters back to zero and will increment
                them as messages are received. If your UI depends on your
                cached values your UI values might be off while the
                reconciliation is taking place.
        """
        for key in self.obsolete_keys(batch_id):
            yield self.redis.delete(key)
        yield self.redis.delete(self.inbound_key(batch_id))
        yield self.redis.delete(self.inbound_count_key(batch_id))
        yield self.redis.delete(self.outbound_key(batch_id))
        yield self.redis.delete(self.outbound_count_key(batch_id))
        yield self.redis.delete(self.event_key(batch_id))
        yield self.redis.delete(self.event_count_key(batch_id))
        yield self.redis.delete(self.status_key(batch_id))
        yield self.redis.srem(self.batch_key(), batch_id)

    @Manager.calls_manager
    def add_inbound_message(self, batch_id, msg):
        """
        Add an inbound message to the cache for the given batch_id.
        """
        timestamp = to_timestamp(msg["timestamp"])
        yield self.add_inbound_message_key(
            batch_id, msg["message_id"], timestamp)
        yield self.add_from_addr(batch_id, msg['from_addr'])

    @Manager.calls_manager
    def add_inbound_message_key(self, batch_id, message_key, timestamp):
        """
        Add a message key, weighted with the timestamp to the batch_id.
        """
        new_entry = yield self.redis.zadd(self.inbound_key(batch_id), **{
            message_key.encode('utf-8'): timestamp,
        })
        if new_entry:
            yield self.redis.incr(self.inbound_count_key(batch_id))
            yield self.truncate_inbound_message_keys(batch_id)

    def add_from_addr(self, batch_id, *from_addrs):
        """
        Add a from address to the HyperLogLog counter for the batch.
        """
        if len(from_addrs) == 0:
            return
        from_addrs = [from_addr.encode('utf-8') for from_addr in from_addrs]
        return self.redis.pfadd(self.from_addr_key(batch_id), *from_addrs)

    @Manager.calls_manager
    def add_outbound_message(self, batch_id, msg):
        """
        Add an outbound message to the cache for the given batch_id.
        """
        timestamp = to_timestamp(msg['timestamp'])
        yield self.add_outbound_message_key(
            batch_id, msg['message_id'], timestamp)
        yield self.add_to_addr(batch_id, msg['to_addr'])

    @Manager.calls_manager
    def add_outbound_message_key(self, batch_id, message_key, timestamp):
        """
        Add a message key, weighted with the timestamp to the batch_id.
        """
        new_entry = yield self.redis.zadd(self.outbound_key(batch_id), **{
            message_key.encode('utf-8'): timestamp,
        })
        if new_entry:
            yield self.increment_event_status(batch_id, 'sent')
            yield self.redis.incr(self.outbound_count_key(batch_id))
            yield self.truncate_outbound_message_keys(batch_id)

    def add_to_addr(self, batch_id, *to_addrs):
        """
        Add a from address to the HyperLogLog counter for the batch.
        """
        if len(to_addrs) == 0:
            return
        to_addrs = [to_addr.encode('utf-8') for to_addr in to_addrs]
        return self.redis.pfadd(self.to_addr_key(batch_id), *to_addrs)

    @Manager.calls_manager
    def add_event(self, batch_id, event):
        """
        Add an event to the cache for the given batch_id
        """
        event_id = event['event_id']
        timestamp = to_timestamp(event['timestamp'])
        event_type = event['event_type']
        if event_type == 'delivery_report':
            event_type = "%s.%s" % (event_type, event['delivery_status'])
        yield self.add_event_key(batch_id, event_id, event_type, timestamp)

    @Manager.calls_manager
    def add_event_key(self, batch_id, event_key, event_type, timestamp):
        """
        Add the event key to the set of known event keys. If the event is a
        delivery report, event_type should include the delivery status.
        """
        new_entry = yield self.redis.zadd(self.event_key(batch_id), **{
            event_key.encode('utf-8'): timestamp,
        })
        if new_entry:
            yield self.redis.incr(self.event_count_key(batch_id))
            yield self.truncate_event_keys(batch_id)
            yield self.increment_event_status(batch_id, event_type)

    @Manager.calls_manager
    def increment_event_status(self, batch_id, event_type, count=1):
        """
        Increment the status for the given event_type for the given batch_id.
        If the event is a delivery report, event_type should include the
        delivery status.
        """
        status_key = self.status_key(batch_id)
        yield self.redis.hincrby(status_key, event_type, count)
        if event_type.startswith("delivery_report."):
            yield self.redis.hincrby(status_key, "delivery_report", count)

    @Manager.calls_manager
    def add_inbound_message_count(self, batch_id, count):
        """
        Add a count to all inbound message counters. (Used for recon.)
        """
        yield self.redis.incr(self.inbound_count_key(batch_id), count)

    @Manager.calls_manager
    def add_outbound_message_count(self, batch_id, count):
        """
        Add a count to all outbound message counters. (Used for recon.)
        """
        yield self.increment_event_status(batch_id, 'sent', count)
        yield self.redis.incr(self.outbound_count_key(batch_id), count)

    @Manager.calls_manager
    def add_event_count(self, batch_id, status, count):
        """
        Add a count to all relevant event counters. (Used for recon.)
        """
        yield self.increment_event_status(batch_id, status, count)
        yield self.redis.incr(self.event_count_key(batch_id), count)

    @Manager.calls_manager
    def get_batch_status(self, batch_id):
        """
        Return a dictionary containing the latest event stats for the given
        batch_id.
        """
        stats = yield self.redis.hgetall(self.status_key(batch_id))
        returnValue(dict([(k, int(v)) for k, v in stats.iteritems()]))

    def list_inbound_message_keys(self, batch_id, with_timestamp=False):
        """
        Return the list of recent inbound message keys in descending order by
        timestamp.
        """
        return self.redis.zrange(
            self.inbound_key(batch_id), 0, -1, desc=True,
            withscores=with_timestamp)

    def list_outbound_message_keys(self, batch_id, with_timestamp=False):
        """
        Return the list of recent outbound message keys in descending order by
        timestamp.
        """
        return self.redis.zrange(
            self.outbound_key(batch_id), 0, -1, desc=True,
            withscores=with_timestamp)

    def list_event_keys(self, batch_id, with_timestamp=False):
        """
        Return the list of recent event keys in descending order by timestamp.
        """
        return self.redis.zrange(
            self.event_key(batch_id), 0, -1, desc=True,
            withscores=with_timestamp)

    @Manager.calls_manager
    def _get_counter_value(self, counter_key):
        count = yield self.redis.get(counter_key)
        returnValue(0 if count is None else int(count))

    def get_inbound_message_count(self, batch_id):
        """
        Return the count of inbound messages.
        """
        return self._get_counter_value(self.inbound_count_key(batch_id))

    def get_outbound_message_count(self, batch_id):
        """
        Return the count of outbound messages.
        """
        return self._get_counter_value(self.outbound_count_key(batch_id))

    def get_event_count(self, batch_id):
        """
        Return the count of events.
        """
        return self._get_counter_value(self.event_count_key(batch_id))

    def get_from_addr_count(self, batch_id):
        """
        Return the count of from addresses.
        """
        return self.redis.pfcount(self.from_addr_key(batch_id))

    def get_to_addr_count(self, batch_id):
        """
        Return the count of to addresses.
        """
        return self.redis.pfcount(self.to_addr_key(batch_id))

    @Manager.calls_manager
    def rebuild_cache(self, batch_id, qms, page_size=None):
        """
        Rebuild the cache using the provided IQueryMessageStore implementation.
        """
        yield self.clear_batch(batch_id)
        yield self.batch_start(batch_id)

        yield self._rebuild_inbound_messages(batch_id, qms, page_size)
        yield self._rebuild_outbound_messages(batch_id, qms, page_size)
        yield self._rebuild_events(batch_id, qms, page_size)

    @Manager.calls_manager
    def _rebuild_inbound_messages(self, batch_id, qms, page_size=None):
        """
        Rebuild the cache by loading the latest inbound messages for the given
        batch into the cache and counting all the messages.
        """
        inbound_page = yield qms.list_batch_inbound_keys_with_addresses(
            batch_id, max_results=page_size)
        count = 0
        recents_added = False
        while inbound_page is not None:
            from_addrs = set()
            for key, timestamp, from_addr in inbound_page:
                count += 1
                from_addrs.add(from_addr)
                # Treat the most recent messages as though we were recording
                # them in flight.
                if not recents_added:
                    yield self.add_inbound_message_key(
                        batch_id, key, to_timestamp(timestamp))
                    yield self.add_from_addr(batch_id, from_addr)
                    if count == self.TRUNCATE_MESSAGE_KEY_ZSET_AT:
                        recents_added = True
                        count = 0

            yield self.add_from_addr(batch_id, *from_addrs)
            # After storing the most recent messages, count the rest, updating
            # the count in Redis after processing each page.
            if recents_added:
                yield self.add_inbound_message_count(batch_id, count)
                count = 0

            inbound_page = yield inbound_page.next_page()

    @Manager.calls_manager
    def _rebuild_outbound_messages(self, batch_id, qms, page_size=None):
        """
        Rebuild the cache by loading the latest outbound messages for the given
        batch into the cache and counting all the messages.
        """
        outbound_page = yield qms.list_batch_outbound_keys_with_addresses(
            batch_id, max_results=page_size)
        count = 0
        recents_added = False
        while outbound_page is not None:
            to_addrs = set()
            for key, timestamp, to_addr in outbound_page:
                count += 1
                to_addrs.add(to_addr)
                # Treat the most recent messages as though we were recording
                # them in flight.
                if not recents_added:
                    yield self.add_outbound_message_key(
                        batch_id, key, to_timestamp(timestamp))
                    if count == self.TRUNCATE_MESSAGE_KEY_ZSET_AT:
                        recents_added = True
                        count = 0

            yield self.add_to_addr(batch_id, *to_addrs)
            # After storing the most recent messages, count the rest, updating
            # the count in Redis after processing each page.
            if recents_added:
                yield self.add_outbound_message_count(batch_id, count)
                count = 0

            outbound_page = yield outbound_page.next_page()

    @Manager.calls_manager
    def _rebuild_events(self, batch_id, qms, page_size=None):
        """
        Rebuild the cache by loading the latest events for the given batch into
        the cache and counting all the events.
        """
        event_page = yield qms.list_batch_events(
            batch_id, max_results=page_size)
        count = 0
        recents_added = False
        statuses = {}
        while event_page is not None:
            for key, timestamp, status in event_page:
                count += 1
                # Treat the most recent events as though we were recording
                # them in flight.
                if not recents_added:
                    yield self.add_event_key(
                        batch_id, key, status, to_timestamp(timestamp))
                    if count == self.TRUNCATE_MESSAGE_KEY_ZSET_AT:
                        recents_added = True
                        count = 0
                else:
                    statuses[status] = statuses.get(status, 0) + 1

            # After storing the most recent events, count the rest, updating
            # the count in Redis after processing each page.
            if recents_added:
                for status, count in statuses.iteritems():
                    yield self.add_event_count(batch_id, status, count)
                statuses.clear()

            event_page = yield event_page.next_page()

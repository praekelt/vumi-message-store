# -*- test-case-name: vumi_message_store.tests.test_riak_backend -*-

"""
Riak backend for message store.
"""

from uuid import uuid4

from twisted.internet.defer import returnValue
from vumi.persist.model import Manager

from vumi_message_store.models import (
    from_reverse_timestamp, to_reverse_timestamp,
    Batch, CurrentTag, InboundMessage, OutboundMessage, Event)


class MessageStoreRiakBackend(object):
    """
    Riak backend for message store operations.

    This implements all message store operations that use Riak. Higher-level
    message store objects should route all Riak things through here.
    """

    # The Python Riak client defaults to max_results=1000 in places.
    DEFAULT_PAGE_SIZE = 1000

    def __init__(self, manager):
        self.manager = manager
        self.batches = manager.proxy(Batch)
        self.current_tags = manager.proxy(CurrentTag)
        self.inbound_messages = manager.proxy(InboundMessage)
        self.outbound_messages = manager.proxy(OutboundMessage)
        self.events = manager.proxy(Event)

    @Manager.calls_manager
    def batch_start(self, tags=(), **metadata):
        """
        Create a new batch and store it in Riak.
        """
        batch_id = uuid4().get_hex()
        batch = self.batches(batch_id)
        batch.tags.extend(tags)
        for key, value in metadata.iteritems():
            batch.metadata[key] = value
        yield batch.save()

        for tag in tags:
            tag_record = yield self.current_tags.load(tag)
            if tag_record is None:
                tag_record = self.current_tags(tag)
            tag_record.current_batch.set(batch)
            yield tag_record.save()

        returnValue(batch_id)

    @Manager.calls_manager
    def batch_done(self, batch_id):
        """
        Clear all references to a batch from its tags.
        """
        batch = yield self.batches.load(batch_id)
        tag_keys = yield batch.backlinks.currenttags()
        for tag_key in tag_keys:
            tag = yield self.current_tags.load(tag_key)
            if tag is not None:
                tag.current_batch.set(None)
                yield tag.save()

    def get_batch(self, batch_id):
        """
        Get a Batch model object from Riak.
        """
        return self.batches.load(batch_id)

    @Manager.calls_manager
    def get_tag_info(self, tag):
        """
        Get a CurrentTag model object from Riak or create it if it isn't found.
        """
        tagmdl = yield self.current_tags.load(tag)
        if tagmdl is None:
            tagmdl = yield self.current_tags(tag)
        returnValue(tagmdl)

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
    def add_event(self, event, batch_ids=()):
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

        for batch_id in batch_ids:
            event_record.batches.add_key(batch_id)

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

    def _start_end_range(self, batch_id, start, end):
        if start is not None:
            start_value = "%s$%s" % (batch_id, start)
        else:
            start_value = "%s%s" % (batch_id, "#")  # chr(ord('$') - 1)
        if end is not None:
            # We append the "%" to this because we may have another field after
            # the timestamp and we want to include that in range.
            end_value = "%s$%s%s" % (batch_id, end, "%")  # chr(ord('$') + 1)
        else:
            end_value = "%s%s" % (batch_id, "%")  # chr(ord('$') + 1)
        return start_value, end_value

    def _start_end_range_reverse(self, batch_id, start, end):
        if start is not None:
            start = to_reverse_timestamp(start)
        if end is not None:
            end = to_reverse_timestamp(end)
        # The index is an inverse timestamp so the start and end range values
        # are swapped.
        start, end = end, start
        return self._start_end_range(batch_id, start, end)

    @Manager.calls_manager
    def list_batch_inbound_messages(self, batch_id, start=None, end=None,
                                    page_size=None):
        """
        List inbound message keys with timestamps and addresses in descending
        timestamp order for the given batch.
        """
        if page_size is None:
            page_size = self.DEFAULT_PAGE_SIZE
        start_range, end_range = (
            self._start_end_range_reverse(batch_id, start, end))
        results = yield self.inbound_messages.index_keys_page(
            'batches_with_addresses_reverse', start_range, end_range,
            return_terms=True, max_results=page_size)
        returnValue(IndexPageWrapper(
            key_with_rts_and_value_formatter, self, batch_id, results))

    @Manager.calls_manager
    def list_batch_outbound_messages(self, batch_id, start=None, end=None,
                                     page_size=None):
        """
        List outbound message keys with timestamps and addresses in descending
        timestamp order for the given batch.
        """
        if page_size is None:
            page_size = self.DEFAULT_PAGE_SIZE
        start_range, end_range = (
            self._start_end_range_reverse(batch_id, start, end))
        results = yield self.outbound_messages.index_keys_page(
            'batches_with_addresses_reverse', start_range, end_range,
            return_terms=True, max_results=page_size)
        returnValue(IndexPageWrapper(
            key_with_rts_and_value_formatter, self, batch_id, results))

    @Manager.calls_manager
    def list_message_events(self, message_id, start=None, end=None,
                            page_size=None):
        """
        List event keys with timestamps and statuses in ascending timestamp
        order for the given outbound message.
        """
        if page_size is None:
            page_size = self.DEFAULT_PAGE_SIZE
        start_value, end_value = self._start_end_range(message_id, start, end)
        results = yield self.events.index_keys_page(
            'message_with_status', start_value, end_value, return_terms=True,
            max_results=page_size)
        returnValue(IndexPageWrapper(
            key_with_ts_and_value_formatter, self, message_id, results))

    @Manager.calls_manager
    def list_batch_events(self, batch_id, start=None, end=None,
                          page_size=None):
        """
        List event keys with timestamps and statuses in descending timestamp
        order for the given batch.
        """
        if page_size is None:
            page_size = self.DEFAULT_PAGE_SIZE
        start_range, end_range = (
            self._start_end_range_reverse(batch_id, start, end))
        results = yield self.events.index_keys_page(
            'batches_with_statuses_reverse', start_range, end_range,
            return_terms=True, max_results=page_size)
        returnValue(IndexPageWrapper(
            key_with_rts_and_value_formatter, self, batch_id, results))


class IndexPageWrapper(object):
    """
    Index page wrapper that reformats index values into something easier to
    work with.

    This is a wrapper around the lower-level index page object from Riak and
    proxies a subset of its functionality.
    """
    def __init__(self, formatter, message_store, batch_id, index_page):
        self._formatter = formatter
        self._message_store = message_store
        self.manager = message_store.manager
        self._batch_id = batch_id
        self._index_page = index_page

    def _wrap_index_page(self, index_page):
        """
        Wrap a raw index page object if it is not None.
        """
        if index_page is not None:
            index_page = type(self)(
                self._formatter, self._message_store, self._batch_id,
                index_page)
        return index_page

    @Manager.calls_manager
    def next_page(self):
        """
        Fetch the next page of results.

        :returns:
            A new :class:`KeysWithTimestamps` object containing the next page
            of results.
        """
        next_page = yield self._index_page.next_page()
        returnValue(self._wrap_index_page(next_page))

    def has_next_page(self):
        """
        Indicate whether there are more results to follow.

        :returns:
            ``True`` if there are more results, ``False`` if this is the last
            page.
        """
        return self._index_page.has_next_page()

    def __iter__(self):
        return (self._formatter(self._batch_id, r) for r in self._index_page)

    def __len__(self):
        return len(self._index_page)


def key_with_ts_and_value_formatter(batch_id, result):
    value, key = result
    prefix = batch_id + "$"
    if not value.startswith(prefix):
        raise ValueError(
            "Index value %r does not begin with expected prefix %r." % (
                value, prefix))
    suffix = value[len(prefix):]
    timestamp, delimiter, address = suffix.partition("$")
    if delimiter != "$":
        raise ValueError(
            "Index value %r does not match expected format." % (value,))
    return (key, timestamp, address)


def key_with_rts_and_value_formatter(batch_id, result):
    key, reverse_ts, value = key_with_ts_and_value_formatter(batch_id, result)
    return (key, from_reverse_timestamp(reverse_ts), value)

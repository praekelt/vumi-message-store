# -*- test-case-name: vumi_message_store.tests.test_riak_backend -*-

"""
Riak backend for message store.
"""

from uuid import uuid4

from twisted.internet.defer import returnValue
from vumi.persist.model import Manager

from vumi_message_store.models import (
    Batch, CurrentTag, InboundMessage, OutboundMessage, Event)


class MessageStoreRiakBackend(object):
    """
    Riak backend for message store operations.

    This implements all message store operations that use Riak. Higher-level
    message store objects should route all Riak things through here.
    """

    # The Python Riak client defaults to max_results=1000 in places.
    DEFAULT_MAX_RESULTS = 1000

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
        for tags_bunch in self.manager.load_all_bunches(CurrentTag, tag_keys):
            tags = yield tags_bunch
            for tag in tags:
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

    def list_batch_inbound_keys(self, batch_id, max_results=None,
                                continuation=None):
        """
        List inbound message keys for the given batch.
        """
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        return self.inbound_messages.index_keys_page(
            'batches', batch_id, max_results=max_results,
            continuation=continuation)

    def list_batch_outbound_keys(self, batch_id, max_results=None,
                                 continuation=None):
        """
        List outbound message keys for the given batch.
        """
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        return self.outbound_messages.index_keys_page(
            'batches', batch_id, max_results=max_results,
            continuation=continuation)

    def list_message_event_keys(self, message_id, max_results=None,
                                continuation=None):
        """
        List event keys for the given outbound message.
        """
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        return self.events.index_keys_page(
            'message', message_id, max_results=max_results,
            continuation=continuation)

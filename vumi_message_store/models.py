"""
Riak models for message store objects.
"""

from vumi.message import TransportEvent, TransportUserMessage
from vumi.persist.model import Model
from vumi.persist.fields import (
    VumiMessage, ForeignKey, ManyToMany, ListOf, Tag, Dynamic, Unicode)
from vumi_message_store.migrators import (
    InboundMessageMigrator, OutboundMessageMigrator, EventMigrator)


class Batch(Model):
    # key is batch_id
    tags = ListOf(Tag())
    metadata = Dynamic(Unicode())


class CurrentTag(Model):
    # key is flattened tag
    current_batch = ForeignKey(Batch, null=True)
    tag = Tag()
    metadata = Dynamic(Unicode())

    @staticmethod
    def _flatten_tag(tag):
        return "%s:%s" % tag

    @staticmethod
    def _split_key(key):
        return tuple(key.split(':', 1))

    @classmethod
    def _tag_and_key(cls, tag_or_key):
        if isinstance(tag_or_key, tuple):
            # key looks like a tag
            tag, key = tag_or_key, cls._flatten_tag(tag_or_key)
        else:
            tag, key = cls._split_key(tag_or_key), tag_or_key
        return tag, key

    def __init__(self, manager, key, _riak_object=None, **kw):
        tag, key = self._tag_and_key(key)
        if _riak_object is None:
            kw['tag'] = tag
        super(CurrentTag, self).__init__(manager, key,
                                         _riak_object=_riak_object, **kw)

    @classmethod
    def load(cls, manager, key, result=None):
        _tag, key = cls._tag_and_key(key)
        return super(CurrentTag, cls).load(manager, key, result)


class InboundMessage(Model):
    VERSION = 3
    MIGRATOR = InboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(Batch)

    # Extra fields for compound indexes
    batches_with_timestamps = ListOf(Unicode(), index=True)
    batches_with_addresses = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        batches_with_timestamps = []
        batches_with_addresses = []
        timestamp = self.msg['timestamp']
        for batch_id in self.batches.keys():
            batches_with_timestamps.append(u"%s$%s" % (batch_id, timestamp))
            batches_with_addresses.append(
                u"%s$%s$%s" % (batch_id, timestamp, self.msg['from_addr']))
        self.batches_with_timestamps = batches_with_timestamps
        self.batches_with_addresses = batches_with_addresses
        return super(InboundMessage, self).save()


class OutboundMessage(Model):
    VERSION = 3
    MIGRATOR = OutboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(Batch)

    # Extra fields for compound indexes
    batches_with_timestamps = ListOf(Unicode(), index=True)
    batches_with_addresses = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        batches_with_timestamps = []
        batches_with_addresses = []
        timestamp = self.msg['timestamp']
        for batch_id in self.batches.keys():
            batches_with_timestamps.append(u"%s$%s" % (batch_id, timestamp))
            batches_with_addresses.append(
                u"%s$%s$%s" % (batch_id, timestamp, self.msg['to_addr']))
        self.batches_with_timestamps = batches_with_timestamps
        self.batches_with_addresses = batches_with_addresses
        return super(OutboundMessage, self).save()


class Event(Model):
    VERSION = 1
    MIGRATOR = EventMigrator

    # key is event_id
    event = VumiMessage(TransportEvent)
    message = ForeignKey(OutboundMessage)

    # Extra fields for compound indexes
    message_with_status = Unicode(index=True, null=True)

    def save(self):
        # We override this method to set our index fields before saving.
        timestamp = self.event['timestamp']
        status = self.event['event_type']
        if status == "delivery_report":
            status = "%s.%s" % (status, self.event['delivery_status'])
        self.message_with_status = u"%s$%s$%s" % (
            self.message.key, timestamp, status)
        return super(Event, self).save()

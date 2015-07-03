"""
Riak models for message store objects.
"""

from calendar import timegm
from datetime import datetime

from vumi.message import (
    TransportEvent, TransportUserMessage, parse_vumi_date, format_vumi_date)
from vumi.persist.model import Model
from vumi.persist.fields import (
    VumiMessage, ForeignKey, ManyToMany, ListOf, Tag, Dynamic, Unicode)
from vumi_message_store.migrators import (
    InboundMessageMigrator, OutboundMessageMigrator, EventMigrator)


def to_reverse_timestamp(vumi_timestamp):
    """
    Turn a vumi_date-formatted string into a string that sorts in reverse order
    and can be turned back into a timestamp later.

    This is done by converting to a unix timestamp and subtracting it from
    0xffffffffff (2**40 - 1) to get a number well outside the range
    representable by the datetime module. The result is returned as a
    hexadecimal string.
    """
    timestamp = timegm(parse_vumi_date(vumi_timestamp).timetuple())
    return "%X" % (0xffffffffff - timestamp)


def from_reverse_timestamp(reverse_timestamp):
    """
    Turn a reverse timestamp string (from `to_reverse_timestamp()`) into a
    vumi_date-formatted string.
    """
    timestamp = 0xffffffffff - int(reverse_timestamp, 16)
    return format_vumi_date(datetime.utcfromtimestamp(timestamp))


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
        """
        CurrentTags can be loaded using keys that are either flat tags in the
        form "tag:key" or tuples in the form (tag, key)
        """
        _tag, key = cls._tag_and_key(key)
        return super(CurrentTag, cls).load(manager, key, result)


class InboundMessage(Model):
    VERSION = 5
    MIGRATOR = InboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(Batch)

    # Extra fields for compound indexes
    batches_with_addresses = ListOf(Unicode(), index=True)
    batches_with_addresses_reverse = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        self.batches_with_addresses = []
        self.batches_with_addresses_reverse = []
        timestamp = self.msg['timestamp']
        if not isinstance(timestamp, basestring):
            timestamp = format_vumi_date(timestamp)
        reverse_ts = to_reverse_timestamp(timestamp)
        for batch_id in self.batches.keys():
            self.batches_with_addresses.append(
                u"%s$%s$%s" % (batch_id, timestamp, self.msg['from_addr']))
            self.batches_with_addresses_reverse.append(
                u"%s$%s$%s" % (batch_id, reverse_ts, self.msg['from_addr']))
        return super(InboundMessage, self).save()


class OutboundMessage(Model):
    VERSION = 5
    MIGRATOR = OutboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(Batch)

    # Extra fields for compound indexes
    batches_with_addresses = ListOf(Unicode(), index=True)
    batches_with_addresses_reverse = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        self.batches_with_addresses = []
        self.batches_with_addresses_reverse = []
        timestamp = self.msg['timestamp']
        if not isinstance(timestamp, basestring):
            timestamp = format_vumi_date(timestamp)
        reverse_ts = to_reverse_timestamp(timestamp)
        for batch_id in self.batches.keys():
            self.batches_with_addresses.append(
                u"%s$%s$%s" % (batch_id, timestamp, self.msg['to_addr']))
            self.batches_with_addresses_reverse.append(
                u"%s$%s$%s" % (batch_id, reverse_ts, self.msg['to_addr']))
        return super(OutboundMessage, self).save()


class Event(Model):
    VERSION = 2
    MIGRATOR = EventMigrator

    # key is event_id
    event = VumiMessage(TransportEvent)
    message = ForeignKey(OutboundMessage)
    batches = ManyToMany(Batch)

    # Extra fields for compound indexes
    message_with_status = Unicode(index=True, null=True)
    batches_with_statuses_reverse = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        timestamp = self.event['timestamp']
        if not isinstance(timestamp, basestring):
            timestamp = format_vumi_date(timestamp)
        status = self.event['event_type']
        if status == "delivery_report":
            status = "%s.%s" % (status, self.event['delivery_status'])
        self.message_with_status = u"%s$%s$%s" % (
            self.message.key, timestamp, status)
        self.batches_with_statuses_reverse = []
        reverse_ts = to_reverse_timestamp(timestamp)
        for batch_id in self.batches.keys():
            self.batches_with_statuses_reverse.append(
                u"%s$%s$%s" % (batch_id, reverse_ts, status))
        return super(Event, self).save()

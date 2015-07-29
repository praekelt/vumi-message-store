# -*- coding: utf-8 -*-

"""
Tests for vumi_message_store.riak_backend.
"""
from twisted.internet.defer import inlineCallbacks
from vumi.message import format_vumi_date
from vumi.tests.helpers import MessageHelper, VumiTestCase, PersistenceHelper

from vumi_message_store.memory_backend_manager import (
    FakeRiakState, FakeMemoryRiakManager)
from vumi_message_store.models import (
    to_reverse_timestamp,
    Batch, CurrentTag, InboundMessage, OutboundMessage, Event)
from vumi_message_store.riak_backend import MessageStoreRiakBackend
from vumi_message_store.tests.helpers import MessageSequenceHelper


class RiakBackendTestMixin(object):

    def set_up_tests(self, manager):
        """
        This should be called from .setUp().
        """
        self.manager = manager
        self.backend = MessageStoreRiakBackend(self.manager)
        self.msg_helper = self.add_helper(MessageHelper())
        self.msg_seq_helper = self.add_helper(
            MessageSequenceHelper(self.backend, self.msg_helper))

    @inlineCallbacks
    def test_batch_start_no_params(self):
        """
        A batch with no tags or metadata can be created.
        """
        batches = self.manager.proxy(Batch)
        batch_id = yield self.backend.batch_start()
        stored_batch = yield batches.load(batch_id)
        self.assertEqual(set(stored_batch.tags), set())
        self.assertEqual(stored_batch.metadata.items(), [])

    @inlineCallbacks
    def test_batch_start_with_tags(self):
        """
        A batch created with tags also updates the relevant CurrentTag objects
        if those objects exist and creates them if they don't.
        """
        batches = self.manager.proxy(Batch)
        current_tags = self.manager.proxy(CurrentTag)
        loose_cut_record = current_tags(("cut", "loose"))
        yield loose_cut_record.save()
        self.assertEqual(loose_cut_record.current_batch.key, None)

        batch_id = yield self.backend.batch_start(
            tags=[("size", "large"), ("cut", "loose")])
        stored_batch = yield batches.load(batch_id)
        self.assertEqual(
            set(stored_batch.tags), set([("size", "large"), ("cut", "loose")]))
        self.assertEqual(stored_batch.metadata.items(), [])

        loose_cut_record = yield current_tags.load("cut:loose")
        self.assertEqual(loose_cut_record.current_batch.key, batch_id)
        large_size_record = yield current_tags.load("size:large")
        self.assertEqual(large_size_record.current_batch.key, batch_id)

    @inlineCallbacks
    def test_batch_start_with_metadata(self):
        """
        Arbitrary key+value metadata can be added to a batch when it is
        created.
        """
        batches = self.manager.proxy(Batch)
        batch_id = yield self.backend.batch_start(meta=u"alt", data=u"stuff")
        stored_batch = yield batches.load(batch_id)
        self.assertEqual(set(stored_batch.tags), set())
        self.assertEqual(
            dict(stored_batch.metadata.items()),
            {u"meta": u"alt", u"data": u"stuff"})

    @inlineCallbacks
    def test_batch_done(self):
        """
        Finishing a batch clears all references to that batch from the relevant
        CurrentTag objects but does not alter the tags referenced by the batch.
        """
        batches = self.manager.proxy(Batch)
        current_tags = self.manager.proxy(CurrentTag)
        batch_id = yield self.backend.batch_start(
            tags=[("size", "large"), ("cut", "loose")])
        loose_cut_record = yield current_tags.load("cut:loose")
        self.assertEqual(loose_cut_record.current_batch.key, batch_id)
        large_size_record = yield current_tags.load("size:large")
        self.assertEqual(large_size_record.current_batch.key, batch_id)
        large_size_record.current_batch.key = "otherbatch"
        yield large_size_record.save()

        yield self.backend.batch_done(batch_id)
        loose_cut_record = yield current_tags.load("cut:loose")
        self.assertEqual(loose_cut_record.current_batch.key, None)
        large_size_record = yield current_tags.load("size:large")
        self.assertEqual(large_size_record.current_batch.key, "otherbatch")
        stored_batch = yield batches.load(batch_id)
        self.assertEqual(
            set(stored_batch.tags), set([("size", "large"), ("cut", "loose")]))
        self.assertEqual(stored_batch.metadata.items(), [])

    @inlineCallbacks
    def test_get_batch(self):
        """
        If we ask for a batch, we get a Batch object.
        """
        batches = self.manager.proxy(Batch)
        new_batch = batches("mybatch", tags=[(u"size", u"large")])
        yield new_batch.save()

        stored_batch = yield self.backend.get_batch("mybatch")
        self.assertEqual(set(stored_batch.tags), set([(u"size", u"large")]))
        self.assertEqual(stored_batch.metadata.items(), [])

    @inlineCallbacks
    def test_get_batch_missing(self):
        """
        If we ask for a batch that doesn't exist, we get None.
        """
        stored_batch = yield self.backend.get_batch("missing")
        self.assertEqual(stored_batch, None)

    @inlineCallbacks
    def test_get_tag_info(self):
        """
        If we ask for tag info, we get a CurrentTag object.
        """
        current_tags = self.manager.proxy(CurrentTag)
        tag_info = yield self.backend.get_tag_info("size:large")
        self.assertEqual(tag_info.current_batch.key, None)
        stored_tag = yield current_tags.load("size:large")
        self.assertEqual(stored_tag, None)

    @inlineCallbacks
    def test_get_tag_info_missing_tag(self):
        """
        If we ask for tag info that doesn't exist, we return a CurrentTag
        object.
        """
        current_tags = self.manager.proxy(CurrentTag)
        tag_info = yield self.backend.get_tag_info("size:large")
        self.assertEqual(tag_info.current_batch.key, None)
        stored_tag = yield current_tags.load("size:large")
        self.assertEqual(stored_tag, None)

    @inlineCallbacks
    def test_add_inbound_message(self):
        """
        When an inbound message is added, it is stored in Riak.
        """
        inbound_messages = self.manager.proxy(InboundMessage)
        msg = self.msg_helper.make_inbound("apples")
        stored_msg = yield inbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg, None)

        yield self.backend.add_inbound_message(msg)
        stored_msg = yield inbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)

    @inlineCallbacks
    def test_add_inbound_message_again(self):
        """
        When an inbound message is added, it overwrites any existing version in
        Riak.
        """
        inbound_messages = self.manager.proxy(InboundMessage)
        msg = self.msg_helper.make_inbound("apples")
        yield self.backend.add_inbound_message(msg)
        old_stored_msg = yield inbound_messages.load(msg["message_id"])
        self.assertEqual(old_stored_msg.msg, msg)

        msg["helper_metadata"]["fruit"] = {"type": "pomaceous"}
        yield self.backend.add_inbound_message(msg)
        new_stored_msg = yield inbound_messages.load(msg["message_id"])
        self.assertEqual(new_stored_msg.msg, msg)
        self.assertNotEqual(new_stored_msg.msg, old_stored_msg.msg)

    @inlineCallbacks
    def test_add_inbound_message_with_batch_id(self):
        """
        When an inbound message is added with a batch identifier, that batch
        identifier is stored with it and indexed.
        """
        inbound_messages = self.manager.proxy(InboundMessage)
        msg = self.msg_helper.make_inbound("apples")
        yield self.backend.add_inbound_message(msg, batch_ids=["mybatch"])
        stored_msg = yield inbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(stored_msg.batches.keys(), ["mybatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", timestamp, msg['from_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("mybatch", reverse_ts, msg['from_addr'])),
        ]))

    @inlineCallbacks
    def test_add_inbound_message_with_multiple_batch_ids(self):
        """
        When an inbound message is added with multiple batch identifiers, it
        belongs to all the specified batches.
        """
        inbound_messages = self.manager.proxy(InboundMessage)
        msg = self.msg_helper.make_inbound("apples")
        yield self.backend.add_inbound_message(
            msg, batch_ids=["mybatch", "yourbatch"])
        stored_msg = yield inbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(
            sorted(stored_msg.batches.keys()), ["mybatch", "yourbatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_bin', "yourbatch"),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", timestamp, msg['from_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("yourbatch", timestamp, msg['from_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("mybatch", reverse_ts, msg['from_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("yourbatch", reverse_ts, msg['from_addr'])),
        ]))

    @inlineCallbacks
    def test_add_inbound_message_to_new_batch(self):
        """
        When an existing inbound message is added with a new batch identifier,
        it belongs to the new batch as well as batches it already belonged to.
        """
        inbound_messages = self.manager.proxy(InboundMessage)
        msg = self.msg_helper.make_inbound("apples")
        yield self.backend.add_inbound_message(msg, batch_ids=["mybatch"])
        yield self.backend.add_inbound_message(msg, batch_ids=["yourbatch"])
        stored_msg = yield inbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(
            sorted(stored_msg.batches.keys()), ["mybatch", "yourbatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_bin', "yourbatch"),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", timestamp, msg['from_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("yourbatch", timestamp, msg['from_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("mybatch", reverse_ts, msg['from_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("yourbatch", reverse_ts, msg['from_addr'])),
        ]))

    @inlineCallbacks
    def test_get_raw_inbound_message(self):
        """
        When we ask for a raw inbound message, we get the InboundMessage model
        object.
        """
        inbound_messages = self.manager.proxy(InboundMessage)
        msg = self.msg_helper.make_inbound("apples")
        msg_record = inbound_messages(msg["message_id"], msg=msg)
        msg_record.batches.add_key("mybatch")
        yield msg_record.save()

        stored_record = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
        self.assertEqual(
            stored_record.batches.keys(), msg_record.batches.keys())
        self.assertEqual(stored_record.msg, msg)

    @inlineCallbacks
    def test_get_raw_inbound_message_missing(self):
        """
        When we ask for a raw inbound message that does not exist, we get
        ``None``.
        """
        stored_record = yield self.backend.get_raw_inbound_message("badmsg")
        self.assertEqual(stored_record, None)

    @inlineCallbacks
    def test_get_inbound_message(self):
        """
        When we ask for an inbound message, we get the TransportUserMessage
        object.
        """
        inbound_messages = self.manager.proxy(InboundMessage)
        msg = self.msg_helper.make_inbound("apples")
        msg_record = inbound_messages(msg["message_id"], msg=msg)
        msg_record.batches.add_key("mybatch")
        yield msg_record.save()

        stored_msg = yield self.backend.get_inbound_message(msg["message_id"])
        self.assertEqual(stored_msg, msg)

    @inlineCallbacks
    def test_get_inbound_message_missing(self):
        """
        When we ask for an inbound message that does not exist, we get
        ``None``.
        """
        stored_record = yield self.backend.get_inbound_message("badmsg")
        self.assertEqual(stored_record, None)

    @inlineCallbacks
    def test_add_outbound_message(self):
        """
        When an outbound message is added, it is stored in Riak.
        """
        outbound_messages = self.manager.proxy(OutboundMessage)
        msg = self.msg_helper.make_outbound("apples")
        stored_msg = yield outbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg, None)

        yield self.backend.add_outbound_message(msg)
        stored_msg = yield outbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)

    @inlineCallbacks
    def test_add_outbound_message_again(self):
        """
        When an outbound message is added, it overwrites any existing version
        in Riak.
        """
        outbound_messages = self.manager.proxy(OutboundMessage)
        msg = self.msg_helper.make_outbound("apples")
        yield self.backend.add_outbound_message(msg)
        old_stored_msg = yield outbound_messages.load(msg["message_id"])
        self.assertEqual(old_stored_msg.msg, msg)

        msg["helper_metadata"]["fruit"] = {"type": "pomaceous"}
        yield self.backend.add_outbound_message(msg)
        new_stored_msg = yield outbound_messages.load(msg["message_id"])
        self.assertEqual(new_stored_msg.msg, msg)
        self.assertNotEqual(new_stored_msg.msg, old_stored_msg.msg)

    @inlineCallbacks
    def test_add_outbound_message_with_batch_id(self):
        """
        When an outbound message is added with a batch identifier, that batch
        identifier is stored with it and indexed.
        """
        outbound_messages = self.manager.proxy(OutboundMessage)
        msg = self.msg_helper.make_outbound("apples")
        yield self.backend.add_outbound_message(msg, batch_ids=["mybatch"])
        stored_msg = yield outbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(stored_msg.batches.keys(), ["mybatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", timestamp, msg['to_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("mybatch", reverse_ts, msg['to_addr'])),
        ]))

    @inlineCallbacks
    def test_add_outbound_message_with_multiple_batch_ids(self):
        """
        When an outbound message is added with multiple batch identifiers, it
        belongs to all the specified batches.
        """
        outbound_messages = self.manager.proxy(OutboundMessage)
        msg = self.msg_helper.make_outbound("apples")
        yield self.backend.add_outbound_message(
            msg, batch_ids=["mybatch", "yourbatch"])
        stored_msg = yield outbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(
            sorted(stored_msg.batches.keys()), ["mybatch", "yourbatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_bin', "yourbatch"),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", timestamp, msg['to_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("yourbatch", timestamp, msg['to_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("mybatch", reverse_ts, msg['to_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("yourbatch", reverse_ts, msg['to_addr'])),
        ]))

    @inlineCallbacks
    def test_add_outbound_message_to_new_batch(self):
        """
        When an existing outbound message is added with a new batch identifier,
        it belongs to the new batch as well as batches it already belonged to.
        """
        outbound_messages = self.manager.proxy(OutboundMessage)
        msg = self.msg_helper.make_outbound("apples")
        yield self.backend.add_outbound_message(msg, batch_ids=["mybatch"])
        yield self.backend.add_outbound_message(msg, batch_ids=["yourbatch"])
        stored_msg = yield outbound_messages.load(msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(
            sorted(stored_msg.batches.keys()), ["mybatch", "yourbatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_bin', "yourbatch"),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", timestamp, msg['to_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("yourbatch", timestamp, msg['to_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("mybatch", reverse_ts, msg['to_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % ("yourbatch", reverse_ts, msg['to_addr'])),
        ]))

    @inlineCallbacks
    def test_get_raw_outbound_message(self):
        """
        When we ask for a raw outbound message, we get the OutboundMessage
        model object.
        """
        outbound_messages = self.manager.proxy(OutboundMessage)
        msg = self.msg_helper.make_outbound("apples")
        msg_record = outbound_messages(msg["message_id"], msg=msg)
        msg_record.batches.add_key("mybatch")
        yield msg_record.save()

        stored_record = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
        self.assertEqual(
            stored_record.batches.keys(), msg_record.batches.keys())
        self.assertEqual(stored_record.msg, msg)

    @inlineCallbacks
    def test_get_raw_outbound_message_missing(self):
        """
        When we ask for a raw outbound message that does not exist, we get
        ``None``.
        """
        stored_record = yield self.backend.get_raw_outbound_message("badmsg")
        self.assertEqual(stored_record, None)

    @inlineCallbacks
    def test_get_outbound_message(self):
        """
        When we ask for an outbound message, we get the TransportUserMessage
        object.
        """
        outbound_messages = self.manager.proxy(OutboundMessage)
        msg = self.msg_helper.make_outbound("apples")
        msg_record = outbound_messages(msg["message_id"], msg=msg)
        msg_record.batches.add_key("mybatch")
        yield msg_record.save()

        stored_msg = yield self.backend.get_outbound_message(msg["message_id"])
        self.assertEqual(stored_msg, msg)

    @inlineCallbacks
    def test_get_outbound_message_missing(self):
        """
        When we ask for an outbound message that does not exist, we get
        ``None``.
        """
        stored_record = yield self.backend.get_outbound_message("badmsg")
        self.assertEqual(stored_record, None)

    @inlineCallbacks
    def test_add_ack_event(self):
        """
        When an event is added, it is stored in Riak.
        """
        events = self.manager.proxy(Event)
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        stored_event = yield events.load(ack["event_id"])
        self.assertEqual(stored_event, None)

        yield self.backend.add_event(ack)
        stored_event = yield events.load(ack["event_id"])
        self.assertEqual(stored_event.event, ack)

        # Make sure we're writing the right indexes.
        self.assertEqual(stored_event._riak_object.get_indexes(), set([
            ("message_bin", ack["user_message_id"]),
            ("message_with_status_bin",
             "%s$%s$%s" % (ack["user_message_id"], ack["timestamp"], "ack")),
        ]))

    @inlineCallbacks
    def test_add_delivery_report_event(self):
        """
        When a delivery report event is added, delivery status is included in
        the indexed status.
        """
        events = self.manager.proxy(Event)
        msg = self.msg_helper.make_outbound("apples")
        dr = self.msg_helper.make_delivery_report(msg)
        stored_event = yield events.load(dr["event_id"])
        self.assertEqual(stored_event, None)

        yield self.backend.add_event(dr)
        stored_event = yield events.load(dr["event_id"])
        self.assertEqual(stored_event.event, dr)

        # Make sure we're writing the right indexes.
        self.assertEqual(stored_event._riak_object.get_indexes(), set([
            ("message_bin", dr["user_message_id"]),
            ("message_with_status_bin",
             "%s$%s$%s" % (dr["user_message_id"], dr["timestamp"],
                           "delivery_report.delivered")),
        ]))

    @inlineCallbacks
    def test_add_ack_event_again(self):
        """
        When an event is added, it overwrites any existing version in Riak.
        """
        events = self.manager.proxy(Event)
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.backend.add_event(ack)
        old_stored_event = yield events.load(ack["event_id"])
        self.assertEqual(old_stored_event.event, ack)

        ack["helper_metadata"]["fruit"] = {"type": "pomaceous"}
        yield self.backend.add_event(ack)
        new_stored_event = yield events.load(ack["event_id"])
        self.assertEqual(new_stored_event.event, ack)
        self.assertNotEqual(new_stored_event.event, old_stored_event.event)

    @inlineCallbacks
    def test_add_ack_event_with_batch_id(self):
        """
        When an event is added with a batch identifier, that batch identifier
        is stored with it and indexed.
        """
        events = self.manager.proxy(Event)
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.backend.add_event(ack, batch_ids=["mybatch"])
        stored_event = yield events.load(ack["event_id"])
        self.assertEqual(stored_event.event, ack)
        self.assertEqual(stored_event.batches.keys(), ["mybatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_event._riak_object.get_indexes(), set([
            ("message_bin", ack["user_message_id"]),
            ("batches_bin", "mybatch"),
            ("message_with_status_bin",
             "%s$%s$%s" % (ack["user_message_id"], ack["timestamp"], "ack")),
            ("batches_with_statuses_reverse_bin",
             "%s$%s$%s" % ("mybatch", reverse_ts, "ack")),
        ]))

    @inlineCallbacks
    def test_add_ack_event_with_multiple_batch_ids(self):
        """
        When an event is added with multiple batch identifiers, it belongs to
        all the specified batches.
        """
        events = self.manager.proxy(Event)
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.backend.add_event(ack, batch_ids=["mybatch", "yourbatch"])
        stored_event = yield events.load(ack["event_id"])
        self.assertEqual(stored_event.event, ack)
        self.assertEqual(
            sorted(stored_event.batches.keys()), ["mybatch", "yourbatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_event._riak_object.get_indexes(), set([
            ("message_bin", ack["user_message_id"]),
            ("batches_bin", "mybatch"),
            ('batches_bin', "yourbatch"),
            ("message_with_status_bin",
             "%s$%s$%s" % (ack["user_message_id"], ack["timestamp"], "ack")),
            ("batches_with_statuses_reverse_bin",
             "%s$%s$%s" % ("mybatch", reverse_ts, "ack")),
            ("batches_with_statuses_reverse_bin",
             "%s$%s$%s" % ("yourbatch", reverse_ts, "ack")),
        ]))

    @inlineCallbacks
    def test_add_ack_event_to_new_batch(self):
        """
        When an existing event is added with a new batch identifier, it belongs
        to the new batch as well as batches it already belonged to.
        """
        events = self.manager.proxy(Event)
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.backend.add_event(ack, batch_ids=["mybatch"])
        yield self.backend.add_event(ack, batch_ids=["yourbatch"])
        stored_event = yield events.load(ack["event_id"])
        self.assertEqual(stored_event.event, ack)
        self.assertEqual(
            sorted(stored_event.batches.keys()), ["mybatch", "yourbatch"])

        # Make sure we're writing the right indexes.
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_event._riak_object.get_indexes(), set([
            ("message_bin", ack["user_message_id"]),
            ("batches_bin", "mybatch"),
            ('batches_bin', "yourbatch"),
            ("message_with_status_bin",
             "%s$%s$%s" % (ack["user_message_id"], ack["timestamp"], "ack")),
            ("batches_with_statuses_reverse_bin",
             "%s$%s$%s" % ("mybatch", reverse_ts, "ack")),
            ("batches_with_statuses_reverse_bin",
             "%s$%s$%s" % ("yourbatch", reverse_ts, "ack")),
        ]))

    @inlineCallbacks
    def test_get_raw_event(self):
        """
        When we ask for a raw event, we get the Event model object.
        """
        events = self.manager.proxy(Event)
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        event_record = events(
            ack["event_id"], event=ack, message=ack["user_message_id"])
        yield event_record.save()

        stored_record = yield self.backend.get_raw_event(ack["event_id"])
        self.assertEqual(stored_record.message.key, event_record.message.key)
        self.assertEqual(stored_record.event, ack)

    @inlineCallbacks
    def test_get_raw_event_missing(self):
        """
        When we ask for a raw event that does not exist, we get ``None``.
        """
        stored_record = yield self.backend.get_raw_event("badevent")
        self.assertEqual(stored_record, None)

    @inlineCallbacks
    def test_get_event(self):
        """
        When we ask for an event, we get the TransportEvent object.
        """
        events = self.manager.proxy(Event)
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        event_record = events(
            ack["event_id"], event=ack, message=ack["user_message_id"])
        yield event_record.save()

        stored_event = yield self.backend.get_event(ack["event_id"])
        self.assertEqual(stored_event, ack)

    @inlineCallbacks
    def test_get_event_missing(self):
        """
        When we ask for an event that does not exist, we get ``None``.
        """
        stored_record = yield self.backend.get_event("badevent")
        self.assertEqual(stored_record, None)

    @inlineCallbacks
    def test_list_batch_inbound_messages(self):
        """
        When we ask for a list of inbound messages for a batch, we get an
        IndexPageWrapper containing the first page of results and can ask for
        following pages until all results are delivered.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_inbound_message_sequence())
        keys_p1 = yield self.backend.list_batch_inbound_messages(
            batch_id, page_size=3)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:])

    @inlineCallbacks
    def test_list_batch_inbound_messages_range_start(self):
        """
        When we ask for a list of inbound messages for a batch, we can specify
        a start timestamp.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_inbound_message_sequence())
        keys_p1 = yield self.backend.list_batch_inbound_messages(
            batch_id, start=all_keys[-2][1], page_size=3)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[0:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_batch_inbound_messages_range_end(self):
        """
        When we ask for a list of inbound messages for a batch, we can specify
        an end timestamp.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_inbound_message_sequence())
        keys_p1 = yield self.backend.list_batch_inbound_messages(
            batch_id, end=all_keys[1][1], page_size=3)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[1:4])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[4:])

    @inlineCallbacks
    def test_list_batch_inbound_messages_range(self):
        """
        When we ask for a list of inbound messages for a batch, we can specify
        both ends of the range.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_inbound_message_sequence())
        keys_p1 = yield self.backend.list_batch_inbound_messages(
            batch_id, start=all_keys[-2][1], end=all_keys[1][1], page_size=2)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[1:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_batch_inbound_messages_empty(self):
        """
        When we ask for a list of inbound messages for an empty batch, we get
        an empty IndexPageWrapper.
        """
        batch_id = yield self.backend.batch_start()
        keys_page = yield self.backend.list_batch_inbound_messages(batch_id)
        self.assertEqual(list(keys_page), [])

    @inlineCallbacks
    def test_list_batch_outbound_messages(self):
        """
        When we ask for a list of outbound messages for a batch, we get an
        IndexPageWrapper containing the first page of results and can ask for
        following pages until all results are delivered.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_outbound_message_sequence())
        keys_p1 = yield self.backend.list_batch_outbound_messages(
            batch_id, page_size=3)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:])

    @inlineCallbacks
    def test_list_batch_outbound_messages_range_start(self):
        """
        When we ask for a list of outbound messages for a batch, we can specify
        a start timestamp.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_outbound_message_sequence())
        keys_p1 = yield self.backend.list_batch_outbound_messages(
            batch_id, start=all_keys[-2][1], page_size=3)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[0:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_batch_outbound_messages_range_end(self):
        """
        When we ask for a list of outbound messages for a batch, we can specify
        an end timestamp.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_outbound_message_sequence())
        keys_p1 = yield self.backend.list_batch_outbound_messages(
            batch_id, end=all_keys[1][1], page_size=3)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[1:4])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[4:])

    @inlineCallbacks
    def test_list_batch_outbound_messages_range(self):
        """
        When we ask for a list of outbound messages for a batch, we can specify
        both ends of the range.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_outbound_message_sequence())
        keys_p1 = yield self.backend.list_batch_outbound_messages(
            batch_id, start=all_keys[-2][1], end=all_keys[1][1], page_size=2)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[1:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_batch_outbound_messages_empty(self):
        """
        When we ask for a list of outbound messages for an empty batch, we get
        an empty IndexPageWrapper.
        """
        batch_id = yield self.backend.batch_start()
        keys_page = yield self.backend.list_batch_outbound_messages(batch_id)
        self.assertEqual(list(keys_page), [])

    @inlineCallbacks
    def test_list_message_events(self):
        """
        When we ask for a list of events for an outbound message, we get an
        IndexPageWrapper containing the first page of results and can ask for
        following pages until all results are delivered.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())

        keys_p1 = yield self.backend.list_message_events(msg_id, page_size=3)
        # Paginated results are sorted by ascending timestamp.
        all_keys.reverse()
        self.assertEqual(list(keys_p1), all_keys[:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:])

    @inlineCallbacks
    def test_list_message_events_range_start(self):
        """
        When we ask for a list of events for an outbound message, we can
        specify a start timestamp.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.backend.list_message_events(
            msg_id, start=all_keys[-2][1], page_size=3)
        # Paginated results are sorted by ascending timestamp.
        all_keys.reverse()
        self.assertEqual(list(keys_p1), all_keys[1:4])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[4:])

    @inlineCallbacks
    def test_list_message_events_range_end(self):
        """
        When we ask for a list of events for an outbound message, we can
        specify an end timestamp.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.backend.list_message_events(
            msg_id, end=all_keys[1][1], page_size=3)
        # Paginated results are sorted by ascending timestamp.
        all_keys.reverse()
        self.assertEqual(list(keys_p1), all_keys[0:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_message_events_range(self):
        """
        When we ask for a list of events for an outbound message, we can
        specify both ends of the range.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.backend.list_message_events(
            msg_id, start=all_keys[-2][1], end=all_keys[1][1], page_size=2)
        # Paginated results are sorted by ascending timestamp.
        all_keys.reverse()
        self.assertEqual(list(keys_p1), all_keys[1:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_message_events_empty(self):
        """
        When we ask for a list of events for an outbound message with no
        events, we get an empty IndexPageWrapper.
        """
        batch_id = yield self.backend.batch_start()
        msg = self.msg_helper.make_outbound("hello")
        yield self.backend.add_outbound_message(msg, batch_ids=[batch_id])
        keys_page = yield self.backend.list_message_events(msg["message_id"])
        self.assertEqual(list(keys_page), [])

    @inlineCallbacks
    def test_list_message_events_no_message(self):
        """
        When we ask for a list of events for an outbound message that does not
        exist, we get an empty IndexPageWrapper.
        """
        keys_page = yield self.backend.list_message_events("badmsg")
        self.assertEqual(list(keys_page), [])

    @inlineCallbacks
    def test_list_batch_events(self):
        """
        When we ask for a list of events for a batch, we get an
        IndexPageWrapper containing the first page of results and can ask for
        following pages until all results are delivered.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())

        keys_p1 = yield self.backend.list_batch_events(batch_id, page_size=3)
        self.assertEqual(list(keys_p1), all_keys[:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:])

    @inlineCallbacks
    def test_list_batch_events_range_start(self):
        """
        When we ask for a list of events for a batch, we can specify a start
        timestamp.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.backend.list_batch_events(
            batch_id, start=all_keys[-2][1], page_size=3)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[0:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_batch_events_range_end(self):
        """
        When we ask for a list of events for a batch, we can specify an end
        timestamp.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.backend.list_batch_events(
            batch_id, end=all_keys[1][1], page_size=3)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[1:4])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[4:])

    @inlineCallbacks
    def test_list_batch_events_range(self):
        """
        When we ask for a list of events for a batch, we can specify both ends
        of the range.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.backend.list_batch_events(
            batch_id, start=all_keys[-2][1], end=all_keys[1][1], page_size=2)
        # Paginated results are sorted by descending timestamp.
        self.assertEqual(list(keys_p1), all_keys[1:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_batch_events_empty(self):
        """
        When we ask for a list of events for an empty batch, we get an empty
        IndexPageWrapper.
        """
        batch_id = yield self.backend.batch_start()
        keys_page = yield self.backend.list_batch_events(batch_id)
        self.assertEqual(list(keys_page), [])


class TestMessageStoreRiakBackend(RiakBackendTestMixin, VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = self.persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        self.set_up_tests(manager)


class TestMessageStoreRiakBackendInMemory(RiakBackendTestMixin, VumiTestCase):

    def setUp(self):
        self.state = FakeRiakState(is_sync=False)
        self.add_cleanup(self.state.teardown)
        self.set_up_tests(FakeMemoryRiakManager(self.state))


class TestMessageStoreRiakBackendSync(RiakBackendTestMixin, VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True, is_sync=True))
        manager = self.persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        self.set_up_tests(manager)


class TestMessageStoreRiakBackendInMemorySync(RiakBackendTestMixin,
                                              VumiTestCase):

    def setUp(self):
        self.state = FakeRiakState(is_sync=True)
        self.add_cleanup(self.state.teardown)
        self.set_up_tests(FakeMemoryRiakManager(self.state))

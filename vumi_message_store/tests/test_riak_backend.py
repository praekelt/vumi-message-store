# -*- coding: utf-8 -*-

"""
Tests for vumi_message_store.riak_backend.
"""

from twisted.internet.defer import inlineCallbacks
from vumi.tests.helpers import VumiTestCase, MessageHelper, PersistenceHelper

from vumi_message_store.riak_backend import MessageStoreRiakBackend
from vumi_message_store.models import InboundMessage, OutboundMessage, Event


class TestMessageStoreRiakBackend(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.manager = self.persistence_helper.get_riak_manager()
        self.backend = MessageStoreRiakBackend(self.manager)
        self.msg_helper = self.add_helper(MessageHelper())

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
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("mybatch", msg['timestamp'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", msg['timestamp'], msg['from_addr'])),
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
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_bin', "yourbatch"),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("mybatch", msg['timestamp'])),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("yourbatch", msg['timestamp'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", msg['timestamp'], msg['from_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("yourbatch", msg['timestamp'], msg['from_addr'])),
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
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_bin', "yourbatch"),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("mybatch", msg['timestamp'])),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("yourbatch", msg['timestamp'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", msg['timestamp'], msg['from_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("yourbatch", msg['timestamp'], msg['from_addr'])),
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
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("mybatch", msg['timestamp'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", msg['timestamp'], msg['to_addr'])),
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
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_bin', "yourbatch"),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("mybatch", msg['timestamp'])),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("yourbatch", msg['timestamp'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", msg['timestamp'], msg['to_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("yourbatch", msg['timestamp'], msg['to_addr'])),
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
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', "mybatch"),
            ('batches_bin', "yourbatch"),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("mybatch", msg['timestamp'])),
            ('batches_with_timestamps_bin',
             "%s$%s" % ("yourbatch", msg['timestamp'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("mybatch", msg['timestamp'], msg['to_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % ("yourbatch", msg['timestamp'], msg['to_addr'])),
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

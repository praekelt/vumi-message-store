"""
Tests for vumi_message_store.message_store.
"""

from twisted.internet.defer import inlineCallbacks
from vumi.tests.helpers import VumiTestCase, MessageHelper, PersistenceHelper
from zope.interface.verify import verifyObject

from vumi_message_store.interfaces import (
    IMessageStoreBatchManager, IOperationalMessageStore)
from vumi_message_store.message_store import (
    RiakOnlyMessageStoreBatchManager, RiakOnlyOperationalMessageStore)


# TODO: Better way to test indexes.


class TestRiakOnlyMessageStoreBatchManager(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.manager = self.persistence_helper.get_riak_manager()
        self.batch_manager = RiakOnlyMessageStoreBatchManager(self.manager)
        self.backend = self.batch_manager.riak_backend

    def test_implements_IOperationalMessageStore(self):
        """
        MessageStoreBatchManager implements the IMessageStoreBatchManager
        interface.
        """
        self.assertTrue(
            IMessageStoreBatchManager.providedBy(self.batch_manager))
        self.assertTrue(
            verifyObject(IMessageStoreBatchManager, self.batch_manager))

    @inlineCallbacks
    def test_batch_start_no_params(self):
        """
        A batch with no tags or metadata can be created.
        """
        batch_id = yield self.batch_manager.batch_start()
        stored_batch = yield self.backend.get_batch(batch_id)
        self.assertEqual(set(stored_batch.tags), set())

    @inlineCallbacks
    def test_batch_start_with_tags(self):
        """
        A batch created with tags also updates the relevant CurrentTag objects.
        """
        batch_id = yield self.batch_manager.batch_start(
            tags=[("size", "large"), ("cut", "loose")])
        stored_batch = yield self.backend.get_batch(batch_id)
        self.assertEqual(
            set(stored_batch.tags), set([("size", "large"), ("cut", "loose")]))
        self.assertEqual(stored_batch.metadata.items(), [])

        loose_cut_record = yield self.backend.get_tag_info("cut:loose")
        self.assertEqual(loose_cut_record.current_batch.key, batch_id)
        large_size_record = yield self.backend.get_tag_info("size:large")
        self.assertEqual(large_size_record.current_batch.key, batch_id)

    @inlineCallbacks
    def test_batch_start_with_metadata(self):
        """
        Arbitrary key+value metadata can be added to a batch when it is
        created.
        """
        batch_id = yield self.batch_manager.batch_start(
            meta=u"alt", data=u"stuff")
        stored_batch = yield self.backend.get_batch(batch_id)
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
        batch_id = yield self.backend.batch_start(
            tags=[("size", "large"), ("cut", "loose")])
        loose_cut_record = yield self.backend.get_tag_info("cut:loose")
        self.assertEqual(loose_cut_record.current_batch.key, batch_id)
        large_size_record = yield self.backend.get_tag_info("size:large")
        self.assertEqual(large_size_record.current_batch.key, batch_id)
        large_size_record.current_batch.key = "otherbatch"
        yield large_size_record.save()

        yield self.batch_manager.batch_done(batch_id)
        loose_cut_record = yield self.backend.get_tag_info("cut:loose")
        self.assertEqual(loose_cut_record.current_batch.key, None)
        large_size_record = yield self.backend.get_tag_info("size:large")
        self.assertEqual(large_size_record.current_batch.key, "otherbatch")
        stored_batch = yield self.backend.get_batch(batch_id)
        self.assertEqual(
            set(stored_batch.tags), set([("size", "large"), ("cut", "loose")]))
        self.assertEqual(stored_batch.metadata.items(), [])

    @inlineCallbacks
    def test_get_batch(self):
        """
        If we ask for a batch, we get a Batch object.
        """
        batch_id = yield self.backend.batch_start(tags=[(u"size", u"large")])
        stored_batch = yield self.batch_manager.get_batch(batch_id)
        self.assertEqual(set(stored_batch.tags), set([(u"size", u"large")]))
        self.assertEqual(stored_batch.metadata.items(), [])

    @inlineCallbacks
    def test_get_batch_missing(self):
        """
        If we ask for a batch that doesn't exist, we get None.
        """
        stored_batch = yield self.batch_manager.get_batch("missing")
        self.assertEqual(stored_batch, None)

    @inlineCallbacks
    def test_get_tag_info(self):
        """
        If we ask for tag info, we get a CurrentTag object.
        """
        # We use the internals of the backend here because there's no other
        # direct way to create a CurrentTag object.
        stored_tag = self.backend.current_tags(
            "size:large", current_batch="mybatch")
        yield stored_tag.save()
        tag_info = yield self.batch_manager.get_tag_info("size:large")
        self.assertEqual(tag_info.current_batch.key, "mybatch")

    @inlineCallbacks
    def test_get_tag_info_missing_tag(self):
        """
        If we ask for tag info that doesn't exist, we get a new CurrentTag
        object.
        """
        tag_info = yield self.batch_manager.get_tag_info("size:large")
        self.assertEqual(tag_info.current_batch.key, None)


class TestRiakOnlyOperationalMessageStore(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.manager = self.persistence_helper.get_riak_manager()
        self.store = RiakOnlyOperationalMessageStore(self.manager)
        self.backend = self.store.riak_backend
        self.msg_helper = self.add_helper(MessageHelper())

    def test_implements_IOperationalMessageStore(self):
        """
        RiakOnlyOperationalMessageStore implements the IOperationalMessageStore
        interface.
        """
        self.assertTrue(IOperationalMessageStore.providedBy(self.store))
        self.assertTrue(verifyObject(IOperationalMessageStore, self.store))

    @inlineCallbacks
    def test_add_inbound_message(self):
        """
        When an inbound message is added, it is stored in Riak.
        """
        msg = self.msg_helper.make_inbound("apples")
        stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg, None)

        yield self.store.add_inbound_message(msg)
        stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)

    @inlineCallbacks
    def test_add_inbound_message_again(self):
        """
        When an inbound message is added, it overwrites any existing version in
        Riak.
        """
        msg = self.msg_helper.make_inbound("apples")
        yield self.store.add_inbound_message(msg)
        old_stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
        self.assertEqual(old_stored_msg.msg, msg)

        msg["helper_metadata"]["fruit"] = {"type": "pomaceous"}
        yield self.store.add_inbound_message(msg)
        new_stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
        self.assertEqual(new_stored_msg.msg, msg)
        self.assertNotEqual(new_stored_msg.msg, old_stored_msg.msg)

    @inlineCallbacks
    def test_add_inbound_message_with_batch_id(self):
        """
        When an inbound message is added with a batch identifier, that batch
        identifier is stored with it and indexed.
        """
        msg = self.msg_helper.make_inbound("apples")
        yield self.store.add_inbound_message(msg, batch_ids=["mybatch"])
        stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
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
        msg = self.msg_helper.make_inbound("apples")
        yield self.store.add_inbound_message(
            msg, batch_ids=["mybatch", "yourbatch"])
        stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
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
        msg = self.msg_helper.make_inbound("apples")
        yield self.store.add_inbound_message(msg, batch_ids=["mybatch"])
        yield self.store.add_inbound_message(msg, batch_ids=["yourbatch"])
        stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
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
    def test_get_inbound_message(self):
        """
        When we ask for an inbound message, we get the TransportUserMessage
        object.
        """
        msg = self.msg_helper.make_inbound("apples")
        yield self.backend.add_inbound_message(msg)
        stored_msg = yield self.store.get_inbound_message(msg["message_id"])
        self.assertEqual(stored_msg, msg)

    @inlineCallbacks
    def test_get_inbound_message_missing(self):
        """
        When we ask for an inbound message that does not exist, we get
        ``None``.
        """
        stored_record = yield self.store.get_inbound_message("badmsg")
        self.assertEqual(stored_record, None)

    @inlineCallbacks
    def test_add_outbound_message(self):
        """
        When an outbound message is added, it is stored in Riak.
        """
        msg = self.msg_helper.make_outbound("apples")
        stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg, None)

        yield self.store.add_outbound_message(msg)
        stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)

    @inlineCallbacks
    def test_add_outbound_message_again(self):
        """
        When an outbound message is added, it overwrites any existing version
        in Riak.
        """
        msg = self.msg_helper.make_outbound("apples")
        yield self.store.add_outbound_message(msg)
        old_stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
        self.assertEqual(old_stored_msg.msg, msg)

        msg["helper_metadata"]["fruit"] = {"type": "pomaceous"}
        yield self.store.add_outbound_message(msg)
        new_stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
        self.assertEqual(new_stored_msg.msg, msg)
        self.assertNotEqual(new_stored_msg.msg, old_stored_msg.msg)

    @inlineCallbacks
    def test_add_outbound_message_with_batch_id(self):
        """
        When an outbound message is added with a batch identifier, that batch
        identifier is stored with it and indexed.
        """
        msg = self.msg_helper.make_outbound("apples")
        yield self.store.add_outbound_message(msg, batch_ids=["mybatch"])
        stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
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
        msg = self.msg_helper.make_outbound("apples")
        yield self.store.add_outbound_message(
            msg, batch_ids=["mybatch", "yourbatch"])
        stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
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
        msg = self.msg_helper.make_outbound("apples")
        yield self.store.add_outbound_message(msg, batch_ids=["mybatch"])
        yield self.store.add_outbound_message(msg, batch_ids=["yourbatch"])
        stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
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
    def test_get_outbound_message(self):
        """
        When we ask for an outbound message, we get the TransportUserMessage
        object.
        """
        msg = self.msg_helper.make_outbound("apples")
        yield self.backend.add_outbound_message(msg)
        stored_msg = yield self.store.get_outbound_message(msg["message_id"])
        self.assertEqual(stored_msg, msg)

    @inlineCallbacks
    def test_get_outbound_message_missing(self):
        """
        When we ask for an outbound message that does not exist, we get
        ``None``.
        """
        stored_record = yield self.store.get_outbound_message("badmsg")
        self.assertEqual(stored_record, None)

    @inlineCallbacks
    def test_add_ack_event(self):
        """
        When an event is added, it is stored in Riak.
        """
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        stored_event = yield self.backend.get_raw_event(ack["event_id"])
        self.assertEqual(stored_event, None)

        yield self.store.add_event(ack)
        stored_event = yield self.backend.get_raw_event(ack["event_id"])
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
        msg = self.msg_helper.make_outbound("apples")
        dr = self.msg_helper.make_delivery_report(msg)
        stored_event = yield self.backend.get_raw_event(dr["event_id"])
        self.assertEqual(stored_event, None)

        yield self.backend.add_event(dr)
        stored_event = yield self.backend.get_raw_event(dr["event_id"])
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
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.backend.add_event(ack)
        old_stored_event = yield self.backend.get_raw_event(ack["event_id"])
        self.assertEqual(old_stored_event.event, ack)

        ack["helper_metadata"]["fruit"] = {"type": "pomaceous"}
        yield self.backend.add_event(ack)
        new_stored_event = yield self.backend.get_raw_event(ack["event_id"])
        self.assertEqual(new_stored_event.event, ack)
        self.assertNotEqual(new_stored_event.event, old_stored_event.event)

    @inlineCallbacks
    def test_get_event(self):
        """
        When we ask for an event, we get the TransportEvent object.
        """
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.backend.add_event(ack)
        stored_event = yield self.store.get_event(ack["event_id"])
        self.assertEqual(stored_event, ack)

    @inlineCallbacks
    def test_get_event_missing(self):
        """
        When we ask for an event that does not exist, we get ``None``.
        """
        stored_record = yield self.store.get_event("badevent")
        self.assertEqual(stored_record, None)

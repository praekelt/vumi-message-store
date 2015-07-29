"""
Tests for vumi_message_store.message_store.
"""
from datetime import datetime

from twisted.internet.defer import inlineCallbacks
from vumi.tests.helpers import VumiTestCase, MessageHelper, PersistenceHelper
from zope.interface.verify import verifyObject

from vumi_message_store.batch_info_cache import to_timestamp
from vumi_message_store.interfaces import (
    IMessageStoreBatchManager, IOperationalMessageStore, IQueryMessageStore)
from vumi_message_store.message_store import (
    MessageStoreBatchManager, OperationalMessageStore, QueryMessageStore)
from vumi_message_store.tests.helpers import MessageSequenceHelper


class TestMessageStoreBatchManager(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.manager = self.persistence_helper.get_riak_manager()
        self.add_cleanup(self.manager.close_manager)
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.batch_manager = MessageStoreBatchManager(self.manager, self.redis)
        self.backend = self.batch_manager.riak_backend
        self.bi_cache = self.batch_manager.batch_info_cache

    def test_implements_IMessageStoreBatchManager(self):
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
        batch_exists_in_cache = yield self.bi_cache.batch_exists(batch_id)
        self.assertEqual(batch_exists_in_cache, True)

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
        batch_exists_in_cache = yield self.bi_cache.batch_exists(batch_id)
        self.assertEqual(batch_exists_in_cache, True)

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
        batch_exists_in_cache = yield self.bi_cache.batch_exists(batch_id)
        self.assertEqual(batch_exists_in_cache, True)

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
    def test_get_tag_info_tuple_form(self):
        """
        We can specify the tag as a (pool, tagname) tuple.
        """
        # We use the internals of the backend here because there's no other
        # direct way to create a CurrentTag object.
        stored_tag = self.backend.current_tags(
            "size:large", current_batch="mybatch")
        yield stored_tag.save()
        tag_info = yield self.batch_manager.get_tag_info(("size", "large"))
        self.assertEqual(tag_info.current_batch.key, "mybatch")

    @inlineCallbacks
    def test_get_tag_info_missing_tag(self):
        """
        If we ask for tag info that doesn't exist, we get a new CurrentTag
        object.
        """
        tag_info = yield self.batch_manager.get_tag_info("size:large")
        self.assertEqual(tag_info.current_batch.key, None)

    @inlineCallbacks
    def test_rebuild_cache(self):
        """
        Rebuilding the info cache for a batch will clear all cached data and
        rebuild it from the given QueryMessageStore.
        """
        msg_helper = self.add_helper(MessageHelper())
        batch_info_cache = self.batch_manager.batch_info_cache

        # Fill the message store backend with the data we want in the rebuilt
        # cache.
        yield self.backend.add_inbound_message(
            msg_helper.make_inbound("in 1"), batch_ids=["mybatch"])
        yield self.backend.add_outbound_message(
            msg_helper.make_outbound("out 1"), batch_ids=["mybatch"])
        yield self.backend.add_outbound_message(
            msg_helper.make_outbound("out 2"), batch_ids=["mybatch"])

        # Fill the cache with some nonsense that we want to throw out when
        # rebuilding.
        yield batch_info_cache.add_inbound_message_key("mybatch", "in1", 12345)
        yield batch_info_cache.add_inbound_message_key("mybatch", "in2", 12345)

        old_in = yield batch_info_cache.get_inbound_message_count("mybatch")
        old_out = yield batch_info_cache.get_outbound_message_count("mybatch")
        self.assertEqual((old_in, old_out), (2, 0))

        # Rebuild the cache.
        qms = QueryMessageStore(self.manager, self.redis)
        yield self.batch_manager.rebuild_cache("mybatch", qms)

        new_in = yield batch_info_cache.get_inbound_message_count("mybatch")
        new_out = yield batch_info_cache.get_outbound_message_count("mybatch")
        self.assertEqual((new_in, new_out), (1, 2))


class TestOperationalMessageStore(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.manager = self.persistence_helper.get_riak_manager()
        self.add_cleanup(self.manager.close_manager)
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.store = OperationalMessageStore(self.manager, self.redis)
        self.backend = self.store.riak_backend
        self.bi_cache = self.store.batch_info_cache
        self.msg_helper = self.add_helper(MessageHelper())

    def test_implements_IOperationalMessageStore(self):
        """
        OperationalMessageStore implements the IOperationalMessageStore
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
        identifier is stored with it. Additionally, it is added to the batch
        info cache.
        """
        yield self.bi_cache.batch_start("mybatch")
        msg = self.msg_helper.make_inbound("apples")
        yield self.store.add_inbound_message(msg, batch_ids=["mybatch"])
        stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(stored_msg.batches.keys(), ["mybatch"])
        batch_keys = yield self.bi_cache.list_inbound_message_keys("mybatch")
        self.assertEqual(set(batch_keys), set([msg["message_id"]]))

    @inlineCallbacks
    def test_add_inbound_message_with_multiple_batch_ids(self):
        """
        When an inbound message is added with multiple batch identifiers, it
        belongs to all the specified batches and is added to all their info
        caches.
        """
        yield self.bi_cache.batch_start("mybatch")
        yield self.bi_cache.batch_start("yourbatch")
        msg = self.msg_helper.make_inbound("apples")
        yield self.store.add_inbound_message(
            msg, batch_ids=["mybatch", "yourbatch"])
        stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(
            sorted(stored_msg.batches.keys()), ["mybatch", "yourbatch"])
        mykeys = yield self.bi_cache.list_inbound_message_keys("mybatch")
        self.assertEqual(mykeys, [msg["message_id"]])
        yourkeys = yield self.bi_cache.list_inbound_message_keys("yourbatch")
        self.assertEqual(yourkeys, [msg["message_id"]])

    @inlineCallbacks
    def test_add_inbound_message_to_new_batch(self):
        """
        When an existing inbound message is added with a new batch identifier,
        it belongs to the new batch as well as batches it already belonged to.
        """
        yield self.bi_cache.batch_start("mybatch")
        yield self.bi_cache.batch_start("yourbatch")
        msg = self.msg_helper.make_inbound("apples")
        yield self.store.add_inbound_message(msg, batch_ids=["mybatch"])
        yield self.store.add_inbound_message(msg, batch_ids=["yourbatch"])
        stored_msg = yield self.backend.get_raw_inbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(
            sorted(stored_msg.batches.keys()), ["mybatch", "yourbatch"])
        mykeys = yield self.bi_cache.list_inbound_message_keys("mybatch")
        self.assertEqual(mykeys, [msg["message_id"]])
        yourkeys = yield self.bi_cache.list_inbound_message_keys("yourbatch")
        self.assertEqual(yourkeys, [msg["message_id"]])

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
        identifier is stored with it. Additionally, it is added to the batch
        info cache.
        """
        yield self.bi_cache.batch_start("mybatch")
        msg = self.msg_helper.make_outbound("apples")
        yield self.store.add_outbound_message(msg, batch_ids=["mybatch"])
        stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(stored_msg.batches.keys(), ["mybatch"])
        batch_keys = yield self.bi_cache.list_outbound_message_keys("mybatch")
        self.assertEqual(batch_keys, [msg["message_id"]])

    @inlineCallbacks
    def test_add_outbound_message_with_multiple_batch_ids(self):
        """
        When an outbound message is added with multiple batch identifiers, it
        belongs to all the specified batches and is added to all their info
        caches.
        """
        yield self.bi_cache.batch_start("mybatch")
        yield self.bi_cache.batch_start("yourbatch")
        msg = self.msg_helper.make_outbound("apples")
        yield self.store.add_outbound_message(
            msg, batch_ids=["mybatch", "yourbatch"])
        stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(
            sorted(stored_msg.batches.keys()), ["mybatch", "yourbatch"])
        mykeys = yield self.bi_cache.list_outbound_message_keys("mybatch")
        self.assertEqual(mykeys, [msg["message_id"]])
        yourkeys = yield self.bi_cache.list_outbound_message_keys("yourbatch")
        self.assertEqual(yourkeys, [msg["message_id"]])

    @inlineCallbacks
    def test_add_outbound_message_to_new_batch(self):
        """
        When an existing outbound message is added with a new batch identifier,
        it belongs to the new batch as well as batches it already belonged to.
        """
        yield self.bi_cache.batch_start("mybatch")
        yield self.bi_cache.batch_start("yourbatch")
        msg = self.msg_helper.make_outbound("apples")
        yield self.store.add_outbound_message(msg, batch_ids=["mybatch"])
        yield self.store.add_outbound_message(msg, batch_ids=["yourbatch"])
        stored_msg = yield self.backend.get_raw_outbound_message(
            msg["message_id"])
        self.assertEqual(stored_msg.msg, msg)
        self.assertEqual(
            sorted(stored_msg.batches.keys()), ["mybatch", "yourbatch"])
        mykeys = yield self.bi_cache.list_outbound_message_keys("mybatch")
        self.assertEqual(mykeys, [msg["message_id"]])
        yourkeys = yield self.bi_cache.list_outbound_message_keys("yourbatch")
        self.assertEqual(yourkeys, [msg["message_id"]])

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

    @inlineCallbacks
    def test_add_delivery_report_event(self):
        """
        When a delivery report event is added it is stored in Riak.
        """
        msg = self.msg_helper.make_outbound("apples")
        dr = self.msg_helper.make_delivery_report(msg)
        stored_event = yield self.backend.get_raw_event(dr["event_id"])
        self.assertEqual(stored_event, None)

        yield self.backend.add_event(dr)
        stored_event = yield self.backend.get_raw_event(dr["event_id"])
        self.assertEqual(stored_event.event, dr)

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
    def test_add_ack_event_with_batch_id(self):
        """
        When an event is added with a batch identifier, that batch identifier
        is stored with it. Additionally, it is added to the batch info cache.
        """
        yield self.bi_cache.batch_start("mybatch")
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.store.add_event(ack, batch_ids=["mybatch"])
        stored_event = yield self.backend.get_raw_event(ack["event_id"])
        self.assertEqual(stored_event.event, ack)
        self.assertEqual(stored_event.batches.keys(), ["mybatch"])
        batch_keys = yield self.bi_cache.list_event_keys("mybatch")
        self.assertEqual(batch_keys, [ack["event_id"]])

    @inlineCallbacks
    def test_add_ack_event_with_multiple_batch_ids(self):
        """
        When an event is added with multiple batch identifiers, it belongs to
        all the specified batches and is added to all their info caches.
        """
        yield self.bi_cache.batch_start("mybatch")
        yield self.bi_cache.batch_start("yourbatch")
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.store.add_event(ack, batch_ids=["mybatch", "yourbatch"])
        stored_event = yield self.backend.get_raw_event(ack["event_id"])
        self.assertEqual(stored_event.event, ack)
        self.assertEqual(
            sorted(stored_event.batches.keys()), ["mybatch", "yourbatch"])
        mykeys = yield self.bi_cache.list_event_keys("mybatch")
        self.assertEqual(mykeys, [ack["event_id"]])
        yourkeys = yield self.bi_cache.list_event_keys("yourbatch")
        self.assertEqual(yourkeys, [ack["event_id"]])

    @inlineCallbacks
    def test_add_ack_event_to_new_batch(self):
        """
        When an existing event is added with a new batch identifier, it belongs
        to the new batch as well as batches it already belonged to.
        """
        yield self.bi_cache.batch_start("mybatch")
        yield self.bi_cache.batch_start("yourbatch")
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.store.add_event(ack, batch_ids=["mybatch"])
        yield self.store.add_event(ack, batch_ids=["yourbatch"])
        stored_event = yield self.backend.get_raw_event(ack["event_id"])
        self.assertEqual(stored_event.event, ack)
        self.assertEqual(
            sorted(stored_event.batches.keys()), ["mybatch", "yourbatch"])
        mykeys = yield self.bi_cache.list_event_keys("mybatch")
        self.assertEqual(mykeys, [ack["event_id"]])
        yourkeys = yield self.bi_cache.list_event_keys("yourbatch")
        self.assertEqual(yourkeys, [ack["event_id"]])

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
        tag_info = yield self.store.get_tag_info("size:large")
        self.assertEqual(tag_info.current_batch.key, "mybatch")

    @inlineCallbacks
    def test_get_tag_info_tuple_form(self):
        """
        We can specify the tag as a (pool, tagname) tuple.
        """
        # We use the internals of the backend here because there's no other
        # direct way to create a CurrentTag object.
        stored_tag = self.backend.current_tags(
            "size:large", current_batch="mybatch")
        yield stored_tag.save()
        tag_info = yield self.store.get_tag_info(("size", "large"))
        self.assertEqual(tag_info.current_batch.key, "mybatch")

    @inlineCallbacks
    def test_get_tag_info_missing_tag(self):
        """
        If we ask for tag info that doesn't exist, we get a new CurrentTag
        object.
        """
        tag_info = yield self.store.get_tag_info("size:large")
        self.assertEqual(tag_info.current_batch.key, None)


class TestQueryMessageStore(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.manager = self.persistence_helper.get_riak_manager()
        self.add_cleanup(self.manager.close_manager)
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.store = QueryMessageStore(self.manager, self.redis)
        self.backend = self.store.riak_backend
        self.bi_cache = self.store.batch_info_cache
        self.msg_helper = self.add_helper(MessageHelper())
        self.msg_seq_helper = self.add_helper(
            MessageSequenceHelper(self.backend, self.msg_helper))

    def test_implements_IQueryMessageStore(self):
        """
        QueryMessageStore implements the IQueryMessageStore interface.
        """
        self.assertTrue(IQueryMessageStore.providedBy(self.store))
        self.assertTrue(verifyObject(IQueryMessageStore, self.store))

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

    @inlineCallbacks
    def test_list_batch_inbound_messages(self):
        """
        When we ask for a list of inbound messages for a batch, we get an
        IndexPageWrapper containing the first page of results and can ask for
        following pages until all results are delivered.
        """
        batch_id, all_keys = (
            yield self.msg_seq_helper.create_inbound_message_sequence())
        keys_p1 = yield self.store.list_batch_inbound_messages(
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
        keys_p1 = yield self.store.list_batch_inbound_messages(
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
        keys_p1 = yield self.store.list_batch_inbound_messages(
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
        keys_p1 = yield self.store.list_batch_inbound_messages(
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
        keys_page = yield self.store.list_batch_inbound_messages(batch_id)
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
        keys_p1 = yield self.store.list_batch_outbound_messages(
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
        keys_p1 = yield self.store.list_batch_outbound_messages(
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
        keys_p1 = yield self.store.list_batch_outbound_messages(
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
        keys_p1 = yield self.store.list_batch_outbound_messages(
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
        keys_page = yield self.store.list_batch_outbound_messages(batch_id)
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

        keys_p1 = yield self.store.list_message_events(msg_id, page_size=3)
        # Paginated results are sorted by ascending timestamp.
        all_keys.reverse()
        self.assertEqual(list(keys_p1), all_keys[:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:])

    @inlineCallbacks
    def test_list_message_events_range_start(self):
        """
        When we ask for a list of event keys for an outbound message, we can
        specify a start timestamp.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.store.list_message_events(
            msg_id, start=all_keys[-2][1], page_size=3)
        # Paginated results are sorted by ascending timestamp.
        all_keys.reverse()
        self.assertEqual(list(keys_p1), all_keys[1:4])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[4:])

    @inlineCallbacks
    def test_list_message_events_range_end(self):
        """
        When we ask for a list of event keys for an outbound message, we can
        specify an end timestamp.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.store.list_message_events(
            msg_id, end=all_keys[1][1], page_size=3)
        # Paginated results are sorted by ascending timestamp.
        all_keys.reverse()
        self.assertEqual(list(keys_p1), all_keys[0:3])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(list(keys_p2), all_keys[3:-1])

    @inlineCallbacks
    def test_list_message_events_range(self):
        """
        When we ask for a list of event keys for an outbound message, we can
        specify both ends of the range.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.store.list_message_events(
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
        keys_page = yield self.store.list_message_events(msg["message_id"])
        self.assertEqual(list(keys_page), [])

    @inlineCallbacks
    def test_list_message_events_no_message(self):
        """
        When we ask for a list of events for an outbound message that does not
        exist, we get an empty IndexPageWrapper.
        """
        keys_page = yield self.store.list_message_events("badmsg")
        self.assertEqual(list(keys_page), [])

    @inlineCallbacks
    def test_get_batch_info_status(self):
        """
        The batch status can be retrieved as a dict of ints.
        """
        yield self.bi_cache.batch_start("mybatch")
        yield self.bi_cache.add_inbound_message_count("mybatch", 4)
        yield self.bi_cache.add_outbound_message_count("mybatch", 3)
        yield self.bi_cache.add_event_count("mybatch", "ack", 2)
        yield self.bi_cache.add_event_count(
            "mybatch", "delivery_report.delivered", 1)

        batch_status = yield self.store.get_batch_info_status("mybatch")
        self.assertEqual(batch_status, {
            "sent": 3,
            "ack": 2,
            "nack": 0,
            "delivery_report": 1,
            "delivery_report.delivered": 1,
            "delivery_report.failed": 0,
            "delivery_report.pending": 0,
        })

    @inlineCallbacks
    def test_get_batch_info_status_no_batch(self):
        """
        The batch status is empty for a batch that doesn't exist.
        """
        batch_status = yield self.store.get_batch_info_status("mybatch")
        self.assertEqual(batch_status, {})

    @inlineCallbacks
    def test_list_batch_recent_inbound_keys(self):
        """
        The list of recent inbound message keys can be retrieved with or
        without timestamps, ordered from newest to oldest.
        """
        yield self.bi_cache.batch_start("batch")
        start = to_timestamp(datetime.utcnow()) - 10
        msgs = [("message%d" % i, start + i) for i in range(5)]
        for msg in msgs:
            yield self.bi_cache.add_inbound_message_key("batch", *msg)

        keys = yield self.store.list_batch_recent_inbound_keys("batch")
        self.assertEqual(keys, [k for k, _ in reversed(msgs)])
        tkeys = yield self.store.list_batch_recent_inbound_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, list(reversed(msgs)))

    @inlineCallbacks
    def test_list_batch_recent_inbound_keys_empty_batch(self):
        """
        The list of recent inbound message keys is empty when there are no
        messages in the batch.
        """
        yield self.bi_cache.batch_start("batch")

        keys = yield self.store.list_batch_recent_inbound_keys("batch")
        self.assertEqual(keys, [])
        tkeys = yield self.store.list_batch_recent_inbound_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, [])

    @inlineCallbacks
    def test_list_batch_recent_outbound_keys(self):
        """
        The list of recent outbound message keys can be retrieved with or
        without timestamps, ordered from newest to oldest.
        """
        yield self.bi_cache.batch_start("batch")
        start = to_timestamp(datetime.utcnow()) - 10
        msgs = [("message%d" % i, start + i) for i in range(5)]
        for msg in msgs:
            yield self.bi_cache.add_outbound_message_key("batch", *msg)

        keys = yield self.store.list_batch_recent_outbound_keys("batch")
        self.assertEqual(keys, [k for k, _ in reversed(msgs)])
        tkeys = yield self.store.list_batch_recent_outbound_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, list(reversed(msgs)))

    @inlineCallbacks
    def test_list_batch_recent_outbound_keys_empty_batch(self):
        """
        The list of recent outbound message keys is empty when there are no
        messages in the batch.
        """
        yield self.bi_cache.batch_start("batch")

        keys = yield self.store.list_batch_recent_outbound_keys("batch")
        self.assertEqual(keys, [])
        tkeys = yield self.store.list_batch_recent_outbound_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, [])

    @inlineCallbacks
    def test_get_batch_inbound_count(self):
        """
        The inbound message count can be queried.
        """
        yield self.bi_cache.batch_start("batch")
        yield self.bi_cache.add_inbound_message_count("batch", 5000)
        yield self.bi_cache.add_inbound_message_key(
            "batch", "foo", to_timestamp(datetime.utcnow()))

        count = yield self.store.get_batch_inbound_count("batch")
        self.assertEqual(count, 5001)

    @inlineCallbacks
    def test_get_batch_inbound_count_no_batch(self):
        """
        The inbound message count returns zero for missing batches.
        """
        count = yield self.store.get_batch_inbound_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_batch_outbound_count(self):
        """
        The outbound message count can be queried.
        """
        yield self.bi_cache.batch_start("batch")
        yield self.bi_cache.add_outbound_message_count("batch", 5000)
        yield self.bi_cache.add_outbound_message_key(
            "batch", "foo", to_timestamp(datetime.utcnow()))

        count = yield self.store.get_batch_outbound_count("batch")
        self.assertEqual(count, 5001)

    @inlineCallbacks
    def test_get_batch_outbound_count_no_batch(self):
        """
        The outbound message count returns zero for missing batches.
        """
        count = yield self.store.get_batch_outbound_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_batch_event_count(self):
        """
        The event count can be queried.
        """
        yield self.bi_cache.batch_start("batch")
        yield self.bi_cache.add_event_count("batch", "ack", 5000)
        yield self.bi_cache.add_event_key(
            "batch", "foo", "delivery_report.delivered",
            to_timestamp(datetime.utcnow()))

        count = yield self.store.get_batch_event_count("batch")
        self.assertEqual(count, 5001)

    @inlineCallbacks
    def test_get_batch_event_count_no_batch(self):
        """
        The outbound message count returns zero for missing batches.
        """
        count = yield self.store.get_batch_event_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_batch_from_addr_count(self):
        """
        The from_addr count can be queried.
        """
        yield self.bi_cache.batch_start("batch")
        yield self.bi_cache.add_from_addr("batch", "addr-1")
        yield self.bi_cache.add_from_addr("batch", "addr-2")
        yield self.bi_cache.add_from_addr("batch", "addr-3")

        count = yield self.store.get_batch_from_addr_count("batch")
        self.assertEqual(count, 3)

    @inlineCallbacks
    def test_get_batch_from_addr_count_no_batch(self):
        """
        The from_addr count returns zero for missing batches.
        """
        count = yield self.store.get_batch_from_addr_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_batch_to_addr_count(self):
        """
        The to_addr count can be queried.
        """
        yield self.bi_cache.batch_start("batch")
        yield self.bi_cache.add_to_addr("batch", "addr-1")
        yield self.bi_cache.add_to_addr("batch", "addr-2")
        yield self.bi_cache.add_to_addr("batch", "addr-3")

        count = yield self.store.get_batch_to_addr_count("batch")
        self.assertEqual(count, 3)

    @inlineCallbacks
    def test_get_batch_to_addr_count_no_batch(self):
        """
        The to_addr count returns zero for missing batches.
        """
        count = yield self.store.get_batch_to_addr_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_list_batch_events(self):
        """
        When we ask for a list of events for a batch, we get an
        IndexPageWrapper containing the first page of results and can ask for
        following pages until all results are delivered.
        """
        batch_id, msg_id, all_keys = (
            yield self.msg_seq_helper.create_ack_event_sequence())
        keys_p1 = yield self.store.list_batch_events(batch_id, page_size=3)
        # Paginated results are sorted by descending timestamp.
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
        keys_p1 = yield self.store.list_batch_events(
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
        keys_p1 = yield self.store.list_batch_events(
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
        keys_p1 = yield self.store.list_batch_events(
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
        keys_page = yield self.store.list_batch_events(batch_id)
        self.assertEqual(list(keys_page), [])

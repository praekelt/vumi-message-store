# -*- coding: utf-8 -*-

"""Tests for vumi_message_store.batch_info_cache."""

from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks
from vumi.tests.helpers import VumiTestCase, MessageHelper, PersistenceHelper

from vumi_message_store.batch_info_cache import to_timestamp, BatchInfoCache
from vumi_message_store.message_store import QueryMessageStore


class TestBatchInfoCacheUtils(VumiTestCase):

    def test_to_timestamp_datetime(self):
        """
        We can convert a datetime object to a unix timestamp.
        """
        timestamp = to_timestamp(datetime(2015, 1, 26, 19, 22, 05))
        self.assertEqual(timestamp, 1422300125)

    def test_to_timestamp_vumi_format_string(self):
        """
        We can convert a VUMI_DATE_FORMAT string to a unix timestamp.
        """
        timestamp = to_timestamp("2015-01-26 19:22:05.000")
        self.assertEqual(timestamp, 1422300125)


class TestBatchInfoCache(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.batch_info_cache = BatchInfoCache(self.redis)
        self.msg_helper = self.add_helper(MessageHelper())

    @inlineCallbacks
    def assert_redis_keys(self, expected_keys):
        keys = yield self.redis.keys()
        self.assertEqual(set(expected_keys), set(keys))

    @inlineCallbacks
    def assert_redis_string(self, key, expected_value):
        value = yield self.redis.get(key)
        self.assertEqual(expected_value, value)

    @inlineCallbacks
    def assert_redis_set(self, key, expected_value):
        value = yield self.redis.smembers(key)
        self.assertEqual(set(expected_value), set(value))

    @inlineCallbacks
    def assert_redis_hash(self, key, expected_value):
        value = yield self.redis.hgetall(key)
        self.assertEqual(expected_value, value)

    @inlineCallbacks
    def assert_redis_zset(self, key, expected_value):
        value = yield self.redis.zrange(
            key, start=0, stop=-1, desc=False, withscores=True)
        self.assertEqual(expected_value, value)

    @inlineCallbacks
    def assert_redis_pfcount(self, key, expected_value):
        value = yield self.redis.pfcount(key)
        self.assertEqual(expected_value, value)

    @inlineCallbacks
    def test_batch_start(self):
        """
        Starting a batch creates and initialises counters and adds the batch
        identifier to the set of batches we're tracking.
        """
        yield self.assert_redis_keys([])
        yield self.batch_info_cache.batch_start("mybatch")
        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])
        yield self.assert_redis_set("batches", ["mybatch"])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_batch_exists(self):
        """
        The existence of a batch can be queried.
        """
        no_batches = yield self.batch_info_cache.batch_exists("mybatch")
        self.assertEqual(no_batches, False)
        yield self.batch_info_cache.batch_start("mybatch")
        mybatch = yield self.batch_info_cache.batch_exists("mybatch")
        self.assertEqual(mybatch, True)
        yourbatch = yield self.batch_info_cache.batch_exists("yourbatch")
        self.assertEqual(yourbatch, False)

    @inlineCallbacks
    def test_clear_batch(self):
        """
        Clearing a batch deletes all Redis keys for that batch and removes the
        batch identifier from the set of batches we're tracking.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        # Obsolete keys that we no longer use but should still clear.
        yield self.redis.zadd("batches:to_addr:mybatch", foo=123)
        yield self.redis.zadd("batches:from_addr:mybatch", foo=123)
        timestamp = to_timestamp(datetime.utcnow())
        yield self.batch_info_cache.add_inbound_message_key(
            "mybatch", "in", timestamp)
        yield self.batch_info_cache.add_outbound_message_key(
            "mybatch", "out", timestamp)
        yield self.batch_info_cache.add_event_key(
            "mybatch", "ack", "ack", timestamp)
        yield self.assert_redis_keys([
            "batches",
            "batches:inbound:mybatch",
            "batches:outbound:mybatch",
            "batches:event:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:to_addr:mybatch",
            "batches:from_addr:mybatch",
        ])
        yield self.assert_redis_set("batches", ["mybatch"])

        yield self.batch_info_cache.clear_batch("mybatch")
        yield self.assert_redis_keys(["batches"])
        yield self.assert_redis_set("batches", [])

    @inlineCallbacks
    def test_add_inbound_message(self):
        """
        Adding an inbound message updates the relevant counters and adds the
        message_id to the inbound messages zset.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        msg = self.msg_helper.make_inbound("apples")
        yield self.batch_info_cache.add_inbound_message("mybatch", msg)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:from_addr_hll:mybatch",
        ])

        timestamp = to_timestamp(msg["timestamp"])
        yield self.assert_redis_zset(
            "batches:inbound:mybatch", [(msg["message_id"], timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "1")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 1)

    @inlineCallbacks
    def test_add_inbound_message_key(self):
        """
        An inbound message can be added with just a key and timestamp.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        message_id = "mymessage"
        timestamp = to_timestamp(datetime.utcnow())
        yield self.batch_info_cache.add_inbound_message_key(
            "mybatch", message_id, timestamp)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        yield self.assert_redis_zset(
            "batches:inbound:mybatch", [(message_id, timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "1")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_inbound_message_key_again(self):
        """
        Adding an inbound message multiple times only updates the batch info
        once.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        message_id = "mymessage"
        timestamp = to_timestamp(datetime.utcnow())
        yield self.batch_info_cache.add_inbound_message_key(
            "mybatch", message_id, timestamp)
        yield self.batch_info_cache.add_inbound_message_key(
            "mybatch", message_id, timestamp)
        yield self.batch_info_cache.add_inbound_message_key(
            "mybatch", message_id, timestamp)

        yield self.assert_redis_zset(
            "batches:inbound:mybatch", [(message_id, timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "1")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_inbound_message_key_truncates_zset(self):
        """
        When our inbound message zset is full, adding a new message key
        truncates it by removing the oldest entries.
        """
        self.batch_info_cache.TRUNCATE_MESSAGE_KEY_ZSET_AT = 3
        start = to_timestamp(datetime.utcnow()) - 10
        msgs = [("message%d" % i, start + i) for i in range(5)]
        yield self.batch_info_cache.batch_start("batch")

        yield self.batch_info_cache.add_inbound_message_key("batch", *msgs[0])
        yield self.assert_redis_zset("batches:inbound:batch", msgs[:1])
        yield self.batch_info_cache.add_inbound_message_key("batch", *msgs[1])
        yield self.assert_redis_zset("batches:inbound:batch", msgs[:2])
        yield self.batch_info_cache.add_inbound_message_key("batch", *msgs[2])
        yield self.assert_redis_zset("batches:inbound:batch", msgs[:3])
        yield self.batch_info_cache.add_inbound_message_key("batch", *msgs[3])
        yield self.assert_redis_zset("batches:inbound:batch", msgs[1:4])
        yield self.batch_info_cache.add_inbound_message_key("batch", *msgs[4])
        yield self.assert_redis_zset("batches:inbound:batch", msgs[2:5])
        yield self.assert_redis_string("batches:inbound_count:batch", "5")

    @inlineCallbacks
    def test_add_from_addrs(self):
        """
        Adding a from_addr updates the HyperLogLog counter for the batch.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        incr = yield self.batch_info_cache.add_from_addr("mybatch", "addr-1")
        self.assertEqual(incr, 1)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:from_addr_hll:mybatch",
        ])
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 1)

        # Adding a second address updates the counter.
        incr = yield self.batch_info_cache.add_from_addr("mybatch", "addr-2")
        self.assertEqual(incr, 1)
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 2)

        # Adding a previously-added address doesn't update the counter.
        incr = yield self.batch_info_cache.add_from_addr("mybatch", "addr-1")
        self.assertEqual(incr, 0)
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 2)

    @inlineCallbacks
    def test_add_unicode_from_addr(self):
        """
        Adding a from_addr with unicode characters updates the HyperLogLog
        counter for the batch.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        incr = yield self.batch_info_cache.add_from_addr("mybatch", u"Zoë")
        self.assertEqual(incr, 1)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:from_addr_hll:mybatch",
        ])
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 1)

    @inlineCallbacks
    def test_add_outbound_message(self):
        """
        Adding an outbound message updates the relevant counters and adds the
        message_id to the outbound messages zset.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        msg = self.msg_helper.make_outbound("apples")
        yield self.batch_info_cache.add_outbound_message("mybatch", msg)

        yield self.assert_redis_keys([
            "batches",
            "batches:outbound:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:to_addr_hll:mybatch",
        ])

        timestamp = to_timestamp(msg["timestamp"])
        yield self.assert_redis_zset(
            "batches:outbound:mybatch", [(msg["message_id"], timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "1")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "1",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 1)

    @inlineCallbacks
    def test_add_outbound_message_key(self):
        """
        An outbound message can be added with just a key and timestamp.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        message_id = "mymessage"
        timestamp = to_timestamp(datetime.utcnow())
        yield self.batch_info_cache.add_outbound_message_key(
            "mybatch", message_id, timestamp)

        yield self.assert_redis_keys([
            "batches",
            "batches:outbound:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        yield self.assert_redis_zset(
            "batches:outbound:mybatch", [(message_id, timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "1")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "1",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_outbound_message_key_again(self):
        """
        Adding an outbound message multiple times only updates the batch info
        once.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        message_id = "mymessage"
        timestamp = to_timestamp(datetime.utcnow())
        yield self.batch_info_cache.add_outbound_message_key(
            "mybatch", message_id, timestamp)
        yield self.batch_info_cache.add_outbound_message_key(
            "mybatch", message_id, timestamp)
        yield self.batch_info_cache.add_outbound_message_key(
            "mybatch", message_id, timestamp)

        yield self.assert_redis_zset(
            "batches:outbound:mybatch", [(message_id, timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "1")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "1",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_outbound_message_key_truncates_zset(self):
        """
        When our outbound message zset is full, adding a new message key
        truncates it by removing the oldest entries.
        """
        self.batch_info_cache.TRUNCATE_MESSAGE_KEY_ZSET_AT = 3
        start = to_timestamp(datetime.utcnow()) - 10
        msgs = [("message%d" % i, start + i) for i in range(5)]
        yield self.batch_info_cache.batch_start("batch")

        yield self.batch_info_cache.add_outbound_message_key("batch", *msgs[0])
        yield self.assert_redis_zset("batches:outbound:batch", msgs[:1])
        yield self.batch_info_cache.add_outbound_message_key("batch", *msgs[1])
        yield self.assert_redis_zset("batches:outbound:batch", msgs[:2])
        yield self.batch_info_cache.add_outbound_message_key("batch", *msgs[2])
        yield self.assert_redis_zset("batches:outbound:batch", msgs[:3])
        yield self.batch_info_cache.add_outbound_message_key("batch", *msgs[3])
        yield self.assert_redis_zset("batches:outbound:batch", msgs[1:4])
        yield self.batch_info_cache.add_outbound_message_key("batch", *msgs[4])
        yield self.assert_redis_zset("batches:outbound:batch", msgs[2:5])
        yield self.assert_redis_string("batches:outbound_count:batch", "5")

    @inlineCallbacks
    def test_add_to_addrs(self):
        """
        Adding a to_addr updates the HyperLogLog counter for the batch.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        incr = yield self.batch_info_cache.add_to_addr("mybatch", "addr-1")
        self.assertEqual(incr, 1)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:to_addr_hll:mybatch",
        ])
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 1)

        # Adding a second address updates the counter.
        incr = yield self.batch_info_cache.add_to_addr("mybatch", "addr-2")
        self.assertEqual(incr, 1)
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 2)

        # Adding a previously-added address doesn't update the counter.
        incr = yield self.batch_info_cache.add_to_addr("mybatch", "addr-1")
        self.assertEqual(incr, 0)
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 2)

    @inlineCallbacks
    def test_add_unicode_to_addr(self):
        """
        Adding a to_addr with unicode characters updates the HyperLogLog
        counter for the batch.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        incr = yield self.batch_info_cache.add_to_addr("mybatch", u"Zoë")
        self.assertEqual(incr, 1)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:to_addr_hll:mybatch",
        ])
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 1)

    @inlineCallbacks
    def test_add_event_ack(self):
        """
        Adding an ack updates the relevant counters and adds the event_id to
        the events zset.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        msg = self.msg_helper.make_outbound("apples")
        ack = self.msg_helper.make_ack(msg)
        yield self.batch_info_cache.add_event("mybatch", ack)

        yield self.assert_redis_keys([
            "batches",
            "batches:event:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        timestamp = to_timestamp(ack["timestamp"])
        yield self.assert_redis_zset(
            "batches:event:mybatch", [(ack["event_id"], timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "1")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "1",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_event_nack(self):
        """
        Adding a nack updates the relevant counters and adds the event_id to
        the events zset.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        msg = self.msg_helper.make_outbound("apples")
        nack = self.msg_helper.make_nack(msg)
        yield self.batch_info_cache.add_event("mybatch", nack)

        yield self.assert_redis_keys([
            "batches",
            "batches:event:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        timestamp = to_timestamp(nack["timestamp"])
        yield self.assert_redis_zset(
            "batches:event:mybatch", [(nack["event_id"], timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "1")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "1",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_event_delivery_report(self):
        """
        Adding a delivery updates the relevant counters and adds the event_id
        to the events zset.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        msg = self.msg_helper.make_outbound("apples")
        dr = self.msg_helper.make_delivery_report(msg)
        yield self.batch_info_cache.add_event("mybatch", dr)

        yield self.assert_redis_keys([
            "batches",
            "batches:event:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        timestamp = to_timestamp(dr["timestamp"])
        yield self.assert_redis_zset(
            "batches:event:mybatch", [(dr["event_id"], timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "1")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "0",
            "delivery_report": "1",
            "delivery_report.delivered": "1",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_event_key_ack(self):
        """
        An ack can be added with just a key, event type, and timestamp.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        event_id = "myevent"
        timestamp = to_timestamp(datetime.utcnow())
        yield self.batch_info_cache.add_event_key(
            "mybatch", event_id, "ack", timestamp)

        yield self.assert_redis_keys([
            "batches",
            "batches:event:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        yield self.assert_redis_zset(
            "batches:event:mybatch", [(event_id, timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "1")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "1",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_event_key_delivery_report(self):
        """
        A delivery report can be added with just a key, event type, and
        timestamp.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        event_id = "myevent"
        timestamp = to_timestamp(datetime.utcnow())
        yield self.batch_info_cache.add_event_key(
            "mybatch", event_id, "delivery_report.delivered", timestamp)

        yield self.assert_redis_keys([
            "batches",
            "batches:event:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        yield self.assert_redis_zset(
            "batches:event:mybatch", [(event_id, timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "1")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "0",
            "delivery_report": "1",
            "delivery_report.delivered": "1",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_event_key_again(self):
        """
        Adding an event multiple times only updates the batch info once.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        event_id = "myevent"
        timestamp = to_timestamp(datetime.utcnow())
        yield self.batch_info_cache.add_event_key(
            "mybatch", event_id, "ack", timestamp)
        yield self.batch_info_cache.add_event_key(
            "mybatch", event_id, "ack", timestamp)
        yield self.batch_info_cache.add_event_key(
            "mybatch", event_id, "ack", timestamp)

        yield self.assert_redis_zset(
            "batches:event:mybatch", [(event_id, timestamp)])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "1")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "1",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_event_key_truncates_zset(self):
        """
        When our event zset is full, adding a new event key truncates it by
        removing the oldest entries.
        """
        self.batch_info_cache.TRUNCATE_MESSAGE_KEY_ZSET_AT = 3
        start = to_timestamp(datetime.utcnow()) - 10
        events = [("event%d" % i, "ack", start + i) for i in range(5)]
        redis_events = [(k, t) for k, _, t in events]
        yield self.batch_info_cache.batch_start("batch")

        yield self.batch_info_cache.add_event_key("batch", *events[0])
        yield self.assert_redis_zset("batches:event:batch", redis_events[:1])
        yield self.batch_info_cache.add_event_key("batch", *events[1])
        yield self.assert_redis_zset("batches:event:batch", redis_events[:2])
        yield self.batch_info_cache.add_event_key("batch", *events[2])
        yield self.assert_redis_zset("batches:event:batch", redis_events[:3])
        yield self.batch_info_cache.add_event_key("batch", *events[3])
        yield self.assert_redis_zset("batches:event:batch", redis_events[1:4])
        yield self.batch_info_cache.add_event_key("batch", *events[4])
        yield self.assert_redis_zset("batches:event:batch", redis_events[2:5])
        yield self.assert_redis_string("batches:event_count:batch", "5")

    @inlineCallbacks
    def test_add_inbound_message_count(self):
        """
        Inbound message counters can be incremented in bulk.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        yield self.batch_info_cache.add_inbound_message_count("mybatch", 10)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        yield self.assert_redis_string("batches:inbound_count:mybatch", "10")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_outbound_message_count(self):
        """
        Outbound message counters can be incremented in bulk.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        yield self.batch_info_cache.add_outbound_message_count("mybatch", 10)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "10")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "10",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_add_event_count(self):
        """
        Event counters can be incremented in bulk.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        yield self.batch_info_cache.add_event_count("mybatch", "ack", 7)
        yield self.batch_info_cache.add_event_count(
            "mybatch", "delivery_report.delivered", 3)

        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])

        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "10")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "7",
            "nack": "0",
            "delivery_report": "3",
            "delivery_report.delivered": "3",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_get_batch_status(self):
        """
        The batch status can be retrieved as a dict of ints.
        """
        yield self.batch_info_cache.batch_start("mybatch")
        yield self.batch_info_cache.add_inbound_message_count("mybatch", 4)
        yield self.batch_info_cache.add_outbound_message_count("mybatch", 3)
        yield self.batch_info_cache.add_event_count("mybatch", "ack", 2)
        yield self.batch_info_cache.add_event_count(
            "mybatch", "delivery_report.delivered", 1)
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "3",
            "ack": "2",
            "nack": "0",
            "delivery_report": "1",
            "delivery_report.delivered": "1",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

        batch_status = yield self.batch_info_cache.get_batch_status("mybatch")
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
    def test_get_batch_status_no_batch(self):
        """
        The batch status is empty for a batch that doesn't exist.
        """
        batch_status = yield self.batch_info_cache.get_batch_status("mybatch")
        self.assertEqual(batch_status, {})

    @inlineCallbacks
    def test_list_inbound_message_keys(self):
        """
        The list of recent inbound message keys can be retrieved with or
        without timestamps, ordered from newest to oldest.
        """
        yield self.batch_info_cache.batch_start("batch")
        start = to_timestamp(datetime.utcnow()) - 10
        msgs = [("message%d" % i, start + i) for i in range(5)]
        for msg in msgs:
            yield self.batch_info_cache.add_inbound_message_key(
                "batch", *msg)
        yield self.assert_redis_zset("batches:inbound:batch", msgs)

        keys = yield self.batch_info_cache.list_inbound_message_keys("batch")
        self.assertEqual(keys, [k for k, _ in reversed(msgs)])
        tkeys = yield self.batch_info_cache.list_inbound_message_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, list(reversed(msgs)))

    @inlineCallbacks
    def test_list_inbound_message_keys_empty_batch(self):
        """
        The list of recent inbound message keys is empty when there are no
        messages in the batch.
        """
        yield self.batch_info_cache.batch_start("batch")
        yield self.assert_redis_zset("batches:inbound:batch", [])

        keys = yield self.batch_info_cache.list_inbound_message_keys("batch")
        self.assertEqual(keys, [])
        tkeys = yield self.batch_info_cache.list_inbound_message_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, [])

    @inlineCallbacks
    def test_list_outbound_message_keys(self):
        """
        The list of recent outbound message keys can be retrieved with or
        without timestamps, ordered from newest to oldest.
        """
        yield self.batch_info_cache.batch_start("batch")
        start = to_timestamp(datetime.utcnow()) - 10
        msgs = [("message%d" % i, start + i) for i in range(5)]
        for msg in msgs:
            yield self.batch_info_cache.add_outbound_message_key(
                "batch", *msg)
        yield self.assert_redis_zset("batches:outbound:batch", msgs)

        keys = yield self.batch_info_cache.list_outbound_message_keys("batch")
        self.assertEqual(keys, [k for k, _ in reversed(msgs)])
        tkeys = yield self.batch_info_cache.list_outbound_message_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, list(reversed(msgs)))

    @inlineCallbacks
    def test_list_outbound_message_keys_empty_batch(self):
        """
        The list of recent outbound message keys is empty when there are no
        messages in the batch.
        """
        yield self.batch_info_cache.batch_start("batch")
        yield self.assert_redis_zset("batches:outbound:batch", [])

        keys = yield self.batch_info_cache.list_outbound_message_keys("batch")
        self.assertEqual(keys, [])
        tkeys = yield self.batch_info_cache.list_outbound_message_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, [])

    @inlineCallbacks
    def test_list_event_keys(self):
        """
        The list of recent event keys can be retrieved with or without
        timestamps, ordered from newest to oldest.
        """
        yield self.batch_info_cache.batch_start("batch")
        start = to_timestamp(datetime.utcnow()) - 10
        addevents = [("event%d" % i, "ack", start + i) for i in range(5)]
        for event in addevents:
            yield self.batch_info_cache.add_event_key("batch", *event)
        events = [(k, t) for k, _, t in addevents]
        yield self.assert_redis_zset("batches:event:batch", events)

        keys = yield self.batch_info_cache.list_event_keys("batch")
        self.assertEqual(keys, [k for k, _ in reversed(events)])
        tkeys = yield self.batch_info_cache.list_event_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, list(reversed(events)))

    @inlineCallbacks
    def test_list_event_keys_empty_batch(self):
        """
        The list of recent outbound message keys is empty when there are no
        messages in the batch.
        """
        yield self.batch_info_cache.batch_start("batch")
        yield self.assert_redis_zset("batches:outbound:batch", [])

        keys = yield self.batch_info_cache.list_event_keys("batch")
        self.assertEqual(keys, [])
        tkeys = yield self.batch_info_cache.list_event_keys(
            "batch", with_timestamp=True)
        self.assertEqual(tkeys, [])

    @inlineCallbacks
    def test_get_inbound_message_count(self):
        """
        The inbound message count can be queried.
        """
        yield self.batch_info_cache.batch_start("batch")
        yield self.batch_info_cache.add_inbound_message_count("batch", 5000)
        yield self.batch_info_cache.add_inbound_message_key(
            "batch", "foo", to_timestamp(datetime.utcnow()))

        count = yield self.batch_info_cache.get_inbound_message_count("batch")
        self.assertEqual(count, 5001)

    @inlineCallbacks
    def test_get_inbound_message_count_no_batch(self):
        """
        The inbound message count returns zero for missing batches.
        """
        count = yield self.batch_info_cache.get_inbound_message_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_outbound_message_count(self):
        """
        The outbound message count can be queried.
        """
        yield self.batch_info_cache.batch_start("batch")
        yield self.batch_info_cache.add_outbound_message_count("batch", 5000)
        yield self.batch_info_cache.add_outbound_message_key(
            "batch", "foo", to_timestamp(datetime.utcnow()))

        count = yield self.batch_info_cache.get_outbound_message_count("batch")
        self.assertEqual(count, 5001)

    @inlineCallbacks
    def test_get_outbound_message_count_no_batch(self):
        """
        The outbound message count returns zero for missing batches.
        """
        count = yield self.batch_info_cache.get_outbound_message_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_event_count(self):
        """
        The event count can be queried.
        """
        yield self.batch_info_cache.batch_start("batch")
        yield self.batch_info_cache.add_event_count("batch", "ack", 5000)
        yield self.batch_info_cache.add_event_key(
            "batch", "foo", "delivery_report.delivered",
            to_timestamp(datetime.utcnow()))

        count = yield self.batch_info_cache.get_event_count("batch")
        self.assertEqual(count, 5001)

    @inlineCallbacks
    def test_get_event_count_no_batch(self):
        """
        The outbound message count returns zero for missing batches.
        """
        count = yield self.batch_info_cache.get_event_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_from_addr_count(self):
        """
        The from_addr count can be queried.
        """
        yield self.batch_info_cache.batch_start("batch")
        yield self.batch_info_cache.add_from_addr("batch", "addr-1")
        yield self.batch_info_cache.add_from_addr("batch", "addr-2")
        yield self.batch_info_cache.add_from_addr("batch", "addr-3")

        count = yield self.batch_info_cache.get_from_addr_count("batch")
        self.assertEqual(count, 3)

    @inlineCallbacks
    def test_get_from_addr_count_no_batch(self):
        """
        The from_addr count returns zero for missing batches.
        """
        count = yield self.batch_info_cache.get_from_addr_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_to_addr_count(self):
        """
        The to_addr count can be queried.
        """
        yield self.batch_info_cache.batch_start("batch")
        yield self.batch_info_cache.add_to_addr("batch", "addr-1")
        yield self.batch_info_cache.add_to_addr("batch", "addr-2")
        yield self.batch_info_cache.add_to_addr("batch", "addr-3")

        count = yield self.batch_info_cache.get_to_addr_count("batch")
        self.assertEqual(count, 3)

    @inlineCallbacks
    def test_get_to_addr_count_no_batch(self):
        """
        The to_addr count returns zero for missing batches.
        """
        count = yield self.batch_info_cache.get_to_addr_count("batch")
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_rebuild_cache(self):
        """
        Rebuilding the cache will clear all cached data and rebuild it from the
        given QueryMessageStore.
        """
        riak_persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = riak_persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        qms = QueryMessageStore(manager, self.redis)
        backend = qms.riak_backend

        start = datetime.utcnow() - timedelta(seconds=10)

        # Fill the message store backend with the data we want in the rebuilt
        # cache.
        inbound_keys = []
        for i in range(5):
            msg = self.msg_helper.make_inbound(
                "in %s" % (i,), timestamp=(start + timedelta(seconds=i)))
            inbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_inbound_message(msg, batch_ids=["mybatch"])

        outbound_msgs = []
        outbound_keys = []
        for i in range(4):
            msg = self.msg_helper.make_outbound(
                "out %s" % (i,), timestamp=(start + timedelta(seconds=i)))
            outbound_msgs.append(msg)
            outbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_outbound_message(msg, batch_ids=["mybatch"])

        events = [
            self.msg_helper.make_nack(
                outbound_msgs[0], timestamp=(start + timedelta(seconds=1))),
            self.msg_helper.make_ack(
                outbound_msgs[1], timestamp=(start + timedelta(seconds=2))),
            self.msg_helper.make_delivery_report(
                outbound_msgs[1], timestamp=(start + timedelta(seconds=3))),
        ]
        event_keys = []
        for event in events:
            event_keys.append(
                (event["event_id"], to_timestamp(event["timestamp"])))
            yield backend.add_event(event, batch_ids=["mybatch"])

        # Fill the cache with some nonsense that we want to throw out when
        # rebuilding.
        yield self.batch_info_cache.add_inbound_message_key(
            "mybatch", "inmsg", 12345)
        yield self.batch_info_cache.add_outbound_message_key(
            "mybatch", "outmsg", 23456)
        yield self.batch_info_cache.add_event_key(
            "mybatch", "event", "ack", 34567)
        yield self.assert_redis_string("batches:inbound_count:mybatch", "1")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "1")
        yield self.assert_redis_string("batches:event_count:mybatch", "1")

        # Rebuild the cache.
        yield self.batch_info_cache.rebuild_cache("mybatch", qms)
        yield self.assert_redis_keys([
            "batches",
            "batches:inbound:mybatch",
            "batches:outbound:mybatch",
            "batches:event:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:to_addr_hll:mybatch",
            "batches:from_addr_hll:mybatch",
        ])
        yield self.assert_redis_set("batches", ["mybatch"])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "5")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "4")
        yield self.assert_redis_string("batches:event_count:mybatch", "3")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "4",
            "ack": "1",
            "nack": "1",
            "delivery_report": "1",
            "delivery_report.delivered": "1",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })
        yield self.assert_redis_zset("batches:inbound:mybatch", inbound_keys)
        yield self.assert_redis_zset("batches:outbound:mybatch", outbound_keys)
        yield self.assert_redis_zset("batches:event:mybatch", event_keys)
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 1)
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 1)

    @inlineCallbacks
    def test_rebuild_cache_uncached_batch(self):
        """
        Rebuilding the cache for a batch works even if the batch is not cached.
        """
        riak_persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = riak_persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        qms = QueryMessageStore(manager, self.redis)
        backend = qms.riak_backend

        start = datetime.utcnow() - timedelta(seconds=10)

        inbound_keys = []
        for i in range(5):
            msg = self.msg_helper.make_inbound(
                "in %s" % (i,), timestamp=(start + timedelta(seconds=i)))
            inbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_inbound_message(msg, batch_ids=["mybatch"])

        outbound_msgs = []
        outbound_keys = []
        for i in range(4):
            msg = self.msg_helper.make_outbound(
                "out %s" % (i,), timestamp=(start + timedelta(seconds=i)))
            outbound_msgs.append(msg)
            outbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_outbound_message(msg, batch_ids=["mybatch"])

        events = [
            self.msg_helper.make_nack(
                outbound_msgs[0], timestamp=(start + timedelta(seconds=1))),
            self.msg_helper.make_ack(
                outbound_msgs[1], timestamp=(start + timedelta(seconds=2))),
            self.msg_helper.make_delivery_report(
                outbound_msgs[1], timestamp=(start + timedelta(seconds=3))),
        ]
        event_keys = []
        for event in events:
            event_keys.append(
                (event["event_id"], to_timestamp(event["timestamp"])))
            yield backend.add_event(event, batch_ids=["mybatch"])

        yield self.assert_redis_keys([])
        yield self.batch_info_cache.rebuild_cache("mybatch", qms)
        yield self.assert_redis_keys([
            "batches",
            "batches:inbound:mybatch",
            "batches:outbound:mybatch",
            "batches:event:mybatch",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
            "batches:to_addr_hll:mybatch",
            "batches:from_addr_hll:mybatch",
        ])
        yield self.assert_redis_set("batches", ["mybatch"])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "5")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "4")
        yield self.assert_redis_string("batches:event_count:mybatch", "3")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "4",
            "ack": "1",
            "nack": "1",
            "delivery_report": "1",
            "delivery_report.delivered": "1",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })
        yield self.assert_redis_zset("batches:inbound:mybatch", inbound_keys)
        yield self.assert_redis_zset("batches:outbound:mybatch", outbound_keys)
        yield self.assert_redis_zset("batches:event:mybatch", event_keys)
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 1)
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 1)

    @inlineCallbacks
    def test_rebuild_cache_missing_batch(self):
        """
        Rebuilding a cache for a batch that doesn't exist is equivalent to
        creating an empty cache for the batch.
        """
        riak_persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = riak_persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        qms = QueryMessageStore(manager, self.redis)

        yield self.assert_redis_keys([])
        yield self.batch_info_cache.rebuild_cache("mybatch", qms)
        yield self.assert_redis_keys([
            "batches",
            "batches:inbound_count:mybatch",
            "batches:outbound_count:mybatch",
            "batches:event_count:mybatch",
            "batches:status:mybatch",
        ])
        yield self.assert_redis_set("batches", ["mybatch"])
        yield self.assert_redis_string("batches:inbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "0")
        yield self.assert_redis_string("batches:event_count:mybatch", "0")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })

    @inlineCallbacks
    def test_rebuild_cache_inbound_messages_beyond_truncation(self):
        """
        Rebuilding the cache with more inbound messages than the truncation
        point results in the most recent messages being stored while the
        remaining messages (and their addresses) are counted.
        """
        riak_persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = riak_persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        qms = QueryMessageStore(manager, self.redis)
        backend = qms.riak_backend

        start = datetime.utcnow() - timedelta(seconds=10)

        inbound_keys = []
        for i in range(5):
            msg = self.msg_helper.make_inbound(
                "in %s" % (i,), timestamp=(start + timedelta(seconds=i)),
                from_addr="addr %s" % i)
            inbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_inbound_message(msg, batch_ids=["mybatch"])

        yield self.assert_redis_keys([])
        self.batch_info_cache.TRUNCATE_MESSAGE_KEY_ZSET_AT = 2

        yield self.batch_info_cache.rebuild_cache("mybatch", qms)
        yield self.assert_redis_string("batches:inbound_count:mybatch", "5")
        yield self.assert_redis_zset("batches:inbound:mybatch",
                                     inbound_keys[-2:])
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 5)

    @inlineCallbacks
    def test_rebuild_cache_outbound_messages_beyond_truncation(self):
        """
        Rebuilding the cache with more outbound messages than the truncation
        point results in the most recent messages being stored while the
        remaining messages (and their addresses) are counted.
        """
        riak_persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = riak_persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        qms = QueryMessageStore(manager, self.redis)
        backend = qms.riak_backend

        start = datetime.utcnow() - timedelta(seconds=10)

        outbound_keys = []
        for i in range(4):
            msg = self.msg_helper.make_outbound(
                "out %s" % (i,), timestamp=(start + timedelta(seconds=i)),
                to_addr="addr %s" % i)
            outbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_outbound_message(msg, batch_ids=["mybatch"])

        yield self.assert_redis_keys([])
        self.batch_info_cache.TRUNCATE_MESSAGE_KEY_ZSET_AT = 2

        yield self.batch_info_cache.rebuild_cache("mybatch", qms)
        yield self.assert_redis_string("batches:outbound_count:mybatch", "4")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "4",
            "ack": "0",
            "nack": "0",
            "delivery_report": "0",
            "delivery_report.delivered": "0",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })
        yield self.assert_redis_zset("batches:outbound:mybatch",
                                     outbound_keys[-2:])
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 4)

    @inlineCallbacks
    def test_rebuild_cache_events_beyond_truncation(self):
        """
        Rebuilding the cache with more events than the truncation point results
        in the most recent events being stored while the remaining events are
        counted.
        """
        riak_persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = riak_persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        qms = QueryMessageStore(manager, self.redis)
        backend = qms.riak_backend

        start = datetime.utcnow() - timedelta(seconds=10)

        msg = self.msg_helper.make_outbound("apples")
        events = [
            self.msg_helper.make_nack(
                msg, timestamp=(start + timedelta(seconds=1))),
            self.msg_helper.make_ack(
                msg, timestamp=(start + timedelta(seconds=2))),
            self.msg_helper.make_delivery_report(
                msg, timestamp=(start + timedelta(seconds=3))),
        ]
        event_keys = []
        for event in events:
            event_keys.append(
                (event["event_id"], to_timestamp(event["timestamp"])))
            yield backend.add_event(event, batch_ids=["mybatch"])

        yield self.assert_redis_keys([])
        self.batch_info_cache.TRUNCATE_MESSAGE_KEY_ZSET_AT = 2

        yield self.batch_info_cache.rebuild_cache("mybatch", qms)
        yield self.assert_redis_string("batches:event_count:mybatch", "3")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "0",
            "ack": "1",
            "nack": "1",
            "delivery_report": "1",
            "delivery_report.delivered": "1",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })
        yield self.assert_redis_zset("batches:event:mybatch", event_keys[-2:])

    @inlineCallbacks
    def test_rebuild_cache_page_size_smaller_than_truncation(self):
        """
        Rebuilding the cache for a batch works even if the page size from the
        query message store is smaller than the truncation point of the cache.
        """
        riak_persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = riak_persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        qms = QueryMessageStore(manager, self.redis)
        backend = qms.riak_backend

        start = datetime.utcnow() - timedelta(seconds=10)

        inbound_keys = []
        for i in range(5):
            msg = self.msg_helper.make_inbound(
                "in %s" % (i,), timestamp=(start + timedelta(seconds=i)),
                from_addr="addr %s" % i)
            inbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_inbound_message(msg, batch_ids=["mybatch"])

        outbound_msgs = []
        outbound_keys = []
        for i in range(4):
            msg = self.msg_helper.make_outbound(
                "out %s" % (i,), timestamp=(start + timedelta(seconds=i)),
                to_addr="addr %s" % i)
            outbound_msgs.append(msg)
            outbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_outbound_message(msg, batch_ids=["mybatch"])

        events = [
            self.msg_helper.make_nack(
                outbound_msgs[0], timestamp=(start + timedelta(seconds=1))),
            self.msg_helper.make_ack(
                outbound_msgs[1], timestamp=(start + timedelta(seconds=2))),
            self.msg_helper.make_delivery_report(
                outbound_msgs[1], timestamp=(start + timedelta(seconds=3))),
        ]
        event_keys = []
        for event in events:
            event_keys.append(
                (event["event_id"], to_timestamp(event["timestamp"])))
            yield backend.add_event(event, batch_ids=["mybatch"])

        yield self.assert_redis_keys([])
        self.batch_info_cache.TRUNCATE_MESSAGE_KEY_ZSET_AT = 3

        yield self.batch_info_cache.rebuild_cache("mybatch", qms, page_size=2)
        yield self.assert_redis_string("batches:inbound_count:mybatch", "5")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "4")
        yield self.assert_redis_string("batches:event_count:mybatch", "3")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "4",
            "ack": "1",
            "nack": "1",
            "delivery_report": "1",
            "delivery_report.delivered": "1",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })
        yield self.assert_redis_zset("batches:inbound:mybatch",
                                     inbound_keys[-3:])
        yield self.assert_redis_zset("batches:outbound:mybatch",
                                     outbound_keys[-3:])
        yield self.assert_redis_zset("batches:event:mybatch", event_keys)
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 4)
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 5)

    @inlineCallbacks
    def test_rebuild_cache_page_size_larger_than_truncation(self):
        """
        Rebuilding the cache for a batch works even if the page size from the
        query message store is larger than the truncation point of the cache.
        """
        riak_persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        manager = riak_persistence_helper.get_riak_manager()
        self.add_cleanup(manager.close_manager)
        qms = QueryMessageStore(manager, self.redis)
        backend = qms.riak_backend

        start = datetime.utcnow() - timedelta(seconds=10)

        inbound_keys = []
        for i in range(5):
            msg = self.msg_helper.make_inbound(
                "in %s" % (i,), timestamp=(start + timedelta(seconds=i)),
                from_addr="addr %s" % i)
            inbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_inbound_message(msg, batch_ids=["mybatch"])

        outbound_msgs = []
        outbound_keys = []
        for i in range(4):
            msg = self.msg_helper.make_outbound(
                "out %s" % (i,), timestamp=(start + timedelta(seconds=i)),
                to_addr="addr %s" % i)
            outbound_msgs.append(msg)
            outbound_keys.append(
                (msg["message_id"], to_timestamp(msg["timestamp"])))
            yield backend.add_outbound_message(msg, batch_ids=["mybatch"])

        events = [
            self.msg_helper.make_nack(
                outbound_msgs[0], timestamp=(start + timedelta(seconds=1))),
            self.msg_helper.make_ack(
                outbound_msgs[1], timestamp=(start + timedelta(seconds=2))),
            self.msg_helper.make_delivery_report(
                outbound_msgs[1], timestamp=(start + timedelta(seconds=3))),
        ]
        event_keys = []
        for event in events:
            event_keys.append(
                (event["event_id"], to_timestamp(event["timestamp"])))
            yield backend.add_event(event, batch_ids=["mybatch"])

        yield self.assert_redis_keys([])
        self.batch_info_cache.TRUNCATE_MESSAGE_KEY_ZSET_AT = 2

        yield self.batch_info_cache.rebuild_cache("mybatch", qms, page_size=3)
        yield self.assert_redis_string("batches:inbound_count:mybatch", "5")
        yield self.assert_redis_string("batches:outbound_count:mybatch", "4")
        yield self.assert_redis_string("batches:event_count:mybatch", "3")
        yield self.assert_redis_hash("batches:status:mybatch", {
            "sent": "4",
            "ack": "1",
            "nack": "1",
            "delivery_report": "1",
            "delivery_report.delivered": "1",
            "delivery_report.failed": "0",
            "delivery_report.pending": "0",
        })
        yield self.assert_redis_zset("batches:inbound:mybatch",
                                     inbound_keys[-2:])
        yield self.assert_redis_zset("batches:outbound:mybatch",
                                     outbound_keys[-2:])
        yield self.assert_redis_zset("batches:event:mybatch", event_keys[-2:])
        yield self.assert_redis_pfcount("batches:to_addr_hll:mybatch", 4)
        yield self.assert_redis_pfcount("batches:from_addr_hll:mybatch", 5)

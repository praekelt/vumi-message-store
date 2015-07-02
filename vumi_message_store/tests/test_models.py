"""Tests for vumi.components.message_store."""

from vumi.tests.helpers import VumiTestCase

from vumi_message_store.models import (
    to_reverse_timestamp, from_reverse_timestamp)


class TestReverseTimestampUtils(VumiTestCase):

    def test_to_reverse_timestamp(self):
        """
        to_reverse_timestamp() turns a vumi_date-formatted string into a
        reverse timestamp.
        """
        self.assertEqual(
            "FFAAE41F25", to_reverse_timestamp("2015-04-01 12:13:14"))
        self.assertEqual(
            "FFAAE41F25", to_reverse_timestamp("2015-04-01 12:13:14.000000"))
        self.assertEqual(
            "FFAAE41F25", to_reverse_timestamp("2015-04-01 12:13:14.999999"))
        self.assertEqual(
            "FFAAE41F24", to_reverse_timestamp("2015-04-01 12:13:15"))
        self.assertEqual(
            "F0F9025FA5", to_reverse_timestamp("4015-04-01 12:13:14"))

    def test_from_reverse_timestamp(self):
        """
        from_reverse_timestamp() is the inverse of to_reverse_timestamp().
        """
        self.assertEqual(
            "2015-04-01 12:13:14.000000", from_reverse_timestamp("FFAAE41F25"))
        self.assertEqual(
            "2015-04-01 12:13:13.000000", from_reverse_timestamp("FFAAE41F26"))
        self.assertEqual(
            "4015-04-01 12:13:14.000000", from_reverse_timestamp("F0F9025FA5"))

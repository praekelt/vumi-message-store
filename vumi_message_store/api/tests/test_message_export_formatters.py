# -*- coding: utf-8 -*-

from twisted.web.test.test_web import DummyRequest

from zope.interface.verify import verifyObject

from vumi_message_store.api.message_export_formatters import (
    IMessageExportFormatter, JsonFormatter, CsvFormatter)

from vumi.tests.helpers import VumiTestCase, MessageHelper


class TestJsonFormatter(VumiTestCase):
    def setUp(self):
        self.msg_helper = self.add_helper(MessageHelper())
        self.request = DummyRequest([''])
        self.formatter = JsonFormatter()

    def test_implements_IMessageFormatter(self):
        """
        JsonFormatter implements the IMessageFormatter interface.
        """
        self.assertTrue(IMessageExportFormatter.providedBy(self.formatter))
        self.assertTrue(verifyObject(IMessageExportFormatter, self.formatter))

    def test_add_http_headers(self):
        """
        The correct Content-Type response header is written.
        """
        self.formatter.add_http_headers(self.request)
        self.assertEqual(
            self.request.responseHeaders.getRawHeaders('Content-Type'),
            ['application/json; charset=utf-8'])

    def test_write_row_header(self):
        """
        No row headers are written.
        """
        self.formatter.write_row_header(self.request)
        self.assertEqual(self.request.written, [])

    def test_write_row(self):
        """
        The message is written correctly as JSON
        """
        msg = self.msg_helper.make_inbound("foo")
        self.formatter.write_row(self.request, msg)
        self.assertEqual(self.request.written, [
            msg.to_json(), "\n",
        ])


class TestCsvFormatter(VumiTestCase):
    def setUp(self):
        self.msg_helper = self.add_helper(MessageHelper())
        self.request = DummyRequest([''])
        self.formatter = CsvFormatter()

    def test_implements_IMessageFormatter(self):
        """
        CsvFormatter implements the IMessageFormatter interface.
        """
        self.assertTrue(IMessageExportFormatter.providedBy(self.formatter))
        self.assertTrue(verifyObject(IMessageExportFormatter, self.formatter))

    def test_add_http_headers(self):
        """
        The correct Content-Type response header is written.
        """
        self.formatter.add_http_headers(self.request)
        self.assertEqual(
            self.request.responseHeaders.getRawHeaders('Content-Type'),
            ['text/csv; charset=utf-8'])

    def test_write_row_header(self):
        """
        The CSV row headers are written correctly.
        """
        self.formatter.write_row_header(self.request)
        self.assertEqual(self.request.written, [
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group\r\n"
        ])

    def _assert_row_written(self, row, row_template, msg):
        self.assertEqual(row, [row_template % {
            'ts': msg['timestamp'].isoformat(),
            'id': msg['message_id'],
        }])

    def test_write_row(self):
        """
        A message is written as a CSV row correctly.
        """
        msg = self.msg_helper.make_inbound("foo")
        self.formatter.write_row(self.request, msg)
        self._assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,9292,+41791234567,,,foo,\r\n", msg)

    def test_write_row_with_in_reply_to(self):
        """
        A message with the in-reply-to field set is written as a CSV row
        correctly.
        """
        msg = self.msg_helper.make_inbound("foo", in_reply_to="msg-2")
        self.formatter.write_row(self.request, msg)
        self._assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,9292,+41791234567,msg-2,,foo,\r\n", msg)

    def test_write_row_with_session_event(self):
        """
        A message with the session event field set is written as a CSV row
        correctly.
        """
        msg = self.msg_helper.make_inbound("foo", session_event="new")
        self.formatter.write_row(self.request, msg)
        self._assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,9292,+41791234567,,new,foo,\r\n", msg)

    def test_write_row_with_group(self):
        """
        A message with the group field set is written as a CSV row correctly.
        """
        msg = self.msg_helper.make_inbound("foo", group="#channel")
        self.formatter.write_row(self.request, msg)
        self._assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,9292,+41791234567,,,foo,#channel\r\n", msg)

    def test_write_row_with_unicode_content(self):
        """
        A message with unicode content is written as a CSV row correctly.
        """
        msg = self.msg_helper.make_inbound(u"føø", group="#channel")
        self.formatter.write_row(self.request, msg)
        self._assert_row_written(
            self.request.written,
            u"%(ts)s,%(id)s,9292,+41791234567,,,føø,#channel\r\n".encode(
                "utf-8"),
            msg)

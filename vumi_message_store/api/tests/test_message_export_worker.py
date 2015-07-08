# -*- coding: utf-8 -*-

import json
from datetime import datetime
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http

from vumi_message_store.message_store import (
    MessageStoreBatchManager, OperationalMessageStore)
from vumi_message_store.api.message_export_worker import MessageExportWorker

from vumi.utils import http_request_full

from vumi.tests.helpers import (
    VumiTestCase, MessageHelper, PersistenceHelper, WorkerHelper)


class TestMessageExportWorker(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.worker_helper = self.add_helper(WorkerHelper())
        self.msg_helper = self.add_helper(MessageHelper())

        riak, redis = yield self.create_managers()
        self.operational_store = OperationalMessageStore(riak, redis)
        self.batch_manager = MessageStoreBatchManager(riak, redis)

    @inlineCallbacks
    def create_managers(self):
        riak = yield self.persistence_helper.get_riak_manager()
        self.add_cleanup(riak.close_manager)
        redis = yield self.persistence_helper.get_redis_manager()
        self.add_cleanup(redis.close_manager)
        returnValue((riak, redis))

    @inlineCallbacks
    def start_server(self):
        config = self.persistence_helper.mk_config({
            'twisted_endpoint': 'tcp:0',
            'web_path': '/resource_path/',
        })

        worker = yield self.worker_helper.get_worker(
            MessageExportWorker, config)
        yield worker.startService()

        port = yield worker.services[0]._waitingForPort
        addr = port.getHost()
        self.url = 'http://%s:%s' % (addr.host, addr.port)

        self.worker_backend = worker.store.riak_backend

        self.addCleanup(self.stop_server, port)

    def stop_server(self, port):
        d = port.stopListening()
        d.addCallback(lambda _: port.loseConnection())
        return d

    def make_batch(self, tag):
        return self.batch_manager.batch_start([tag])

    def make_outbound(self, batch_id, content, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        msg = self.msg_helper.make_outbound(content, timestamp=timestamp)
        d = self.operational_store.add_outbound_message(msg,
                                                        batch_ids=[batch_id])
        d.addCallback(lambda _: msg)
        return d

    def make_inbound(self, batch_id, content, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        msg = self.msg_helper.make_inbound(content, timestamp=timestamp)
        d = self.operational_store.add_inbound_message(msg,
                                                       batch_ids=[batch_id])
        d.addCallback(lambda _: msg)
        return d

    def make_request(self, method, batch_id, leaf, **params):
        url = '%s/%s/%s/%s' % (self.url, 'resource_path', batch_id, leaf)
        if params:
            url = '%s?%s' % (url, urlencode(params))
        return http_request_full(method=method, url=url)

    def get_batch_resource(self, batch_id):
        return self.store_resource.getChild(batch_id, None)

    def assert_csv_rows(self, rows, expected):
        self.assertEqual(sorted(rows), sorted([
            row_template % {
                'id': msg['message_id'],
                'ts': msg['timestamp'].isoformat(),
            } for row_template, msg in expected
        ]))

    @inlineCallbacks
    def test_get_invalid_path(self):
        """
        An HTTP request to the server to an unrouted path should return a 404
        response code.
        """
        yield self.start_server()
        url = "%s/%s" % (self.url, 'bad_path',)
        resp = yield http_request_full(method='GET', url=url)

        self.assertEqual(resp.code, http.NOT_FOUND)

    @inlineCallbacks
    def test_get_invalid_batch(self):
        """
        An HTTP request to the server for a non-existant batch should return a
        200 response code and an empty response body.
        """
        yield self.start_server()
        resp = yield self.make_request('GET', 'made_up_batch', 'inbound.json')

        self.assertEqual(resp.code, http.OK)
        self.assertEqual(resp.delivered_body, "")

    @inlineCallbacks
    def test_get_invalid_export_path(self):
        """
        An HTTP request to the server for a valid batch but with an unknown
        export format should return a 404 response code.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        resp = yield self.make_request('GET', batch_id, 'inbound.xml')

        self.assertEqual(resp.code, http.NOT_FOUND)

    @inlineCallbacks
    def test_health_check_ok(self):
        """
        An HTTP request to the server for its health should return a 200
        response code as well as the message body "OK".
        """
        yield self.start_server()
        url = "%s/%s" % (self.url, 'health',)
        resp = yield http_request_full(method='GET', url=url)

        self.assertEqual(resp.code, http.OK)
        self.assertEqual(resp.delivered_body, 'OK')

    @inlineCallbacks
    def test_get_inbound(self):
        """
        Fetch some inbound messages via the export API in JSON format.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_inbound(batch_id, 'føø')
        msg2 = yield self.make_inbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'inbound.json')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id']]))

    @inlineCallbacks
    def test_get_inbound_csv(self):
        """
        Fetch some inbound messages via the export API in CSV format.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_inbound(batch_id, 'føø')
        msg2 = yield self.make_inbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'inbound.csv')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group"))
        self.assert_csv_rows(rows, [
            ("%(ts)s,%(id)s,9292,+41791234567,,,føø,", msg1),
            ("%(ts)s,%(id)s,9292,+41791234567,,,føø,", msg2),
        ])

    @inlineCallbacks
    def test_get_outbound(self):
        """
        Fetch some outbound messages via the export API in JSON format.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_outbound(batch_id, 'føø')
        msg2 = yield self.make_outbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'outbound.json')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id']]))

    @inlineCallbacks
    def test_get_outbound_csv(self):
        """
        Fetch some outbound messages via the export API in CSV format.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_outbound(batch_id, 'føø')
        msg2 = yield self.make_outbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'outbound.csv')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group"))
        self.assert_csv_rows(rows, [
            ("%(ts)s,%(id)s,+41791234567,9292,,,føø,", msg1),
            ("%(ts)s,%(id)s,+41791234567,9292,,,føø,", msg2),
        ])

    @inlineCallbacks
    def test_get_inbound_multiple_pages(self):
        """
        Multi-page results from the backend are processed by the server
        correctly.
        """
        yield self.start_server()
        # Poke at the riak backend to ensure multiple pages are transferred
        self.worker_backend.DEFAULT_MAX_RESULTS = 1
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_inbound(batch_id, 'føø')
        msg2 = yield self.make_inbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'inbound.json')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id']]))

    def test_connection_drop_during_page_iteration_stops(self):
        """
        If the connection drops while the server is iterating through index
        pages then the iteration stops.
        """
    test_connection_drop_during_page_iteration_stops.skip = (
        'TODO: This is difficult to test.')

    @inlineCallbacks
    def test_get_inbound_for_time_range(self):
        """
        Fetch some inbound messages from the server over a specific time range.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg2['message_id'], msg3['message_id']]))

    @inlineCallbacks
    def test_get_inbound_for_time_range_bad_args(self):
        """
        The server rejects requests for inbound messages with badly-formed time
        range arguments and returns a 400 response code.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))

        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', start='foo')
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'start' parameter: Unable to parse date string 'foo'")

        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', end='bar')
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'end' parameter: Unable to parse date string 'bar'")

        url = '%s/%s/%s/%s?start=foo&start=bar' % (
            self.url, 'resource_path', batch_id, 'inbound.json')
        resp = yield http_request_full(method='GET', url=url)
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'start' parameter: Exactly one value required")

    @inlineCallbacks
    def test_get_inbound_for_time_range_no_start(self):
        """
        Fetch some inbound messages from the server over a time range with an
        end time but no specified start time.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        msg1 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', end='2014-11-04 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id'], msg3['message_id']]))

    @inlineCallbacks
    def test_get_inbound_for_time_range_no_end(self):
        """
        Fetch some inbound messages from the server over a time range with a
        start time but no specified end time.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(3))
        msg4 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', start='2014-11-02 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg2['message_id'], msg3['message_id'], msg4['message_id']]))

    @inlineCallbacks
    def test_get_inbound_csv_for_time_range(self):
        """
        Fetch some inbound messages from the server over a specific time range.
        Get the exported messages in CSV format.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'inbound.csv', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group"))
        self.assert_csv_rows(rows, [
            ("%(ts)s,%(id)s,9292,+41791234567,,,føø,", msg2),
            ("%(ts)s,%(id)s,9292,+41791234567,,,føø,", msg3),
        ])

    @inlineCallbacks
    def test_get_outbound_for_time_range(self):
        """
        Fetch some outbound messages from the server over a specific time
        range.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg2['message_id'], msg3['message_id']]))

    @inlineCallbacks
    def test_get_outbound_for_time_range_bad_args(self):
        """
        The server rejects requests for outbound messages with badly-formed
        time range arguments and returns a 400 response code.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))

        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', start='foo')
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'start' parameter: Unable to parse date string 'foo'")

        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', end='bar')
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'end' parameter: Unable to parse date string 'bar'")

        url = '%s/%s/%s/%s?start=foo&start=bar' % (
            self.url, 'resource_path', batch_id, 'outbound.json')
        resp = yield http_request_full(method='GET', url=url)
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'start' parameter: Exactly one value required")

    @inlineCallbacks
    def test_get_outbound_for_time_range_no_start(self):
        """
        Fetch some outbound messages from the server over a time range with an
        end time but no specified start time.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        msg1 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', end='2014-11-04 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id'], msg3['message_id']]))

    @inlineCallbacks
    def test_get_outbound_for_time_range_no_end(self):
        """
        Fetch some outbound messages from the server over a time range with a
        start time but no specified end time.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(3))
        msg4 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', start='2014-11-02 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg2['message_id'], msg3['message_id'], msg4['message_id']]))

    @inlineCallbacks
    def test_get_outbound_csv_for_time_range(self):
        """
        Fetch some outbound messages from the server over a specific time
        range. Get the exported messages in CSV format.
        """
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'outbound.csv', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group"))
        self.assert_csv_rows(rows, [
            ("%(ts)s,%(id)s,+41791234567,9292,,,føø,", msg2),
            ("%(ts)s,%(id)s,+41791234567,9292,,,føø,", msg3),
        ])

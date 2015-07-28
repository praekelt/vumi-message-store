""" HTTP API for exporting messages as CSV/JSON """

import iso8601

from twisted.internet.defer import DeferredList, inlineCallbacks
from twisted.web.resource import NoResource, Resource
from twisted.web.server import NOT_DONE_YET

from vumi_message_store.api.message_export_formatters import (
    JsonFormatter, CsvFormatter)
from vumi.message import format_vumi_date


class ParameterError(Exception):
    """
    Exception raised while trying to parse a parameter.
    """


class MessageExportProxyResource(Resource):

    isLeaf = True

    def __init__(self, message_store, batch_id, formatter):
        Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id
        self.formatter = formatter

    def _extract_arg(self, request, argname):
        if argname not in request.args:
            return None
        if len(request.args[argname]) != 1:
            raise ParameterError(("Invalid '%s' parameter: " +
                                  "Exactly one value required") % (argname,))
        return request.args[argname][0]

    def _extract_date_arg(self, request, argname):
        arg = self._extract_arg(request, argname)
        if arg is None:
            return None
        try:
            timestamp = iso8601.parse_date(arg)
            return format_vumi_date(timestamp)
        except iso8601.ParseError as e:
            raise ParameterError(
                "Invalid '%s' parameter: %s" % (argname, str(e)))

    def render_GET(self, request):
        try:
            start = self._extract_date_arg(request, 'start')
            end = self._extract_date_arg(request, 'end')
        except ParameterError as e:
            request.setResponseCode(400)
            return str(e)

        self.formatter.add_http_headers(request)
        self.formatter.write_row_header(request)

        d = self.get_keys_page(self.message_store, self.batch_id, start, end)

        request.connection_has_been_closed = False
        request.notifyFinish().addBoth(
            lambda _: setattr(request, 'connection_has_been_closed', True))
        d.addCallback(self.fetch_pages, request)
        return NOT_DONE_YET

    def get_keys_page(self, message_store, batch_id, start, end):
        """
        Query the message store for the relevant messages and return a
        paginated response.
        """
        raise NotImplementedError('To be implemented by sub-class.')

    def get_message_keys(self, keys_page):
        """
        Get the list of message keys from a keys page. This is needed as a keys
        page does not necessarily contain only message keys.
        """
        raise NotImplementedError('To be implemented by sub-class.')

    def get_message(self, message_store, message_key):
        """
        Fetch the actual message from the message store using the message key.
        """
        raise NotImplementedError('To be implemented by sub-class.')

    def fetch_pages(self, keys_page, request):
        """
        Process a page of keys and each subsequent page.

        The keys for the current page are handed off to :meth:`fetch_page` for
        processing. If there is another page, we fetch that while the current
        page is being handled and add a callback to process it when the
        current page is finished.

        When there are no more pages, we add a callback to close the request.
        """
        if request.connection_has_been_closed:
            # We're no longer connected, so stop doing work.
            return
        d = self.fetch_page(keys_page, request)
        if keys_page.has_next_page():
            # We fetch the next page before waiting for the current page to be
            # processed.
            next_page_d = keys_page.next_page()
            d.addCallback(lambda _: next_page_d)
            # Add this method as a callback to operate on the next page. It's
            # like recursion, but without worrying about stack size.
            d.addCallback(self.fetch_pages, request)
        else:
            # No more pages, so close the request.
            d.addCallback(self.finish_request_cb, request)
        return d

    def finish_request_cb(self, _result, request):
        if not request.connection_has_been_closed:
            # We need to check for this here in case we lose the connection
            # while delivering the last page.
            return request.finish()

    @inlineCallbacks
    def fetch_page(self, keys_page, request):
        """
        Process a page of keys in chunks of concurrently-fetched messages.
        """
        message_keys = self.get_message_keys(keys_page)
        yield DeferredList([
            self.handle_message(key, request) for key in message_keys])

    def handle_message(self, message_key, request):
        d = self.get_message(self.message_store, message_key)
        d.addCallback(self.write_message, request)
        return d

    def write_message(self, message, request):
        self.formatter.write_row(request, message)


class InboundResource(MessageExportProxyResource):

    def get_keys_page(self, message_store, batch_id, start, end):
        return message_store.list_batch_inbound_messages(
            batch_id, start=start, end=end)

    def get_message_keys(self, keys_page):
        return [key for key, _, _ in keys_page]

    def get_message(self, message_store, message_key):
        return message_store.get_inbound_message(message_key)


class OutboundResource(MessageExportProxyResource):

    def get_keys_page(self, message_store, batch_id, start, end):
        return message_store.list_batch_outbound_messages(
            batch_id, start=start, end=end)

    def get_message_keys(self, keys_page):
        return [key for key, _, _ in keys_page]

    def get_message(self, message_store, message_key):
        return message_store.get_outbound_message(message_key)


class BatchResource(Resource):

    RESOURCES = {
        'inbound.json': (InboundResource, JsonFormatter),
        'outbound.json': (OutboundResource, JsonFormatter),
        'inbound.csv': (InboundResource, CsvFormatter),
        'outbound.csv': (OutboundResource, CsvFormatter),
    }

    def __init__(self, message_store, batch_id):
        Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id

    def getChild(self, path, request):
        if path not in self.RESOURCES:
            return NoResource()
        resource_class, message_formatter = self.RESOURCES.get(path)
        return resource_class(
            self.message_store, self.batch_id, message_formatter())


class MessageExportResource(Resource):

    def __init__(self, message_store):
        Resource.__init__(self)
        self.message_store = message_store

    def getChild(self, path, request):
        return BatchResource(self.message_store, path)

from twisted.application.internet import StreamServerEndpointService
from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.config import (
    ConfigDict, ConfigText, ConfigServerEndpoint, ServerEndpointFallback)
from vumi.persist.txriak_manager import TxRiakManager
from vumi.persist.txredis_manager import TxRedisManager
from vumi.utils import build_web_site
from vumi.worker import BaseWorker
from vumi_message_store.message_store import QueryMessageStore
from vumi_message_store.api.message_export_resources import (
    MessageExportResource)


class HealthResource(Resource):
    def render_GET(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return 'OK'


class MessageExportWorker(BaseWorker):

    class CONFIG_CLASS(BaseWorker.CONFIG_CLASS):
        worker_name = ConfigText(
            'Name of the this message store resource worker',
            required=True, static=True)
        twisted_endpoint = ConfigServerEndpoint(
            'Twisted endpoint to listen on.', required=True, static=True,
            fallbacks=[ServerEndpointFallback()])
        web_path = ConfigText(
            'The path to serve this resource on.', required=True, static=True)
        health_path = ConfigText(
            'The path to serve the health resource on.', default='/health/',
            static=True)
        riak_manager = ConfigDict(
            'Riak client configuration.', default={}, static=True)
        redis_manager = ConfigDict(
            'Redis client configuration.', default={}, static=True)

    @inlineCallbacks
    def setup_worker(self):
        config = self.get_static_config()
        self._riak = yield self.create_riak_manager(config)
        self._redis = yield self.create_redis_manager(config)
        self.store = QueryMessageStore(self._riak, self._redis)

        site = build_web_site({
            config.web_path: MessageExportResource(self.store),
            config.health_path: HealthResource(),
        })
        self.addService(
            StreamServerEndpointService(config.twisted_endpoint, site))

    def create_riak_manager(self, config):
        return TxRiakManager.from_config(config.riak_manager)

    def create_redis_manager(self, config):
        return TxRedisManager.from_config(config.redis_manager)

    @inlineCallbacks
    def teardown_worker(self):
        yield self._riak.close_manager()
        yield self._redis.close_manager()

    def setup_connectors(self):
        # NOTE: not doing anything AMQP
        pass

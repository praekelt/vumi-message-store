# -*- test-case-name: vumi_message_store.tests.test_riak_backend -*-

"""
Fake in-memory Riak manager that implements enough for the message store.
"""

# TODO: Sync and async.

import base64
import json

from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from vumi.persist.model import ModelProxy
from vumi.utils import flatten_generator


class FakeMemoryRiakManager(object):
    """
    This is an in-memory implementation of a subset of the Riak manager's
    functionality.

    NOTE: This is the minimal subset necessary for MessageStoreRiakBackend.
    """

    def __init__(self, state, store_versions=None):
        self._state = state
        self._is_sync = state._is_sync
        if self._is_sync:
            self.call_decorator = flatten_generator
        else:
            self.call_decorator = inlineCallbacks
        self.store_versions = store_versions or {}
        # Wrap the methods that need to handle async stuff.
        self.load = self.call_decorator(self._internal_load)
        self.store = self.call_decorator(self._internal_store)

    def proxy(self, modelcls):
        """
        Return a real proxy using the fake manager.
        """
        return ModelProxy(self, modelcls)

    def bucket_name(self, modelcls_or_obj):
        return modelcls_or_obj.bucket

    def bucket_for_modelcls(self, modelcls):
        """
        Return a fake bucket object for the given model class.
        """
        bucket_name = self.bucket_name(modelcls)
        return FakeRiakBucket(self._state, bucket_name)

    def riak_object(self, modelcls, key):
        """
        Create and return a fake RiakObject.
        """
        bucket_name = self.bucket_name(modelcls)
        riak_object = FakeRiakObject(self._state, bucket_name, key)
        riak_object.set_content_type("application/json")
        riak_object.set_data({'$VERSION': modelcls.VERSION})
        return riak_object

    def _internal_store(self, modelobj):
        """
        This method gets wrapped in a sync or async decorator in __init__().
        """
        riak_object = modelobj._riak_object
        modelcls = type(modelobj)
        model_name = "%s.%s" % (modelcls.__module__, modelcls.__name__)
        store_version = self.store_versions.get(model_name, modelcls.VERSION)
        # Run reverse migrators until we have the correct version of the data.
        data_version = riak_object.get_data().get('$VERSION', None)
        while data_version != store_version:
            migrator = modelcls.MIGRATOR(
                modelcls, self, data_version, reverse=True)
            riak_object = migrator(riak_object).get_riak_object()
            data_version = riak_object.get_data().get('$VERSION', None)
        yield riak_object.store()
        returnValue(modelobj)

    def _internal_load(self, modelcls, key, result=None):
        """
        This method gets wrapped in a sync or async decorator in __init__().
        """
        assert result is None
        riak_object = self.riak_object(modelcls, key)
        yield riak_object.reload()
        was_migrated = False

        # Run migrators until we have the correct version of the data.
        while riak_object.get_data() is not None:
            data_version = riak_object.get_data().get('$VERSION', None)
            if data_version == modelcls.VERSION:
                obj = modelcls(self, key, _riak_object=riak_object)
                obj.was_migrated = was_migrated
                returnValue(obj)
            migrator = modelcls.MIGRATOR(modelcls, self, data_version)
            riak_object = migrator(riak_object).get_riak_object()
            was_migrated = True
        returnValue(None)

    def index_keys(self, model, index_name, start_value, end_value=None,
                   return_terms=None):
        bucket = self.bucket_for_modelcls(model)
        return bucket.get_index(
            index_name, start_value, end_value, return_terms=return_terms)

    def index_keys_page(self, model, index_name, start_value, end_value=None,
                        return_terms=None, max_results=None,
                        continuation=None):
        bucket = self.bucket_for_modelcls(model)
        return bucket.get_index_page(
            index_name, start_value, end_value, return_terms=return_terms,
            max_results=max_results, continuation=continuation)


class FakeRiakState(object):
    """
    An object to hold fake Riak state.
    """

    def __init__(self, is_sync):
        self._buckets = {}
        self._is_sync = is_sync
        self._delayed_calls = []

    def teardown(self):
        for delayed in self._delayed_calls:
            if not (delayed.cancelled or delayed.called):
                delayed.cancel()

    def _with_async_delay(self, func, *args, **kw):
        """
        If in async mode, add some delay to catch code that doesn't properly
        wait for the deferred to fire.
        """
        if self._is_sync:
            return func(*args, **kw)

        # Add some latency to catch things that don't wait on deferreds. We
        # can't use deferLater() here because we want to keep track of the
        # delayed call object.
        d = Deferred()
        delayed = reactor.callLater(
            0.002, lambda _: d.callback(func(*args, **kw)), None)
        self._delayed_calls.append(delayed)
        return d

    def _get_bucket(self, bucket_name):
        """
        Get the data for a bucket.
        """
        return self._buckets.setdefault(bucket_name, {
            "objects": {},
            "indexes": {},
        })

    def store_object(self, fake_object):
        """
        Store an object.
        """
        bucket = self._get_bucket(fake_object._bucket_name)
        bucket["objects"][fake_object.key] = {
            "content_type": fake_object._current_data["content_type"],
            "data": json.dumps(fake_object._current_data["data"]),
        }

        bucket["indexes"] = (
            self._rebuild_bucket_indexes(fake_object, bucket["indexes"]))

    def _rebuild_bucket_indexes(self, fake_object, bucket_indexes):
        """
        Clears any existing indexes for the object and adds the new indexes.
        """
        new_bucket_indexes = {}
        for index_name, index_values in bucket_indexes.iteritems():
            new_index_values = [(value, key) for value, key in index_values
                                if key != fake_object.key]

            if new_index_values:
                new_bucket_indexes[index_name] = new_index_values

        for index_name, index_value in fake_object._current_data["indexes"]:
            new_index_values = new_bucket_indexes.setdefault(index_name, [])
            new_index_values.append((index_value, fake_object.key))
            new_index_values.sort()

        return new_bucket_indexes

    def _reload_object_state(self, fake_object):
        """
        Set an object's data from stored state.
        """
        bucket = self._get_bucket(fake_object._bucket_name)
        obj_state = bucket["objects"].get(fake_object.key)
        if obj_state is None:
            fake_object._current_data["data"] = None
            return
        fake_object._current_data["data"] = json.loads(obj_state["data"])
        fake_object._current_data["content_type"] = obj_state["content_type"]
        fake_object._current_data["indexes"] = set()
        for index_name, index_data in bucket["indexes"].iteritems():
            for index_value, key in index_data:
                if key == fake_object.key:
                    fake_object._current_data["indexes"].add(
                        (index_name, index_value))

    def get_index_page(self, bucket_name, index_name, start_value, end_value,
                       return_terms=False, max_results=None,
                       continuation=None):
        """
        Perform an index query.
        """
        # Get all matching results
        index = self._get_bucket(bucket_name)["indexes"].get(index_name, [])
        if end_value is None:
            in_range = lambda v: v[0] == start_value
        else:
            in_range = lambda v: start_value <= v[0] <= end_value
        results = filter(in_range, index)

        # Drop all results we've returned previously, if any.
        if continuation is not None:
            continuation = tuple(json.loads(base64.b64decode(continuation)))
            while results and results[0] < continuation:
                results.pop(0)

        # If we're not paginated, we're done.
        if max_results is None:
            return self._return_index_page(
                bucket_name, index_name, start_value, end_value, return_terms,
                max_results, results, continuation=None)

        # Truncate the results and build the continuation token.
        continuation = None
        truncated_results = results[:max_results]
        if len(truncated_results) < len(results):
            continuation = base64.b64encode(json.dumps(results[max_results]))
        return self._return_index_page(
            bucket_name, index_name, start_value, end_value, return_terms,
            max_results, truncated_results, continuation=continuation)

    def _return_index_page(self, bucket_name, index_name, start_value,
                           end_value, return_terms, max_results, results,
                           continuation):
        """
        Construct and return an index page object.
        """
        if not return_terms:
            results = [key for _, key in results]
        return FakeMemoryIndexPage(self, continuation, results, {
            "bucket_name": bucket_name,
            "index_name": index_name,
            "start_value": start_value,
            "end_value": end_value,
            "return_terms": return_terms,
            "max_results": max_results,
        })


class FakeMemoryIndexPage(object):
    """
    Index page implementation for traversing in-memory indexes.
    """
    def __init__(self, state, continuation, results, params):
        self._state = state
        self._continuation = continuation
        self._results = results
        self._params = params

    def next_page(self):
        assert self.has_next_page()
        return self._state.get_index_page(
            continuation=self._continuation, **self._params)

    def has_next_page(self):
        return self._continuation is not None

    def __iter__(self):
        return iter(self._results)


class FakeRiakBucket(object):
    """
    A fake implementation of a Riak bucket object using in-memory state.

    NOTE: This is the minimal subset necessary for MessageStoreRiakBackend.
    """

    def __init__(self, state, bucket_name):
        self._state = state
        self._bucket_name = bucket_name

    # Methods that "touch the network".

    def get_index(self, index_name, start_value, end_value=None,
                  return_terms=None):
        return self.get_index_page(
            index_name, start_value, end_value, return_terms=return_terms)

    def get_index_page(self, index_name, start_value, end_value=None,
                       return_terms=None, max_results=None, continuation=None):
        return self._state._with_async_delay(
            self._state.get_index_page,
            self._bucket_name, index_name, start_value, end_value,
            return_terms=return_terms, max_results=max_results,
            continuation=continuation)


class FakeRiakObject(object):
    """
    A fake implementation of a Riak object using in-memory state.

    NOTE: This is the minimal subset necessary for MessageStoreRiakBackend.
    """

    def __init__(self, state, bucket_name, key):
        self._state = state
        self._bucket_name = bucket_name
        self._key = key
        self._current_data = {
            "content_type": None,
            "data": None,
            "indexes": set(),
        }

    @property
    def key(self):
        return self._key

    def set_content_type(self, content_type):
        self._current_data["content_type"] = content_type

    def get_data(self):
        return self._current_data["data"]

    def set_data(self, data):
        self._current_data["data"] = data

    def set_data_field(self, key, value):
        self._current_data["data"][key] = value

    def delete_data_field(self, key):
        del self._current_data["data"][key]

    def get_indexes(self):
        return self._current_data["indexes"]

    def add_index(self, index_name, index_value):
        self._current_data["indexes"].add((index_name, index_value))

    def remove_index(self, index_name, index_value=None):
        indexes = self._current_data["indexes"]
        if index_name is None:
            assert index_value is None
            indexes.clear()
        elif index_value is None:
            for index in [(k, v) for k, v in indexes if k == index_name]:
                indexes.remove(index)
        else:
            indexes.remove((index_name, index_value))

    # Methods that "touch the network".

    def _reload(self):
        new_obj = type(self)(self._state, self._bucket_name, self._key)
        new_obj._current_data = self._current_data
        self._state._reload_object_state(new_obj)
        return new_obj

    def _store(self):
        self._state.store_object(self)
        return self._reload()

    def store(self):
        return self._state._with_async_delay(self._store)

    def reload(self):
        return self._state._with_async_delay(self._reload)
